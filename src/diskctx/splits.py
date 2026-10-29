"""Studies, split windows, drive assignment and the rows of each split (M3).

Two studies, all dates in the config (`studies:`):
  W (workload)  S/W/N features. Per model, from the first quarter of its S/W/N window to
                its last quarter with >= min_quarter_failures failures (so a model's
                retirement tail is left out; windows.csv says where each model ends).
                The last test_quarters are test, the val_quarters before are val, the
                rest is train.
  P (pod)       all feature groups. Fixed train/val/test dates, the same for every model.
                Only models with at least one failure in the test window.

A model stays in a study only if it has at least min_failed_drives failed drives in
each split (config, e.g. 30 train, 1 val, 1 test); otherwise it is left out of the study
entirely and summary.csv says why. So no model trains on healthy drives only.

Drive assignment, per study and model (a serial is in one split only):
  - a drive that fails inside a split window goes to that split ("failure"), so every
    failure in the test window stays in test;
  - every other drive goes by a seeded hash of its serial, with the config shares
    ("hash"). Its weight is 1 / share of its split, so that M5 can weight healthy drives
    back to the real fleet ratio (precision, F1, MCC);
  - train only: healthy (hashed) drives are downsampled by drive, healthy_per_failed per
    failed train drive, by a second seeded hash ("keep").

Rows of a split (split_rows): a drive contributes only its rows inside its split's
window; earlier history is warm-up for features only. A row's label must be decidable
from data up to the window end:
  - the drive fails inside the window (its label is known), or
  - the label is 0 and t+N <= the drive's last record up to the window end (censoring,
    applied per window, so data after the window end cannot change a label).
Embargo: train and val drop the days whose (t, t+N] crosses the window end.
The rows never depend on the feature set, so every feature set of a study is evaluated
on the same drive-days (paired ablations).

splits stage -> splits/
  <study>/drives.parquet   model, serial_number, split, by, weight, keep
  windows.csv              study, model, split, start, end, note (skipped models too)
  summary.csv              per study, model, split: drives, failed drives, kept healthy,
                           included, reason (every model of the study, also left-out ones)
"""

import hashlib
from datetime import timedelta

import polars as pl

from diskctx.config import SPLITS, parse_date, quarter_days, quarters_between
from diskctx.ingest import model_dir
from diskctx.labels import KEY, label_rows, read_drives, y_col
from diskctx.status import format_table
from diskctx.storage import DURABLE, Stage

FEATURE_SETS = ("S", "S+W", "S+N", "S+P", "S+W+N", "S+W+N+P")
WINDOW_SCHEMA = {"study": pl.Utf8, "model": pl.Utf8, "split": pl.Utf8, "start": pl.Date, "end": pl.Date,
                 "note": pl.Utf8}
ASSIGN_SCHEMA = {"model": pl.Utf8, "serial_number": pl.Utf8, "split": pl.Utf8, "by": pl.Utf8,
                 "weight": pl.Float64, "keep": pl.Boolean}
SUMMARY_SCHEMA = {"study": pl.Utf8, "model": pl.Utf8, "split": pl.Utf8, "start": pl.Date, "end": pl.Date,
                  "drives": pl.Int64, "failed_drives": pl.Int64, "healthy_kept": pl.Int64,
                  "included": pl.Boolean, "reason": pl.Utf8}


def feature_sets(groups):
    """The feature sets a study with these groups evaluates, e.g. S, S+W, S+N, S+W+N."""
    return [fs for fs in FEATURE_SETS if set(fs.split("+")) <= set(groups)]


def check_feature_set(feature_set, groups):
    missing = [g for g in feature_set.split("+") if g not in groups]
    if missing:
        raise ValueError(f"feature set {feature_set} uses groups {missing} that this study "
                         f"(groups {groups}) does not have")


def hash_unit(*parts):
    """A seeded, reproducible number in [0, 1) from text parts (e.g. seed and serial)."""
    text = "|".join(str(p) for p in parts)
    return int(hashlib.sha256(text.encode()).hexdigest()[:15], 16) / 16**15


def quarter_of(day):
    return f"{day.year}Q{(day.month - 1) // 3 + 1}"


# --- windows ------------------------------------------------------------------------------


def window(study, model, split, start, end, note):
    return {"study": study, "model": model, "split": split, "start": start, "end": end, "note": note}


def skipped(study, model, reason):
    return window(study, model, "skipped", None, None, reason)


def w_windows(study, drives, models):
    k, n_val, n_test = study["min_quarter_failures"], study["val_quarters"], study["test_quarters"]
    rows = []
    for model, (first_q, last_q) in study["models"].items():
        if model not in models:
            rows.append(skipped("W", model, "not in the pass-2 model list"))
            continue
        fails = drives.filter((pl.col("model") == model) & pl.col("fail_date").is_not_null())["fail_date"]
        counts = {}
        for d in fails:
            counts[quarter_of(d)] = counts.get(quarter_of(d), 0) + 1
        quarters = quarters_between(first_q, last_q)
        enough = [q for q in quarters if counts.get(q, 0) >= k]
        if not enough:
            rows.append(skipped("W", model, f"no quarter in {first_q}-{last_q} with >= {k} failures"))
            continue
        quarters = quarters[:quarters.index(enough[-1]) + 1]
        if len(quarters) <= n_val + n_test:
            rows.append(skipped("W", model, f"{first_q}-{enough[-1]} has {len(quarters)} quarters: nothing "
                                            f"left for train after {n_val} val + {n_test} test"))
            continue
        parts = {"train": quarters[:-(n_val + n_test)], "val": quarters[-(n_val + n_test):-n_test],
                 "test": quarters[-n_test:]}
        note = f"config {first_q}-{last_q}; ends {enough[-1]}, the last quarter with >= {k} failures"
        for split, qs in parts.items():
            rows.append(window("W", model, split, quarter_days(qs[0])[0], quarter_days(qs[-1])[-1], note))
    return rows


def p_windows(study, drives, models):
    dates = {s: [parse_date(d) for d in study[s]] for s in SPLITS}
    test_start, test_end = dates["test"]
    rows = []
    for model in models:
        n = drives.filter((pl.col("model") == model) & pl.col("fail_date").is_between(test_start, test_end)).height
        if n == 0:
            rows.append(skipped("P", model, "no failures in the test window"))
            continue
        for split in SPLITS:
            rows.append(window("P", model, split, *dates[split], f"{n} failures in the test window"))
    return rows


def study_windows(name, study, drives, models):
    """Windows of study `name` ('W' or 'P') for `models`, one row per model and split."""
    rows = w_windows(study, drives, models) if name == "W" else p_windows(study, drives, models)
    return pl.DataFrame(rows, schema=WINDOW_SCHEMA)


def used(windows):
    return windows.filter(pl.col("split").is_in(SPLITS))


# --- assignment ---------------------------------------------------------------------------


def assign_drives(drives, windows, shares, healthy_per_failed, seed):
    """One row per drive of every windowed model: its split, why (failure | hash), its
    weight, and whether it is kept after train downsampling."""
    parts = []
    for model in used(windows)["model"].unique(maintain_order=True):
        w = {r["split"]: (r["start"], r["end"])
             for r in used(windows).filter(pl.col("model") == model).iter_rows(named=True)}
        lo, hi = min(a for a, _ in w.values()), max(b for _, b in w.values())
        seen_until = pl.max_horizontal("last_date", "fail_date")
        d = drives.filter((pl.col("model") == model) & (pl.col("first_date") <= hi) & (seen_until >= lo))
        d = d.with_columns(pl.Series("u", [hash_unit(seed, s) for s in d["serial_number"]], pl.Float64),
                           pl.Series("u_keep", [hash_unit(seed, "downsample", s) for s in d["serial_number"]],
                                     pl.Float64))

        fail_split = pl.lit(None, pl.Utf8)
        for split, (a, b) in w.items():
            fail_split = pl.when(pl.col("fail_date").is_between(a, b)).then(pl.lit(split)).otherwise(fail_split)
        hash_split = (pl.when(pl.col("u") < shares["train"]).then(pl.lit("train"))
                      .when(pl.col("u") < shares["train"] + shares["val"]).then(pl.lit("val"))
                      .otherwise(pl.lit("test")))
        d = d.with_columns(fail_split.alias("fail_split")).with_columns(
            pl.coalesce("fail_split", hash_split).alias("split"),
            pl.when(pl.col("fail_split").is_null()).then(pl.lit("hash")).otherwise(pl.lit("failure")).alias("by"))
        d = d.with_columns(pl.when(pl.col("by") == "hash")
                           .then(1 / pl.col("split").replace_strict(shares, return_dtype=pl.Float64))
                           .otherwise(1.0).alias("weight"))

        # Train downsampling, by drive: keep healthy_per_failed healthy drives per failed one.
        n_failed = d.filter((pl.col("split") == "train") & (pl.col("by") == "failure")).height
        healthy_train = (pl.col("split") == "train") & (pl.col("by") == "hash")
        rank = pl.col("u_keep").rank("ordinal").over(healthy_train)
        d = d.with_columns((~healthy_train | (rank <= healthy_per_failed * n_failed)).alias("keep"))
        parts.append(d.select(list(ASSIGN_SCHEMA)))
    if not parts:
        return pl.DataFrame(schema=ASSIGN_SCHEMA)
    return pl.concat(parts).sort(KEY)


def failure_summary(name, windows, assignment, min_failed, min_test_failures):
    """One row per model and split: drives, failed drives, kept healthy drives, and
    whether the model is in the study. A model is left out (included = false) if a split
    has fewer failed drives than min_failed[split]; the reason says which. A model
    without windows gets one row whose reason is its window note. Also returns a warning
    for every test cell of an included model with fewer than min_test_failures."""
    rows, warnings = [], []
    for model in windows["model"].unique(maintain_order=True):
        w = windows.filter(pl.col("model") == model)
        if w["split"][0] == "skipped":
            rows.append({"study": name, "model": model, "included": False, "reason": w["note"][0]})
            continue
        cells = []
        for r in w.iter_rows(named=True):
            a = assignment.filter((pl.col("model") == model) & (pl.col("split") == r["split"]))
            cells.append({"study": name, "model": model, "split": r["split"], "start": r["start"],
                          "end": r["end"], "drives": a.height,
                          "failed_drives": a.filter(pl.col("by") == "failure").height,
                          "healthy_kept": a.filter((pl.col("by") == "hash") & pl.col("keep")).height})
        short = [f"{c['failed_drives']} failed drives in {c['split']} (< {min_failed[c['split']]})"
                 for c in cells if c["failed_drives"] < min_failed[c["split"]]]
        for c in cells:
            c.update(included=not short, reason="; ".join(short) or None)
            if not short and c["split"] == "test" and c["failed_drives"] < min_test_failures:
                warnings.append(f"study {name}, {model}: {c['failed_drives']} failed drives in test "
                                f"(< {min_test_failures})")
        rows += cells
    return pl.DataFrame(rows, schema=SUMMARY_SCHEMA), warnings


def leave_out(windows, assignment, summary):
    """Remove the models the summary leaves out for too few failed drives: their windows
    become one 'skipped' row with the reason, and their drives leave the assignment."""
    out = summary.filter(pl.col("split").is_not_null() & ~pl.col("included"))
    reasons = dict(out.select("model", "reason").unique(maintain_order=True).iter_rows())
    if not reasons:
        return windows, assignment
    study = windows["study"][0]
    dropped = pl.DataFrame([skipped(study, m, f"too few failed drives: {r}") for m, r in reasons.items()],
                           schema=WINDOW_SCHEMA)
    windows = pl.concat([windows.filter(~pl.col("model").is_in(list(reasons))), dropped])
    return windows, assignment.filter(~pl.col("model").is_in(list(reasons)))


def weighted_precision(flagged, failed, weight):
    """Precision with per-drive weights: sum(w, flagged and failed) / sum(w, flagged).
    With healthy drives weighted 1/share, it estimates precision on the whole fleet."""
    hits = sum(w for f, y, w in zip(flagged, failed, weight) if f and y)
    alarms = sum(w for f, w in zip(flagged, weight) if f)
    return hits / alarms if alarms else 0.0


# --- rows ---------------------------------------------------------------------------------


def split_rows(labelled, assignment, windows, split, horizon, feature_set, groups, last_seen=None):
    """The drive-days of one split for one horizon (see the module docstring).

    labelled: drive-day rows from labels.label_rows (model, serial_number, date,
    fail_date, y_N, ...). feature_set is only checked against the study's groups: the
    rows are the same for every feature set.
    last_seen: optional (model, serial_number, last_seen) = each drive's last record up to
    the split's window end (see last_records). Pass it when `labelled` holds only part of
    the history (one quarter); otherwise it is computed from `labelled`."""
    check_feature_set(feature_set, groups)
    y, n = y_col(horizon), timedelta(days=horizon)
    drives = assignment.lazy().filter((pl.col("split") == split) & pl.col("keep")).select(KEY)
    win = used(windows).lazy().filter(pl.col("split") == split).select("model", "start", "end")
    lf = (labelled.lazy().join(drives, on=KEY).join(win, on="model")
          .filter(pl.col("date") <= pl.col("end")))
    if last_seen is None:
        lf = lf.with_columns(pl.col("date").max().over(KEY).alias("last_seen"))  # last record <= end
    else:
        lf = lf.join(last_seen.lazy().select(*KEY, "last_seen"), on=KEY, how="left")
    lf = lf.filter((pl.col("date") >= pl.col("start")) & pl.col(y).is_not_null())
    fails_inside = pl.col("fail_date").is_not_null() & (pl.col("fail_date") <= pl.col("end"))
    survived = (pl.col(y) == 0) & (pl.col("date") + n <= pl.col("last_seen"))
    lf = lf.filter(fails_inside | survived)
    if split != "test":
        lf = lf.filter(pl.col("date") + n <= pl.col("end"))                     # embargo
    return lf.drop("start", "end", "last_seen").collect().sort(*KEY, "date")


def last_records(rows, windows, split):
    """(model, serial_number, last_seen): each drive's last record on or before the end of
    its model's `split` window, from drive-day rows (model, serial_number, date; may be
    a LazyFrame over many files). For split_rows(..., last_seen=)."""
    win = used(windows).lazy().filter(pl.col("split") == split).select("model", "end")
    return (rows.lazy().select(*KEY, "date").join(win, on="model")
            .filter(pl.col("date") <= pl.col("end"))
            .group_by(KEY).agg(pl.col("date").max().alias("last_seen"))
            .collect(engine="streaming"))


def read_study(storage, study):
    windows = pl.read_csv(storage.path(DURABLE, "splits/windows.csv"), schema=WINDOW_SCHEMA)
    assignment = pl.read_parquet(storage.path(DURABLE, f"splits/{study}/drives.parquet"))
    return windows.filter(pl.col("study") == study), assignment


def study_rows(storage, cfg, study, split, horizon, feature_set):
    """Labelled drive-days (model, serial_number, date, fail_date, y_N...) of one split,
    read from the pass-2 Parquet."""
    windows, assignment = read_study(storage, study)
    drives = read_drives(storage)
    models = used(windows)["model"].unique().to_list()
    paths = [p for m in models for p in sorted(storage.path(DURABLE, f"parquet/{model_dir(m)}").glob("*.parquet"))]
    if paths:
        rows = pl.scan_parquet(paths).select(*KEY, "date")
    else:
        rows = pl.LazyFrame(schema={"model": pl.Utf8, "serial_number": pl.Utf8, "date": pl.Date})
    labelled = label_rows(rows, drives, cfg["horizons"])
    return split_rows(labelled, assignment, windows, split, horizon, feature_set, cfg["studies"][study]["groups"])


# --- stage --------------------------------------------------------------------------------


def splits_stage(cfg, models):
    s = cfg["splits"]

    def build(work, storage):
        drives = read_drives(storage)
        out = work / "splits"
        out.mkdir(parents=True)
        all_windows, summaries, warnings = [], [], []
        for name, study in cfg["studies"].items():
            windows = study_windows(name, study, drives, models)
            assignment = assign_drives(drives, windows, s["shares"], s["healthy_per_failed"], cfg["seed"])
            summary, warns = failure_summary(name, windows, assignment, s["min_failed_drives"],
                                             s["min_test_failures"])
            windows, assignment = leave_out(windows, assignment, summary)
            (out / name).mkdir()
            assignment.with_columns(pl.lit(storage.data_source).alias("data_source")).write_parquet(
                out / name / "drives.parquet", compression="zstd")
            all_windows.append(windows)
            summaries.append(summary)
            warnings += warns
            for w in windows.filter(pl.col("split") == "skipped").iter_rows(named=True):
                print(f"study {name}: {w['model']} skipped: {w['note']}")
        windows = pl.concat(all_windows) if all_windows else pl.DataFrame(schema=WINDOW_SCHEMA)
        windows.write_csv(out / "windows.csv")
        summary = pl.concat(summaries) if summaries else pl.DataFrame(schema=SUMMARY_SCHEMA)
        summary.write_csv(out / "summary.csv")
        print("failing drives per study, model and split:")
        print(format_table(summary.filter(pl.col("split").is_not_null()).drop("reason").to_dicts()))
        for w in warnings:
            print(f"WARNING: {w}")
        return {"warnings": warnings}

    return Stage(
        name="splits",
        tier=DURABLE,
        outputs=["splits"],
        build=build,
        keys=["data_source", "seed", "splits", "studies"],
        deps=["labels"],
        params={"models": sorted(models)},
    )
