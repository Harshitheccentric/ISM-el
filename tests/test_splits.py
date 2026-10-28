import random
from datetime import date, timedelta

import polars as pl
import pytest

from diskctx.labels import build_drives, first_failures, label_rows
from diskctx.pipeline import run_data
from diskctx.splits import (
    assign_drives,
    failure_summary,
    feature_sets,
    leave_out,
    split_rows,
    study_rows,
    study_windows,
    weighted_precision,
)
from diskctx.storage import Storage
from diskctx.synthetic import MODEL_LBA, MODEL_NO_LBA, SyntheticFleet

SHARES = {"train": 0.5, "val": 0.2, "test": 0.3}
ANY_FAILURE = {"train": 1, "val": 1, "test": 1}
D0 = date(2026, 1, 1)
P_STUDY = {"groups": ["S", "W", "N", "P"], "start": "2026-01-01", "train": ["2026-01-01", "2026-04-10"],
           "val": ["2026-04-11", "2026-05-30"], "test": ["2026-05-31", "2026-07-19"]}
W_STUDY = {"groups": ["S", "W", "N"], "val_quarters": 2, "test_quarters": 2, "min_quarter_failures": 10,
           "models": {"MA": ["2016Q1", "2025Q1"], "MB": ["2024Q1", "2024Q4"]}}


def day(n):
    return D0 + timedelta(days=n)


def drives_frame(rows):
    """Drive table from dicts with model, serial_number, first_date, last_date, fail_date."""
    return pl.DataFrame(rows, schema={"model": pl.Utf8, "serial_number": pl.Utf8, "first_date": pl.Date,
                                      "last_date": pl.Date, "fail_date": pl.Date})


def fleet(models=("MA",), healthy=2000, failed_per_split=20):
    """Healthy drives alive over the whole P study, plus failures inside each split window."""
    rows = []
    for m in models:
        for i in range(healthy):
            rows.append({"model": m, "serial_number": f"{m}-H{i:05d}", "first_date": day(0),
                         "last_date": day(199), "fail_date": None})
        for k, (lo, hi) in enumerate([(5, 95), (105, 145), (155, 195)]):
            for i in range(failed_per_split):
                fail = day(lo + (i * 7) % (hi - lo))
                rows.append({"model": m, "serial_number": f"{m}-F{k}{i:03d}", "first_date": day(0),
                             "last_date": fail - timedelta(days=1), "fail_date": fail})
    return drives_frame(rows)


def p_windows(drives, models=("MA",)):
    return study_windows("P", P_STUDY, drives, list(models))


def assign(drives, windows, seed=42, ratio=20):
    return assign_drives(drives, windows, SHARES, ratio, seed)


def split_of(assignment):
    return dict(zip(assignment["serial_number"], assignment["split"]))


# --- windows ----------------------------------------------------------------------------


def quarter_failures(model, quarter_counts):
    """Failed drives, failing mid-quarter, from {quarter: count}."""
    rows = []
    for quarter, n in quarter_counts.items():
        year, q = int(quarter[:4]), int(quarter[-1])
        fail = date(year, 3 * q - 1, 15)
        for i in range(n):
            rows.append({"model": model, "serial_number": f"{model}-{quarter}-{i}", "first_date": date(2015, 1, 1),
                         "last_date": fail - timedelta(days=1), "fail_date": fail})
    return rows


def test_w_windows_last_quarters_and_end_at_last_quarter_with_enough_failures():
    ma = {f"{y}Q{q}": 10 for y in range(2016, 2025) for q in range(1, 5)}
    ma.update({"2024Q4": 3, "2025Q1": 0})                    # retirement: too few failures
    mb = {"2024Q1": 12, "2024Q2": 12, "2024Q3": 12, "2024Q4": 12}
    drives = drives_frame(quarter_failures("MA", ma) + quarter_failures("MB", mb))
    w = study_windows("W", W_STUDY, drives, ["MA", "MB"])
    got = {(r["model"], r["split"]): (r["start"], r["end"]) for r in w.iter_rows(named=True)}
    assert got[("MA", "train")] == (date(2016, 1, 1), date(2023, 9, 30))
    assert got[("MA", "val")] == (date(2023, 10, 1), date(2024, 3, 31))
    assert got[("MA", "test")] == (date(2024, 4, 1), date(2024, 9, 30))     # ends 2024Q3, not 2025Q1
    note = w.filter(pl.col("model") == "MA")["note"][0]
    assert "2024Q3" in note and "2025Q1" in note
    # MB has only 4 quarters: nothing left for train -> skipped, with the reason
    mb_rows = w.filter(pl.col("model") == "MB")
    assert mb_rows["split"].to_list() == ["skipped"]
    assert "train" in mb_rows["note"][0]


def test_p_windows_fixed_dates_and_only_models_with_test_failures():
    drives = pl.concat([fleet(("MA",), healthy=10, failed_per_split=2),
                        fleet(("MB",), healthy=10, failed_per_split=0)])
    w = p_windows(drives, ("MA", "MB"))
    used = w.filter(pl.col("split") != "skipped")
    assert set(used["model"]) == {"MA"}
    got = {r["split"]: (r["start"], r["end"]) for r in used.iter_rows(named=True)}
    assert got == {"train": (day(0), day(99)), "val": (day(100), day(149)), "test": (day(150), day(199))}
    assert w.filter(pl.col("model") == "MB")["split"].to_list() == ["skipped"]


# --- assignment -------------------------------------------------------------------------


def test_no_serial_in_two_splits():
    drives = fleet(("MA", "MB"))
    a = assign(drives, p_windows(drives, ("MA", "MB")))
    assert a.height == a["serial_number"].n_unique()
    assert set(a["split"]) == {"train", "val", "test"}


def test_failed_drives_go_to_the_split_of_their_failure_date():
    drives = fleet()
    windows = p_windows(drives)
    a = assign(drives, windows)
    split = split_of(a)
    for r in drives.filter(pl.col("fail_date").is_not_null()).iter_rows(named=True):
        w = windows.filter((pl.col("start") <= r["fail_date"]) & (pl.col("end") >= r["fail_date"]))
        assert split[r["serial_number"]] == w["split"][0]
    failed = a.join(drives, on=["model", "serial_number"]).filter(pl.col("fail_date").is_not_null())
    counts = dict(failed.group_by("split").len().iter_rows())
    assert counts == {"train": 20, "val": 20, "test": 20}       # failures in every split, all kept


def test_healthy_drives_hashed_by_shares_and_seed():
    drives = fleet(healthy=10000)
    windows = p_windows(drives)
    a = assign(drives, windows)
    healthy = a.filter(pl.col("by") == "hash")
    for s, share in SHARES.items():
        got = (healthy["split"] == s).sum() / healthy.height
        assert abs(got - share) < 0.02, s
        w = healthy.filter(pl.col("split") == s)["weight"].unique().to_list()
        assert w == [pytest.approx(1 / share)]
    assert split_of(assign(drives, windows)) == split_of(a)              # same seed, same split
    other = split_of(assign(drives, windows, seed=7))
    assert sum(other[k] != v for k, v in split_of(a).items()) > 1000      # another seed differs


def test_downsampling_train_only_by_drive_and_seeded():
    drives = fleet(("MA", "MB"), healthy=2000, failed_per_split=5)
    windows = p_windows(drives, ("MA", "MB"))
    a = assign(drives, windows)
    for m in ("MA", "MB"):
        train = a.filter((pl.col("model") == m) & (pl.col("split") == "train"))
        assert train.filter(pl.col("by") == "failure")["keep"].all()          # failed: all kept
        assert train.filter((pl.col("by") == "hash") & pl.col("keep")).height == 20 * 5
    assert a.filter(pl.col("split") != "train")["keep"].all()                # val/test untouched
    kept = set(a.filter(pl.col("keep"))["serial_number"])
    assert set(assign(drives, windows).filter(pl.col("keep"))["serial_number"]) == kept
    assert set(assign(drives, windows, seed=7).filter(pl.col("keep") & (pl.col("split") == "train")
                                                     & (pl.col("by") == "hash"))["serial_number"]) != \
        set(a.filter(pl.col("keep") & (pl.col("split") == "train") & (pl.col("by") == "hash"))["serial_number"])
    # without downsampling, val and test are the same drives
    none = assign(drives, windows, ratio=10**6)
    for s in ("val", "test"):
        assert set(none.filter(pl.col("split") == s)["serial_number"]) == \
            set(a.filter(pl.col("split") == s)["serial_number"])


def test_failure_summary_counts_and_warns_below_minimum():
    drives = pl.concat([fleet(("MA",), healthy=50, failed_per_split=60),
                        fleet(("MB",), healthy=50, failed_per_split=12)])
    windows = p_windows(drives, ("MA", "MB"))
    a = assign(drives, windows)
    summary, warnings = failure_summary("P", windows, a, ANY_FAILURE, min_test_failures=50)
    cells = {(r["model"], r["split"]): r["failed_drives"] for r in summary.iter_rows(named=True)}
    assert cells == {(m, s): n for m, n in (("MA", 60), ("MB", 12)) for s in ("train", "val", "test")}
    assert len(warnings) == 1 and "MB" in warnings[0] and "12" in warnings[0]


def test_model_needs_min_failed_drives_in_every_split():
    no_val = fleet(("MC",), healthy=50, failed_per_split=40)
    no_val = no_val.filter(~pl.col("fail_date").is_between(day(100), day(149)))     # 0 val failures
    drives = pl.concat([fleet(("MA",), healthy=50, failed_per_split=30),
                        fleet(("MB",), healthy=50, failed_per_split=29), no_val])
    windows = p_windows(drives, ("MA", "MB", "MC"))
    a = assign(drives, windows)
    need = {"train": 30, "val": 1, "test": 1}
    summary, warnings = failure_summary("P", windows, a, need, min_test_failures=50)
    included = dict(summary.select("model", "included").unique().iter_rows())
    assert included == {"MA": True, "MB": False, "MC": False}
    reason = dict(summary.select("model", "reason").unique().iter_rows())
    assert reason["MA"] is None
    assert reason["MB"] == "29 failed drives in train (< 30)"
    assert reason["MC"] == "0 failed drives in val (< 1)"
    assert len(warnings) == 1 and "MA" in warnings[0]               # only included models warn
    kept_windows, kept = leave_out(windows, a, summary)
    assert set(kept["model"]) == {"MA"}
    assert set(kept_windows.filter(pl.col("split") != "skipped")["model"]) == {"MA"}
    left = kept_windows.filter(pl.col("split") == "skipped")
    assert dict(zip(left["model"], left["note"])) == {"MB": "too few failed drives: 29 failed drives in train (< 30)",
                                                      "MC": "too few failed drives: 0 failed drives in val (< 1)"}


def test_weighted_precision_matches_full_population():
    drives = fleet(healthy=20000, failed_per_split=300)
    windows = p_windows(drives)
    a = assign(drives, windows)
    rng = random.Random(1)
    # A fake alarm per drive, independent of the split: 5% of healthy, 60% of failed drives.
    in_test = drives.filter(pl.col("fail_date").is_null() | (pl.col("fail_date") >= day(150)))
    flags = {s: rng.random() < (0.6 if f else 0.05)
             for s, f in zip(in_test["serial_number"], in_test["fail_date"].is_not_null())}
    failed_serials = set(drives.filter(pl.col("fail_date").is_not_null())["serial_number"])
    population = weighted_precision(list(flags.values()), [s in failed_serials for s in flags],
                                    [1.0] * len(flags))
    test = a.filter(pl.col("split") == "test").join(drives, on=["model", "serial_number"])
    flagged = [flags[s] for s in test["serial_number"]]
    failed = test["fail_date"].is_not_null().to_list()
    weighted = weighted_precision(flagged, failed, test["weight"].to_list())
    unweighted = weighted_precision(flagged, failed, [1.0] * test.height)
    assert abs(weighted - population) < 0.02
    assert unweighted - population > 0.1            # without weights, precision is inflated


# --- rows -------------------------------------------------------------------------------


def daily(serial, first, last, model="MA"):
    return [{"model": model, "serial_number": serial, "date": day(i)} for i in range(first, last + 1)]


def label_table(row_dicts, event_pairs, horizons=(7, 14, 30)):
    r = pl.DataFrame(row_dicts, schema={"model": pl.Utf8, "serial_number": pl.Utf8, "date": pl.Date})
    e = pl.DataFrame([{"model": "MA", "serial_number": s, "date": d} for s, d in event_pairs],
                     schema={"model": pl.Utf8, "serial_number": pl.Utf8, "date": pl.Date})
    return label_rows(r, build_drives([r], first_failures(e)), horizons)


def fixed_assignment(splits):
    """Assignment from {serial: split}, all kept."""
    return pl.DataFrame({"model": ["MA"] * len(splits), "serial_number": list(splits),
                         "split": list(splits.values()), "keep": [True] * len(splits)})


def windows_100_50_50():
    return pl.DataFrame({"study": ["P"] * 3, "model": ["MA"] * 3, "split": ["train", "val", "test"],
                         "start": [day(0), day(100), day(150)], "end": [day(99), day(149), day(199)],
                         "note": [""] * 3})


def got_dates(frame, serial):
    return set(frame.filter(pl.col("serial_number") == serial)["date"].to_list())


def test_drive_contributes_only_rows_inside_its_split_window():
    lab = label_table(daily("V", 0, 199), [])
    out = split_rows(lab, fixed_assignment({"V": "val"}), windows_100_50_50(), "val", 7, "S", ["S"])
    assert got_dates(out, "V") == {day(i) for i in range(100, 150 - 7)}  # val window, minus embargo


def test_embargo_removes_exactly_the_crossing_days():
    rows = daily("F", 0, 96) + daily("H", 0, 199) + daily("G", 0, 146) + daily("T", 0, 197) + daily("U", 0, 199)
    lab = label_table(rows, [("F", day(97)), ("G", day(147)), ("T", day(198))])
    assignment = fixed_assignment({"F": "train", "H": "train", "G": "val", "T": "test", "U": "test"})
    windows = windows_100_50_50()
    for n in (7, 14, 30):
        y = f"y_{n}"
        for split, end in (("train", day(99)), ("val", day(149))):
            out = split_rows(lab, assignment, windows, split, n, "S", ["S"])
            w = windows.filter(pl.col("split") == split).row(0, named=True)
            serials = assignment.filter(pl.col("split") == split)["serial_number"].to_list()
            inside = lab.filter(pl.col("serial_number").is_in(serials) & pl.col(y).is_not_null()
                                & (pl.col("date") >= w["start"]) & (pl.col("date") <= w["end"]))
            crossing = inside.filter(pl.col("date") + timedelta(days=n) > end)
            assert crossing.height > 0
            kept = set(zip(out["serial_number"], out["date"]))
            expected = set(zip(inside["serial_number"], inside["date"])) - set(zip(crossing["serial_number"],
                                                                                  crossing["date"]))
            assert kept == expected, (split, n)
        test = split_rows(lab, assignment, windows, "test", n, "S", ["S"])
        assert got_dates(test, "T") == {day(i) for i in range(150, 198)}    # test is not embargoed
        assert test.filter(pl.col("serial_number") == "T")[y].sum() == n


def test_data_after_train_end_cannot_change_train_labels():
    rows = (daily("A", 0, 199)                              # healthy throughout
            + daily("B", 0, 96) + daily("B", 110, 199)      # gap across the train end
            + daily("C", 0, 103)                            # fails just after the train end
            + daily("D", 0, 49)                             # fails inside train
            + daily("E", 0, 150))                           # leaves (censored) after the train end
    event_pairs = [("C", day(104)), ("D", day(50))]
    end = day(99)
    full = label_table(rows, event_pairs)
    cut = label_table([r for r in rows if r["date"] <= end], [e for e in event_pairs if e[1] <= end])
    assignment = fixed_assignment(dict.fromkeys("ABCDE", "train"))
    for n in (7, 14, 30):
        cols = ["serial_number", "date", f"y_{n}"]
        a = split_rows(full, assignment, windows_100_50_50(), "train", n, "S", ["S"]).select(cols).sort(cols[:2])
        b = split_rows(cut, assignment, windows_100_50_50(), "train", n, "S", ["S"]).select(cols).sort(cols[:2])
        assert a.equals(b), n
        assert a.height > 0 and a[f"y_{n}"].sum() > 0


def test_every_feature_set_in_a_study_gets_identical_rows():
    rows = daily("F", 0, 96) + daily("H", 0, 199)
    lab = label_table(rows, [("F", day(97))])
    assignment = fixed_assignment({"F": "train", "H": "train"})
    for groups, sets in ((["S", "W", "N"], ["S", "S+W", "S+N", "S+W+N"]),
                         (["S", "W", "N", "P"], ["S", "S+W", "S+N", "S+P", "S+W+N", "S+W+N+P"])):
        assert feature_sets(groups) == sets
        frames = [split_rows(lab, assignment, windows_100_50_50(), "train", 14, fs, groups) for fs in sets]
        assert all(f.equals(frames[0]) for f in frames)
    with pytest.raises(ValueError, match="P"):
        split_rows(lab, assignment, windows_100_50_50(), "train", 14, "S+P", ["S", "W", "N"])


# --- stage on synthetic data ------------------------------------------------------------


def test_splits_stage_on_synthetic_pipeline(cfg, fleet_fetch):
    cfg.update(start_quarter="2025Q3", end_quarter="2026Q1", models=[MODEL_LBA, MODEL_NO_LBA])
    cfg["studies"] = {
        "W": {"groups": ["S", "W", "N"], "val_quarters": 1, "test_quarters": 1, "min_quarter_failures": 1,
              "models": {MODEL_LBA: ["2025Q3", "2026Q1"], MODEL_NO_LBA: ["2025Q3", "2026Q1"]}},
        "P": {"groups": ["S", "W", "N", "P"], "start": "2025-07-01", "train": ["2025-08-01", "2025-11-30"],
              "val": ["2025-12-01", "2026-01-10"], "test": ["2026-01-11", "2026-03-31"]},
    }
    fleet_ = SyntheticFleet(cfg["seed"], "2025Q3", "2026Q1", n_drives=48)
    results = run_data(cfg, stages=("sources", "ingest", "labels", "splits"), fetch=fleet_fetch(fleet_))
    assert results["splits"] == "built"
    storage = Storage.from_config(cfg)
    assert "_synthetic" in str(storage.run_dir)
    summary = pl.read_csv(storage.run_dir / "splits" / "summary.csv")
    assert set(summary["study"]) == {"W", "P"}
    assert summary["included"].any()
    assert summary.filter(pl.col("split") == "test")["failed_drives"].sum() > 0
    for study in ("W", "P"):
        a = pl.read_parquet(storage.run_dir / "splits" / study / "drives.parquet")
        assert a.height == a["serial_number"].n_unique()
        for split in ("train", "val", "test"):
            sets = feature_sets(cfg["studies"][study]["groups"])
            frames = [study_rows(storage, cfg, study, split, 7, fs) for fs in sets]
            assert all(f.equals(frames[0]) for f in frames)
            assert set(frames[0]["serial_number"]) <= set(a.filter(pl.col("split") == split)["serial_number"])
