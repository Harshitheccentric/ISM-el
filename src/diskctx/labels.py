"""Labels (M3): failure dates from the pass-1 events, and horizon labels per drive-day.

Failure dates come only from the failure events (audit/quarters/<Q>/events.parquet),
which pass 1 takes from the raw rows before any cleaning. A Parquet row's own `failure`
column is never used. Events are matched to drive-days on (model, serial_number).

A drive's failure date is its FIRST event. The failure-day row is the event, not a
sample, so it gets no label. Rows after the first failure are dropped and counted; a
serial that shows up again after failing (or has a second event) is reported in
reappeared.csv, never merged into a new drive.

Label for horizon N on drive-day t (calendar dates, so gaps in a drive's history cannot
shift a label):
  failed drive:   y_N = 1 if the failure date is in (t, t+N], else 0.
  no failure:     y_N = 0 if t <= last record - N, else null (censored tail: the drive
                  left, was migrated, or is alive at the end of its data, so we do not
                  know whether it failed within N days).

labels stage -> labels/
  drives.parquet   one row per (model, serial): first and last record, failure date,
                   events, failure-day rows, rows after the first failure.
  report.csv       per model and horizon: samples, positives, negatives, censored rows,
                   plus the drive-level counts.
  reappeared.csv   drives with rows after their first failure or more than one event.
`horizons` is a key of this stage, so changing it reruns labels and everything after.
"""

import csv

import polars as pl

from diskctx.ingest import parquet_path
from diskctx.storage import DURABLE, Stage

KEY = ["model", "serial_number"]
REPORT_COLUMNS = ["model", "horizon", "samples", "positives", "negatives", "censored", "drives",
                  "failed_drives", "fail_day_rows", "rows_after_failure", "reappeared_drives",
                  "events_without_rows", "data_source"]


def y_col(horizon):
    return f"y_{horizon}"


def first_failures(events):
    """Events (model, serial_number, date) -> one row per drive: fail_date (first event)
    and n_events."""
    return (pl.LazyFrame(events).group_by(KEY)
            .agg(pl.col("date").min().alias("fail_date"), pl.len().alias("n_events"))
            .collect())


def build_drives(row_frames, failures):
    """One row per (model, serial) from drive-day frames (one per Parquet file) and the
    first failures. Each frame is aggregated on its own, then the parts are combined, so
    only one quarter is in memory at a time."""
    fails = failures.lazy().select(*KEY, "fail_date")
    parts = []
    for frame in row_frames:
        part = (frame.lazy().select(*KEY, "date")
                .join(fails, on=KEY, how="left")
                .group_by(KEY)
                .agg(pl.col("date").min().alias("first_date"),
                     pl.col("date").max().alias("last_date"),
                     pl.len().alias("rows"),
                     (pl.col("date") == pl.col("fail_date")).sum().alias("fail_day_rows"),
                     (pl.col("date") > pl.col("fail_date")).sum().alias("rows_after_failure"))
                .collect())
        parts.append(part)
    drives = (pl.concat(parts).group_by(KEY)
              .agg(pl.col("first_date").min(), pl.col("last_date").max(), pl.col("rows").sum(),
                   pl.col("fail_day_rows").sum(), pl.col("rows_after_failure").sum()))
    return (drives.join(failures, on=KEY, how="left")
            .with_columns(pl.col("n_events").fill_null(0))
            .sort(KEY))


def label_rows(rows, drives, horizons):
    """Drive-day rows (model, serial_number, date, ...) -> the same rows with fail_date
    and y_<N> for each horizon. Drops the failure-day row and every later row.
    Returns a DataFrame (or a LazyFrame if given one)."""
    lazy = isinstance(rows, pl.LazyFrame)
    facts = drives.lazy().select(*KEY, "fail_date", "last_date")
    lf = (rows.lazy().join(facts, on=KEY, how="left")
          .filter(pl.col("fail_date").is_null() | (pl.col("date") < pl.col("fail_date"))))
    failed = pl.col("fail_date").is_not_null()
    labels = []
    for n in horizons:
        days_to_failure = (pl.col("fail_date") - pl.col("date")).dt.total_days()
        y = (pl.when(failed).then((days_to_failure <= n).cast(pl.Int8))
             .when(pl.col("date") <= pl.col("last_date") - pl.duration(days=n)).then(pl.lit(0, pl.Int8))
             .otherwise(pl.lit(None, pl.Int8)))
        labels.append(y.alias(y_col(n)))
    lf = lf.with_columns(labels).drop("last_date")
    return lf if lazy else lf.collect()


# --- stage ------------------------------------------------------------------------------


def read_events(storage, quarters, models):
    """All failure events of `models` from the audit folders of `quarters`."""
    frames = []
    for quarter in quarters:
        path = storage.path(DURABLE, f"audit/quarters/{quarter}/events.parquet")
        frames.append(pl.read_parquet(path, columns=[*KEY, "date"]))
    events = pl.concat(frames) if frames else pl.DataFrame(schema={"model": pl.Utf8, "serial_number": pl.Utf8,
                                                                    "date": pl.Date})
    return events.filter(pl.col("model").is_in(models))


def row_files(storage, models, quarters):
    """{model: [Parquet paths]} for the pass-2 files that exist."""
    out = {}
    for model in models:
        paths = [storage.path(DURABLE, parquet_path(model, q)) for q in quarters]
        out[model] = [p for p in paths if p.exists()]
    return out


def scan(path):
    return pl.scan_parquet(path).select(*KEY, "date")


def label_report(files, drives, failures, horizons, data_source):
    """Per model and horizon: label counts, plus the drive-level counts."""
    rows = []
    for model, paths in files.items():
        d = drives.filter(pl.col("model") == model)
        no_rows = failures.filter(pl.col("model") == model).join(d, on=KEY, how="anti").height
        counts = {n: {"samples": 0, "positives": 0, "negatives": 0, "censored": 0} for n in horizons}
        for path in paths:
            part = label_rows(scan(path), d, horizons).select(
                [pl.len().alias("samples")]
                + [agg for n in horizons for agg in (
                    (pl.col(y_col(n)) == 1).sum().alias(f"pos_{n}"),
                    (pl.col(y_col(n)) == 0).sum().alias(f"neg_{n}"),
                    pl.col(y_col(n)).is_null().sum().alias(f"cen_{n}"))]).collect().row(0, named=True)
            for n in horizons:
                c = counts[n]
                c["samples"] += part["samples"]
                c["positives"] += part[f"pos_{n}"]
                c["negatives"] += part[f"neg_{n}"]
                c["censored"] += part[f"cen_{n}"]
        for n in horizons:
            rows.append({
                "model": model, "horizon": n, **counts[n],
                "drives": d.height,
                "failed_drives": d.filter(pl.col("fail_date").is_not_null()).height,
                "fail_day_rows": d["fail_day_rows"].sum(),
                "rows_after_failure": d["rows_after_failure"].sum(),
                "reappeared_drives": d.filter((pl.col("rows_after_failure") > 0) | (pl.col("n_events") > 1)).height,
                "events_without_rows": no_rows,
                "data_source": data_source,
            })
    return rows


def write_csv(path, rows, columns):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def labels_stage(cfg, models, quarters, audit_names, ingest_names):
    """The labels stage. Reads the events of every audited quarter and the pass-2 Parquet
    of `models` in `quarters`."""
    audit_quarters = [name.removeprefix("audit_") for name in audit_names]

    def build(work, storage):
        failures = first_failures(read_events(storage, audit_quarters, models))
        files = row_files(storage, models, quarters)
        drives = build_drives([scan(p) for paths in files.values() for p in paths], failures)
        drives = drives.with_columns(pl.lit(storage.data_source).alias("data_source"))
        out = work / "labels"
        out.mkdir(parents=True)
        drives.write_parquet(out / "drives.parquet", compression="zstd")
        report = label_report(files, drives, failures, cfg["horizons"], storage.data_source)
        write_csv(out / "report.csv", report, REPORT_COLUMNS)
        reappeared = drives.filter((pl.col("rows_after_failure") > 0) | (pl.col("n_events") > 1))
        reappeared.select(*KEY, "fail_date", "n_events", "last_date", "rows_after_failure",
                          "data_source").write_csv(out / "reappeared.csv")
        for r in report:
            if r["horizon"] == cfg["horizons"][0]:
                print(f"labels {r['model']}: {r['drives']} drives, {r['failed_drives']} failed, "
                      f"{r['fail_day_rows']} failure-day rows and {r['rows_after_failure']} rows after "
                      f"a failure dropped, {r['reappeared_drives']} reappeared, "
                      f"{r['events_without_rows']} events without rows", flush=True)
        return {"drives": drives.height, "reappeared": reappeared.height}

    return Stage(
        name="labels",
        tier=DURABLE,
        outputs=["labels"],
        build=build,
        keys=["data_source", "horizons"],
        deps=[*audit_names, *ingest_names],
        params={"models": sorted(models), "quarters": list(quarters)},
    )


def read_drives(storage):
    return pl.read_parquet(storage.path(DURABLE, "labels/drives.parquet"))
