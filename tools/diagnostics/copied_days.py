"""Copied day files: days on which no drive's SMART 9 (power-on hours) advanced. Read-only.

Reads the pass-2 Parquet (no downloads). For every pair of consecutive calendar days
(D-1, D), compares SMART 9 per drive; a genuine day advances ~24 h. A day D is flagged
when at least --min-share of the drives seen on both days did not advance: D-1 and D
then hold the same snapshot. Which file is the copy comes from the neighbours:
  - D-2 -> D-1 about +48 h and D -> D+1 about +24 h: the snapshot is D's, so the file
    D-1 is a copy of D (the "matches the next day" case);
  - D-2 -> D-1 about +24 h and D -> D+1 about +48 h: the file D is a copy of D-1;
  - anything else: unclear.
Days already in the config's dropped_days are not in the Parquet; they are listed with
their evidence, so the table stays complete after the drop.

    DISKCTX_LOCAL_ROOT=~/diskctx_data python tools/diagnostics/copied_days.py \
        --config configs/full.yaml

Writes to --out (default results/diagnostics/): copied_days.csv, one row per flagged or
dropped day; copy_file is the file to drop (empty when unclear).
"""

import argparse
from datetime import timedelta
from pathlib import Path

import polars as pl

from diskctx.config import load_config, parse_date
from diskctx.storage import DURABLE, Storage

TWO_DAYS = 36   # hours: above this a one-day step is really two days


def day_steps(parquet_dir):
    """Per calendar day D (with D-1 present): rows and failures on D, drives compared,
    drives whose SMART 9 did not advance, and the median SMART 9 step (from a histogram of
    steps per day, so the whole fleet is never in memory at once)."""
    rows, hist = [], []
    for model_dir in sorted(parquet_dir.iterdir()):
        lf = pl.scan_parquet(sorted(model_dir.glob("*.parquet"))).select(
            "serial_number", "date", "smart_9_raw", "failure")
        rows.append(lf.group_by("date").agg(pl.len().alias("rows"), pl.col("failure").sum().alias("failures"))
                    .collect())
        hist.append(lf.sort("serial_number", "date")
                    .with_columns(pl.col("date").shift(1).over("serial_number").alias("prev_date"),
                                  (pl.col("smart_9_raw") - pl.col("smart_9_raw").shift(1).over("serial_number"))
                                  .alias("dh"))
                    .filter(((pl.col("date") - pl.col("prev_date")).dt.total_days() == 1) & pl.col("dh").is_not_null())
                    .group_by("date", "dh").len().collect())
    rows = pl.concat(rows).group_by("date").agg(pl.col("rows").sum(), pl.col("failures").sum())
    hist = pl.concat(hist).group_by("date", "dh").agg(pl.col("len").sum()).sort("date", "dh")
    steps = (hist.with_columns(pl.col("len").cum_sum().over("date").alias("cum"),
                               pl.col("len").sum().over("date").alias("pairs"))
             .group_by("date")
             .agg(pl.col("pairs").first(), pl.col("len").filter(pl.col("dh") == 0).sum().alias("unchanged"),
                  pl.col("dh").filter(pl.col("cum") * 2 >= pl.col("pairs")).first().alias("median_dh")))
    return rows.join(steps, on="date", how="left").sort("date")


def copy_verdict(day, before, after):
    """(verdict, the copied file's day or None)."""
    if before is not None and after is not None and before > TWO_DAYS and after <= TWO_DAYS:
        return "previous day is a copy of this day", day - timedelta(days=1)
    if before is not None and after is not None and before <= TWO_DAYS and after > TWO_DAYS:
        return "this day is a copy of the previous day", day
    return "unclear", None


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--min-share", type=float, default=0.99,
                        help="flag a day when at least this share of drives did not advance")
    parser.add_argument("--out", default="results/diagnostics")
    args = parser.parse_args()

    cfg = load_config(args.config)
    storage = Storage.from_config(cfg)
    days = day_steps(storage.path(DURABLE, "parquet"))
    by_day = {r["date"]: r for r in days.iter_rows(named=True)}
    out_rows = []
    for r in days.filter(pl.col("unchanged") >= args.min_share * pl.col("pairs")).iter_rows(named=True):
        d, prev, nxt = r["date"], r["date"] - timedelta(days=1), r["date"] + timedelta(days=1)
        before = by_day.get(prev, {}).get("median_dh")     # step D-2 -> D-1
        after = by_day.get(nxt, {}).get("median_dh")       # step D -> D+1
        verdict, copy_file = copy_verdict(d, before, after)
        out_rows.append({"date": d, "previous_day": prev, "copy_file": copy_file, "drives_compared": r["pairs"],
                         "share_unchanged": round(r["unchanged"] / r["pairs"], 4),
                         "median_h_before": before, "median_h_after": after,
                         "failures_previous_day": by_day.get(prev, {}).get("failures"),
                         "failures_this_day": r["failures"], "verdict": verdict,
                         "status": "in the data"})
    for day, why in cfg["dropped_days"].items():
        out_rows.append({"date": parse_date(day), "copy_file": parse_date(day), "verdict": "dropped by config",
                         "status": why})
    table = pl.DataFrame(out_rows, infer_schema_length=None).sort("date")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table.write_csv(out / "copied_days.csv")
    pl.Config.set_tbl_width_chars(220)
    pl.Config.set_fmt_str_lengths(60)
    print(f"{days.height} days compared; {table.filter(pl.col('status') == 'in the data').height} flagged "
          f"(share unchanged >= {args.min_share}); {len(cfg['dropped_days'])} dropped by config")
    print(table)
    print(out / "copied_days.csv")


if __name__ == "__main__":
    main()
