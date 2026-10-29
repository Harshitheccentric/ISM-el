"""Feature missingness before failure vs healthy drive-days, train and val only. Read-only.

Per study, model, split and feature: the null (or NaN) rate on a failing drive's last 7
days before its failure (f-7..f-1) and on healthy drive-days (drives that never fail). A
feature whose two rates differ by more than --ratio (default 2) in either direction is
flagged, as is one that is never null on one side but null on the other. Missingness is
never a feature, but a column that goes silent before failure would still let a model
learn "NaN means failure" through XGBoost's default direction; this shows where.

The test split is locked (CLAUDE.md: read only by M5's final evaluation). This script
never opens a test file and refuses --splits test. Train rows are hash-sampled uniformly
among the rows away from a failure, and every pre-failure row is kept, so unweighted
rates are fair estimates.

    DISKCTX_LOCAL_ROOT=~/diskctx_data python tools/diagnostics/feature_missingness.py \
        --config configs/full.yaml

Writes --out (default results/diagnostics/): feature_missingness.csv, one row per study,
model, split and feature.
"""

import argparse
import json
from pathlib import Path

import polars as pl

from diskctx.config import load_config
from diskctx.storage import DURABLE, Storage

ALLOWED = ("train", "val")


def check_splits(splits):
    locked = [s for s in splits if s not in ALLOWED]
    if locked:
        raise ValueError(f"splits {locked} are not allowed: only {ALLOWED}; the test split is locked")


def missing(col):
    return pl.col(col).is_null() | pl.col(col).is_nan()


def missingness(lf, features, days=7):
    """One row per feature: rows and null rows on pre-failure days (1..days before the
    failure) and on healthy drive-days. lf: a feature file (LazyFrame)."""
    pre = pl.col("fail_date").is_not_null() & (pl.col("fail_date") - pl.col("date")).dt.total_days().is_between(1, days)
    healthy = pl.col("fail_date").is_null()
    sums = lf.select(pre.sum().alias("pre_rows"), healthy.sum().alias("healthy_rows"),
                     *[(missing(f) & pre).sum().alias(f"pre|{f}") for f in features],
                     *[(missing(f) & healthy).sum().alias(f"healthy|{f}") for f in features]
                     ).collect(engine="streaming").row(0, named=True)
    rows = []
    for f in features:
        pre_rate = sums[f"pre|{f}"] / sums["pre_rows"] if sums["pre_rows"] else None
        healthy_rate = sums[f"healthy|{f}"] / sums["healthy_rows"] if sums["healthy_rows"] else None
        rows.append({"feature": f, "pre_rows": sums["pre_rows"], "pre_null": sums[f"pre|{f}"], "pre_rate": pre_rate,
                     "healthy_rows": sums["healthy_rows"], "healthy_null": sums[f"healthy|{f}"],
                     "healthy_rate": healthy_rate})
    return rows


def rate_ratio(pre_rate, healthy_rate):
    """pre / healthy (inf if only the pre-failure days have nulls, 0 the other way round)."""
    if pre_rate is None or healthy_rate is None or (pre_rate == 0 and healthy_rate == 0):
        return None
    if healthy_rate == 0:
        return float("inf")
    return pre_rate / healthy_rate


def flagged(ratio, limit):
    return ratio is not None and (ratio > limit or ratio < 1 / limit)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--splits", default="train,val", help="comma-separated, from train,val")
    parser.add_argument("--days", type=int, default=7, help="pre-failure days: f-days .. f-1")
    parser.add_argument("--ratio", type=float, default=2.0, help="flag rates that differ by more than this factor")
    parser.add_argument("--out", default="results/diagnostics")
    args = parser.parse_args()
    splits = [s.strip() for s in args.splits.split(",") if s.strip()]
    check_splits(splits)

    cfg = load_config(args.config)
    storage = Storage.from_config(cfg)
    rows = []
    for about_path in sorted(storage.path(DURABLE, "features").glob("*/*/columns.json")):
        about = json.loads(about_path.read_text())
        features = [f for group in about["groups"].values() for f in group]
        for split in splits:
            lf = pl.scan_parquet(about_path.parent / f"{split}.parquet")
            for r in missingness(lf, features, args.days):
                ratio = rate_ratio(r["pre_rate"], r["healthy_rate"])
                rows.append({"study": about["study"], "model": about["model"], "split": split, **r,
                             "ratio": ratio, "flagged": flagged(ratio, args.ratio),
                             "data_source": about["data_source"]})
    table = pl.DataFrame(rows, infer_schema_length=None)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table.write_csv(out / "feature_missingness.csv")
    flags = table.filter(pl.col("flagged")).sort("study", "model", "split", "feature")
    pl.Config.set_tbl_rows(200)
    pl.Config.set_tbl_width_chars(200)
    print(f"{table.height} (study, model, split, feature) rows; {flags.height} flagged "
          f"(null rate on f-{args.days}..f-1 vs healthy differs by > {args.ratio}x)")
    print(flags.select("study", "model", "split", "feature", "pre_null", "pre_rows", "pre_rate", "healthy_rate",
                       "ratio"))
    print(out / "feature_missingness.csv")


if __name__ == "__main__":
    main()
