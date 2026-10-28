"""Leak check (b): are there pods or vaults with extreme failure rates? Read-only.

Raw: failures per drive-day per pod and vault from the pass-1 pod_days tables
(audit/quarters/<Q>/pod_days.parquet, all non-boot drives), against the fleet rate.
Model-adjusted: for one quarter, expected failures = sum over models of the unit's
drive-days x the model's failure rate, from pod_model_days_<Q>.csv (pod_moves.py), so
a pod full of a failure-prone model is not called hot.

A unit is extreme if its ratio is >= 3 and the Poisson chance of at least that many
failures is < 0.001 / number of units (Bonferroni).

    DISKCTX_LOCAL_ROOT=~/diskctx_data python tools/diagnostics/hot_pods.py \
        --config configs/full.yaml --start 2023Q3 --end 2026Q2 --adjust-quarter 2026Q1

Writes to --out (default results/diagnostics/), every unit, most extreme first:
  hot_pods_<start>_<end>_{pods,vaults}.csv     raw
  hot_pods_adjusted_<Q>_{pods,vaults}.csv      model-adjusted (if the input exists)
"""

import argparse
import math
from pathlib import Path

import polars as pl

from diskctx.config import load_config, quarters_between
from diskctx.storage import DURABLE, Storage

MIN_RATIO = 3
ALPHA = 0.001


def poisson_sf(k, mu):
    """P(X >= k) for X ~ Poisson(mu)."""
    if k <= 0:
        return 1.0
    term, total = math.exp(-mu), 0.0
    for i in range(k):
        total += term
        term *= mu / (i + 1)
    return max(0.0, 1.0 - total)


def vault_of(pod_key):
    return pod_key.str.split("|").list.slice(0, 3).list.join("|")


def flag_extreme(units, ratio):
    """Adds p_high (Poisson P(X >= failures | expected)) and extreme; sorts by p_high."""
    alpha = ALPHA / units.height
    p = [poisson_sf(f, e) for f, e in zip(units["failures"], units["expected"])]
    return (units.with_columns(pl.Series("p_high", p, pl.Float64))
            .with_columns(((pl.col("p_high") < alpha) & (pl.col(ratio) >= MIN_RATIO)).alias("extreme"))
            .sort("p_high", ratio, descending=[False, True]))


def report(units, path, what):
    extreme = units.filter("extreme")
    print(f"\n{path}: {units.height} {what}, {extreme.height} extreme, holding {extreme['failures'].sum()} of "
          f"{units['failures'].sum()} failures")
    print(units.head(10))
    units.write_csv(path)


def raw(storage, quarters, out):
    for q in quarters:
        if not storage.marker_path(f"audit_{q}").exists():
            raise SystemExit(f"audit_{q} has no done-marker; run pass 1 first")
    days = pl.concat([pl.read_parquet(storage.path(DURABLE, f"audit/quarters/{q}/pod_days.parquet"),
                                      columns=["date", "pod_key", "vault_key", "drives", "failures"])
                      for q in quarters])
    rate = days["failures"].sum() / days["drives"].sum()
    print(f"{quarters[0]}-{quarters[-1]}: {days['drives'].sum():,} drive-days, {days['failures'].sum():,} "
          f"failures, fleet rate {1e5 * rate:.3f} per 100k drive-days")
    for unit, what in (("pod_key", "pods"), ("vault_key", "vaults")):
        per_day = days.group_by(unit, "date").agg(pl.col("drives").sum(), pl.col("failures").sum())
        units = (per_day.group_by(unit)
                 .agg(pl.col("date").n_unique().alias("days"), pl.col("drives").sum().alias("drive_days"),
                      pl.col("drives").mean().round(1).alias("mean_drives"), pl.col("failures").sum())
                 .with_columns((pl.col("drive_days") * rate).alias("expected"))
                 .with_columns((pl.col("failures") / pl.col("expected")).alias("ratio")))
        report(flag_extreme(units, "ratio"), out / f"hot_pods_{quarters[0]}_{quarters[-1]}_{what}.csv", what)
        if unit == "pod_key":
            small = units.filter(pl.col("mean_drives") < 20)
            print(f"pods with < 20 drives on average (spare/quarantine signature): {small.height}, "
                  f"{small['failures'].sum()} failures")


def adjusted(model_days_csv, quarter, out):
    md = pl.read_csv(model_days_csv, schema_overrides={"pod_key": pl.Utf8, "model": pl.Utf8})
    rates = md.group_by("model").agg((pl.col("failures").sum() / pl.col("drive_days").sum()).alias("rate"))
    fleet = md["failures"].sum() / md["drive_days"].sum()
    md = md.join(rates, on="model").with_columns(vault_of(pl.col("pod_key")).alias("vault_key"))
    for unit, what in (("pod_key", "pods"), ("vault_key", "vaults")):
        units = (md.group_by(unit)
                 .agg(pl.col("drive_days").sum(), pl.col("failures").sum(),
                      (pl.col("drive_days") * pl.col("rate")).sum().alias("expected"))
                 .with_columns((pl.col("failures") / pl.col("expected")).alias("ratio"),
                               (pl.col("failures") / (pl.col("drive_days") * fleet)).alias("raw_ratio")))
        raw_alpha = ALPHA / units.height
        raw_extreme = sum(poisson_sf(f, d * fleet) < raw_alpha and r >= MIN_RATIO
                          for f, d, r in zip(units["failures"], units["drive_days"], units["raw_ratio"]))
        print(f"\n{quarter} {what}: {raw_extreme} extreme before model adjustment")
        report(flag_extreme(units, "ratio"), out / f"hot_pods_adjusted_{quarter}_{what}.csv", what)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--start", default="2023Q3")
    parser.add_argument("--end", default="2026Q2")
    parser.add_argument("--adjust-quarter", default="2026Q1",
                        help="quarter of <out>/pod_model_days_<Q>.csv (from pod_moves.py)")
    parser.add_argument("--out", default="results/diagnostics")
    args = parser.parse_args()

    storage = Storage.from_config(load_config(args.config))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    pl.Config.set_tbl_width_chars(160)
    raw(storage, quarters_between(args.start, args.end), out)
    model_days = out / f"pod_model_days_{args.adjust_quarter}.csv"
    if model_days.exists():
        adjusted(model_days, args.adjust_quarter, out)
    else:
        print(f"\n{model_days} not found; run pod_moves.py --quarter {args.adjust_quarter} for the "
              f"model-adjusted tables")


if __name__ == "__main__":
    main()
