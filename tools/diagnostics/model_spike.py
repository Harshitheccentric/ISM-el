"""Leak check (c): is one model's failure spike a few pods, or wider? Read-only.

From the pass-1 failure events (audit/quarters/<Q>/events.parquet) and pod_days: the
model's failures per quarter, and for the spike quarter by vault, pod and week. The
vault table also gives the vault's size (all models, mean drives per day in the spike
quarter) and, if pod_model_days_<Q>.csv (pod_moves.py) exists, the model's own drives
per vault in that quarter.

    DISKCTX_LOCAL_ROOT=~/diskctx_data python tools/diagnostics/model_spike.py \
        --config configs/full.yaml --model "TOSHIBA MG08ACA16TEY" --quarter 2025Q3

Writes to --out (default results/diagnostics/), <M> = model with spaces as _:
  spike_<M>_per_quarter.csv           quarter, failures, vaults, pods (start..end)
  spike_<M>_<Q>_by_vault.csv          vault failures in <Q>, cumulative %, vault size
  spike_<M>_<Q>_vaults_by_quarter.csv those vaults' failures in every quarter
  spike_<M>_<Q>_by_pod.csv            pod failures in <Q>
  spike_<M>_<Q>_by_week.csv           failures per week (Monday start) in <Q>
"""

import argparse
from pathlib import Path

import polars as pl

from diskctx.config import load_config, quarters_between
from diskctx.ingest import model_dir
from diskctx.storage import DURABLE, Storage


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--model", default="TOSHIBA MG08ACA16TEY")
    parser.add_argument("--quarter", default="2025Q3", help="the spike quarter")
    parser.add_argument("--start", default="2023Q3", help="first quarter of the per-quarter history")
    parser.add_argument("--end", default="2026Q2")
    parser.add_argument("--size-quarter", default="2026Q1",
                        help="quarter of <out>/pod_model_days_<Q>.csv for the model's drives per vault")
    parser.add_argument("--out", default="results/diagnostics")
    args = parser.parse_args()

    storage = Storage.from_config(load_config(args.config))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    quarters = quarters_between(args.start, args.end)
    for q in quarters:
        if not storage.marker_path(f"audit_{q}").exists():
            raise SystemExit(f"audit_{q} has no done-marker; run pass 1 first")
    pl.Config.set_tbl_rows(30)
    events = pl.concat([pl.read_parquet(storage.path(DURABLE, f"audit/quarters/{q}/events.parquet"))
                        for q in quarters]).filter(pl.col("model") == args.model)
    spike = events.filter(pl.col("quarter") == args.quarter)
    prefix = out / f"spike_{model_dir(args.model)}"
    tables = {}

    tables[f"{prefix}_per_quarter.csv"] = (
        events.group_by("quarter").agg(pl.len().alias("failures"), pl.col("vault_key").n_unique().alias("vaults"),
                                       pl.col("pod_key").n_unique().alias("pods")).sort("quarter"))

    size = (pl.read_parquet(storage.path(DURABLE, f"audit/quarters/{args.quarter}/pod_days.parquet"),
                            columns=["date", "vault_key", "drives"])
            .group_by("vault_key", "date").agg(pl.col("drives").sum())
            .group_by("vault_key").agg(pl.col("drives").mean().round(0).cast(pl.Int64).alias("vault_drives")))
    # The model's vaults: where it failed (any quarter) or, if known, where it has drives.
    vaults = pl.DataFrame({"vault_key": events["vault_key"].drop_nulls().unique()})
    model_days = out / f"pod_model_days_{args.size_quarter}.csv"
    if model_days.exists():
        md = (pl.read_csv(model_days, schema_overrides={"pod_key": pl.Utf8, "model": pl.Utf8})
              .filter(pl.col("model") == args.model)
              .with_columns(pl.col("pod_key").str.split("|").list.slice(0, 3).list.join("|").alias("vault_key"))
              .group_by("vault_key").agg(pl.col("drives").sum().alias(f"model_drives_{args.size_quarter}")))
        vaults = vaults.join(md, on="vault_key", how="full", coalesce=True)
    by_vault = (vaults.join(spike.group_by("vault_key").agg(pl.len().alias("failures")), on="vault_key", how="left")
                .join(size, on="vault_key", how="left")
                .with_columns(pl.col("failures").fill_null(0))
                .sort("failures", "vault_key", descending=[True, False])
                .with_columns((pl.col("failures").cum_sum() / spike.height * 100).round(1).alias("cum_pct"))
                .select("vault_key", "failures", "cum_pct", "vault_drives", pl.exclude("vault_key", "failures",
                                                                                   "cum_pct", "vault_drives")))
    tables[f"{prefix}_{args.quarter}_by_vault.csv"] = by_vault

    spike_vaults = spike["vault_key"].unique().to_list()
    tables[f"{prefix}_{args.quarter}_vaults_by_quarter.csv"] = (
        events.filter(pl.col("vault_key").is_in(spike_vaults)).group_by("vault_key", "quarter").len()
        .pivot(on="quarter", index="vault_key", values="len", sort_columns=True).fill_null(0)
        .join(by_vault.select("vault_key", "failures"), on="vault_key")
        .sort("failures", "vault_key", descending=[True, False]).drop("failures"))
    tables[f"{prefix}_{args.quarter}_by_pod.csv"] = (
        spike.group_by("pod_key").agg(pl.len().alias("failures")).sort("failures", "pod_key",
                                                                         descending=[True, False]))
    tables[f"{prefix}_{args.quarter}_by_week.csv"] = (
        spike.with_columns(pl.col("date").dt.truncate("1w").alias("week")).group_by("week")
        .agg(pl.len().alias("failures")).sort("week"))

    pods = tables[f"{prefix}_{args.quarter}_by_pod.csv"]
    print(f"{args.model} {args.quarter}: {spike.height} failures, {len(spike_vaults)} vaults, {pods.height} pods "
          f"(max {pods['failures'].max()} per pod, {pods.filter(pl.col('failures') == 1).height} pods with 1), "
          f"null pod_key {spike['pod_key'].null_count()}")
    for path, table in tables.items():
        table.write_csv(path)
        print(f"\n{path}")
        print(table)


if __name__ == "__main__":
    main()
