"""Leak check (a): do drives move to another pod before they fail? Read-only.

Reads one quarter zip one day at a time (as the pipeline does) and compares pod_key
changes between a drive's consecutive rows, for failing drives (by period before the
failure) and healthy drives. Boot models (audit/boot_models.csv) are left out. The zip
is never moved or deleted; the scratch DuckDB file is deleted at the end.

    DISKCTX_LOCAL_ROOT=~/diskctx_data python tools/diagnostics/pod_moves.py \
        --config configs/full.yaml --quarter 2026Q1

Writes to --out (default results/diagnostics/):
  pod_moves_<Q>_by_period.csv  per period: drives, comparable drive-days, moves, moves per
                               10k drive-days, drives moved, moves within / across vaults,
                               moves after a gap in the drive's rows
  pod_moves_<Q>_by_day.csv     days with moves
  pod_model_days_<Q>.csv       per model and pod: drives, drive-days, failures (input for
                               the model-adjusted part of hot_pods.py and model_spike.py)
"""

import argparse
from pathlib import Path

import duckdb

from diskctx.config import load_config, parse_quarter
from diskctx.rawzip import day_members, extract_member
from diskctx.storage import DURABLE, Storage

PERIODS = """
CASE WHEN fail_date IS NULL THEN 'healthy'
     WHEN date = fail_date THEN 'failing: day f'
     WHEN date >= fail_date - 7 AND date < fail_date THEN 'failing: f-7..f-1'
     WHEN date >= fail_date - 30 AND date < fail_date - 7 THEN 'failing: f-30..f-8'
     WHEN date < fail_date - 30 THEN 'failing: before f-30'
     ELSE 'after failure' END"""


def vault(col):
    return f"split_part({col}, '|', 1) || '|' || split_part({col}, '|', 2) || '|' || split_part({col}, '|', 3)"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--quarter", default="2026Q1")
    parser.add_argument("--zip", help="quarter zip (default: <local>/<mode>/downloads/data_QN_YYYY.zip)")
    parser.add_argument("--scratch", help="scratch folder (default: <local>/<mode>/diagnostics)")
    parser.add_argument("--out", default="results/diagnostics")
    args = parser.parse_args()

    storage = Storage.from_config(load_config(args.config))
    year, q = parse_quarter(args.quarter)
    zip_path = Path(args.zip) if args.zip else storage.download_dir / f"data_Q{q}_{year}.zip"
    boot_csv = storage.path(DURABLE, "audit/boot_models.csv")
    scratch = Path(args.scratch) if args.scratch else storage.download_dir.parent / "diagnostics"
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    db = scratch / f"pod_moves_{args.quarter}.duckdb"
    db.unlink(missing_ok=True)

    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE rows (date DATE, serial VARCHAR, model VARCHAR, failure VARCHAR, pod_key VARCHAR)")
    members = day_members(zip_path)
    for day, member in members:
        day_csv = scratch / "day.csv"
        extract_member(zip_path, member, day_csv)
        con.execute(f"""
            INSERT INTO rows
            SELECT DATE '{day}', serial_number, model, failure,
                   datacenter || '|' || cluster_id || '|' || vault_id || '|' || pod_id
            FROM read_csv('{day_csv}', all_varchar=true, header=true)""")
        day_csv.unlink()
    print(f"{args.quarter}: {len(members)} days read from {zip_path}")

    con.execute(f"""
        CREATE TABLE d AS
        SELECT *, lag(pod_key) OVER w AS prev_pod, lag(date) OVER w AS prev_date
        FROM rows WHERE model NOT IN (SELECT model FROM read_csv('{boot_csv}', header=true))
        WINDOW w AS (PARTITION BY serial ORDER BY date)""")
    con.execute("CREATE TABLE f AS SELECT serial, min(date) AS fail_date FROM d WHERE failure = '1' GROUP BY serial")
    con.execute(f"""
        CREATE TABLE x AS
        SELECT d.*, f.fail_date, {PERIODS} AS period,
               (prev_pod IS NOT NULL AND pod_key IS NOT NULL) AS comparable,
               (prev_pod IS NOT NULL AND pod_key IS NOT NULL AND pod_key <> prev_pod) AS moved
        FROM d LEFT JOIN f USING (serial)""")

    tables = {
        "by_period": f"""
            SELECT period, count(DISTINCT serial) AS drives, sum(comparable::INT) AS drive_days,
                   sum(moved::INT) AS moves,
                   round(1e4 * sum(moved::INT) / sum(comparable::INT), 4) AS moves_per_10k_days,
                   count(DISTINCT serial) FILTER (moved) AS drives_moved,
                   sum((moved AND {vault('pod_key')} = {vault('prev_pod')})::INT) AS moves_same_vault,
                   sum((moved AND {vault('pod_key')} <> {vault('prev_pod')})::INT) AS moves_other_vault,
                   sum((moved AND date - prev_date > 1)::INT) AS moves_after_gap
            FROM x GROUP BY period ORDER BY period""",
        "by_day": "SELECT date, sum(moved::INT) AS moves FROM x GROUP BY date HAVING sum(moved::INT) > 0 "
                  "ORDER BY date",
    }
    for name, sql in tables.items():
        path = out / f"pod_moves_{args.quarter}_{name}.csv"
        con.execute(f"COPY ({sql}) TO '{path}' (HEADER)")
        print(f"\n{path}")
        print(con.execute(sql).df().to_string(index=False))

    path = out / f"pod_model_days_{args.quarter}.csv"
    con.execute(f"""
        COPY (SELECT model, pod_key, count(DISTINCT serial) AS drives, count(*) AS drive_days,
                     sum((failure = '1')::INT) AS failures
              FROM d WHERE pod_key IS NOT NULL GROUP BY model, pod_key ORDER BY model, pod_key)
        TO '{path}' (HEADER)""")
    print(f"\n{path}")
    con.close()
    db.unlink()


if __name__ == "__main__":
    main()
