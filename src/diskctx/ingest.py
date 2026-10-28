"""Read one day CSV with an explicit schema, and pass 2: filtered zstd Parquet per quarter.

Reading a day (shared with the audit):
- DuckDB reads the CSV with every column as text (all_varchar), so nothing is guessed.
- Only the needed columns are kept. Empty strings become NULL. ID columns stay strings
  (zero-padded: '031', '0028'). SMART raw values, capacity and failure go through
  TRY_CAST (BIGINT / INTEGER); the audit counts values that the cast turned to NULL.
- Date: every row's `date` is the day in the file's name (YYYY-MM-DD.csv), the snapshot
  day. A few files write dates as M/D/YY (2018-02-25.csv, 2019-06-17.csv), so a row's
  own date is never trusted. It is still parsed (YYYY-MM-DD or M/D/YY only) and the
  audit counts rows whose own date cannot be read or is not the file's day.
- A needed column missing from the file becomes a typed NULL, never an error. Because
  each day is projected to this fixed schema, quarters with different columns line up
  without union_by_name.
- Row rule: a row with no SMART values at all (any smart_* column) is a boot drive and
  is dropped. Whole boot *models* are found by the audit (see audit.py) and are never
  selected, so pass 2 never writes them.
- pod_key = datacenter|cluster_id|vault_id|pod_id (pod_id alone is not unique);
  NULL if any part is missing. The day view also has vault_key = datacenter|cluster_id|
  vault_id (used by the audit's pod-day table; not written to the Parquet).
"""

import csv
import re
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from diskctx.rawzip import day_members, extract_member
from diskctx.sources import local_zip
from diskctx.status import check_budget, log_download
from diskctx.storage import DURABLE, Stage

ID_COLUMNS = ["serial_number", "model", "datacenter", "cluster_id", "vault_id", "pod_id", "pod_slot_num"]
POD_FIELDS = ["datacenter", "cluster_id", "vault_id", "pod_id"]
CAST_TYPES = {"capacity_bytes": "BIGINT", "failure": "INTEGER"}
DAY_TEXT_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
# A row's own date: YYYY-MM-DD or M/D/YY, nothing else.
ROW_DATE = "coalesce(try_strptime(\"date\", '%Y-%m-%d'), try_strptime(\"date\", '%m/%d/%y'))::DATE"


def smart_columns(smart_ids):
    return [f"smart_{i}_raw" for i in smart_ids]


def cast_types(smart_ids):
    """{column: SQL type} for every needed column that goes through TRY_CAST."""
    return {**CAST_TYPES, **{c: "BIGINT" for c in smart_columns(smart_ids)}}


def input_columns(smart_ids):
    """Columns read from a day CSV (when present)."""
    return ["date", "serial_number", "model", "capacity_bytes", "failure",
            *POD_FIELDS, "pod_slot_num", *smart_columns(smart_ids)]


def output_columns(smart_ids):
    return ["date", "serial_number", "model", "capacity_bytes", "failure",
            *POD_FIELDS, "pod_slot_num", "pod_key", *smart_columns(smart_ids), "data_source"]


def arrow_schema(smart_ids):
    types = {"date": pa.date32(), "capacity_bytes": pa.int64(), "failure": pa.int32()}
    types.update({c: pa.int64() for c in smart_columns(smart_ids)})
    return pa.schema([(c, types.get(c, pa.string())) for c in output_columns(smart_ids)])


def q(col):
    """Quote a column name for SQL."""
    return '"' + col.replace('"', '""') + '"'


def clean(col):
    """Text value with empty / blank strings turned into NULL."""
    return f"NULLIF(TRIM({q(col)}), '')"


def read_header(csv_path):
    with open(csv_path, newline="") as f:
        return next(csv.reader(f))


def load_day(con, csv_path, smart_ids, day):
    """Load one day CSV into DuckDB. Returns the needed columns present in the file.
    `day` = the file name's date ('2026-01-01'); it becomes every row's date.

    Creates two things in `con`:
      raw_day  table: needed columns as cleaned text, plus no_smart (row has no SMART value)
      day      view:  the same rows with the explicit schema (missing column -> typed NULL)
    The CSV is parsed once; every later query reads the small raw_day table.
    """
    if not DAY_TEXT_RE.fullmatch(day):
        raise ValueError(f"day must look like 2026-01-01, got {day!r}")
    header = read_header(csv_path)
    present = [c for c in input_columns(smart_ids) if c in header]
    all_smart = [c for c in header if c.startswith("smart_")]
    no_smart = f"COALESCE({', '.join(clean(c) for c in all_smart)}) IS NULL" if all_smart else "TRUE"
    path = str(csv_path).replace("'", "''")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE raw_day AS
        SELECT {', '.join(f'{clean(c)} AS {q(c)}' for c in present)}, {no_smart} AS no_smart
        FROM read_csv('{path}', header = true, all_varchar = true)
    """)

    types = cast_types(smart_ids)

    def typed(col):
        if col == "date":
            return f"DATE '{day}'"
        if col not in present:
            return f"CAST(NULL AS {types.get(col, 'VARCHAR')})"
        if col in types:
            return f"TRY_CAST({q(col)} AS {types[col]})"
        return q(col)

    pod_key = " || '|' || ".join(typed(c) for c in POD_FIELDS)  # NULL if any part is NULL
    vault_key = " || '|' || ".join(typed(c) for c in POD_FIELDS[:3])
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW day AS
        SELECT {', '.join(f'{typed(c)} AS {q(c)}' for c in input_columns(smart_ids))},
               {pod_key} AS pod_key, {vault_key} AS vault_key, no_smart
        FROM raw_day
    """)
    return present


def date_checks(con, present, day):
    """(rows whose own date cannot be read, rows whose own date is not the file's day)
    for the loaded day. A file without a date column counts every row as unreadable."""
    if "date" not in present:
        return con.execute("SELECT count(*) FROM raw_day").fetchone()[0], 0
    return con.execute(f"""
        SELECT count(*) FILTER ({ROW_DATE} IS NULL),
               count(*) FILTER ({ROW_DATE} <> DATE '{day}')
        FROM raw_day""").fetchone()


def day_table(con, smart_ids, data_source, models=None):
    """Rows of the loaded day (boot rows dropped, optionally only `models`) as an Arrow
    table with the fixed output schema."""
    where = "NOT no_smart"
    params = []
    if models is not None:
        where += " AND list_contains(?::VARCHAR[], model)"
        params.append(list(models))
    cols = [c for c in output_columns(smart_ids) if c != "data_source"]
    sql = f"SELECT {', '.join(q(c) for c in cols)}, ? AS data_source FROM day WHERE {where}"
    table = con.execute(sql, [data_source, *params]).to_arrow_table()
    return table.cast(arrow_schema(smart_ids))


def read_day(csv_path, smart_ids, data_source, models=None, day=None):
    """One day CSV -> Arrow table (explicit schema, boot rows dropped). `day` defaults to
    the date in the file's name."""
    if day is None:
        found = DAY_TEXT_RE.search(Path(csv_path).name)
        if not found:
            raise ValueError(f"no YYYY-MM-DD in {Path(csv_path).name}; pass day=")
        day = found.group(0)
    con = duckdb.connect()
    load_day(con, csv_path, smart_ids, day)
    return day_table(con, smart_ids, data_source, models)


def ingest_quarter(zip_path, out_path, scratch, smart_ids, models, data_source):
    """Pass 2 for one quarter: every day CSV in the zip, one at a time -> one zstd
    Parquet (one row group per day). Returns the largest day CSV's bytes."""
    con = duckdb.connect()
    day_csv = Path(scratch) / "day.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    largest = 0
    with pq.ParquetWriter(out_path, arrow_schema(smart_ids), compression="zstd") as writer:
        for day, member in day_members(zip_path):
            largest = max(largest, extract_member(zip_path, member, day_csv))
            load_day(con, day_csv, smart_ids, day)
            writer.write_table(day_table(con, smart_ids, data_source, models))
            day_csv.unlink()
    return largest


def ingest_stages(cfg, sources, models, quarters, fetch=None, keep_zips=False):
    """One pass-2 stage per quarter in `quarters` that has a zip.

    The hash covers the resolved model list, the quarter, its URL and the column config,
    not select's hash: extending the range reruns ingest only if the chosen models change.
    """
    stages = []
    for row in sources:
        quarter, url = row["quarter"], row["url"]
        if row["kind"] == "missing" or quarter not in quarters:
            continue
        out = f"parquet/{quarter}.parquet"

        def build(work, storage, quarter=quarter, url=url, out=out):
            check_budget(storage, quarter, cfg["budget"])
            zip_path = local_zip(storage, url, fetch)
            largest = ingest_quarter(zip_path, work / out, work, cfg["smart_ids"], models,
                                     storage.data_source)
            log_download(storage, quarter, zip_path.stat().st_size, largest, (work / out).stat().st_size)
            if not keep_zips:
                zip_path.unlink()

        stages.append(Stage(
            name=f"ingest_{quarter}",
            tier=DURABLE,
            outputs=[out],
            build=build,
            keys=["data_source", "smart_ids"],
            params={"quarter": quarter, "url": url, "models": sorted(models)},
        ))
    return stages
