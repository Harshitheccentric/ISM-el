"""Features (M4): groups S, W, N, P for the drive-days the splits select.

Timing rule, the same for every feature: a feature at drive-day t uses only rows dated
<= t-1 (labels cover (t, t+N]). A *reading* of a column is a row of the drive where that
column is not null. Every feature is computed as of the drive's last reading a <= t-1 of
its column and carried forward, so a silent drive keeps its last values. Missingness is
never a feature (no "is null" or "days since" columns; XGBoost learns from NaN).
  value     the column at a.
  delta_k   per-day change over k days: (x_a - x_b) / (a - b) in calendar days, where b is
            the last reading <= a - k, else the drive's earliest reading. Null only if the
            drive has no earlier reading. 241/242 never fall: a fall is a counter reset,
            so the delta is null and the reset is counted. Error counters keep their sign
            (197, pending sectors, falls when sectors are remapped).
  rate      per-day change since the previous reading (241/242); max_k = the largest rate
            among the readings in (a - k, a].

Groups (GROUPS, a registry). Each maps the drive's as-of table (plus, for P, the pod
tables joined on its pod_key) to {feature name: SQL expression}:
  S  raw SMART values the model reports (non-null share >= min_coverage over the study's
     train + val windows, from the audit's days.csv) and 7/30-day deltas of the error
     counters.
  W  daily TB written/read (delta 241/242 x 512 B / calendar days), the same / capacity,
     7/30-day mean and max, recent (7-day) / lifetime average, age from SMART 9 power-on
     hours, lifetime fills (241 x 512 B / capacity) and the fill-phase flag.
  N  error-counter growth per TB written, and per TB of all workload (written + read),
     over 7/30 days. Every workload denominator is floored at n_floor_tb_per_day.
  P  Study P only. The drive's pod_key as of its last reading <= t-1 (its location as of
     yesterday): pod means and temperature (pod_days), slot; failures in its pod, vault
     and cluster (datacenter|cluster_id) over [t-30, t-1] without its own events; the
     expected count from the units' model mix (pod_models x per-model base rates from
     Study P's train window only), without its own drive-days; and the smoothed excess
     (observed + s) / (expected + s).

Processing: per model, quarter by quarter, in one DuckDB connection with a memory limit
(config `resources`). Each quarter reads its own rows plus the previous quarter's last
`warmup` days (the longest window), and two carried rows per drive and column: the last
reading before the warm-up (the reference across long gaps) and the as-of row of the last
reading before the quarter (carried forward while the drive is silent). So the result
equals one pass over the whole history. Warm-up rows are read, never stored.

Rows: the drive-days of splits.split_rows for every horizon (in_N says which), with
fail_date, y_N and sample_weight. Train only: every row within near_failure_days of a
failure is kept (weight 1); the others are kept if a seeded hash of (serial, date) < r
(weight 1/r). r is set per study so its stored train set has at most max_rows rows. Val and
test are complete (weight 1). The test split's features are stored, but no statistic that
uses labels is computed on it here.

Stages -> features/ (durable)
  features_sampling  sampling.csv: rows per study, model and split, train near-failure /
                     other rows and r per study (counts only; train labels only)
  features_rates     P/base_rates.csv: per-model failures / drive-days over Study P's
                     train window, plus the pooled fleet rate
  features_<model>   <study>/<model>/{train,val,test}.parquet and columns.json
"""

import csv
import json
import time
from dataclasses import dataclass, field
from datetime import timedelta

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from diskctx.config import FEATURE_SMART, SPLITS, parse_date, previous_quarter, quarter_days, quarters_between
from diskctx.ingest import model_dir, parquet_path
from diskctx.labels import KEY, label_rows, read_drives, y_col
from diskctx.splits import WINDOW_SCHEMA, last_records, read_study, split_rows, used
from diskctx.status import BudgetError, free_bytes, gb, same_disk
from diskctx.storage import DURABLE, Stage

LBA_IDS = (241, 242)            # LBAs written, read
TB = 1e12
UNITS = ("pod", "vault", "cluster")
POOLED = "(pooled)"             # base-rate row for models without train drive-days
SAMPLING_FILE = "features/sampling.csv"
RATES_FILE = "features/P/base_rates.csv"
SAMPLING_COLUMNS = ["study", "model", "split", "rows", "near_failure_rows", "other_rows", "r", "data_source"]


# --- the registry -------------------------------------------------------------------------


@dataclass
class Spec:
    smart_set: list     # SMART IDs this study and model use (S)
    settings: dict      # config `features`


def floored(expr, floor):
    """max(expr, floor), but null stays null (DuckDB's greatest() skips nulls)."""
    return f"(CASE WHEN ({expr}) IS NULL THEN NULL ELSE greatest({expr}, {floor}) END)"


def s_group(spec):
    cols = {}
    for i in spec.smart_set:
        cols[f"s_smart_{i}"] = f"smart_{i}"
        if i in spec.settings["error_counters"]:
            for k in spec.settings["windows"]:
                cols[f"s_smart_{i}_d{k}"] = f"smart_{i}_d{k}"
    return cols


def w_group(spec):
    s = spec.settings
    lba, floor, windows = s["lba_bytes"], s["n_floor_tb_per_day"], s["windows"]
    tb = lba / TB                                   # TB per LBA
    age = "smart_9 / 24.0"                          # power-on hours -> days
    pairs = (("w", 241), ("r", 242))
    cols = {}
    for name, i in pairs:
        cols[f"w_tb{name}_1d"] = f"smart_{i}_r1 * {tb}"
    for name, i in pairs:
        cols[f"w_fill{name}_1d"] = f"smart_{i}_r1 * {lba} / cap"
    for k in windows:
        for name, i in pairs:
            cols[f"w_tb{name}_mean{k}"] = f"smart_{i}_d{k} * {tb}"
    for k in windows:
        for name, i in pairs:
            cols[f"w_tb{name}_max{k}"] = f"smart_{i}_max{k} * {tb}"
    for name, i in pairs:
        lifetime = f"smart_{i} * {tb} / {floored(age, 1)}"           # TB per day of age
        cols[f"w_tb{name}_recent_ratio"] = f"smart_{i}_d{min(windows)} * {tb} / {floored(lifetime, floor)}"
    fills = f"smart_241 * {lba} / cap"
    cols["w_age_days"] = age
    cols["w_lifetime_fills"] = fills
    cols["w_fill_phase"] = (f"CASE WHEN {fills} IS NULL THEN NULL "
                            f"WHEN {fills} < {s['fill_phase_max_fills']} THEN 1 ELSE 0 END")
    return cols


def n_group(spec):
    s = spec.settings
    tb, floor = s["lba_bytes"] / TB, s["n_floor_tb_per_day"]
    cols = {}
    for i in spec.smart_set:
        if i not in s["error_counters"]:
            continue
        for k in s["windows"]:
            written = floored(f"smart_241_d{k} * {tb}", floor)
            workload = floored(f"(smart_241_d{k} + smart_242_d{k}) * {tb}", floor)
            cols[f"n_{i}_per_tbw{k}"] = f"smart_{i}_d{k} / {written}"
            cols[f"n_{i}_per_tb{k}"] = f"smart_{i}_d{k} / {workload}"
    return cols


def p_group(spec):
    w, smooth = spec.settings["pod_window_days"], spec.settings["excess_smoothing"]
    cols = {"p_pod_tbw_mean": "pod_tbw_mean", "p_pod_tbr_mean": "pod_tbr_mean",
            "p_pod_temp_mean": "pod_temp_mean", "p_temp_minus_pod": "smart_194 - pod_temp_mean",
            "p_slot": "slot"}
    observed = {u: f"(obs_{u} - own_fail_{u})" for u in UNITS}
    expected = {u: f"(CASE WHEN exp_{u} IS NULL THEN NULL ELSE greatest(exp_{u} - own_days * own_rate, 0) END)"
                for u in UNITS}
    for u in UNITS:
        cols[f"p_fail_{u}{w}"] = observed[u]
    for u in UNITS:
        cols[f"p_expected_{u}{w}"] = expected[u]
    for u in UNITS:
        cols[f"p_excess_{u}{w}"] = f"({observed[u]} + {smooth}) / ({expected[u]} + {smooth})"
    return cols


GROUPS = {"S": s_group, "W": w_group, "N": n_group, "P": p_group}


def study_columns(study, groups, smart_set, settings):
    """{feature name: SQL} for a study's groups, in registry order. Study W has no pod
    context, so it refuses P."""
    if study == "W" and "P" in groups:
        raise ValueError("study W has no pod features: it cannot use group P")
    unknown = [g for g in groups if g not in GROUPS]
    if unknown:
        raise ValueError(f"unknown feature groups {unknown}")
    spec = Spec(sorted(smart_set), settings)
    cols = {}
    for name, group in GROUPS.items():
        if name in groups:
            cols.update(group(spec))
    return cols


def smart_set(days, model, start, end, ids, min_coverage):
    """(IDs with non-null share >= min_coverage over the model's rows from start to end,
    {id: share}). days: the audit's days.csv rows (model, date, rows, nonnull_smart_<i>_raw)."""
    d = days.filter((pl.col("model") == model) & pl.col("date").is_between(start, end))
    rows = d["rows"].sum()
    shares = {i: (d[f"nonnull_smart_{i}_raw"].sum() / rows if rows else 0.0) for i in ids}
    return [i for i in ids if shares[i] >= min_coverage], shares


def base_rates(pod_models, start, end):
    """Per model failures / drive-days over [start, end] (Study P's train window), from the
    audit's pod_models (all non-boot models), plus a POOLED row for the whole fleet."""
    m = (pod_models.filter(pl.col("date").is_between(start, end))
         .group_by("model").agg(pl.col("drives").sum().alias("drive_days"), pl.col("failures").sum())
         .sort("model"))
    pooled = pl.DataFrame({"model": [POOLED], "drive_days": [m["drive_days"].sum()],
                           "failures": [m["failures"].sum()]}, schema=m.schema)
    out = pl.concat([m, pooled])
    return out.with_columns((pl.col("failures") / pl.col("drive_days")).alias("rate"))


def rate_of(rates, model):
    """The model's base rate, or the pooled rate if it has no train drive-days."""
    row = rates.filter((pl.col("model") == model) & (pl.col("drive_days") > 0))
    if row.is_empty():
        row = rates.filter(pl.col("model") == POOLED)
    return float(row["rate"][0] or 0.0)


# --- one period of one model, in DuckDB ---------------------------------------------------


def value_columns(settings):
    """{short name: expression on a pass-2 row}: the columns readings are taken of."""
    ids = sorted(set(settings["s_smart_ids"]) | set(FEATURE_SMART))
    return {"cap": "capacity_bytes", "slot": "TRY_CAST(pod_slot_num AS BIGINT)",
            **{f"smart_{i}": f"smart_{i}_raw" for i in ids}}


def delta_names(settings):
    return [f"smart_{i}" for i in (*settings["error_counters"], *LBA_IDS)]


def warmup_days(settings):
    """Days of history before a period that its windows can reach back to."""
    return max(*settings["windows"], settings["pod_window_days"])


def sql_list(names):
    return ", ".join(f"'{n}'" for n in names)


def vault_of(pod_key):
    return f"split_part({pod_key}, '|', 1) || '|' || split_part({pod_key}, '|', 2) || '|' || split_part({pod_key}, '|', 3)"


def cluster_of(pod_key):
    return f"split_part({pod_key}, '|', 1) || '|' || split_part({pod_key}, '|', 2)"


def connect(resources, temp_dir=None):
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{resources['duckdb_memory_limit']}'")
    con.execute(f"SET threads = {int(resources['duckdb_threads'])}")
    con.execute("SET preserve_insertion_order = false")         # every output has ORDER BY
    if temp_dir is not None:
        con.execute(f"SET temp_directory = '{temp_dir}'")
    return con


def new_state(con, settings):
    """Empty carried rows: carry_ref (last reading before the next warm-up), carry_anchor
    (as-of row of the last reading before the next period) and carry_pod (last pod_key)."""
    windows = settings["windows"]
    extra = ", ".join(["r1 DOUBLE", "reset BOOLEAN", *[f"d{k} DOUBLE" for k in windows],
                       *[f"max{k} DOUBLE" for k in windows]])
    con.execute("CREATE OR REPLACE TABLE carry_ref (serial_number VARCHAR, col VARCHAR, date DATE, x BIGINT)")
    con.execute(f"CREATE OR REPLACE TABLE carry_anchor (serial_number VARCHAR, col VARCHAR, date DATE, x BIGINT, "
                f"{extra})")
    con.execute("CREATE OR REPLACE TABLE carry_pod (serial_number VARCHAR, date DATE, pod_key VARCHAR)")


def run_period(con, settings, start, end, model=None, own_rate=None):
    """Features for the period [start, end] of one model.

    Needs the tables `frame` (the model's rows dated start - warmup .. end) and `want`
    (serial_number, date: the drive-days to compute, dated start .. end). If own_rate is
    given, also the pod tables pod_days, pod_models, events and rates (P). Creates `feat`
    (one row per wanted drive-day: the as-of inputs every group's SQL reads) and updates
    the carried rows for the next period, which must start at end + 1.
    Returns {column: counter resets on readings dated start .. end} for 241/242."""
    w = warmup_days(settings)
    windows = settings["windows"]
    next_warm = end + timedelta(days=1 - w)
    cols = value_columns(settings)
    lba = sql_list(f"smart_{i}" for i in LBA_IDS)

    # 1. readings: one row per drive, column and day that has a value
    unpivot = ", ".join(f"({expr})::BIGINT AS {name}" for name, expr in cols.items())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE readings AS
        SELECT serial_number, col, date, x FROM carry_ref
        UNION ALL
        SELECT serial_number, col, date, x FROM (
            UNPIVOT (SELECT serial_number, date, {unpivot} FROM frame)
            ON {', '.join(cols)} INTO NAME col VALUE x)
    """)

    # 2. the as-of row at every reading: value, rate since the previous reading, deltas, maxima.
    # Only counters get the window columns; the others (capacity, slot, SMART 9, 194) need x.
    deltas = delta_names(settings)
    refs = ", ".join(f"last_value(date) OVER ref{k} AS ref{k}_date, last_value(x) OVER ref{k} AS ref{k}_x"
                     for k in windows)
    ref_windows = ", ".join(f"ref{k} AS (PARTITION BY serial_number, col ORDER BY date "
                            f"RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL {k} DAY PRECEDING)" for k in windows)
    raw = ", ".join(f"(x - coalesce(ref{k}_x, first_x)) / nullif(date - coalesce(ref{k}_date, first_date), 0) "
                    f"AS d{k}_raw" for k in windows)
    clean = ", ".join(f"CASE WHEN col IN ({lba}) AND d{k}_raw < 0 THEN NULL ELSE d{k}_raw END AS d{k}"
                      for k in windows)
    maxes = ", ".join(f"max(r1) OVER (PARTITION BY serial_number, col ORDER BY date "
                      f"RANGE BETWEEN INTERVAL {k - 1} DAY PRECEDING AND CURRENT ROW) AS max{k}" for k in windows)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE anchored AS
        WITH w AS (
            SELECT *, lag(date) OVER drive AS prev_date, lag(x) OVER drive AS prev_x,
                   first_value(date) OVER drive AS first_date, first_value(x) OVER drive AS first_x,
                   {refs}
            FROM readings WHERE col IN ({sql_list(deltas)})
            WINDOW drive AS (PARTITION BY serial_number, col ORDER BY date), {ref_windows}),
        d AS (
            SELECT serial_number, col, date, x, (x - prev_x) / (date - prev_date) AS r1_raw, {raw}
            FROM w),
        c AS (
            SELECT serial_number, col, date, x,
                   CASE WHEN col IN ({lba}) AND r1_raw < 0 THEN NULL ELSE r1_raw END AS r1,
                   col IN ({lba}) AND coalesce(r1_raw < 0, false) AS reset,
                   {clean}
            FROM d)
        SELECT *, {maxes} FROM c
        UNION ALL BY NAME
        SELECT serial_number, col, date, x, false AS reset FROM readings WHERE col NOT IN ({sql_list(deltas)})
    """)
    resets = dict(con.execute(f"""
        SELECT col, count(*) FILTER (reset) FROM anchored
        WHERE date >= DATE '{start}' AND col IN ({lba}) GROUP BY col""").fetchall())

    # Readings before the period: only the carried as-of row is exact (it was computed in
    # its own period), so it replaces the warm-up rows.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE anchor AS
        SELECT * FROM anchored WHERE date >= DATE '{start}'
        UNION ALL BY NAME SELECT * FROM carry_anchor""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pods_seen AS
        SELECT * FROM carry_pod
        UNION ALL BY NAME
        SELECT serial_number, date, pod_key FROM frame WHERE pod_key IS NOT NULL AND date >= DATE '{start}'""")

    # 3. for each wanted drive-day t: the as-of row of the last reading before t
    rates = [f"smart_{i}" for i in LBA_IDS]
    picks, joins = [], []
    for name in cols:
        fields = {name: "x"}
        if name in deltas:
            fields.update({f"{name}_d{k}": f"d{k}" for k in windows})
        if name in rates:
            fields.update({f"{name}_r1": "r1", **{f"{name}_max{k}": f"max{k}" for k in windows}})
        alias = f"a_{name}"
        sub = ", ".join(f"{src} AS {out}" for out, src in fields.items())
        joins.append(f"ASOF LEFT JOIN (SELECT serial_number, date, {sub} FROM anchor WHERE col = '{name}') {alias} "
                     f"ON w.serial_number = {alias}.serial_number AND w.date > {alias}.date")
        picks += [f"{alias}.{out}" for out in fields]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE latest AS
        SELECT w.serial_number, w.date, {', '.join(picks)}, pk.pod_key
        FROM want w
        {' '.join(joins)}
        ASOF LEFT JOIN pods_seen pk ON w.serial_number = pk.serial_number AND w.date > pk.date
    """)
    if own_rate is None:
        con.execute("CREATE OR REPLACE TEMP TABLE feat AS SELECT * FROM latest")
    else:
        pod_features(con, settings, own_rate)

    # 4. carry to the next period
    con.execute(f"""
        CREATE OR REPLACE TABLE carry_ref AS SELECT * FROM readings WHERE date < DATE '{next_warm}'
        QUALIFY row_number() OVER (PARTITION BY serial_number, col ORDER BY date DESC) = 1""")
    con.execute("""
        CREATE OR REPLACE TABLE carry_anchor AS SELECT * FROM anchor
        QUALIFY row_number() OVER (PARTITION BY serial_number, col ORDER BY date DESC) = 1""")
    con.execute("""
        CREATE OR REPLACE TABLE carry_pod AS SELECT * FROM pods_seen
        QUALIFY row_number() OVER (PARTITION BY serial_number ORDER BY date DESC) = 1""")
    return {f"{c}_raw": int(resets.get(c, 0)) for c in rates}


def pod_features(con, settings, own_rate):
    """P inputs for every row of `latest` -> `feat`. Counts over [t-W, t-1] (a unit's
    failures and expected failures, the drive's own days) are a difference of cumulative
    sums: the last day <= t-1 minus the last day <= t-W-1. So days missing from a table
    cannot shift the window, and nothing dated t is read."""
    w = settings["pod_window_days"]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE unit_cum AS
        WITH m AS (
            SELECT p.date, p.pod_key, p.failures,
                   p.drives * coalesce(r.rate, (SELECT rate FROM rates WHERE model = '{POOLED}')) AS expected
            FROM pod_models p LEFT JOIN (SELECT * FROM rates WHERE drive_days > 0) r USING (model)),
        u AS (
            SELECT 'pod' AS level, pod_key AS unit, date, failures, expected FROM m
            UNION ALL SELECT 'vault', {vault_of('pod_key')}, date, failures, expected FROM m
            UNION ALL SELECT 'cluster', {cluster_of('pod_key')}, date, failures, expected FROM m),
        d AS (SELECT level, unit, date, sum(failures) AS failures, sum(expected) AS expected FROM u GROUP BY ALL)
        SELECT level, unit, date,
               sum(failures) OVER c AS cum_failures, sum(expected) OVER c AS cum_expected
        FROM d WINDOW c AS (PARTITION BY level, unit ORDER BY date ROWS UNBOUNDED PRECEDING)
    """)
    counts, joins = [], []
    for u in UNITS:
        for end, cond in (("c1", f"a.date > {end_alias(u, 'c1')}.date"),
                          ("c0", f"a.date_w > {end_alias(u, 'c0')}.date")):
            alias = end_alias(u, end)
            joins.append(f"ASOF LEFT JOIN (SELECT * FROM unit_cum WHERE level = '{u}') {alias} "
                         f"ON a.{u} = {alias}.unit AND {cond}")
        c1, c0 = end_alias(u, "c1"), end_alias(u, "c0")
        counts += [f"{c1}.cum_failures - coalesce({c0}.cum_failures, 0) AS obs_{u}",
                   f"{c1}.cum_expected - coalesce({c0}.cum_expected, 0) AS exp_{u}"]
    own = ", ".join(f"count(*) FILTER (WHERE {unit_expr(u, 'e.pod_key')} = a.{u}) AS own_fail_{u}" for u in UNITS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE feat AS
        WITH a AS (
            SELECT *, pod_key AS pod, {vault_of('pod_key')} AS vault, {cluster_of('pod_key')} AS cluster,
                   date - {w} AS date_w
            FROM latest),
        own AS (
            SELECT a.serial_number, a.date, {own}
            FROM a JOIN events e ON e.serial_number = a.serial_number
                               AND e.date BETWEEN a.date - {w} AND a.date - 1
            GROUP BY ALL),
        drive_cum AS (
            SELECT serial_number, date,
                   count(pod_key) OVER (PARTITION BY serial_number ORDER BY date ROWS UNBOUNDED PRECEDING) AS cum_days
            FROM frame),
        pod AS (
            SELECT a.serial_number, a.date, pd.tb_written_mean AS pod_tbw_mean, pd.tb_read_mean AS pod_tbr_mean,
                   pd.temp_mean AS pod_temp_mean, {', '.join(counts)},
                   d1.cum_days - coalesce(d0.cum_days, 0) AS own_days
            FROM a
            ASOF LEFT JOIN pod_days pd ON a.pod = pd.pod_key AND a.date > pd.date
            ASOF LEFT JOIN drive_cum d1 ON a.serial_number = d1.serial_number AND a.date > d1.date
            ASOF LEFT JOIN drive_cum d0 ON a.serial_number = d0.serial_number AND a.date_w > d0.date
            {' '.join(joins)})
        SELECT latest.*, pod.* EXCLUDE (serial_number, date, own_days),
               {', '.join(f'coalesce(own.own_fail_{u}, 0) AS own_fail_{u}' for u in UNITS)},
               coalesce(pod.own_days, 0) AS own_days, {own_rate}::DOUBLE AS own_rate
        FROM latest
        LEFT JOIN pod USING (serial_number, date)
        LEFT JOIN own USING (serial_number, date)
    """)


def end_alias(unit, end):
    return f"{end}_{unit}"


def unit_expr(unit, pod_key):
    return {"pod": pod_key, "vault": vault_of(pod_key), "cluster": cluster_of(pod_key)}[unit]


POD_TABLES = {
    "pod_days": {"date": pl.Date, "pod_key": pl.Utf8, "temp_mean": pl.Float64, "tb_written_mean": pl.Float64,
                 "tb_read_mean": pl.Float64},
    "pod_models": {"date": pl.Date, "pod_key": pl.Utf8, "model": pl.Utf8, "drives": pl.Int64, "failures": pl.Int64},
    "events": {"serial_number": pl.Utf8, "date": pl.Date, "pod_key": pl.Utf8},
    "rates": {"model": pl.Utf8, "drive_days": pl.Int64, "failures": pl.Int64, "rate": pl.Float64},
}


def register(con, name, frame):
    """A DuckDB table `name` from a polars frame."""
    con.register("_src", frame.to_arrow())
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM _src")
    con.unregister("_src")


def feature_sql(columns):
    return ", ".join(f"CAST({expr} AS FLOAT) AS {name}" for name, expr in columns.items())


def compute_features(rows, want, settings, columns, resources, pods=None, model=None, periods=None):
    """Features `columns` ({name: SQL}) for the drive-days `want` (serial_number, date),
    from one model's drive-day `rows` (pass-2 columns). pods: {pod_days, pod_models, events,
    rates} polars frames for P. periods: [(start, end), ...], contiguous, default one period
    over all dates; the stage uses quarters. Returns (DataFrame sorted by serial and date,
    {column: resets}). Used by the tests; the stage runs the same run_period."""
    if periods is None:
        dates = pl.concat([rows["date"], want["date"]])
        periods = [(dates.min(), dates.max())]
    w = warmup_days(settings)
    con = connect(resources)
    new_state(con, settings)
    own_rate = rate_of(pods["rates"], model) if pods is not None else None
    if pods is not None:
        register(con, "rates", pods["rates"].select(list(POD_TABLES["rates"])).cast(POD_TABLES["rates"]))
    out, resets = [], {}
    for start, end in periods:
        register(con, "frame", rows.filter(pl.col("date").is_between(start - timedelta(days=w), end)))
        register(con, "want", want.select("serial_number", "date")
                 .filter(pl.col("date").is_between(start, end)).unique())
        if pods is not None:            # as the stage loads them: from start - warmup - 1 to end
            since = start - timedelta(days=w + 1)
            for name in ("pod_days", "pod_models", "events"):
                schema = POD_TABLES[name]
                register(con, name, pods[name].select(list(schema)).cast(schema)
                         .filter(pl.col("date").is_between(since, end)))
        for col, n in run_period(con, settings, start, end, model, own_rate).items():
            resets[col] = resets.get(col, 0) + n
        out.append(con.execute(f"SELECT serial_number, date, {feature_sql(columns)} FROM feat "
                               f"ORDER BY serial_number, date").pl())
    return pl.concat(out).sort("serial_number", "date"), resets


# --- samples ------------------------------------------------------------------------------


@dataclass
class StudyInfo:
    name: str
    groups: list
    windows: pl.DataFrame          # this model's train/val/test windows
    assignment: pl.DataFrame       # this model's drives
    last_seen: dict = field(default_factory=dict)   # split -> last record <= window end


def study_infos(storage, cfg, model):
    """The studies `model` is in, with its windows and drives."""
    infos = []
    for name, study in cfg["studies"].items():
        windows, assignment = read_study(storage, name)
        w = used(windows).filter(pl.col("model") == model)
        if not w.is_empty():
            infos.append(StudyInfo(name, study["groups"], w, assignment.filter(pl.col("model") == model)))
    return infos


def flag_union(frames, horizons):
    """(model, serial_number, date, in_N...) over the rows of split_rows for each horizon."""
    keys = [*KEY, "date"]
    tagged = [f.select(keys).with_columns(pl.lit(n).alias("n")) for f, n in zip(frames, horizons)]
    return (pl.concat(tagged).group_by(keys)
            .agg([(pl.col("n") == n).any().alias(f"in_{n}") for n in horizons]))


def quarter_samples(rows, drives, infos, horizons):
    """Sample rows of one quarter of one model: study, split, model, serial_number, date,
    fail_date, y_N, in_N. rows: (model, serial_number, date) of that quarter. None if empty."""
    labelled = label_rows(rows, drives, horizons)
    parts = []
    for info in infos:
        for split in SPLITS:
            sets = [split_rows(labelled, info.assignment, info.windows, split, n, "S", info.groups,
                               last_seen=info.last_seen[split]) for n in horizons]
            merged = flag_union(sets, horizons)
            if merged.height:
                parts.append(merged.with_columns(pl.lit(info.name).alias("study"), pl.lit(split).alias("split")))
    if not parts:
        return None
    labels = labelled.select(*KEY, "date", "fail_date", *[y_col(n) for n in horizons])
    return pl.concat(parts).join(labels, on=[*KEY, "date"], how="left")


def model_quarters(storage, cfg, model, infos):
    """[(quarter, path, previous quarter's path or None)] from the model's first non-empty
    Parquet file to the last quarter any of its windows reaches."""
    last_day = max(info.windows["end"].max() for info in infos)
    out, started = [], False
    for quarter in quarters_between(cfg["start_quarter"], cfg["end_quarter"]):
        if quarter_days(quarter)[0] > last_day:
            break
        path = storage.path(DURABLE, parquet_path(model, quarter))
        if not path.exists():
            continue
        started = started or pq.ParquetFile(path).metadata.num_rows > 0
        if started:
            prev = storage.path(DURABLE, parquet_path(model, previous_quarter(quarter)))
            out.append((quarter, path, prev if prev.exists() else None))
    return out


def prepare_model(storage, cfg, model):
    """(infos with last_seen, kept serials, the model's label drives, its quarters)."""
    infos = study_infos(storage, cfg, model)
    quarters = model_quarters(storage, cfg, model, infos)
    paths = [p for _, p, _ in quarters]
    scan = pl.scan_parquet(paths).select(*KEY, "date")
    for info in infos:
        info.last_seen = {split: last_records(scan, info.windows, split) for split in SPLITS}
    kept = pl.concat([i.assignment.filter(pl.col("keep")).select("serial_number") for i in infos]).unique()
    drives = read_drives(storage).filter(pl.col("model") == model)
    return infos, kept, drives, quarters


def iter_samples(quarters, infos, kept, drives, horizons):
    """(quarter, path, prev path, samples or None) per quarter of one model."""
    for quarter, path, prev in quarters:
        rows = (pl.read_parquet(path, columns=[*KEY, "date"])
                .join(kept, on="serial_number", how="semi"))
        yield quarter, path, prev, quarter_samples(rows, drives, infos, horizons)


def near_failure(near_days):
    return pl.col("fail_date").is_not_null() & ((pl.col("fail_date") - pl.col("date")).dt.total_days() <= near_days)


def row_hash_sql(seed, serial, day):
    """splits.hash_unit(seed, 'rows', serial, day) in DuckDB: the same sha256, as a number in [0, 1)."""
    text = f"concat_ws('|', '{seed}', 'rows', {serial}, CAST({day} AS VARCHAR))"
    return f"(('0x' || substr(sha256({text}), 1, 15))::BIGINT / {16 ** 15})"


def sample_r(near, other, max_rows):
    """The rate for a study's other train rows so near + r x other <= max_rows."""
    if other == 0:
        return 1.0
    r = min(1.0, (max_rows - near) / other)
    if r <= 0:
        raise ValueError(f"train rows within the near-failure window alone ({near}) reach max_rows ({max_rows}); "
                         f"raise features.train_sample.max_rows or lower near_failure_days")
    return r


# --- stages -------------------------------------------------------------------------------


def write_csv(path, rows, columns):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def sampling_stage(cfg, models):
    s = cfg["features"]["train_sample"]

    def build(work, storage):
        counts = {}
        for model in models:
            if not study_infos(storage, cfg, model):
                continue
            infos, kept, drives, quarters = prepare_model(storage, cfg, model)
            for _, _, _, samples in iter_samples(quarters, infos, kept, drives, cfg["horizons"]):
                if samples is None:
                    continue
                part = (samples.with_columns(near_failure(s["near_failure_days"]).alias("near"))
                        .group_by("study", "split")
                        .agg(pl.len().alias("rows"), pl.col("near").sum().alias("near")))
                for r in part.iter_rows(named=True):
                    c = counts.setdefault((r["study"], model, r["split"]), {"rows": 0, "near": 0})
                    c["rows"] += r["rows"]
                    c["near"] += r["near"]
        rows, r_by_study = [], {}
        for study in cfg["studies"]:
            train = [c for (st, _, sp), c in counts.items() if st == study and sp == "train"]
            near = sum(c["near"] for c in train)
            other = sum(c["rows"] - c["near"] for c in train)
            r_by_study[study] = sample_r(near, other, s["max_rows"]) if train else 1.0
        for (study, model, split), c in sorted(counts.items()):
            train = split == "train"                    # label-based counts: train only
            rows.append({"study": study, "model": model, "split": split, "rows": c["rows"],
                         "near_failure_rows": c["near"] if train else None,
                         "other_rows": c["rows"] - c["near"] if train else None,
                         "r": r_by_study[study] if train else None, "data_source": storage.data_source})
        (work / "features").mkdir(parents=True)
        write_csv(work / SAMPLING_FILE, rows, SAMPLING_COLUMNS)
        for study, r in r_by_study.items():
            train = [x for x in rows if x["study"] == study and x["split"] == "train"]
            near = sum(x["near_failure_rows"] for x in train)
            other = sum(x["other_rows"] for x in train)
            print(f"features sampling, study {study}: train {near + other} rows ({near} within "
                  f"{s['near_failure_days']} days of a failure), r = {r:.4f}, about {near + round(r * other)} stored",
                  flush=True)
        return {"r": r_by_study}

    return Stage(name="features_sampling", tier=DURABLE, outputs=[SAMPLING_FILE], build=build,
                 keys=["data_source", "seed", "features"], deps=["splits"], params={"models": sorted(models)})


def read_pod_models(storage, quarters):
    frames = []
    for quarter in quarters:
        path = storage.path(DURABLE, f"audit/quarters/{quarter}/pod_models.parquet")
        if not path.exists():
            raise FileNotFoundError(f"{path} is missing: the audit writes pod_models only for quarters from "
                                    f"Study P's start; rerun audit_{quarter}")
        frames.append(pl.read_parquet(path, columns=list(POD_TABLES["pod_models"])))
    return pl.concat(frames)


def rates_stage(cfg):
    start, end = (parse_date(d) for d in cfg["studies"]["P"]["train"])

    def build(work, storage):
        quarters = quarters_between(quarter_name(start), quarter_name(end))
        rates = base_rates(read_pod_models(storage, quarters), start, end)
        (work / "features" / "P").mkdir(parents=True)
        rates.with_columns(pl.lit(storage.data_source).alias("data_source")).write_csv(work / RATES_FILE)
        pooled = rates.filter(pl.col("model") == POOLED).row(0, named=True)
        print(f"features base rates (Study P train {start} - {end}): {rates.height - 1} models, pooled "
              f"{pooled['failures']} failures in {pooled['drive_days']} drive-days", flush=True)
        return {"models": rates.height - 1}

    return Stage(name="features_rates", tier=DURABLE, outputs=[RATES_FILE], build=build,
                 keys=["data_source"], deps=["splits"])


def read_rates(storage):
    return pl.read_csv(storage.path(DURABLE, RATES_FILE), schema_overrides=POD_TABLES["rates"])


def quarter_name(day):
    return f"{day.year}Q{(day.month - 1) // 3 + 1}"


def read_days(storage, start, end, model):
    """The audit's days.csv rows of `model` for the quarters from start to end."""
    frames = []
    for quarter in quarters_between(quarter_name(start), quarter_name(end)):
        path = storage.path(DURABLE, f"audit/quarters/{quarter}/days.csv")
        if path.exists():
            frames.append(pl.read_csv(path, try_parse_dates=True, infer_schema_length=0)
                          .filter(pl.col("model") == model))
    if not frames:
        return pl.DataFrame(schema={"model": pl.Utf8, "date": pl.Date, "rows": pl.Int64})
    days = pl.concat(frames)
    counts = [c for c in days.columns if c == "rows" or c.startswith("nonnull_")]
    return days.with_columns(pl.col("date").str.to_date(), *[pl.col(c).cast(pl.Int64) for c in counts])


def output_schema(horizons, names):
    return pa.schema([("model", pa.string()), ("serial_number", pa.string()), ("date", pa.date32()),
                      ("fail_date", pa.date32()), *[(y_col(n), pa.int8()) for n in horizons],
                      *[(f"in_{n}", pa.bool_()) for n in horizons], ("sample_weight", pa.float64()),
                      *[(n, pa.float32()) for n in names], ("data_source", pa.string())])


def measured_bytes_per_row(storage):
    """The largest bytes per stored row of any finished features_* stage (0 if none)."""
    best = 0.0
    for marker in storage.marker_dir.glob("features_*.json"):
        info = json.loads(marker.read_text()).get("info", {})
        if info.get("rows"):
            best = max(best, info["bytes"] / info["rows"])
    return best


def check_feature_budget(storage, model, rows, budget, local_free=None, root_free=None, shared_disk=None):
    """Raise BudgetError if `rows` feature rows of `model` would not fit (the files are built
    locally, then copied to ROOT). Returns the bytes needed."""
    per_row = max(budget["default_feature_bytes_per_row"], 1.5 * measured_bytes_per_row(storage))
    need = int(rows * per_row)
    margin = budget["margin_bytes"]
    local_free = free_bytes(storage.work_dir) if local_free is None else local_free
    root_free = free_bytes(storage.run_dir) if root_free is None else root_free
    shared = same_disk(storage.work_dir, storage.run_dir) if shared_disk is None else shared_disk
    need_local, need_root = (2 * need, 0) if shared else (need, need)
    if local_free < need_local + margin:
        raise BudgetError(f"Stopping before features of {model}: local disk has {gb(local_free)} free, needs "
                          f"{gb(need_local)} ({rows} rows x {per_row:.0f} B) plus {gb(margin)} margin.")
    if root_free < need_root + margin:
        raise BudgetError(f"Stopping before features of {model}: ROOT has {gb(root_free)} free, needs "
                          f"{gb(need_root)} ({rows} rows x {per_row:.0f} B) plus {gb(margin)} margin. "
                          f"Finished models are kept and will be skipped.")
    return need


def expected_rows(storage, model):
    """Stored rows of `model` from sampling.csv: all val/test rows, near + r x other on train."""
    total = 0
    with open(storage.path(DURABLE, SAMPLING_FILE), newline="") as f:
        for row in csv.DictReader(f):
            if row["model"] != model:
                continue
            if row["split"] == "train":
                total += int(row["near_failure_rows"]) + float(row["r"]) * int(row["other_rows"])
            else:
                total += int(row["rows"])
    return int(total)


def read_sampling_r(storage):
    """{study: r} from sampling.csv (1.0 for a study without train rows)."""
    out = {}
    with open(storage.path(DURABLE, SAMPLING_FILE), newline="") as f:
        for row in csv.DictReader(f):
            if row["split"] == "train":
                out[row["study"]] = float(row["r"])
    return out


def model_stage(cfg, model, studies):
    settings, horizons = cfg["features"], cfg["horizons"]
    folder = {study: f"features/{study}/{model_dir(model)}" for study in studies}

    def build(work, storage):
        started = time.time()
        infos, kept, drives, quarters = prepare_model(storage, cfg, model)
        rows_needed = expected_rows(storage, model)
        check_feature_budget(storage, model, rows_needed, cfg["budget"])
        r_by_study = read_sampling_r(storage)
        rates = read_rates(storage) if any(i.name == "P" for i in infos) else None

        columns, about = {}, {}
        for info in infos:
            start = info.windows.filter(pl.col("split") == "train")["start"][0]
            end = info.windows.filter(pl.col("split") == "val")["end"][0]
            ids, shares = smart_set(read_days(storage, start, end, model), model, start, end,
                                    settings["s_smart_ids"], settings["min_coverage"])
            columns[info.name] = study_columns(info.name, info.groups, ids, settings)
            spec = Spec(ids, settings)
            about[info.name] = {"study": info.name, "model": model, "smart_set": ids,
                                "coverage": {str(i): round(v, 4) for i, v in shares.items()},
                                "coverage_window": [str(start), str(end)],
                                "groups": {g: list(GROUPS[g](spec)) for g in info.groups},
                                "data_source": storage.data_source}

        tmp = storage.work_dir / "duckdb_tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        con = connect(cfg["resources"], tmp)
        new_state(con, settings)
        register(con, "kept", kept)
        own_rate = rate_of(rates, model) if rates is not None else None
        if rates is not None:
            register(con, "rates", rates.select(list(POD_TABLES["rates"])))
        smart = ", ".join(f"smart_{i}_raw" for i in sorted(set(settings["s_smart_ids"]) | set(FEATURE_SMART)))
        w = warmup_days(settings)

        writers, schemas, written = {}, {}, {}
        for info in infos:
            (work / folder[info.name]).mkdir(parents=True)
            for split in SPLITS:
                schema = output_schema(horizons, list(columns[info.name]))
                path = work / folder[info.name] / f"{split}.parquet"
                writers[(info.name, split)] = pq.ParquetWriter(path, schema, compression="zstd")
                schemas[(info.name, split)] = schema
                written[(info.name, split)] = 0
        resets = {}
        try:
            for quarter, path, prev, samples in iter_samples(quarters, infos, kept, drives, horizons):
                start, end = quarter_days(quarter)[0], quarter_days(quarter)[-1]
                files = ", ".join(f"'{p}'" for p in ([prev] if prev else []) + [path])
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE frame AS
                    SELECT serial_number, date, capacity_bytes, pod_key, pod_slot_num, {smart}
                    FROM read_parquet([{files}])
                    WHERE date BETWEEN DATE '{start - timedelta(days=w)}' AND DATE '{end}'
                      AND serial_number IN (SELECT serial_number FROM kept)""")
                with_pods = False
                if samples is None:
                    con.execute("CREATE OR REPLACE TEMP TABLE want (serial_number VARCHAR, date DATE)")
                else:
                    keep_samples(con, samples, r_by_study, cfg["seed"], settings["train_sample"]["near_failure_days"])
                    with_pods = own_rate is not None and con.execute(
                        "SELECT count(*) FROM samples WHERE study = 'P'").fetchone()[0] > 0
                    if with_pods:
                        load_pod_tables(con, storage, quarter, start - timedelta(days=w + 1), model)
                resets[quarter] = run_period(con, settings, start, end, model, own_rate if with_pods else None)
                if samples is None:
                    continue
                for study, split in con.execute("SELECT DISTINCT study, split FROM samples").fetchall():
                    written[(study, split)] += write_split(
                        con, writers[(study, split)], schemas[(study, split)], study, split,
                        horizons, columns[study], storage.data_source)
                print(f"features {model} {quarter}: {con.execute('SELECT count(*) FROM want').fetchone()[0]} "
                      f"drive-days, resets {resets[quarter]}", flush=True)
        finally:
            for writer in writers.values():
                writer.close()
            con.close()
        for study, info in about.items():
            (work / folder[study] / "columns.json").write_text(json.dumps(info, indent=2))
        nbytes = sum(p.stat().st_size for f in folder.values() for p in (work / f).glob("*.parquet"))
        rows = sum(written.values())
        print(f"features {model}: {rows} rows, {gb(nbytes)}, {time.time() - started:.0f} s; "
              + ", ".join(f"{st}/{sp} {n}" for (st, sp), n in written.items()), flush=True)
        return {"rows": rows, "bytes": nbytes, "files": {f"{st}/{sp}": n for (st, sp), n in written.items()},
                "resets": resets, "seconds": round(time.time() - started, 1),
                "smart_sets": {st: a["smart_set"] for st, a in about.items()}}

    deps = ["splits", "features_sampling", *(["features_rates"] if "P" in studies else [])]
    return Stage(name=f"features_{model_dir(model)}", tier=DURABLE, outputs=list(folder.values()), build=build,
                 keys=["data_source", "seed", "features"], deps=deps, params={"model": model})


def keep_samples(con, samples, r_by_study, seed, near_days):
    """`samples` in DuckDB after train sampling, with sample_weight; `want` = their drive-days."""
    register(con, "samples_src", samples)
    r = " ".join(f"WHEN '{study}' THEN {value}" for study, value in r_by_study.items())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE samples AS
        SELECT * EXCLUDE (near, u, r), CASE WHEN split = 'train' AND NOT near THEN 1.0 / r ELSE 1.0 END AS sample_weight
        FROM (SELECT *, coalesce(fail_date - date <= {near_days}, false) AS near,
                     {row_hash_sql(seed, 'serial_number', 'date')} AS u,
                     CASE study {r} ELSE 1.0 END AS r
              FROM samples_src)
        WHERE split <> 'train' OR near OR u < r""")
    con.execute("CREATE OR REPLACE TEMP TABLE want AS SELECT DISTINCT serial_number, date FROM samples")


def load_pod_tables(con, storage, quarter, since, model):
    """pod_days, pod_models and the model's events of `quarter` and the one before, from
    `since`. Each file is cast to the POD_TABLES types on its own: a quarter before the pod
    fields has an empty pod_days whose pod_key column was written as an integer."""
    quarters = [previous_quarter(quarter), quarter]
    base = [storage.path(DURABLE, f"audit/quarters/{q}") for q in quarters]
    for name in ("pod_days", "pod_models", "events"):
        paths = [b / f"{name}.parquet" for b in base if (b / f"{name}.parquet").exists()]
        if not storage.path(DURABLE, f"audit/quarters/{quarter}/{name}.parquet").exists():
            raise FileNotFoundError(f"no {name}.parquet for {quarter}; rerun audit_{quarter}")
        schema = POD_TABLES[name]
        extra = ["model"] if name == "events" else []          # events: this model's only
        frame = pl.concat([pl.read_parquet(p, columns=[*schema, *extra]).cast(schema) for p in paths])
        frame = frame.filter(pl.col("date") >= since)
        if extra:
            frame = frame.filter(pl.col("model") == model)
        register(con, name, frame.select(list(schema)))


def write_split(con, writer, schema, study, split, horizons, columns, data_source):
    """Append the quarter's rows of one study and split to its file. Returns the row count."""
    labels = ", ".join([*(f"s.{y_col(n)}" for n in horizons), *(f"s.in_{n}" for n in horizons)])
    reader = con.execute(f"""
        SELECT s.model, s.serial_number, s.date, s.fail_date, {labels}, s.sample_weight,
               {feature_sql(columns)}, '{data_source}' AS data_source
        FROM samples s JOIN feat f ON s.serial_number = f.serial_number AND s.date = f.date
        WHERE s.study = '{study}' AND s.split = '{split}'
        ORDER BY s.serial_number, s.date""").to_arrow_reader(500_000)
    n = 0
    for batch in reader:
        table = pa.Table.from_batches([batch]).cast(schema)
        writer.write_table(table)
        n += table.num_rows
    return n


def feature_stages(cfg, storage, models):
    """features_sampling, features_rates (if Study P has models) and one stage per model in
    a study. Needs splits/windows.csv, so it is built after the splits stage ran."""
    windows = used(pl.read_csv(storage.path(DURABLE, "splits/windows.csv"), schema=WINDOW_SCHEMA))
    stages = [sampling_stage(cfg, models)]
    if not windows.filter(pl.col("study") == "P").is_empty():
        stages.append(rates_stage(cfg))
    for model in sorted(models):
        studies = [s for s in cfg["studies"]
                   if not windows.filter((pl.col("study") == s) & (pl.col("model") == model)).is_empty()]
        if studies:
            stages.append(model_stage(cfg, model, studies))
    return stages


def read_features(storage, study, model, split):
    """One feature file. The test split is for M5's final evaluation only."""
    return pl.read_parquet(storage.path(DURABLE, f"features/{study}/{model_dir(model)}/{split}.parquet"))
