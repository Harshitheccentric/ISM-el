"""Pass 1 audit (per exact model string, per quarter) and model selection.

Pass 1 reads every drive of every model, so it also captures what later stages need
from the whole fleet (pass 2 keeps only the selected models).

audit_<quarter> stage -> audit/quarters/<quarter>/
  days.csv          per model per day: row counts, failures, non-null counts of each
                    needed SMART column and pod field, rows with a pod_key
                    (pod_key_rows) and with a pod_key and a slot (pod_rows).
  models.csv        per model: the same counts summed over the quarter, plus distinct
                    drives and distinct failed drives.
  quality.csv       per column: values that were non-empty but became NULL after
                    TRY_CAST; duplicate (serial_number, date) rows; 241/242 counter
                    resets; whether the previous quarter's end state was found; rows
                    whose own date is unreadable or not the file's day (the file's
                    day is used either way, see ingest.py).
  events.parquet    failure events: every failure=1 row of every model, taken BEFORE any
                    cleaning (serial, model, date, pod_key, vault_key, had_smart). The
                    no-SMART rule removes a row's features, never its failure event.
                    days.csv failures count these same rows.
  pod_days.parquet  per pod per day, over all non-boot drives (any model): drives,
                    drives reporting 241/242, mean/max SMART 194, mean/max daily TB
                    written/read, counter resets, failures that day (from the events).
                    Keeps vault_key for vault-level counts. Empty if no pod fields.
  end_state.parquet per drive seen on the quarter's last day: its 241/242 counters, so
                    the next quarter's first day gets a daily delta.
  pod_models.parquet per pod, model and day (non-boot models, the same rule as
                    pod_days): drives and failures. M4's "excess" pod features need the
                    model mix. Written only for quarters from Study P's start (stage
                    param pod_models), so adding it reran only those quarters.

Daily TB = (counter today - counter yesterday) x 512 B / 1e12. The delta is null when the
drive was not seen yesterday (including when the previous quarter's end state is
missing) or a counter is null; a negative delta is a counter reset: null, and counted.
The end state is read from the previous quarter's durable folder when the stage runs;
it is not part of the stage hash.

AUDIT_VERSION is in each audit stage's hash: bump it whenever the audit's outputs change,
so finished quarters rerun automatically.

Boot-drive rules (counted per model, never applied silently):
  row rule    a row with no SMART values at all loses its features (rows_no_smart);
              its failure event is kept.
  model rule  a model is a boot model if >= boot_share of its rows have no SMART values,
              or >= boot_share of its rows have an empty slot, counted only in files that
              have slots (at least one row with a non-empty slot). A file whose slot
              column is absent or empty for every row is treated like an old file. Boot
              models are never selected; pod_days applies the rule within the quarter.

select stage -> audit/selection.json, summary.csv, boot_models.csv, quality.csv
  S/W/N window: longest run of consecutive quarters where 241 and 242 are each non-null
                on >= min_coverage of the model's rows (ties: the latest run).
  P window:     from the first date with >= min_coverage pod_key coverage to the end of
                the range. The slot is only a position feature, not part of coverage.
  A model is eligible if it is not a boot model and has >= min_failures failures inside
  its S/W/N window. pod_ok: >= min_failures failures and >= min_coverage pod coverage
  inside its P window. Failures come from the failure events. Eligible models are ranked by (pod_ok, S/W/N failures), since pod
  context is the core novelty; the top max_models are selected.
"""

import csv
import json

import duckdb

from diskctx.config import (
    dropped_days,
    dropped_param,
    parse_date,
    parse_quarter,
    previous_quarter,
    quarters_between,
)
from diskctx.ingest import POD_FIELDS, cast_types, date_checks, load_day, q, smart_columns
from diskctx.rawzip import day_members, extract_member
from diskctx.sources import local_zip
from diskctx.status import SELECTION_FILE, check_budget, log_download
from diskctx.storage import DURABLE, Stage

AUDIT_VERSION = 2   # 2: failure events before cleaning, pod_days, end_state, pod_key coverage
LBA_BYTES = 512
POD_DAY_SMART = (194, 241, 242)   # pod_days needs these in smart_ids
SELECT_OUTPUTS = [SELECTION_FILE, "audit/summary.csv", "audit/boot_models.csv", "audit/quality.csv"]


def coverage_columns(smart_ids):
    return [*smart_columns(smart_ids), *POD_FIELDS, "pod_slot_num"]


def count_columns(smart_ids):
    """The per-model counts in days.csv / models.csv (all summable)."""
    return ["rows_total", "rows_no_smart", "rows_slot_file", "rows_empty_slot", "rows",
            "failures", "pod_key_rows", "pod_rows",
            *(f"nonnull_{c}" for c in coverage_columns(smart_ids))]


# --- pass 1: one quarter ---------------------------------------------------------------


def audit_day(con, present, smart_ids, day, first):
    """Add one loaded day (see ingest.load_day) to the quarter's tables in `con`.
    Returns {(check, column): count} for the day's data-quality checks."""
    slot_populated = "pod_slot_num" in present and con.execute(
        "SELECT count(pod_slot_num) FROM raw_day").fetchone()[0] > 0
    slot_rows = "count(*)" if slot_populated else "0"
    empty_slot = "count(*) FILTER (pod_slot_num IS NULL)" if slot_populated else "0"
    nonnull = ", ".join(
        f"count({q(c)}) FILTER (NOT no_smart) AS {q('nonnull_' + c)}" for c in coverage_columns(smart_ids))
    select = f"""
        SELECT coalesce(model, '') AS model, DATE '{day}' AS date,
               count(*) AS rows_total,
               count(*) FILTER (no_smart) AS rows_no_smart,
               {slot_rows} AS rows_slot_file,
               {empty_slot} AS rows_empty_slot,
               count(*) FILTER (NOT no_smart) AS rows,
               count(*) FILTER (failure = 1) AS failures,          -- all rows: = failure events
               count(*) FILTER (NOT no_smart AND pod_key IS NOT NULL) AS pod_key_rows,
               count(*) FILTER (NOT no_smart AND pod_key IS NOT NULL AND pod_slot_num IS NOT NULL) AS pod_rows,
               {nonnull}
        FROM day GROUP BY ALL
    """
    con.execute(f"CREATE TABLE day_models AS {select}" if first else f"INSERT INTO day_models {select}")
    con.execute("""
        INSERT INTO drives SELECT DISTINCT coalesce(model, ''), serial_number
        FROM day WHERE NOT no_smart AND serial_number IS NOT NULL
        ON CONFLICT DO NOTHING
    """)
    # Failure events, before any cleaning: a failure row without SMART values is kept.
    con.execute("""
        INSERT INTO events
        SELECT serial_number, coalesce(model, ''), date, pod_key, vault_key, NOT no_smart
        FROM day WHERE failure = 1
    """)
    resets = pod_day_parts(con, day, first)

    quality = {("counter_reset", "smart_241_raw"): resets[0], ("counter_reset", "smart_242_raw"): resets[1]}
    for col, sql_type in cast_types(smart_ids).items():
        if col in present:
            quality[("cast_failed", col)] = con.execute(
                f"SELECT count(*) FROM raw_day WHERE {q(col)} IS NOT NULL AND TRY_CAST({q(col)} AS {sql_type}) IS NULL"
            ).fetchone()[0]
    quality[("duplicate_serial_date", "serial_number,date")] = con.execute(
        "SELECT count(serial_number) - count(DISTINCT serial_number) FROM raw_day").fetchone()[0]
    unreadable, other_day = date_checks(con, present, day)
    quality[("date_unparseable", "date")] = unreadable
    quality[("date_not_file_day", "date")] = other_day
    return quality


def pod_day_parts(con, day, first):
    """Daily TB deltas against the carried counter state, partial pod sums per
    (pod, model) for this day, then today's counters become the state.
    Returns (241 resets, 242 resets) seen today."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE today AS
        SELECT d.serial_number, coalesce(d.model, '') AS model, d.pod_key, d.vault_key,
               d.smart_194_raw,
               d.smart_241_raw IS NOT NULL AND d.smart_242_raw IS NOT NULL AS has_lba,
               s.last_date = DATE '{day}' - 1 AS seen_yesterday,
               d.smart_241_raw - s.smart_241_raw AS dw,
               d.smart_242_raw - s.smart_242_raw AS dr
        FROM day d LEFT JOIN state s ON s.serial = d.serial_number
    """)
    tb = f"* {LBA_BYTES} / 1e12"
    parts = f"""
        SELECT DATE '{day}' AS date, pod_key, vault_key, model,
               count(*) AS drives,
               count(*) FILTER (has_lba) AS drives_lba,
               sum(smart_194_raw) AS temp_sum, count(smart_194_raw) AS temp_n,
               max(smart_194_raw) AS temp_max,
               sum(tbw) AS tbw_sum, count(tbw) AS tbw_n, max(tbw) AS tbw_max,
               sum(tbr) AS tbr_sum, count(tbr) AS tbr_n, max(tbr) AS tbr_max,
               count(*) FILTER (reset_w) AS resets_written,
               count(*) FILTER (reset_r) AS resets_read
        FROM (SELECT *,
                     CASE WHEN seen_yesterday AND dw >= 0 THEN dw {tb} END AS tbw,
                     CASE WHEN seen_yesterday AND dr >= 0 THEN dr {tb} END AS tbr,
                     coalesce(seen_yesterday AND dw < 0, false) AS reset_w,
                     coalesce(seen_yesterday AND dr < 0, false) AS reset_r
              FROM today)
        WHERE pod_key IS NOT NULL
        GROUP BY ALL
    """
    con.execute(f"CREATE TABLE pod_parts AS {parts}" if first else f"INSERT INTO pod_parts {parts}")
    resets = con.execute("""
        SELECT count(*) FILTER (seen_yesterday AND dw < 0), count(*) FILTER (seen_yesterday AND dr < 0)
        FROM today""").fetchone()
    con.execute(f"""
        INSERT INTO state
        SELECT serial_number, DATE '{day}', max(smart_241_raw), max(smart_242_raw)
        FROM day WHERE NOT no_smart AND serial_number IS NOT NULL GROUP BY serial_number
        ON CONFLICT DO UPDATE SET last_date = excluded.last_date,
            smart_241_raw = excluded.smart_241_raw, smart_242_raw = excluded.smart_242_raw
    """)
    return resets


def quarter_boot_models(con, boot_share):
    """Boot models by the model rule, on this quarter's rows only (for pod_days)."""
    rows = con.execute("""
        SELECT model, sum(rows_total), sum(rows_no_smart), sum(rows_slot_file), sum(rows_empty_slot)
        FROM day_models GROUP BY model""").fetchall()
    keys = ("rows_total", "rows_no_smart", "rows_slot_file", "rows_empty_slot")
    return [r[0] for r in rows if boot_check(dict(zip(keys, r[1:])), boot_share)[0]]


def audit_quarter(zip_path, out_dir, scratch, smart_ids, quarter, data_source, boot_share,
                  prev_state=None, drop_days=(), pod_models=False):
    """Audit every day CSV of one quarter zip, one day at a time, and write the quarter
    tables into out_dir. prev_state: the previous quarter's end_state.parquet (may not
    exist). drop_days: day files left out (config dropped_days), recorded in quality.csv.
    pod_models: also write pod_models.parquet.
    Returns (largest day CSV's bytes, whether the previous end state was used)."""
    missing = [i for i in POD_DAY_SMART if i not in smart_ids]
    if missing:
        raise ValueError(f"smart_ids must include {missing} for the pod-day table")
    days = day_members(zip_path, drop_days)
    if not days:
        raise RuntimeError(f"{zip_path} has no day CSVs")
    con = duckdb.connect()
    con.execute("CREATE TABLE drives (model VARCHAR, serial_number VARCHAR, PRIMARY KEY (model, serial_number))")
    con.execute("CREATE TABLE events (serial_number VARCHAR, model VARCHAR, date DATE, "
                "pod_key VARCHAR, vault_key VARCHAR, had_smart BOOLEAN)")
    con.execute("CREATE TABLE state (serial VARCHAR PRIMARY KEY, last_date DATE, "
                "smart_241_raw BIGINT, smart_242_raw BIGINT)")
    found = prev_state is not None and prev_state.exists()
    if found:
        con.execute(f"INSERT INTO state SELECT serial, last_date, smart_241_raw, smart_242_raw "
                    f"FROM read_parquet('{prev_state}')")
    quality = {("previous_end_state", "found"): int(found)}
    quality.update({("day_dropped", day): 1 for day in drop_days})

    day_csv = scratch / "day.csv"
    largest = 0
    for i, (day, member) in enumerate(days):
        largest = max(largest, extract_member(zip_path, member, day_csv))
        present = load_day(con, day_csv, smart_ids, day)
        for key, n in audit_day(con, present, smart_ids, day, first=(i == 0)).items():
            quality[key] = quality.get(key, 0) + n
        day_csv.unlink()

    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f"'{quarter}' AS quarter, '{data_source}' AS data_source"
    sums = ", ".join(f"sum({c})::BIGINT AS {c}" for c in count_columns(smart_ids))
    con.execute(f"COPY (SELECT *, {tag} FROM day_models ORDER BY model, date) "
                f"TO '{out_dir / 'days.csv'}' (HEADER)")
    con.execute(f"""
        COPY (
            SELECT m.*, coalesce(d.drives, 0) AS drives, coalesce(e.failed_drives, 0) AS failed_drives, {tag}
            FROM (SELECT model, {sums} FROM day_models GROUP BY model) m
            LEFT JOIN (SELECT model, count(*) AS drives FROM drives GROUP BY model) d USING (model)
            LEFT JOIN (SELECT model, count(DISTINCT serial_number) AS failed_drives
                       FROM events GROUP BY model) e USING (model)
            ORDER BY model
        ) TO '{out_dir / 'models.csv'}' (HEADER)
    """)
    parquet = "(FORMAT parquet, COMPRESSION zstd)"
    con.execute(f"COPY (SELECT *, {tag} FROM events ORDER BY date, serial_number) "
                f"TO '{out_dir / 'events.parquet'}' {parquet}")

    con.execute("CREATE TEMP TABLE boot AS SELECT unnest(?::VARCHAR[]) AS model",
                [quarter_boot_models(con, boot_share)])
    con.execute(f"""
        COPY (
            WITH p AS (
                SELECT date, pod_key, vault_key,
                       sum(drives)::BIGINT AS drives, sum(drives_lba)::BIGINT AS drives_lba,
                       sum(temp_sum) / nullif(sum(temp_n), 0) AS temp_mean, max(temp_max) AS temp_max,
                       sum(tbw_sum) / nullif(sum(tbw_n), 0) AS tb_written_mean, max(tbw_max) AS tb_written_max,
                       sum(tbr_sum) / nullif(sum(tbr_n), 0) AS tb_read_mean, max(tbr_max) AS tb_read_max,
                       sum(resets_written)::BIGINT AS resets_written, sum(resets_read)::BIGINT AS resets_read
                FROM pod_parts WHERE model NOT IN (SELECT model FROM boot) GROUP BY ALL),
            f AS (
                SELECT date, pod_key, count(*) AS failures FROM events
                WHERE pod_key IS NOT NULL AND model NOT IN (SELECT model FROM boot) GROUP BY ALL)
            SELECT p.*, coalesce(f.failures, 0) AS failures, {tag}
            FROM p LEFT JOIN f USING (date, pod_key)
            ORDER BY date, pod_key
        ) TO '{out_dir / 'pod_days.parquet'}' {parquet}
    """)
    if pod_models:
        con.execute(f"""
            COPY (
                WITH m AS (
                    SELECT date, pod_key, model, sum(drives)::BIGINT AS drives
                    FROM pod_parts WHERE model NOT IN (SELECT model FROM boot) GROUP BY ALL),
                f AS (
                    SELECT date, pod_key, model, count(*) AS failures FROM events
                    WHERE pod_key IS NOT NULL AND model NOT IN (SELECT model FROM boot) GROUP BY ALL)
                SELECT m.*, coalesce(f.failures, 0) AS failures, {tag}
                FROM m LEFT JOIN f USING (date, pod_key, model)
                ORDER BY date, pod_key, model
            ) TO '{out_dir / 'pod_models.parquet'}' {parquet}
        """)
    con.execute(f"COPY (SELECT * FROM state WHERE last_date = DATE '{days[-1][0]}' ORDER BY serial) "
                f"TO '{out_dir / 'end_state.parquet'}' {parquet}")

    with open(out_dir / "quality.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["quarter", "check", "column", "count", "data_source"])
        for (check, col), n in quality.items():
            writer.writerow([quarter, check, col, n, data_source])
    return largest, found


def audit_stages(cfg, sources, fetch=None, keep_zips=False):
    """One pass-1 stage per quarter that has a zip. Hashed on AUDIT_VERSION, its quarter,
    URL, boot_share and the column config, so extending the range only adds stages.

    Stages come in source order, which is chronological, and run one after another, so
    on a fresh run each quarter finds the previous quarter's end state. The marker's
    info records whether it was used for day 1 (carried_state_day1)."""
    stages = []
    for row in sources:
        quarter, url = row["quarter"], row["url"]
        if row["kind"] == "missing":
            continue
        out = f"audit/quarters/{quarter}"

        def build(work, storage, quarter=quarter, url=url, out=out):
            check_budget(storage, quarter, cfg["budget"])
            zip_path = local_zip(storage, url, fetch)
            prev_quarter = previous_quarter(quarter)
            prev = storage.path(DURABLE, f"audit/quarters/{prev_quarter}/end_state.parquet")
            largest, carried = audit_quarter(zip_path, work / out, work, cfg["smart_ids"], quarter,
                                             storage.data_source, cfg["selection"]["boot_share"], prev,
                                             dropped_days(cfg, quarter), bool(pod_models_param(cfg, quarter)))
            log_download(storage, quarter, zip_path.stat().st_size, largest, None)
            if not keep_zips:
                zip_path.unlink()
            return {"carried_state_day1": carried, "previous_quarter": prev_quarter}

        stages.append(Stage(
            name=f"audit_{quarter}",
            tier=DURABLE,
            outputs=[out],
            build=build,
            keys=["data_source", "smart_ids"],
            params={"version": AUDIT_VERSION, "quarter": quarter, "url": url,
                    "boot_share": cfg["selection"]["boot_share"], **dropped_param(cfg, quarter),
                    **pod_models_param(cfg, quarter)},
        ))
    return stages


def pod_models_param(cfg, quarter):
    """{"pod_models": True} for quarters from Study P's start quarter, else {}. Only those
    quarters write pod_models.parquet, so only their audit stages got a new hash."""
    if "P" not in cfg["studies"]:
        return {}
    start = parse_date(cfg["studies"]["P"]["start"])
    first = f"{start.year}Q{(start.month - 1) // 3 + 1}"
    return {"pod_models": True} if parse_quarter(quarter) >= parse_quarter(first) else {}


# --- selection -----------------------------------------------------------------------


def read_csv_rows(path):
    """Rows of a CSV, with every all-digit value turned into an int."""
    with open(path, newline="") as f:
        return [{k: int(v) if v.isdigit() else v for k, v in row.items()} for row in csv.DictReader(f)]


def share(part, whole):
    return round(part / whole, 4) if whole else 0.0


def quarter_of_date(day):
    """'2026-02-15' -> '2026Q1'"""
    return f"{day[:4]}Q{(int(day[5:7]) - 1) // 3 + 1}"


def boot_check(totals, boot_share):
    """(is_boot, reason) for one model from its summed counts."""
    no_smart = share(totals["rows_no_smart"], totals["rows_total"])
    empty_slot = share(totals["rows_empty_slot"], totals["rows_slot_file"])
    reasons = []
    if no_smart >= boot_share:
        reasons.append(f"{no_smart:.1%} rows with no SMART values")
    if totals["rows_slot_file"] and empty_slot >= boot_share:
        reasons.append(f"{empty_slot:.1%} rows with empty slot")
    return bool(reasons), "; ".join(reasons)


def swn_window(by_quarter, quarters, min_coverage):
    """Longest run of consecutive quarters with 241 and 242 coverage >= min_coverage.
    Ties go to the latest run. Returns the list of quarters (empty if none)."""
    best, run = [], []
    for quarter in quarters:
        row = by_quarter.get(quarter)
        ok = row is not None and row["rows"] > 0 and min(
            row["nonnull_smart_241_raw"], row["nonnull_smart_242_raw"]) / row["rows"] >= min_coverage
        run = run + [quarter] if ok else []
        if run and len(run) >= len(best):
            best = run
    return best


def model_summary(model, by_quarter, days, quarters, smart_ids, rule):
    """Everything select needs to know about one model, as one flat dict."""
    totals = {c: sum(r[c] for r in by_quarter.values()) for c in count_columns(smart_ids)}
    is_boot, boot_reason = boot_check(totals, rule["boot_share"])
    s = {"model": model, "boot": is_boot, "rows": totals["rows"],
         "drives_max_quarter": max(r["drives"] for r in by_quarter.values()),
         "failed_drives": sum(r["failed_drives"] for r in by_quarter.values())}
    for col in coverage_columns(smart_ids):
        s[f"share_{col}"] = share(totals[f"nonnull_{col}"], totals["rows"])
    s["share_pod_key"] = share(totals["pod_key_rows"], totals["rows"])

    window = swn_window(by_quarter, quarters, rule["min_coverage"])
    in_window = [d for d in days if quarter_of_date(d["date"]) in window]
    s["swn_window"] = f"{window[0]}-{window[-1]}" if window else ""
    s["swn_failures"] = sum(d["failures"] for d in in_window)
    s["swn_coverage"] = share(
        min(sum(d["nonnull_smart_241_raw"] for d in in_window), sum(d["nonnull_smart_242_raw"] for d in in_window)),
        sum(d["rows"] for d in in_window))

    pod_from = next((d["date"] for d in days if d["rows"] and d["pod_key_rows"] / d["rows"] >= rule["min_coverage"]), "")
    in_pod = [d for d in days if pod_from and d["date"] >= pod_from]
    s["pod_from"] = pod_from
    s["pod_failures"] = sum(d["failures"] for d in in_pod)
    s["pod_coverage"] = share(sum(d["pod_key_rows"] for d in in_pod), sum(d["rows"] for d in in_pod))
    s["pod_ok"] = bool(pod_from) and s["pod_failures"] >= rule["min_failures"] \
        and s["pod_coverage"] >= rule["min_coverage"]

    if is_boot:
        reason = f"boot model: {boot_reason}"
    elif not window:
        reason = f"241/242 coverage below {rule['min_coverage']:.0%} in every quarter"
    elif s["swn_failures"] < rule["min_failures"]:
        reason = f"{s['swn_failures']} failures in S/W/N window < {rule['min_failures']}"
    else:
        reason = "eligible"
    s["eligible"] = reason == "eligible"
    s["reason"] = reason
    s["boot_counts"] = {c: totals[c] for c in ("rows_total", "rows_no_smart", "rows_slot_file", "rows_empty_slot")}
    return s


def select_models(summaries, rule):
    """Rank eligible models by (pod_ok, S/W/N failures) and take the top max_models.
    Marks each summary with selected / rank and returns the selected ones in order."""
    eligible = sorted((s for s in summaries if s["eligible"]),
                      key=lambda s: (not s["pod_ok"], -s["swn_failures"], s["model"]))
    chosen = eligible[:rule["max_models"]]
    for s in summaries:
        s["selected"] = s in chosen
        if s["selected"]:
            s["reason"] = f"selected: rank {chosen.index(s) + 1} by (pod_ok, S/W/N failures)"
        elif s["eligible"]:
            s["reason"] = f"eligible, ranked below the top {rule['max_models']}"
    return chosen


def selection_warning(chosen, rule):
    if not chosen:
        return (f"no model qualifies (>= {rule['min_failures']} failures in its S/W/N window); "
                f"widen the range or revisit the rule (HANDOFF gate)")
    if not any(s["pod_ok"] for s in chosen):
        return ("no selected model is pod_ok: pod context (P), the core novelty, has fewer than "
                f"{rule['min_failures']} failures or under {rule['min_coverage']:.0%} coverage "
                "in every selected model's P window")
    return ""


def ingest_quarters(chosen, available, end_quarter):
    """Quarters pass 2 needs: from the earliest window start of any selected model to the
    end of the range, limited to quarters that have a zip."""
    starts = [s["swn_window"].split("-")[0] for s in chosen]
    starts += [quarter_of_date(s["pod_from"]) for s in chosen if s["pod_from"]]
    if not starts:
        return []
    wanted = set(quarters_between(min(starts), end_quarter))
    return [q for q in available if q in wanted]


SELECTION_FIELDS = ["model", "swn_window", "swn_failures", "swn_coverage",
                    "pod_from", "pod_failures", "pod_coverage", "pod_ok"]


def build_selection(storage, cfg, quarters, available):
    """Read the quarter tables and write the four select outputs into a work folder."""
    rule = cfg["selection"]
    smart_ids = cfg["smart_ids"]
    by_model, days_by_model, quality = {}, {}, []
    for quarter in available:
        base = storage.path(DURABLE, f"audit/quarters/{quarter}")
        for row in read_csv_rows(base / "models.csv"):
            by_model.setdefault(row["model"], {})[quarter] = row
        for row in read_csv_rows(base / "days.csv"):
            days_by_model.setdefault(row["model"], []).append(row)
        quality += read_csv_rows(base / "quality.csv")

    summaries = [model_summary(m, by_model[m], sorted(days_by_model[m], key=lambda d: d["date"]),
                               quarters, smart_ids, rule)
                 for m in sorted(by_model)]
    chosen = select_models(summaries, rule)
    selection = {
        "data_source": storage.data_source,
        "rule": rule,
        "quarters_audited": available,
        "missing_quarters": [q for q in quarters if q not in available],
        "quarters": ingest_quarters(chosen, available, quarters[-1]),
        "models": [{k: s[k] for k in SELECTION_FIELDS} for s in chosen],
        "warning": selection_warning(chosen, rule),
    }
    boot_rows = [{"model": s["model"], **s["boot_counts"], "reason": s["reason"]}
                 for s in summaries if s["boot"]]
    summary_rows = [{k: v for k, v in s.items() if k != "boot_counts"} for s in summaries]
    return selection, summary_rows, boot_rows, quality


def write_csv(path, rows, columns):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def select_stage(cfg, sources, audit_stage_names):
    quarters = [r["quarter"] for r in sources]
    available = [r["quarter"] for r in sources if r["kind"] != "missing"]

    def build(work, storage):
        selection, summary, boot, quality = build_selection(storage, cfg, quarters, available)
        (work / "audit").mkdir(parents=True)
        (work / SELECTION_FILE).write_text(json.dumps(selection, indent=2))
        summary_cols = list(summary[0]) if summary else ["model"]
        write_csv(work / "audit/summary.csv", summary, summary_cols)
        write_csv(work / "audit/boot_models.csv", boot,
                  ["model", "rows_total", "rows_no_smart", "rows_slot_file", "rows_empty_slot", "reason"])
        write_csv(work / "audit/quality.csv", quality, ["quarter", "check", "column", "count", "data_source"])
        if selection["warning"]:
            print(f"WARNING: {selection['warning']}")

    return Stage(
        name="select",
        tier=DURABLE,
        outputs=SELECT_OUTPUTS,
        build=build,
        keys=["selection", "smart_ids", "end_quarter"],
        deps=list(audit_stage_names),
        params={"quarters": quarters, "available": available},
    )


def read_selection(storage):
    return json.loads(storage.path(DURABLE, SELECTION_FILE).read_text())
