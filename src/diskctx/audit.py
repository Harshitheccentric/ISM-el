"""Pass 1 audit (per exact model string, per quarter) and model selection.

audit_<quarter> stage -> audit/quarters/<quarter>/
  days.csv     per model per day: row counts, failures, non-null counts of each needed
               SMART column and pod field, rows with a full pod location (pod_rows).
  models.csv   per model: the same counts summed over the quarter, plus distinct drives
               and distinct failed drives.
  quality.csv  per column: values that were non-empty but became NULL after TRY_CAST;
               and duplicate (serial_number, date) rows.

Boot-drive rules (counted per model, never applied silently):
  row rule    a row with no SMART values at all is dropped (rows_no_smart).
  model rule  a model is a boot model if >= boot_share of its rows have no SMART values,
              or >= boot_share of its rows have an empty slot, counted only in files where
              the slot column is populated (at least one row has a slot). A file whose
              slot column is absent or empty for every row predates the slot field and
              says nothing about boot drives. Boot models are never selected.

select stage -> audit/selection.json, summary.csv, boot_models.csv, quality.csv
  S/W/N window: longest run of consecutive quarters where 241 and 242 are each non-null
                on >= min_coverage of the model's rows (ties: the latest run).
  P window:     from the first date with >= min_coverage pod coverage (all four pod key
                fields and the slot non-null) to the end of the range.
  A model is eligible if it is not a boot model and has >= min_failures failures inside
  its S/W/N window. pod_ok: >= min_failures failures and >= min_coverage pod coverage
  inside its P window. Eligible models are ranked by (pod_ok, S/W/N failures), since pod
  context is the core novelty; the top max_models are selected.
"""

import csv
import json

import duckdb

from diskctx.config import quarters_between
from diskctx.ingest import POD_FIELDS, cast_types, load_day, q, smart_columns
from diskctx.rawzip import day_members, extract_member
from diskctx.sources import local_zip
from diskctx.status import SELECTION_FILE, check_budget, log_download
from diskctx.storage import DURABLE, Stage

QUARTER_FILES = ("days.csv", "models.csv", "quality.csv")
SELECT_OUTPUTS = [SELECTION_FILE, "audit/summary.csv", "audit/boot_models.csv", "audit/quality.csv"]


def coverage_columns(smart_ids):
    return [*smart_columns(smart_ids), *POD_FIELDS, "pod_slot_num"]


def count_columns(smart_ids):
    """The per-model counts in days.csv / models.csv (all summable)."""
    return ["rows_total", "rows_no_smart", "rows_slot_file", "rows_empty_slot", "rows",
            "failures", "pod_rows", *(f"nonnull_{c}" for c in coverage_columns(smart_ids))]


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
               count(*) FILTER (NOT no_smart AND failure = 1) AS failures,
               count(*) FILTER (NOT no_smart AND pod_key IS NOT NULL AND pod_slot_num IS NOT NULL) AS pod_rows,
               {nonnull}
        FROM day GROUP BY ALL
    """
    con.execute(f"CREATE TABLE day_models AS {select}" if first else f"INSERT INTO day_models {select}")
    con.execute("""
        INSERT INTO drives
        SELECT coalesce(model, ''), serial_number, bool_or(coalesce(failure, 0) = 1)
        FROM day WHERE NOT no_smart AND serial_number IS NOT NULL GROUP BY ALL
        ON CONFLICT DO UPDATE SET failed = failed OR excluded.failed
    """)

    quality = {}
    for col, sql_type in cast_types(smart_ids).items():
        if col in present:
            quality[("cast_failed", col)] = con.execute(
                f"SELECT count(*) FROM raw_day WHERE {q(col)} IS NOT NULL AND TRY_CAST({q(col)} AS {sql_type}) IS NULL"
            ).fetchone()[0]
    quality[("duplicate_serial_date", "serial_number,date")] = con.execute(
        "SELECT count(serial_number) - count(DISTINCT serial_number) FROM raw_day").fetchone()[0]
    return quality


def audit_quarter(zip_path, out_dir, scratch, smart_ids, quarter, data_source):
    """Audit every day CSV of one quarter zip, one day at a time. Writes the three quarter
    tables into out_dir. Returns the largest day CSV's bytes."""
    days = day_members(zip_path)
    if not days:
        raise RuntimeError(f"{zip_path} has no day CSVs")
    con = duckdb.connect()
    con.execute("CREATE TABLE drives (model VARCHAR, serial_number VARCHAR, failed BOOLEAN, "
                "PRIMARY KEY (model, serial_number))")
    day_csv = scratch / "day.csv"
    largest = 0
    quality = {}
    for i, (day, member) in enumerate(days):
        largest = max(largest, extract_member(zip_path, member, day_csv))
        present = load_day(con, day_csv, smart_ids)
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
            SELECT m.*, coalesce(d.drives, 0) AS drives, coalesce(d.failed_drives, 0) AS failed_drives, {tag}
            FROM (SELECT model, {sums} FROM day_models GROUP BY model) m
            LEFT JOIN (SELECT model, count(*) AS drives, count(*) FILTER (failed) AS failed_drives
                       FROM drives GROUP BY model) d USING (model)
            ORDER BY model
        ) TO '{out_dir / 'models.csv'}' (HEADER)
    """)
    with open(out_dir / "quality.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["quarter", "check", "column", "count", "data_source"])
        for (check, col), n in quality.items():
            writer.writerow([quarter, check, col, n, data_source])
    return largest


def audit_stages(cfg, sources, fetch=None, keep_zips=False):
    """One pass-1 stage per quarter that has a zip. Hashed on its quarter, URL and the
    column config, so extending the range only adds stages."""
    kwargs = {"fetch": fetch} if fetch else {}
    stages = []
    for row in sources:
        quarter, url = row["quarter"], row["url"]
        if row["kind"] == "missing":
            continue
        out = f"audit/quarters/{quarter}"

        def build(work, storage, quarter=quarter, url=url, out=out):
            check_budget(storage, quarter, cfg["budget"])
            zip_path = local_zip(storage, url, **kwargs)
            largest = audit_quarter(zip_path, work / out, work, cfg["smart_ids"], quarter,
                                    storage.data_source)
            log_download(storage, quarter, zip_path.stat().st_size, largest, None)
            if not keep_zips:
                zip_path.unlink()

        stages.append(Stage(
            name=f"audit_{quarter}",
            tier=DURABLE,
            outputs=[out],
            build=build,
            keys=["data_source", "smart_ids"],
            params={"quarter": quarter, "url": url},
        ))
    return stages


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
    s["share_pod"] = share(totals["pod_rows"], totals["rows"])

    window = swn_window(by_quarter, quarters, rule["min_coverage"])
    in_window = [d for d in days if quarter_of_date(d["date"]) in window]
    s["swn_window"] = f"{window[0]}-{window[-1]}" if window else ""
    s["swn_failures"] = sum(d["failures"] for d in in_window)
    s["swn_coverage"] = share(
        min(sum(d["nonnull_smart_241_raw"] for d in in_window), sum(d["nonnull_smart_242_raw"] for d in in_window)),
        sum(d["rows"] for d in in_window))

    pod_from = next((d["date"] for d in days if d["rows"] and d["pod_rows"] / d["rows"] >= rule["min_coverage"]), "")
    in_pod = [d for d in days if pod_from and d["date"] >= pod_from]
    s["pod_from"] = pod_from
    s["pod_failures"] = sum(d["failures"] for d in in_pod)
    s["pod_coverage"] = share(sum(d["pod_rows"] for d in in_pod), sum(d["rows"] for d in in_pod))
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
