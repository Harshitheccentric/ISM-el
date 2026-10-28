"""Audit numbers on synthetic data, checked against values computed by hand.
Synthetic data is for tests only; none of these numbers is a result."""

import csv
import json
import re
from datetime import date

import pyarrow.parquet as pq
import pytest
from conftest import csv_text

import diskctx.audit as audit
from diskctx.audit import read_csv_rows, read_selection, select_models, selection_warning, swn_window
from diskctx.ingest import read_day
from diskctx.pipeline import run_data
from diskctx.storage import stage_hashes
from diskctx.status import status_report
from diskctx.storage import DURABLE, Storage
from diskctx.synthetic import COLUMNS, MODEL_BOOT, MODEL_LBA, MODEL_NO_LBA, SyntheticFleet

Q1_START, Q1_END = date(2026, 1, 1), date(2026, 3, 31)


def rows_of(storage, name):
    return read_csv_rows(storage.path(DURABLE, name))


def by_model(rows):
    return {r["model"]: r for r in rows}


def active_days(drive, start, end):
    first, last = max(drive.first_day, start), min(drive.last_day, end)
    return max(0, (last - first).days + 1)


def make_row(day, serial, model, slot="0001", smart=True, dc="sac0", failure="0"):
    """One crafted CSV row ('' = empty), with 241/242 when smart=True."""
    row = dict.fromkeys(COLUMNS, "")
    row.update(date=day, serial_number=serial, model=model, capacity_bytes="14000519643136",
               failure=failure, datacenter=dc, cluster_id="000", vault_id="1154", pod_id="02",
               pod_slot_num=slot, is_legacy_format="false")
    if smart:
        for i in (5, 9, 194, 241, 242):
            row[f"smart_{i}_raw"] = "3000000000" if i in (241, 242) else "1"
    return row


def run_crafted(cfg, zip_fetch, days_rows):
    """Run the pipeline on one crafted 2026Q1 zip: {day: [rows]}."""
    members = {f"data_Q1_2026/{day}.csv": csv_text(rows) for day, rows in days_rows.items()}
    run_data(cfg, fetch=zip_fetch({"2026Q1": members}))
    return Storage.from_config(cfg)


# --- pass 1 counts -------------------------------------------------------------------


def test_audit_matches_hand_counts(cfg, fleet_fetch):
    fleet = SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1")
    run_data(cfg, fetch=fleet_fetch(fleet))
    storage = Storage.from_config(cfg)
    models = by_model(rows_of(storage, "audit/quarters/2026Q1/models.csv"))

    for model in (MODEL_LBA, MODEL_NO_LBA):
        drives = [d for d in fleet.drives if d.model == model]
        rows = sum(active_days(d, Q1_START, Q1_END) for d in drives)
        m = models[model]
        assert m["rows_total"] == m["rows"] == rows
        assert m["rows_no_smart"] == 0 and m["rows_empty_slot"] == 0
        assert m["drives"] == len(drives)
        assert m["failures"] == m["failed_drives"] == sum(d.failed for d in drives)
        assert m["pod_rows"] == rows
        has_lba = model == MODEL_LBA
        assert m["nonnull_smart_241_raw"] == m["nonnull_smart_242_raw"] == (rows if has_lba else 0)
        assert m["nonnull_smart_197_raw"] == (0 if has_lba else rows)
        assert m["nonnull_smart_9_raw"] == rows

    boot = models[MODEL_BOOT]
    n_boot = 3 * 90
    assert (boot["rows_total"], boot["rows_no_smart"], boot["rows"], boot["drives"]) == (n_boot, n_boot, 0, 0)
    assert boot["rows_slot_file"] == boot["rows_empty_slot"] == n_boot

    days = rows_of(storage, "audit/quarters/2026Q1/days.csv")
    assert sum(d["rows"] for d in days if d["model"] == MODEL_LBA) == models[MODEL_LBA]["rows"]

    selection = read_selection(storage)
    lba = [d for d in fleet.drives if d.model == MODEL_LBA]
    assert [m["model"] for m in selection["models"]] == [MODEL_LBA]
    assert selection["models"][0]["swn_window"] == "2026Q1-2026Q1"
    assert selection["models"][0]["swn_failures"] == sum(d.failed for d in lba)
    assert selection["models"][0]["pod_from"] == "2026-01-01"
    assert selection["quarters"] == ["2026Q1"]
    summary = by_model(rows_of(storage, "audit/summary.csv"))
    assert summary[MODEL_NO_LBA]["reason"] == "241/242 coverage below 95% in every quarter"
    assert [r["model"] for r in rows_of(storage, "audit/boot_models.csv")] == [MODEL_BOOT]
    quality = {(r["check"], r["column"]): r["count"] for r in rows_of(storage, "audit/quality.csv")}
    planned_resets = sum(d.reset_day is not None for d in lba)       # one per reset drive
    assert quality.pop(("counter_reset", "smart_241_raw")) == planned_resets == 2
    assert quality.pop(("counter_reset", "smart_242_raw")) == planned_resets
    assert quality.pop(("previous_end_state", "found")) == 0
    assert set(quality.values()) == {0}


def test_pod_coverage_first_date(cfg, zip_fetch):
    # 20 drives over Jan 1-10. S00 fails Jan 2, S01 fails Jan 8. S18 and S19 have no
    # datacenter on Jan 1-3, so pod coverage is 18/20, 18/20, 17/19 (< 95%), then 100%.
    days_rows = {}
    for n in range(1, 11):
        day = f"2026-01-{n:02d}"
        rows = []
        for i in range(20):
            if (i == 0 and n > 2) or (i == 1 and n > 8):
                continue
            failure = "1" if (i, n) in ((0, 2), (1, 8)) else "0"
            dc = "" if i >= 18 and n <= 3 else "sac0"
            rows.append(make_row(day, f"S{i:02d}", "SYNTH POD", slot=f"{i:04d}", dc=dc, failure=failure))
        days_rows[day] = rows
    storage = run_crafted(cfg, zip_fetch, days_rows)

    s = by_model(rows_of(storage, "audit/summary.csv"))["SYNTH POD"]
    assert s["rows"] == 20 + 20 + 19 + 5 * 19 + 2 * 18 == 190
    assert s["pod_from"] == "2026-01-04"
    assert s["swn_failures"] == 2                      # both failures are in 2026Q1
    assert s["pod_failures"] == 1                      # only the Jan 8 failure is after Jan 4
    assert float(s["pod_coverage"]) == 1.0
    assert float(s["share_pod_key"]) == round((18 + 18 + 17 + 131) / 190, 4)
    assert s["pod_ok"] == "True" and s["selected"] == "True"


# --- boot drives -------------------------------------------------------------------


def test_half_rolled_out_slots_keep_data_drives(cfg, zip_fetch):
    day = "2026-01-01"
    rows = [make_row(day, f"H{i}", "SYNTH HALF", slot=f"{i:04d}" if i < 6 else "") for i in range(10)]
    rows += [make_row(day, f"B{i}", "SYNTH BOOT VD", slot="", smart=False) for i in range(2)]
    rows += [make_row(day, f"X{i}", "SYNTH SSDBOOT", slot="") for i in range(2)]
    storage = run_crafted(cfg, zip_fetch, {day: rows})

    half = by_model(rows_of(storage, "audit/quarters/2026Q1/models.csv"))["SYNTH HALF"]
    assert (half["rows"], half["rows_slot_file"], half["rows_empty_slot"]) == (10, 10, 4)
    summary = by_model(rows_of(storage, "audit/summary.csv"))
    assert summary["SYNTH HALF"]["boot"] == "False"
    boot = by_model(rows_of(storage, "audit/boot_models.csv"))
    assert sorted(boot) == ["SYNTH BOOT VD", "SYNTH SSDBOOT"]
    assert "no SMART values" in boot["SYNTH BOOT VD"]["reason"]
    assert boot["SYNTH SSDBOOT"]["reason"] == "boot model: 100.0% rows with empty slot"
    assert (boot["SYNTH BOOT VD"]["rows_total"], boot["SYNTH BOOT VD"]["rows_no_smart"]) == (2, 2)


def test_slot_column_empty_everywhere_does_not_make_boot_models(cfg, zip_fetch):
    # A legacy file whose slot column exists but is empty in every row says nothing.
    day = "2026-01-01"
    rows = [make_row(day, f"L{i}", "SYNTH LEGACY", slot="") for i in range(5)]
    storage = run_crafted(cfg, zip_fetch, {day: rows})
    legacy = by_model(rows_of(storage, "audit/summary.csv"))["SYNTH LEGACY"]
    assert legacy["boot"] == "False"
    assert rows_of(storage, "audit/boot_models.csv") == []


# --- data quality --------------------------------------------------------------------


def test_cast_failures_and_duplicates_are_counted(cfg, zip_fetch):
    day = "2026-01-01"
    rows = [make_row(day, f"Q{i}", "SYNTH QA", slot=f"{i:04d}") for i in range(4)]
    rows[0]["smart_5_raw"] = "12abc"              # non-empty, fails the cast -> counted
    rows[1]["smart_9_raw"] = ""                   # empty -> plain null, not counted
    rows.append(dict(rows[2]))                    # duplicate (serial, date)
    storage = run_crafted(cfg, zip_fetch, {day: rows})

    quality = {(r["check"], r["column"]): r["count"] for r in rows_of(storage, "audit/quality.csv")}
    assert quality[("cast_failed", "smart_5_raw")] == 1
    assert quality[("cast_failed", "smart_9_raw")] == 0
    assert ("cast_failed", "date") not in quality                  # the date is never cast
    assert quality[("date_unparseable", "date")] == quality[("date_not_file_day", "date")] == 0
    assert quality[("duplicate_serial_date", "serial_number,date")] == 1


# --- windows and selection -------------------------------------------------------------


def test_windows_over_three_quarters(cfg, fleet_fetch):
    cfg.update(start_quarter="2025Q3", end_quarter="2026Q1")
    fleet = SyntheticFleet(cfg["seed"], "2025Q3", "2026Q1")
    lba_cols = ["smart_241_normalized", "smart_241_raw", "smart_242_normalized", "smart_242_raw"]
    pod_cols = ["datacenter", "cluster_id", "vault_id", "pod_id", "pod_slot_num"]
    drop = {"2025Q3": lba_cols + pod_cols, "2025Q4": pod_cols}
    run_data(cfg, fetch=fleet_fetch(fleet, drop))
    storage = Storage.from_config(cfg)

    lba = [d for d in fleet.drives if d.model == MODEL_LBA]
    swn_failures = sum(d.failed and d.last_day >= date(2025, 10, 1) for d in lba)
    pod_failures = sum(d.failed and d.last_day >= Q1_START for d in lba)
    s = by_model(rows_of(storage, "audit/summary.csv"))[MODEL_LBA]
    assert s["swn_window"] == "2025Q4-2026Q1"
    assert s["swn_failures"] == swn_failures == 2
    assert s["pod_from"] == "2026-01-01"
    assert s["pod_failures"] == pod_failures
    selection = read_selection(storage)
    assert selection["quarters"] == ["2025Q4", "2026Q1"]
    assert selection["quarters_audited"] == ["2025Q3", "2025Q4", "2026Q1"]


def test_swn_window_is_longest_then_latest():
    def q(ok):
        return {"rows": 10, "nonnull_smart_241_raw": 10 if ok else 0, "nonnull_smart_242_raw": 10}

    quarters = ["2025Q1", "2025Q2", "2025Q3", "2025Q4", "2026Q1"]
    pattern = dict(zip(quarters, [True, True, False, True, True]))
    assert swn_window({k: q(v) for k, v in pattern.items()}, quarters, 0.95) == ["2025Q4", "2026Q1"]
    del pattern["2025Q4"]                      # a missing quarter breaks the run
    assert swn_window({k: q(v) for k, v in pattern.items()}, quarters, 0.95) == ["2025Q1", "2025Q2"]


RULE = {"min_failures": 100, "min_coverage": 0.95, "max_models": 2, "boot_share": 0.95}


def summary(model, failures, pod_ok, eligible=True):
    return {"model": model, "swn_failures": failures, "pod_ok": pod_ok, "eligible": eligible,
            "reason": "eligible" if eligible else "too few"}


def test_ranking_puts_pod_ok_first():
    s = [summary("A", 500, False), summary("B", 150, True), summary("C", 300, True), summary("D", 900, True, False)]
    chosen = select_models(s, RULE)
    assert [c["model"] for c in chosen] == ["C", "B"]          # pod_ok beats more failures
    assert s[0]["reason"] == "eligible, ranked below the top 2"
    assert s[3]["selected"] is False
    assert selection_warning(chosen, RULE) == ""


def test_warning_when_no_selected_model_is_pod_ok(cfg):
    chosen = select_models([summary("A", 500, False), summary("B", 200, False)], RULE)
    warning = selection_warning(chosen, RULE)
    assert warning.startswith("no selected model is pod_ok")
    assert selection_warning([], RULE).startswith("no model qualifies")

    storage = Storage.from_config(cfg)
    path = storage.path(DURABLE, "audit/selection.json")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"models": [{"model": "A", "pod_ok": False}], "warning": warning}))
    assert f"WARNING: {warning}" in status_report(storage, [], cfg)


def test_summary_csv_is_readable(cfg, fleet_fetch):
    run_data(cfg, fetch=fleet_fetch(SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1")))
    with open(Storage.from_config(cfg).path(DURABLE, "audit/summary.csv"), newline="") as f:
        header = next(csv.reader(f))
    for col in ("model", "boot", "drives_max_quarter", "share_smart_241_raw", "share_pod_key",
                "swn_window", "pod_from", "pod_ok", "selected", "reason"):
        assert col in header


# --- failure events, pod-day table, counter deltas -------------------------------------


def parquet_rows(storage, name):
    return pq.read_table(storage.path(DURABLE, name)).to_pylist()


def lba_row(day, serial, model, written, read=1_000_000_000, temp="30", vault="1154", **kw):
    row = make_row(day, serial, model, **kw)
    row.update(smart_241_raw=str(written), smart_242_raw=str(read), smart_194_raw=temp, vault_id=vault)
    return row


def test_failure_row_without_smart_is_kept_as_event(cfg, zip_fetch):
    day1, day2 = "2026-01-01", "2026-01-02"
    rows = {
        day1: [make_row(day1, "F1", "SYNTH X", slot="0001"), make_row(day1, "F2", "SYNTH X", slot="0002")],
        # F1 fails with every SMART field empty (dead drive); F2 fails normally
        day2: [make_row(day2, "F1", "SYNTH X", slot="0001", smart=False, failure="1"),
               make_row(day2, "F2", "SYNTH X", slot="0002", failure="1")],
    }
    storage = run_crafted(cfg, zip_fetch, rows)

    events = parquet_rows(storage, "audit/quarters/2026Q1/events.parquet")
    assert [(e["serial_number"], e["had_smart"], e["pod_key"], e["vault_key"]) for e in events] == [
        ("F1", False, "sac0|000|1154|02", "sac0|000|1154"), ("F2", True, "sac0|000|1154|02", "sac0|000|1154")]
    x = by_model(rows_of(storage, "audit/quarters/2026Q1/models.csv"))["SYNTH X"]
    assert (x["failures"], x["failed_drives"], x["rows_no_smart"], x["rows"]) == (2, 2, 1, 3)
    assert by_model(rows_of(storage, "audit/summary.csv"))["SYNTH X"]["swn_failures"] == 2


def test_pod_days_cover_all_non_boot_models(cfg, zip_fetch):
    day = "2026-01-01"
    rows = [
        lba_row(day, "S1", "SYNTH SEL", 1, temp="30", slot="0001", failure="1"),
        lba_row(day, "S2", "SYNTH SEL", 1, temp="40", slot="0002"),
        make_row(day, "O1", "SYNTH OTHER", slot="0003", failure="1"),             # no 241/242
        make_row(day, "O2", "SYNTH OTHER", slot="0004", smart=False, failure="1"),
        make_row(day, "B1", "SYNTH BOOT VD", slot="", smart=False),               # boot model
        make_row(day, "B2", "SYNTH BOOT VD", slot="", smart=False),
        lba_row(day, "S3", "SYNTH SEL", 1, temp="35", slot="0001", vault="2017"),  # same pod_id, other vault
    ]
    rows[2].update(smart_194_raw="50", smart_241_raw="", smart_242_raw="")
    storage = run_crafted(cfg, zip_fetch, {day: rows})
    assert [m["model"] for m in read_selection(storage)["models"]] == ["SYNTH SEL"]

    pods = {p["pod_key"]: p for p in parquet_rows(storage, "audit/quarters/2026Q1/pod_days.parquet")}
    assert sorted(pods) == ["sac0|000|1154|02", "sac0|000|2017|02"]
    p = pods["sac0|000|1154|02"]
    assert p["vault_key"] == "sac0|000|1154"
    assert (p["drives"], p["drives_lba"]) == (4, 2)          # SEL + OTHER, boot drives left out
    assert p["temp_mean"] == pytest.approx((30 + 40 + 50) / 3) and p["temp_max"] == 50
    assert p["failures"] == 3                                 # S1, O1 and O2 (no SMART)
    assert p["tb_written_mean"] is None                       # no yesterday to compare with
    assert pods["sac0|000|2017|02"]["drives"] == 1


def crafted_zip(quarter, days_rows):
    year, q = quarter.split("Q")
    return {f"data_Q{q}_{year}/{day}.csv": csv_text(rows) for day, rows in days_rows.items()}


def tb_by_day(storage, quarter):
    rows = parquet_rows(storage, f"audit/quarters/{quarter}/pod_days.parquet")
    return {str(r["date"]): r for r in rows}


def test_quarter_boundary_delta_uses_carried_state(cfg, zip_fetch):
    gb2 = 2_000_000_000                      # LBAs per day -> 2e9 x 512 B = 1.024 TB
    base = 10_000_000_000
    zips = {
        "2025Q4": crafted_zip("2025Q4", {"2025-12-31": [lba_row("2025-12-31", "D1", "SYNTH LBA", base)]}),
        "2026Q1": crafted_zip("2026Q1", {
            "2026-01-01": [lba_row("2026-01-01", "D1", "SYNTH LBA", base + gb2)],
            "2026-01-02": [lba_row("2026-01-02", "D1", "SYNTH LBA", base + 2 * gb2)]}),
    }
    cfg.update(start_quarter="2025Q4", end_quarter="2026Q1")
    results = run_data(cfg, fetch=zip_fetch(zips))
    storage = Storage.from_config(cfg)
    audits = [name for name in results if name.startswith("audit_")]
    assert audits == ["audit_2025Q4", "audit_2026Q1"]                 # chronological order
    assert storage.read_marker("audit_2025Q4")["info"] == {"carried_state_day1": False,
                                                            "previous_quarter": "2025Q3"}
    assert storage.read_marker("audit_2026Q1")["info"] == {"carried_state_day1": True,
                                                            "previous_quarter": "2025Q4"}

    state = parquet_rows(storage, "audit/quarters/2025Q4/end_state.parquet")
    assert [(s["serial"], str(s["last_date"]), s["smart_241_raw"]) for s in state] == [("D1", "2025-12-31", base)]
    q1 = tb_by_day(storage, "2026Q1")
    assert q1["2026-01-01"]["tb_written_mean"] == pytest.approx(gb2 * 512 / 1e12)   # from the carried state
    assert q1["2026-01-02"]["tb_written_mean"] == pytest.approx(1.024)
    assert q1["2026-01-01"]["tb_read_mean"] == 0.0
    quality = {(r["check"], r["column"]): r["count"] for r in rows_of(storage, "audit/quarters/2026Q1/quality.csv")}
    assert quality[("previous_end_state", "found")] == 1


def test_missing_previous_state_gives_null_not_error(cfg, zip_fetch):
    zips = {"2026Q1": crafted_zip("2026Q1", {
        "2026-01-01": [lba_row("2026-01-01", "D1", "SYNTH LBA", 10_000_000_000)],
        "2026-01-02": [lba_row("2026-01-02", "D1", "SYNTH LBA", 12_000_000_000)]})}
    run_data(cfg, fetch=zip_fetch(zips))
    storage = Storage.from_config(cfg)
    q1 = tb_by_day(storage, "2026Q1")
    assert q1["2026-01-01"]["tb_written_mean"] is None
    assert q1["2026-01-02"]["tb_written_mean"] == pytest.approx(1.024)
    quality = {(r["check"], r["column"]): r["count"] for r in rows_of(storage, "audit/quarters/2026Q1/quality.csv")}
    assert quality[("previous_end_state", "found")] == 0
    assert storage.read_marker("audit_2026Q1")["info"]["carried_state_day1"] is False


def test_file_name_date_wins_and_odd_row_dates_are_counted(cfg, zip_fetch):
    # Like data_Q1_2018.zip's 2018-02-25.csv: dates written M/D/YY, plus one row dated
    # another day (like the 6/15/19 row in 2019-06-17.csv) and one unreadable date.
    day = "2026-01-02"
    rows = [make_row(day, f"M{i}", "SYNTH X", slot=f"{i:04d}") for i in range(4)]
    for r in rows:
        r["date"] = "1/2/26"
    rows[1]["failure"] = "1"
    rows[2]["date"] = "12/31/25"        # readable, but not the file's day
    rows[3]["date"] = "2026/01/02"      # neither YYYY-MM-DD nor M/D/YY
    storage = run_crafted(cfg, zip_fetch, {day: rows})

    events = parquet_rows(storage, "audit/quarters/2026Q1/events.parquet")
    assert [(e["serial_number"], str(e["date"])) for e in events] == [("M1", day)]
    quality = {(r["check"], r["column"]): r["count"] for r in rows_of(storage, "audit/quarters/2026Q1/quality.csv")}
    assert quality[("date_unparseable", "date")] == 1
    assert quality[("date_not_file_day", "date")] == 1
    assert {str(d["date"]) for d in rows_of(storage, "audit/quarters/2026Q1/days.csv")} == {day}

    report = status_report(storage, [], cfg)
    assert "date checks (rows; the file-name date is used): 1 quarters checked, 1 with issues" in report
    assert re.search(r"2026Q1\s+1\s+1", report)

    members = crafted_zip("2026Q1", {day: rows})
    path = storage.work_dir / f"{day}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(members[f"data_Q1_2026/{day}.csv"])
    t = read_day(path, cfg["smart_ids"], "synthetic")
    assert {str(d) for d in t.column("date").to_pylist()} == {day}  # every row: the file's day
    assert t.num_rows == 4


def test_status_marks_quarters_audited_before_the_date_check(cfg):
    storage = Storage.from_config(cfg)
    for quarter, checks in (("2025Q4", "counter_reset,smart_241_raw,0\n"),
                            ("2026Q1", "date_unparseable,date,0\ndate_not_file_day,date,0\n")):
        path = storage.path(DURABLE, f"audit/quarters/{quarter}/quality.csv")
        path.parent.mkdir(parents=True)
        lines = "".join(f"{quarter},{c},synthetic\n" for c in checks.strip().split("\n"))
        path.write_text("quarter,check,column,count,data_source\n" + lines)
    report = status_report(storage, [], cfg)
    assert "1 quarters checked, 0 with issues" in report
    assert "not checked yet (read before the date check; pass 2 fills these in): 1 quarters (2025Q4)" in report

    # once pass 2 has read 2025Q4, its marker's counts fill the gap
    storage.marker_dir.mkdir(parents=True, exist_ok=True)
    storage.marker_path("ingest_2025Q4").write_text(
        json.dumps({"info": {"date_unparseable": 0, "date_not_file_day": 3}}))
    report = status_report(storage, [], cfg)
    assert "2 quarters checked, 1 with issues" in report
    assert re.search(r"2025Q4\s+0\s+3", report)
    assert "not checked yet" not in report


def test_counter_reset_gives_null_and_is_counted(cfg, zip_fetch):
    days = {"2026-01-01": 5_000_000_000, "2026-01-02": 1_000_000, "2026-01-03": 2_000_000}
    rows = {d: [lba_row(d, "R1", "SYNTH LBA", w, read=1_000_000_000 + i)] for i, (d, w) in enumerate(days.items())}
    run_data(cfg, fetch=zip_fetch({"2026Q1": crafted_zip("2026Q1", rows)}))
    storage = Storage.from_config(cfg)
    q1 = tb_by_day(storage, "2026Q1")
    assert q1["2026-01-02"]["tb_written_mean"] is None                 # 1e6 < 5e9: reset
    assert q1["2026-01-02"]["resets_written"] == 1
    assert q1["2026-01-02"]["tb_read_mean"] == pytest.approx(512 / 1e12)  # read kept rising
    assert q1["2026-01-03"]["tb_written_mean"] == pytest.approx(1_000_000 * 512 / 1e12)
    quality = {(r["check"], r["column"]): r["count"] for r in rows_of(storage, "audit/quarters/2026Q1/quality.csv")}
    assert quality[("counter_reset", "smart_241_raw")] == 1
    assert quality[("counter_reset", "smart_242_raw")] == 0


def test_audit_version_is_in_the_stage_hash(cfg, monkeypatch):
    sources = [{"quarter": "2026Q1", "url": "https://x/data_Q1_2026.zip", "kind": "listed"}]
    before = stage_hashes(audit.audit_stages(cfg, sources), cfg)["audit_2026Q1"]
    monkeypatch.setattr(audit, "AUDIT_VERSION", audit.AUDIT_VERSION + 1)
    assert stage_hashes(audit.audit_stages(cfg, sources), cfg)["audit_2026Q1"] != before
