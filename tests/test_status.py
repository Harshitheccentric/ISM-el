import json

import pytest

from diskctx.status import (
    BudgetError,
    check_budget,
    estimate_sizes,
    format_table,
    log_download,
    read_download_log,
    status_report,
)
from diskctx.storage import CACHE, DURABLE, Stage, Storage


@pytest.fixture
def storage(cfg):
    return Storage.from_config(cfg)


def test_download_log_roundtrip_and_replace(storage):
    log_download(storage, "2026Q1", 1000, 100, 200)
    log_download(storage, "2025Q4", 900, 90, 180)
    log_download(storage, "2026Q1", 1100, 110, 210)  # re-measured
    log = read_download_log(storage)
    assert sorted(log) == ["2025Q4", "2026Q1"]
    assert log["2026Q1"]["zip_bytes"] == 1100
    assert log["2026Q1"]["data_source"] == "synthetic"


def test_estimate_uses_defaults_when_nothing_measured(cfg):
    sizes = estimate_sizes({}, cfg["budget"])
    assert sizes == {"zip_bytes": 1000, "largest_day_bytes": 100, "parquet_bytes": 200}


def test_estimate_is_max_of_default_and_1_5x_largest(cfg, storage):
    log_download(storage, "2025Q3", 500, 300, 50)
    log_download(storage, "2025Q4", 800, 20, 70)
    sizes = estimate_sizes(read_download_log(storage), cfg["budget"])
    # zip: max(1000, 1.5*800); day: max(100, 1.5*300); parquet: max(200, 1.5*70)
    assert sizes == {"zip_bytes": 1200, "largest_day_bytes": 450, "parquet_bytes": 200}


def test_small_later_quarter_does_not_lower_estimate(cfg, storage):
    log_download(storage, "2026Q1", 4000, 100, 100)
    log_download(storage, "2016Q1", 10, 10, 10)  # a tiny old quarter, measured last
    sizes = estimate_sizes(read_download_log(storage), cfg["budget"])
    assert sizes["zip_bytes"] == 6000


def test_budget_fits(cfg, storage):
    # defaults: local needs 1000+100+200 (+100 margin), ROOT needs 200 (+100 margin)
    sizes = check_budget(storage, "2026Q1", cfg["budget"], local_free=1400, root_free=300,
                         shared_disk=False)
    assert sizes["parquet_bytes"] == 200


def test_budget_stops_on_local_disk(cfg, storage):
    with pytest.raises(BudgetError, match="2026Q1: local disk"):
        check_budget(storage, "2026Q1", cfg["budget"], local_free=1399, root_free=10**9,
                     shared_disk=False)


def test_budget_stops_on_root(cfg, storage):
    with pytest.raises(BudgetError, match="2026Q1: ROOT"):
        check_budget(storage, "2026Q1", cfg["budget"], local_free=10**9, root_free=299,
                     shared_disk=False)


def test_budget_shared_disk_adds_both_needs(cfg, storage):
    # one disk must hold local 1300 + ROOT 200 + margin 100
    check_budget(storage, "2026Q1", cfg["budget"], local_free=1600, root_free=1600, shared_disk=True)
    with pytest.raises(BudgetError):
        check_budget(storage, "2026Q1", cfg["budget"], local_free=1599, root_free=1599,
                     shared_disk=True)


def test_budget_uses_measurements(cfg, storage):
    log_download(storage, "2025Q4", 10**12, 1, 1)  # a huge measured zip
    with pytest.raises(BudgetError, match="largest measured"):
        check_budget(storage, "2026Q1", cfg["budget"], local_free=10**9, root_free=10**9,
                     shared_disk=False)


def test_budget_measures_real_disk(cfg, storage):
    # Without overrides it reads real free space; tiny defaults must fit.
    check_budget(storage, "2026Q1", cfg["budget"])


def test_format_table():
    text = format_table([{"a": "x", "bb": 1}, {"a": "long", "bb": 22}])
    lines = text.splitlines()
    assert lines[0].split() == ["a", "bb"]
    assert lines[3].split() == ["long", "22"]
    assert format_table([]) == "(none)"


def build_out(work, storage):
    (work / "out.csv").write_text("x")


def report_stages():
    return [
        Stage("audit", DURABLE, ["out.csv"], build_out, keys=["start_quarter"]),
        Stage("features", CACHE, ["out.csv"], build_out, keys=[], deps=["audit"]),
        Stage("splits", DURABLE, ["splits.csv"],
              lambda w, s: (w / "splits.csv").write_text("x"), keys=["seed"]),
        Stage("models", DURABLE, ["m.json"], build_out, keys=[]),
    ]


def stage_states(report):
    names = {"audit", "features", "splits", "models"}
    return {line.split()[0]: line.split()[2] for line in report.splitlines()
            if line.split()[:1] and line.split()[0] in names}


def test_status_report_shows_states_space_and_sizes(cfg, storage):
    stages = report_stages()
    storage.run_stages(stages[:2], cfg)
    storage.run_stages([stages[2]], dict(cfg, seed=1))  # later run with seed 42 -> stale
    storage.path(CACHE, "out.csv").unlink()
    log_download(storage, "2026Q1", 2_500_000_000, 150_000_000, 300_000_000)

    report = status_report(storage, stages, cfg)
    assert stage_states(report) == {
        "audit": "done", "features": "missing", "splits": "stale", "models": "pending",
    }
    assert "free ROOT" in report and "free local" in report
    assert "2026Q1" in report and "2.50 GB" in report
    assert "not selected yet" in report


def test_status_report_shows_audit_selection(cfg, storage):
    path = storage.path(DURABLE, "audit/selection.json")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"models": [
        {"model": "SYNTH HDD14TEY", "failures": 120, "coverage_241": 0.99, "window": "2023Q2-2026Q2"},
    ]}))
    report = status_report(storage, report_stages(), cfg)
    assert "selected by audit" in report
    assert "SYNTH HDD14TEY" in report and "120" in report and "2023Q2-2026Q2" in report


def test_status_report_config_models_override_audit(cfg, storage):
    path = storage.path(DURABLE, "audit/selection.json")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"models": [{"model": "SYNTH HDD14TEY"}]}))
    report = status_report(storage, report_stages(), dict(cfg, models=["SYNTH HDD14TA"]))
    assert "config allowlist (overrides audit): SYNTH HDD14TA" in report
    assert "SYNTH HDD14TEY" not in report
