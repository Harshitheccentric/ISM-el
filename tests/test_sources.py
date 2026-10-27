import pytest

from diskctx.audit import read_selection
from diskctx.config import quarters_between
from diskctx.pipeline import data_stages, run_data
from diskctx.sources import (
    SourceError,
    base_url,
    derived_url,
    listed_urls,
    local_zip,
    quarter_of,
    read_sources,
    resolve_sources,
    sources_stage,
)
from diskctx.status import status_report
from diskctx.storage import Storage
from diskctx.synthetic import SyntheticFleet

BASE = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data"


def test_listed_urls_come_from_the_doc():
    listed = listed_urls()
    assert sorted(listed) == quarters_between("2024Q2", "2026Q2")
    assert listed["2026Q1"] == f"{BASE}/data_Q1_2026.zip"


def test_derived_url_uses_the_listed_base():
    base = base_url(listed_urls())
    assert base == BASE
    assert derived_url(base, "2016Q1") == f"{BASE}/data_Q1_2016.zip"
    assert quarter_of(derived_url(base, "2016Q1")) == "2016Q1"


def test_base_must_be_unique(tmp_path):
    doc = tmp_path / "sources.md"
    doc.write_text("https://a.example/x/data_Q1_2026.zip\nhttps://b.example/y/data_Q4_2025.zip\n")
    with pytest.raises(SourceError, match="do not share one base"):
        base_url(listed_urls(doc))


def test_listed_derived_missing():
    asked = []

    def head(url):
        asked.append(url)
        return url.endswith("data_Q1_2024.zip")

    rows = resolve_sources(["2023Q4", "2024Q1", "2024Q2"], listed_urls(), head)
    assert [(r["quarter"], r["kind"]) for r in rows] == [
        ("2023Q4", "missing"), ("2024Q1", "derived"), ("2024Q2", "listed")]
    # listed URLs are never HEAD-checked; derived ones always are
    assert asked == [f"{BASE}/data_Q4_2023.zip", f"{BASE}/data_Q1_2024.zip"]


def test_sources_stage_logs_every_quarter_and_status_shows_missing(cfg):
    cfg.update(start_quarter="2023Q3", end_quarter="2024Q2")
    storage = Storage.from_config(cfg)
    storage.run_stages([sources_stage(cfg, head=lambda url: False)], cfg)
    rows = read_sources(storage)
    assert [r["kind"] for r in rows] == ["missing", "missing", "missing", "listed"]
    assert {r["data_source"] for r in rows} == {"synthetic"}
    report = status_report(storage, data_stages(cfg), cfg)
    assert "listed 1  derived 0  missing 3" in report
    assert "missing quarters (skipped): 2023Q3, 2023Q4, 2024Q1" in report


def test_missing_quarter_is_skipped_not_fatal(cfg, fleet_fetch):
    cfg.update(start_quarter="2024Q1", end_quarter="2024Q2")
    fleet = SyntheticFleet(cfg["seed"], "2024Q1", "2024Q2")
    fetch = fleet_fetch(fleet)
    results = run_data(cfg, head=lambda url: False, fetch=fetch)
    assert set(fetch.calls) == {"2024Q2"}               # the missing quarter is never fetched
    assert "audit_2024Q1" not in results
    assert results["audit_2024Q2"] == "built"
    assert read_selection(Storage.from_config(cfg))["missing_quarters"] == ["2024Q1"]


def test_local_zip_reuses_a_complete_copy(cfg):
    storage = Storage.from_config(cfg)
    calls = []

    def fetch(url, dest):
        calls.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"zip")

    url = f"{BASE}/data_Q1_2026.zip"
    assert local_zip(storage, url, fetch).name == "data_Q1_2026.zip"
    local_zip(storage, url, fetch)
    assert calls == [url]
