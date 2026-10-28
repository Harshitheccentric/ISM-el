import io
import zipfile

import pytest
from conftest import write_zip

import diskctx.sources as sources
from diskctx.audit import read_selection
from diskctx.config import quarters_between
from diskctx.pipeline import data_stages, run_data
from diskctx.sources import (
    DownloadError,
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
        write_zip(dest, {"data_Q1_2026/2026-01-01.csv": "x\n"})

    url = f"{BASE}/data_Q1_2026.zip"
    assert local_zip(storage, url, fetch).name == "data_Q1_2026.zip"
    local_zip(storage, url, fetch)
    assert calls == [url]


def test_local_zip_replaces_a_cut_short_file(cfg):
    storage = Storage.from_config(cfg)
    url = f"{BASE}/data_Q1_2023.zip"
    good = write_zip(storage.download_dir / "good.zip", {"data_Q1_2023/2023-01-01.csv": "x\n"})
    bad = storage.download_dir / "data_Q1_2023.zip"
    bad.write_bytes(good.read_bytes()[:20])        # the start of a zip, cut short
    calls = []

    def fetch(url, dest):
        calls.append(url)
        dest.write_bytes(good.read_bytes())

    assert zipfile.is_zipfile(local_zip(storage, url, fetch))
    assert calls == [url]


def test_local_zip_refuses_a_fetch_that_is_not_a_zip(cfg):
    storage = Storage.from_config(cfg)

    def fetch(url, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"<html>error page</html>")

    with pytest.raises(DownloadError, match="not give a readable zip"):
        local_zip(storage, f"{BASE}/data_Q1_2023.zip", fetch)
    assert list(storage.download_dir.iterdir()) == []


BODY = bytes(range(256)) * 4    # a 1,024-byte "zip"


class FakeResponse(io.BytesIO):
    """What urlopen returns: status, headers and a byte stream, used as a context manager."""

    def __init__(self, data, status, headers):
        super().__init__(data)
        self.status = status
        self.headers = headers


class FakeServer:
    """Serves BODY like Backblaze: honours Range requests with 206 + Content-Range.
    cut[i] = how many bytes reply i sends before the connection drops (None = all)."""

    def __init__(self, cut, honour_range=True):
        self.cut = list(cut)
        self.honour_range = honour_range
        self.ranges = []

    def __call__(self, request, timeout):
        rng = request.get_header("Range")
        self.ranges.append(rng)
        start = int(rng[len("bytes="):-1]) if rng and self.honour_range else 0
        rest = BODY[start:]
        keep = self.cut.pop(0)
        data = rest if keep is None else rest[:keep]
        if start:
            headers = {"Content-Range": f"bytes {start}-{len(BODY) - 1}/{len(BODY)}"}
            return FakeResponse(data, 206, headers)
        return FakeResponse(data, 200, {"Content-Length": str(len(BODY))})


@pytest.fixture
def no_wait(monkeypatch):
    monkeypatch.setattr(sources.time, "sleep", lambda s: None)


def test_download_resumes_after_a_drop(tmp_path, monkeypatch, capsys, no_wait):
    server = FakeServer([300, None])
    monkeypatch.setattr(sources.urllib.request, "urlopen", server)
    dest = tmp_path / "q.zip"
    sources.download("https://x/data_Q1_2023.zip", dest)
    assert dest.read_bytes() == BODY
    assert server.ranges == [None, "bytes=300-"]        # only the rest was requested
    assert "got 300 of 1024 bytes (try 1 of 5)" in capsys.readouterr().out
    assert not (tmp_path / "q.zip.part").exists()


def test_download_resumes_a_partial_file_from_an_earlier_run(tmp_path, monkeypatch, no_wait):
    (tmp_path / "q.zip.part").write_bytes(BODY[:600])
    server = FakeServer([None])
    monkeypatch.setattr(sources.urllib.request, "urlopen", server)
    sources.download("https://x/data_Q1_2023.zip", tmp_path / "q.zip")
    assert (tmp_path / "q.zip").read_bytes() == BODY
    assert server.ranges == ["bytes=600-"]


def test_download_starts_over_if_the_server_ignores_the_range(tmp_path, monkeypatch, no_wait):
    (tmp_path / "q.zip.part").write_bytes(BODY[:600])
    monkeypatch.setattr(sources.urllib.request, "urlopen", FakeServer([None], honour_range=False))
    sources.download("https://x/data_Q1_2023.zip", tmp_path / "q.zip")
    assert (tmp_path / "q.zip").read_bytes() == BODY


def test_download_gives_up_but_keeps_the_partial_file(tmp_path, monkeypatch, no_wait):
    waits = []
    monkeypatch.setattr(sources.time, "sleep", waits.append)
    monkeypatch.setattr(sources.urllib.request, "urlopen", FakeServer([100] * 5))
    with pytest.raises(DownloadError, match="after 5 tries"):
        sources.download("https://x/data_Q1_2023.zip", tmp_path / "q.zip")
    assert waits == [30, 60, 90, 120]
    assert not (tmp_path / "q.zip").exists()
    assert (tmp_path / "q.zip.part").read_bytes() == BODY[:500]   # kept for the next run
