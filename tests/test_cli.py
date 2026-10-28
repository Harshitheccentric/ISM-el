import json
import re
import shutil

import pytest
import yaml

import diskctx.sources as sources
from diskctx.__main__ import main
from diskctx.pipeline import STAGE_GROUPS, data_stages, run_data
from diskctx.storage import Storage, stage_hashes
from diskctx.synthetic import SyntheticFleet


@pytest.fixture
def cli_config(cfg, tmp_path, monkeypatch, fleet_fetch):
    """A synthetic config file, DISKCTX_LOCAL_ROOT set, and downloads replaced by a fleet."""
    path = tmp_path / "synthetic.yaml"
    path.write_text(yaml.safe_dump({k: v for k, v in cfg.items() if k not in ("root", "local_dir")}
                                   | {"root": "unused/root", "local_dir": "unused/local"}))
    monkeypatch.setenv("DISKCTX_LOCAL_ROOT", str(tmp_path / "diskctx_data"))
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1"))
    monkeypatch.setattr(sources, "download", fetch)
    return path, fetch


def test_cli_runs_pass_1_only_and_resumes(cli_config, tmp_path, capsys):
    path, fetch = cli_config
    assert main(["run", "--config", str(path), "--stages", "sources,audit"]) == 0
    out = capsys.readouterr().out
    assert "audit_2026Q1: built" in out
    assert "select: " not in out.split("\n\n")[0]          # select did not run
    assert re.search(r"audit_2026Q1\s+durable\s+done", out)   # status table at the end
    run_dir = tmp_path / "diskctx_data" / "root" / "_synthetic" / "smoke"
    assert (run_dir / "_markers" / "audit_2026Q1.json").exists()
    assert not (run_dir / "_markers" / "select.json").exists()

    assert main(["run", "--config", str(path), "--stages", "sources,audit"]) == 0
    assert "audit_2026Q1: skipped" in capsys.readouterr().out
    assert fetch.calls == ["2026Q1"]


def test_cli_rejects_unknown_stage(cli_config):
    path, _ = cli_config
    with pytest.raises(SystemExit):
        main(["run", "--config", str(path), "--stages", "sources,bogus"])


def test_cli_stops_cleanly_on_budget(cli_config, capsys, monkeypatch):
    path, fetch = cli_config
    text = yaml.safe_load(path.read_text())
    text["budget"]["margin_bytes"] = 10**18
    path.write_text(yaml.safe_dump(text))
    assert main(["run", "--config", str(path), "--stages", "sources,audit"]) == 1
    out = capsys.readouterr().out
    assert "STOPPED: Stopping before 2026Q1" in out
    assert re.search(r"audit_2026Q1\s+durable\s+pending", out)
    assert fetch.calls == []


def test_moved_root_skips_finished_stages(cfg, tmp_path, fleet_fetch):
    """Markers hold only paths relative to ROOT, so copying ROOT elsewhere (e.g. to Drive)
    keeps every finished stage done, even with a fresh, empty local dir."""
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1"))
    first = run_data(cfg, stages=STAGE_GROUPS, fetch=fetch)
    assert [first[s] for s in ("audit_2026Q1", "select", "ingest_2026Q1", "labels", "splits")] == ["built"] * 5

    for marker in (Storage.from_config(cfg).marker_dir).iterdir():
        text = marker.read_text()
        assert str(tmp_path) not in text
        assert all(not f["path"].startswith("/") for f in json.loads(text)["files"])

    moved = dict(cfg, root=str(tmp_path / "drive" / "root"), local_dir=str(tmp_path / "new_local"))
    shutil.copytree(cfg["root"], moved["root"])
    shutil.rmtree(cfg["root"])
    storage = Storage.from_config(moved)
    stages = data_stages(moved)
    hashes = stage_hashes(stages, moved)
    assert {storage.stage_state(s, hashes[s.name]) for s in stages} == {"done"}
    assert set(run_data(moved, stages=STAGE_GROUPS, fetch=fetch).values()) == {"skipped"}
    assert fetch.calls == ["2026Q1", "2026Q1"]            # pass 1 + pass 2, never again
