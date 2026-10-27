import hashlib
import shutil

import polars as pl
import pytest

from diskctx.storage import (
    CACHE,
    DURABLE,
    Stage,
    StageError,
    Storage,
    append_result,
    read_results,
    results_keys,
    stage_hashes,
)


def make_stages(calls):
    """A durable stage ('audit') and a cache stage ('features') that reads it.

    `calls` counts how often each build runs.
    """

    def build_audit(work, storage):
        calls["audit"] += 1
        (work / "audit.csv").write_text("model,drives\nA,10\n")
        (work / "parquet").mkdir()
        write_parquet(work / "parquet" / "2026Q1.parquet", 50)

    def build_features(work, storage):
        calls["features"] += 1
        audit = storage.path(DURABLE, "audit.csv").read_text()  # durable input
        (work / "features.csv").write_text(audit.upper())

    audit = Stage("audit", DURABLE, ["audit.csv", "parquet"], build_audit)
    features = Stage("features", CACHE, ["features.csv"], build_features)
    return audit, features


@pytest.fixture
def calls():
    return {"audit": 0, "features": 0}


def test_layout_separates_modes_and_synthetic(tmp_path):
    smoke = Storage(tmp_path / "r", tmp_path / "l", "smoke", "backblaze")
    full = Storage(tmp_path / "r", tmp_path / "l", "full", "backblaze")
    synth = Storage(tmp_path / "r", tmp_path / "l", "smoke", "synthetic")
    assert smoke.run_dir == tmp_path / "r" / "smoke"
    assert full.run_dir == tmp_path / "r" / "full"
    assert synth.run_dir == tmp_path / "r" / "_synthetic" / "smoke"
    assert smoke.cache_dir == tmp_path / "l" / "smoke" / "cache"
    assert smoke.path(DURABLE, "a") != smoke.path(CACHE, "a")


def test_from_config_uses_synthetic_folder(cfg):
    storage = Storage.from_config(cfg)
    assert "_synthetic" in storage.run_dir.parts


def test_stage_builds_copies_and_marks(cfg, calls):
    storage = Storage.from_config(cfg)
    audit, _ = make_stages(calls)
    assert storage.run_stage(audit, "h1") == "built"
    assert storage.path(DURABLE, "audit.csv").read_text().startswith("model")
    assert pl.read_parquet(storage.path(DURABLE, "parquet/2026Q1.parquet")).height == 50
    marker = storage.read_marker("audit")
    assert marker["config_hash"] == "h1"
    assert marker["data_source"] == "synthetic"
    assert marker["timestamp"]
    assert not (storage.work_dir / "audit").exists()  # scratch cleaned up


def test_done_stage_is_skipped(cfg, calls):
    storage = Storage.from_config(cfg)
    audit, _ = make_stages(calls)
    storage.run_stage(audit, "h1")
    assert storage.run_stage(audit, "h1") == "skipped"
    assert calls["audit"] == 1


def test_changed_hash_reruns(cfg, calls):
    storage = Storage.from_config(cfg)
    audit, _ = make_stages(calls)
    storage.run_stage(audit, "h1")
    assert storage.stage_state(audit, "h2") == "stale"
    assert storage.run_stage(audit, "h2") == "built"
    assert calls["audit"] == 2
    assert storage.read_marker("audit")["config_hash"] == "h2"


def test_delete_cache_tier_rebuilds_cache_and_skips_durable(cfg, calls):
    storage = Storage.from_config(cfg)
    audit, features = make_stages(calls)
    assert storage.run_stage(audit, "h") == "built"
    assert storage.run_stage(features, "h") == "built"

    # A new Colab session: local disk (cache tier) is gone, ROOT is kept.
    shutil.rmtree(storage.cache_dir)
    assert storage.stage_state(features, "h") == "missing"

    assert storage.run_stage(audit, "h") == "skipped"
    assert storage.run_stage(features, "h") == "rebuilt"
    assert calls == {"audit": 1, "features": 2}
    assert storage.path(CACHE, "features.csv").read_text() == "MODEL,DRIVES\nA,10\n"


def test_failed_build_leaves_no_marker(cfg):
    storage = Storage.from_config(cfg)

    def crash(work, storage):
        (work / "out.csv").write_text("half")
        raise RuntimeError("Colab disconnected")

    stage = Stage("crashy", DURABLE, ["out.csv"], crash)
    with pytest.raises(RuntimeError):
        storage.run_stage(stage, "h")
    assert storage.read_marker("crashy") is None
    assert storage.stage_state(stage, "h") == "pending"


def test_failed_rebuild_removes_old_marker(cfg, calls):
    storage = Storage.from_config(cfg)
    audit, _ = make_stages(calls)
    storage.run_stage(audit, "h1")

    def crash(work, storage):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        storage.run_stage(Stage("audit", DURABLE, audit.outputs, crash), "h2")
    assert storage.read_marker("audit") is None


def test_missing_output_is_an_error(cfg):
    storage = Storage.from_config(cfg)
    stage = Stage("lazy", DURABLE, ["out.csv"], lambda work, storage: None)
    with pytest.raises(StageError, match="out.csv"):
        storage.run_stage(stage, "h")
    assert storage.read_marker("lazy") is None


def test_rebuild_replaces_old_folder_output(cfg):
    storage = Storage.from_config(cfg)

    def build(work, storage):
        (work / "parts").mkdir()
        write_parquet(work / "parts" / "new.parquet", 1)

    old = storage.path(DURABLE, "parts")
    old.mkdir(parents=True)
    write_parquet(old / "old.parquet", 1)
    storage.run_stage(Stage("parts", DURABLE, ["parts"], build), "h")
    assert sorted(p.name for p in old.iterdir()) == ["new.parquet"]


# --- results CSV -------------------------------------------------------------------


def row(key, source="synthetic", far=0.1):
    return {"run_key": key, "data_source": source, "model": "S", "far": far}


def test_results_append_and_skip_existing_key(cfg):
    path = Storage.from_config(cfg).results_path
    assert append_result(path, row("k1")) is True
    assert append_result(path, row("k2")) is True
    assert append_result(path, row("k1", far=0.9)) is False
    assert results_keys(path) == {"k1", "k2"}
    rows = read_results(path, allow_synthetic=True)
    assert [r["far"] for r in rows] == ["0.1", "0.1"]


def test_results_row_needs_key_and_source(tmp_path):
    with pytest.raises(ValueError, match="data_source"):
        append_result(tmp_path / "r.csv", {"run_key": "k"})


def test_results_columns_must_match(tmp_path):
    path = tmp_path / "r.csv"
    append_result(path, row("k1"))
    with pytest.raises(ValueError, match="header"):
        append_result(path, {"run_key": "k2", "data_source": "synthetic"})


def test_reporting_refuses_synthetic_rows(tmp_path):
    path = tmp_path / "r.csv"
    append_result(path, row("k1", source="backblaze"))
    assert len(read_results(path)) == 1
    append_result(path, row("k2", source="synthetic"))
    with pytest.raises(ValueError, match="synthetic"):
        read_results(path)


# --- hash propagation -----------------------------------------------------------------


def chain(calls):
    """audit -> select -> ingest -> features (cache); 'unrelated' depends on nothing."""

    def builder(name):
        def build(work, storage):
            calls[name] = calls.get(name, 0) + 1
            (work / f"{name}.csv").write_text(name)
        return build

    return [
        Stage("audit", DURABLE, ["audit.csv"], builder("audit"), keys=["start_quarter", "end_quarter"]),
        Stage("select", DURABLE, ["select.csv"], builder("select"), keys=["models"], deps=["audit"]),
        Stage("ingest", DURABLE, ["ingest.csv"], builder("ingest"), keys=[], deps=["select"]),
        Stage("features", CACHE, ["features.csv"], builder("features"), keys=["horizons"],
              deps=["ingest"]),
        Stage("unrelated", DURABLE, ["unrelated.csv"], builder("unrelated"), keys=["seed"]),
    ]


def test_upstream_change_reruns_downstream_only(cfg):
    storage = Storage.from_config(cfg)
    stages = chain({})
    first = storage.run_stages(stages, cfg)
    assert set(first.values()) == {"built"}

    changed = dict(cfg, models=["SYNTH HDD14TEY"])
    second = storage.run_stages(stages, changed)
    assert second == {
        "audit": "skipped",
        "select": "built",
        "ingest": "built",      # hashes no config keys itself; reruns through its dep
        "features": "built",
        "unrelated": "skipped",
    }
    assert storage.run_stages(stages, changed) == dict.fromkeys(second, "skipped")


def test_downstream_change_does_not_rerun_upstream(cfg):
    storage = Storage.from_config(cfg)
    stages = chain({})
    storage.run_stages(stages, cfg)
    result = storage.run_stages(stages, dict(cfg, horizons=[7]))
    assert [n for n, r in result.items() if r != "skipped"] == ["features"]


def test_stage_hashes_need_deps_first(cfg):
    with pytest.raises(StageError, match="earlier"):
        stage_hashes(list(reversed(chain({}))), cfg)


# --- marker contents and verification on resume --------------------------------------


def write_parquet(path, n):
    pl.DataFrame({"x": list(range(n))}).write_parquet(path, compression="zstd")


def parquet_stage(calls):
    def build(work, storage):
        calls["n"] = calls.get("n", 0) + 1
        (work / "audit.csv").write_text("model,drives\nA,10\n")
        (work / "parquet").mkdir()
        write_parquet(work / "parquet" / "2026Q1.parquet", 1000)
    return Stage("ingest", DURABLE, ["audit.csv", "parquet"], build)


def test_marker_records_path_size_and_checksum_or_rows(cfg):
    storage = Storage.from_config(cfg)
    storage.run_stage(parquet_stage({}), "h")
    files = {f["path"]: f for f in storage.read_marker("ingest")["files"]}
    assert sorted(files) == ["audit.csv", "parquet/2026Q1.parquet"]

    csv_path = storage.path(DURABLE, "audit.csv")
    assert files["audit.csv"]["bytes"] == csv_path.stat().st_size
    assert files["audit.csv"]["sha256"] == hashlib.sha256(csv_path.read_bytes()).hexdigest()

    pq_entry = files["parquet/2026Q1.parquet"]
    assert pq_entry["rows"] == 1000
    assert pq_entry["bytes"] == storage.path(DURABLE, "parquet/2026Q1.parquet").stat().st_size


def test_half_copied_parquet_is_caught_on_resume(cfg):
    storage = Storage.from_config(cfg)
    calls = {}
    stage = parquet_stage(calls)
    storage.run_stage(stage, "h")

    p = storage.path(DURABLE, "parquet/2026Q1.parquet")
    p.write_bytes(p.read_bytes()[: p.stat().st_size // 2])  # copy broke off halfway
    assert storage.stage_state(stage, "h") == "mismatch"
    assert storage.run_stage(stage, "h") == "rebuilt"
    assert calls["n"] == 2
    assert storage.stage_state(stage, "h") == "done"


def test_same_size_corruption_is_caught_by_checksum(cfg):
    storage = Storage.from_config(cfg)
    stage = parquet_stage({})
    storage.run_stage(stage, "h")
    p = storage.path(DURABLE, "audit.csv")
    p.write_text(p.read_text().replace("10", "99"))
    assert storage.stage_state(stage, "h") == "mismatch"


def test_parquet_with_other_row_count_is_caught(cfg):
    storage = Storage.from_config(cfg)
    stage = parquet_stage({})
    storage.run_stage(stage, "h")
    write_parquet(storage.path(DURABLE, "parquet/2026Q1.parquet"), 999)
    assert storage.stage_state(stage, "h") == "mismatch"


def test_extra_file_in_folder_output_is_caught(cfg):
    storage = Storage.from_config(cfg)
    stage = parquet_stage({})
    storage.run_stage(stage, "h")
    write_parquet(storage.path(DURABLE, "parquet/2026Q2.parquet"), 5)
    assert storage.stage_state(stage, "h") == "mismatch"
