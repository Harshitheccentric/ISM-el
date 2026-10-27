from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import csv_text, write_zip

from diskctx.ingest import arrow_schema, ingest_quarter, ingest_stages, output_columns, read_day
from diskctx.pipeline import run_data
from diskctx.status import BudgetError, read_download_log
from diskctx.storage import Storage, stage_hashes
from diskctx.synthetic import MODEL_BOOT, MODEL_LBA, MODEL_NO_LBA, SyntheticFleet

FIXTURE = Path(__file__).parent / "fixtures" / "sample_2026-01-01.csv"
SMART_IDS = [5, 9, 187, 188, 194, 197, 198, 241, 242]
POD_COLUMNS = ["datacenter", "cluster_id", "vault_id", "pod_id", "pod_slot_num"]
OLD_FORMAT_DROP = [*POD_COLUMNS, "is_legacy_format",
                   "smart_241_normalized", "smart_241_raw", "smart_242_normalized", "smart_242_raw"]


def by_model(table, column):
    out = {}
    for model, value in zip(table.column("model").to_pylist(), table.column(column).to_pylist()):
        out.setdefault(model, []).append(value)
    return out


# --- the real fixture -------------------------------------------------------------------


def test_fixture_loads_with_explicit_types():
    t = read_day(FIXTURE, SMART_IDS, "backblaze")
    assert t.schema == arrow_schema(SMART_IDS)
    assert t.column_names == output_columns(SMART_IDS)
    assert t.schema.field("serial_number").type == pa.string()
    assert t.schema.field("smart_241_raw").type == pa.int64()
    assert t.schema.field("date").type == pa.date32()
    # zero-padded IDs survive as strings
    assert all(len(s) == 4 for s in t.column("pod_slot_num").to_pylist())
    assert "000" in t.column("cluster_id").to_pylist()
    assert all(len(p) == 2 for p in t.column("pod_id").to_pylist())
    assert set(t.column("data_source").to_pylist()) == {"backblaze"}


def test_fixture_boot_drives_dropped():
    t = read_day(FIXTURE, SMART_IDS, "backblaze")
    assert sum(1 for _ in open(FIXTURE)) - 1 == 36
    assert t.num_rows == 17                                   # 19 of 36 dropped
    counts = {m: len(v) for m, v in by_model(t, "serial_number").items()}
    assert counts == {"TOSHIBA MG07ACA14TA": 12, "TOSHIBA MG07ACA14TEY": 5}


def test_fixture_241_242_and_197_per_model_string():
    t = read_day(FIXTURE, SMART_IDS, "backblaze")
    for col in ("smart_241_raw", "smart_242_raw"):
        values = by_model(t, col)
        assert all(v is not None for v in values["TOSHIBA MG07ACA14TEY"])
        assert all(v is None for v in values["TOSHIBA MG07ACA14TA"])
    assert all(v is None for v in by_model(t, "smart_197_raw")["TOSHIBA MG07ACA14TEY"])
    assert max(t.column("smart_241_raw").drop_null().to_pylist()) > 2**31   # BIGINT needed


# --- pod key ------------------------------------------------------------------------


def pod_key_checks(t):
    keys = t.column("pod_key").to_pylist()
    tuples = list(zip(*(t.column(c).to_pylist() for c in POD_COLUMNS[:4])))
    assert keys == ["|".join(tup) for tup in tuples]
    assert len(set(keys)) == len(set(tuples))
    # one serial per (pod_key, slot, date)
    places = list(zip(keys, t.column("pod_slot_num").to_pylist(), t.column("date").to_pylist()))
    assert len(places) == len(set(places))


def test_pod_key_on_fixture():
    pod_key_checks(read_day(FIXTURE, SMART_IDS, "backblaze"))


def test_pod_key_tells_apart_same_pod_id_in_two_vaults(tmp_path):
    fleet = SyntheticFleet(7, "2026Q1", "2026Q1")
    (tmp_path / "day.csv").write_text(fleet.day_csv(fleet.days[0]))
    t = read_day(tmp_path / "day.csv", SMART_IDS, "synthetic")
    pod_key_checks(t)
    keys_for_02 = {k for k, p in zip(t.column("pod_key").to_pylist(), t.column("pod_id").to_pylist()) if p == "02"}
    assert keys_for_02 == {"sac0|000|1154|02", "ams5|031|2017|02"}


# --- old format, nested folders ------------------------------------------------------


def test_old_format_quarter_missing_columns_become_null(tmp_path):
    fleet = SyntheticFleet(3, "2016Q1", "2016Q1")
    days = fleet.days
    members = {f"outer/2016/data_Q1_2016/{d.isoformat()}.csv": fleet.day_csv(d, OLD_FORMAT_DROP) for d in days}
    members["__MACOSX/outer/._2016-01-01.csv"] = "junk"
    zip_path = write_zip(tmp_path / "data_Q1_2016.zip", members)

    out = tmp_path / "out" / "2016Q1.parquet"
    largest = ingest_quarter(zip_path, out, tmp_path, SMART_IDS, None, "synthetic")
    t = pq.read_table(out)
    assert largest == max(len(text.encode()) for name, text in members.items() if name.endswith(".csv")
                          and "__MACOSX" not in name)
    assert pq.ParquetFile(out).num_row_groups == len(days) == 91      # 2016 is a leap year
    assert not (tmp_path / "day.csv").exists()

    data_drives = [d for d in fleet.drives if d.model != MODEL_BOOT]
    expected_rows = sum(1 for d in data_drives for day in days if d.first_day <= day <= d.last_day)
    assert t.num_rows == expected_rows                                  # data drives kept, boot dropped
    assert MODEL_BOOT not in set(t.column("model").to_pylist())
    for col in ["smart_241_raw", "smart_242_raw", *POD_COLUMNS, "pod_key"]:
        assert t.column(col).null_count == t.num_rows, col
    assert t.column("smart_9_raw").null_count == 0


# --- pass 2 stage ---------------------------------------------------------------------


def test_pass2_allowlist_writes_selected_models_only(cfg, fleet_fetch):
    cfg["models"] = [MODEL_LBA]
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1"))
    results = run_data(cfg, fetch=fetch)
    assert results == {"sources": "built", "ingest_2026Q1": "built"}   # allowlist skips audit + select

    storage = Storage.from_config(cfg)
    path = storage.run_dir / "parquet" / "2026Q1.parquet"
    pf = pq.ParquetFile(path)
    t = pf.read()
    assert set(t.column("model").to_pylist()) == {MODEL_LBA}
    assert t.column_names == output_columns(SMART_IDS)
    assert pf.metadata.row_group(0).column(0).compression == "ZSTD"
    assert pf.num_row_groups == 90
    assert set(t.column("data_source").to_pylist()) == {"synthetic"}

    log = read_download_log(storage)["2026Q1"]
    assert log["parquet_bytes"] == path.stat().st_size
    assert log["zip_bytes"] > 0 and log["largest_day_bytes"] > 0
    assert list(storage.download_dir.iterdir()) == []                   # zip deleted

    assert run_data(cfg, fetch=fetch)["ingest_2026Q1"] == "skipped"
    assert fetch.calls == ["2026Q1"]


def test_pass2_keep_zips_reuses_the_zip(cfg, fleet_fetch):
    cfg["models"] = [MODEL_NO_LBA]
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1"))
    run_data(cfg, fetch=fetch, keep_zips=True)
    storage = Storage.from_config(cfg)
    assert [p.name for p in storage.download_dir.iterdir()] == ["data_Q1_2026.zip"]
    (storage.marker_dir / "ingest_2026Q1.json").unlink()
    run_data(cfg, fetch=fetch, keep_zips=True)
    assert fetch.calls == ["2026Q1"]


def test_ingest_hash_follows_models_not_range(cfg):
    sources = [{"quarter": "2026Q1", "url": "https://x/data_Q1_2026.zip", "kind": "listed"}]

    def ingest_hash(c, models):
        return stage_hashes(ingest_stages(c, sources, models, ["2026Q1"]), c)["ingest_2026Q1"]

    base = ingest_hash(cfg, [MODEL_LBA])
    assert ingest_hash({**cfg, "start_quarter": "2025Q1"}, [MODEL_LBA]) == base
    assert ingest_hash(cfg, [MODEL_LBA, MODEL_NO_LBA]) != base
    assert ingest_hash({**cfg, "smart_ids": [5, 9]}, [MODEL_LBA]) != base


def test_budget_stops_before_download(cfg, fleet_fetch):
    cfg["models"] = [MODEL_LBA]
    cfg["budget"]["margin_bytes"] = 10**18
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2026Q1", "2026Q1"))
    with pytest.raises(BudgetError, match="Stopping before 2026Q1"):
        run_data(cfg, fetch=fetch)
    assert fetch.calls == []
    assert not Storage.from_config(cfg).marker_path("ingest_2026Q1").exists()


def test_bad_value_becomes_null_in_ingest(tmp_path):
    row = dict.fromkeys(["date", "serial_number", "model", "smart_5_raw", "smart_9_raw"], "")
    row.update(date="2026-01-01", serial_number="S1", model="M", smart_5_raw="12abc", smart_9_raw="100")
    (tmp_path / "day.csv").write_text(csv_text([row], list(row)))
    t = read_day(tmp_path / "day.csv", SMART_IDS, "synthetic")
    assert t.column("smart_5_raw").to_pylist() == [None]
    assert t.column("smart_9_raw").to_pylist() == [100]
    assert t.column("pod_key").to_pylist() == [None]
