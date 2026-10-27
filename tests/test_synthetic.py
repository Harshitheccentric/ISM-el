import io
import zipfile
from datetime import date

import polars as pl
import pytest

from diskctx.synthetic import (
    COLUMNS,
    MODEL_BOOT,
    MODEL_LBA,
    MODEL_NO_LBA,
    SyntheticFleet,
)


@pytest.fixture(scope="module")
def fleet():
    return SyntheticFleet(seed=42, start_quarter="2026Q1", end_quarter="2026Q2")


@pytest.fixture(scope="module")
def rows(fleet):
    """Every row of the fleet, read as strings like the real pipeline does."""
    frames = [
        pl.read_csv(io.StringIO(fleet.day_csv(d)), infer_schema=False) for d in fleet.days
    ]
    return pl.concat(frames)


def test_same_seed_same_data():
    a = SyntheticFleet(1, "2026Q1", "2026Q1")
    b = SyntheticFleet(1, "2026Q1", "2026Q1")
    c = SyntheticFleet(2, "2026Q1", "2026Q1")
    day = date(2026, 2, 10)
    assert a.day_csv(day) == b.day_csv(day)
    assert a.day_csv(day) != c.day_csv(day)


def test_columns_follow_backblaze_layout(rows):
    assert rows.columns == COLUMNS
    assert rows.columns[:5] == ["date", "serial_number", "model", "capacity_bytes", "failure"]


def test_ids_are_zero_padded_strings(rows):
    assert rows["serial_number"].str.starts_with("00").all()
    assert "031" in rows["cluster_id"].to_list()
    data = rows.filter(pl.col("model") != MODEL_BOOT)
    assert data["pod_slot_num"].str.len_chars().eq(4).all()
    assert data["pod_slot_num"].str.starts_with("00").all()


def test_pod_id_alone_is_not_unique(rows):
    vaults_per_pod = rows.group_by("pod_id").agg(pl.col("vault_id").n_unique())
    assert vaults_per_pod["vault_id"].max() > 1


def test_boot_drives_have_no_slot_and_no_smart(rows):
    boot = rows.filter(pl.col("model") == MODEL_BOOT)
    assert boot.height > 0
    assert (boot["pod_slot_num"] == "").all() or boot["pod_slot_num"].is_null().all()
    smart = [c for c in COLUMNS if c.startswith("smart_")]
    assert boot.select(pl.col(smart).is_null().all()).row(0) == (True,) * len(smart)


def test_lba_columns_depend_on_model_string(rows):
    no_lba = rows.filter(pl.col("model") == MODEL_NO_LBA)
    lba = rows.filter(pl.col("model") == MODEL_LBA)
    assert no_lba["smart_241_raw"].is_null().all()
    assert no_lba["smart_242_raw"].is_null().all()
    assert no_lba["smart_197_raw"].is_not_null().all()
    assert lba["smart_241_raw"].is_not_null().all()
    assert lba["smart_197_raw"].is_null().all()


def test_raw_values_exceed_int32(rows):
    raw = rows["smart_241_raw"].cast(pl.Int64, strict=True)
    assert raw.max() > 2**31


def test_lba_counter_reset_exists(rows):
    lba = (
        rows.filter(pl.col("model") == MODEL_LBA)
        .with_columns(pl.col("smart_241_raw").cast(pl.Int64))
        .sort("serial_number", "date")
        .with_columns(step=pl.col("smart_241_raw").diff().over("serial_number"))
    )
    assert (lba["step"] < 0).sum() >= 1


def test_failed_drives_flagged_on_last_day_per_model(rows, fleet):
    last = rows.group_by("serial_number", "model").agg(
        pl.col("date").max().alias("last"),
        pl.col("failure").eq("1").sum().alias("n_fail"),
        pl.col("date").filter(pl.col("failure") == "1").first().alias("fail_date"),
    )
    failed = last.filter(pl.col("n_fail") > 0)
    assert (failed["n_fail"] == 1).all()
    assert (failed["fail_date"] == failed["last"]).all()
    assert set(failed["model"]) == {MODEL_NO_LBA, MODEL_LBA}


def test_failing_drives_show_error_growth(rows):
    fail_rows = rows.filter(pl.col("failure") == "1")
    assert fail_rows["smart_5_raw"].cast(pl.Int64).min() > 0


def test_censored_and_late_drives_exist(rows, fleet):
    span = rows.group_by("serial_number").agg(
        pl.col("date").min().alias("first"),
        pl.col("date").max().alias("last"),
        pl.col("failure").eq("1").any().alias("failed"),
    )
    end, start = fleet.days[-1].isoformat(), fleet.days[0].isoformat()
    censored = span.filter((pl.col("last") < end) & ~pl.col("failed"))
    late = span.filter(pl.col("first") > start)
    assert censored.height >= 2
    assert late.height >= 2


def test_quarter_zip_has_nested_days_and_junk(fleet, tmp_path):
    path = tmp_path / "data_Q1_2026.zip"
    names = fleet.write_quarter_zip("2026Q1", path)
    assert len(names) == 90
    assert all(n.startswith("data_Q1_2026/") and n.endswith(".csv") for n in names)
    with zipfile.ZipFile(path) as z:
        members = z.namelist()
        text = z.read(names[0]).decode()
    junk = set(members) - set(names)
    assert any(m.startswith("__MACOSX/") for m in junk)
    assert any(m.split("/")[-1].startswith(".") and m.endswith(".csv") for m in junk)
    assert any(m.endswith(".DS_Store") for m in junk)
    assert any(m.endswith("README.txt") for m in junk)
    assert text.splitlines()[0] == ",".join(COLUMNS)


def test_quarter_zip_can_drop_columns(fleet, tmp_path):
    path = tmp_path / "old.zip"
    names = fleet.write_quarter_zip("2026Q2", path, drop_columns=["pod_slot_num", "smart_241_raw"])
    with zipfile.ZipFile(path) as z:
        header = z.read(names[0]).decode().splitlines()[0].split(",")
    assert "pod_slot_num" not in header and "smart_241_raw" not in header
    assert "smart_242_raw" in header
