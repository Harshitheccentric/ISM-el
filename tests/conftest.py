import csv
import io
import zipfile

import pytest

from diskctx.config import check_config
from diskctx.sources import quarter_of
from diskctx.synthetic import COLUMNS


@pytest.fixture
def cfg(tmp_path):
    """A small synthetic config with ROOT and local dir under tmp_path."""
    c = {
        "mode": "smoke",
        "data_source": "synthetic",
        "root": str(tmp_path / "root"),
        "local_dir": str(tmp_path / "local"),
        "start_quarter": "2026Q1",
        "end_quarter": "2026Q1",
        "models": [],
        "horizons": [7, 14, 30],
        "seed": 42,
        "smart_ids": [5, 9, 187, 188, 194, 197, 198, 241, 242],
        "selection": {"min_failures": 1, "min_coverage": 0.95, "max_models": 3,
                      "boot_share": 0.95},
        "budget": {
            "margin_bytes": 100,
            "default_zip_bytes": 1000,
            "default_day_bytes": 100,
            "default_parquet_bytes": 200,
            "default_feature_bytes_per_row": 1,
        },
        "dropped_days": {},
        "splits": {"shares": {"train": 0.5, "val": 0.2, "test": 0.3}, "healthy_per_failed": 20,
                   "min_failed_drives": {"train": 1, "val": 1, "test": 1}, "min_test_failures": 50},
        "studies": {
            "W": {"groups": ["S", "W", "N"], "val_quarters": 1, "test_quarters": 1,
                  "min_quarter_failures": 1, "models": {}},
            "P": {"groups": ["S", "W", "N", "P"], "start": "2026-01-01", "train": ["2026-01-08", "2026-02-10"],
                  "val": ["2026-02-11", "2026-03-01"], "test": ["2026-03-02", "2026-03-31"]},
        },
        "features": {
            "s_smart_ids": [5, 9, 187, 188, 194, 197, 198], "error_counters": [5, 187, 188, 197, 198],
            "min_coverage": 0.95, "windows": [7, 30], "lba_bytes": 512, "fill_phase_max_fills": 1.0,
            "n_floor_tb_per_day": 0.01, "pod_window_days": 30, "excess_smoothing": 1,
            "train_sample": {"max_rows": 15000000, "near_failure_days": 60},
        },
        "resources": {"duckdb_memory_limit": "1GB", "duckdb_threads": 2},
    }
    check_config(c)
    return c


def csv_text(rows, columns=COLUMNS):
    """Day CSV text from row dicts ('' = empty)."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def write_zip(path, members):
    """Write a zip from {member name: text}."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        for name, text in members.items():
            z.writestr(name, text)
    return path


@pytest.fixture
def fleet_fetch():
    """fleet_fetch(fleet, drop_columns={quarter: [...]}) -> fetch(url, dest) that writes the
    fleet's quarter zip (instead of downloading). fetch.calls lists the quarters fetched."""

    def make(fleet, drop_columns=None):
        def fetch(url, dest):
            quarter = quarter_of(url)
            fetch.calls.append(quarter)
            dest.parent.mkdir(parents=True, exist_ok=True)
            fleet.write_quarter_zip(quarter, dest, (drop_columns or {}).get(quarter, ()))

        fetch.calls = []
        return fetch

    return make


@pytest.fixture
def zip_fetch():
    """zip_fetch({quarter: {member: text}}) -> fetch(url, dest) that writes crafted zips."""

    def make(zips):
        def fetch(url, dest):
            write_zip(dest, zips[quarter_of(url)])

        return fetch

    return make
