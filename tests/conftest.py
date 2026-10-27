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
        },
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
