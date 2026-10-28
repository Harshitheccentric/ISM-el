from datetime import date, timedelta

import polars as pl

from diskctx.labels import build_drives, first_failures, label_rows
from diskctx.pipeline import label_split_stages, run_data
from diskctx.sources import read_sources, sources_stage
from diskctx.storage import CACHE, Stage, Storage
from diskctx.synthetic import MODEL_LBA, MODEL_NO_LBA, SyntheticFleet

M = "SYNTH HDD14TEY"
D0 = date(2026, 1, 1)


def day(n):
    """D0 + n days."""
    return D0 + timedelta(days=n)


def rows(*drives):
    """Drive-day rows from (serial, [dates]) pairs, model M."""
    out = [{"model": M, "serial_number": s, "date": d} for s, dates in drives for d in dates]
    return pl.DataFrame(out, schema={"model": pl.Utf8, "serial_number": pl.Utf8, "date": pl.Date})


def events(*pairs):
    """Failure events from (serial, date) pairs, model M."""
    out = [{"model": M, "serial_number": s, "date": d} for s, d in pairs]
    return pl.DataFrame(out, schema={"model": pl.Utf8, "serial_number": pl.Utf8, "date": pl.Date})


def labelled(drive_rows, failure_events, horizons=(7, 14, 30)):
    drives = build_drives([drive_rows], first_failures(failure_events))
    return label_rows(drive_rows, drives, horizons).sort("serial_number", "date"), drives


def labels_of(frame, serial, horizon):
    sub = frame.filter(pl.col("serial_number") == serial)
    return dict(zip(sub["date"].to_list(), sub[f"y_{horizon}"].to_list()))


def test_labels_with_gaps_use_calendar_dates():
    fail = day(30)
    seen = [day(9), day(19), day(23), day(29)]     # gaps between records
    out, _ = labelled(rows(("A", seen)), events(("A", fail)))
    # By row count day 9 is 4 records before the failure; by calendar it is 21 days.
    assert labels_of(out, "A", 7) == {day(9): 0, day(19): 0, day(23): 1, day(29): 1}
    assert labels_of(out, "A", 14) == {day(9): 0, day(19): 1, day(23): 1, day(29): 1}
    assert labels_of(out, "A", 30) == {day(9): 1, day(19): 1, day(23): 1, day(29): 1}


def test_failure_on_t_plus_n_is_positive_and_t_plus_n_plus_1_is_negative():
    fail = day(60)
    out, _ = labelled(rows(("A", [day(i) for i in range(60)])), events(("A", fail)))
    for n in (7, 14, 30):
        y = labels_of(out, "A", n)
        assert y[fail - timedelta(days=n)] == 1          # failure on t+N
        assert y[fail - timedelta(days=n + 1)] == 0      # failure on t+N+1
        assert sum(y.values()) == n                       # exactly t in [f-N, f-1]


def test_failure_day_row_is_not_a_sample():
    fail = day(20)
    out, drives = labelled(rows(("A", [day(i) for i in range(21)])), events(("A", fail)))
    assert fail not in out["date"].to_list()
    assert out["date"].max() == day(19)
    assert drives.row(0, named=True)["fail_day_rows"] == 1


def test_rows_after_first_failure_dropped_and_reappearing_serial_not_merged():
    first, second = day(20), day(50)
    seen = [day(i) for i in range(61)]                  # back after the failure
    out, drives = labelled(rows(("A", seen)), events(("A", first), ("A", second)))
    assert out["date"].max() == day(19)                  # nothing at or after the first failure
    assert drives.height == 1                            # one drive, not two
    d = drives.row(0, named=True)
    assert d["fail_date"] == first
    assert d["n_events"] == 2
    assert d["rows_after_failure"] == 40                 # days 21..60
    assert d["fail_day_rows"] == 1


def test_censored_tail_dropped_per_horizon():
    seen = [day(i) for i in range(90)]                   # last record day 89, no failure
    out, drives = labelled(rows(("H", seen)), events())
    assert drives.row(0, named=True)["fail_date"] is None
    last = day(89)
    for n in (7, 14, 30):
        y = labels_of(out, "H", n)
        known = {d for d, v in y.items() if v is not None}
        assert known == {d for d in seen if d <= last - timedelta(days=n)}
        assert set(y[d] for d in known) == {0}
        assert sum(v is None for v in y.values()) == n


def test_failure_dates_come_only_from_events():
    seen = [day(i) for i in range(30)]
    # B: a Parquet row may say failure=1, but with no event B is not a failed drive.
    # C: its failure-day row was dropped (no SMART); the event still labels it.
    out, drives = labelled(rows(("B", seen), ("C", seen[:27])), events(("C", day(30))))
    by_serial = {r["serial_number"]: r for r in drives.iter_rows(named=True)}
    assert by_serial["B"]["fail_date"] is None
    assert by_serial["C"]["fail_date"] == day(30)
    assert by_serial["C"]["last_date"] == day(26)
    y = labels_of(out, "C", 7)
    assert y[day(26)] == 1 and y[day(23)] == 1 and y[day(22)] == 0


def test_events_join_on_model_and_serial():
    other = pl.DataFrame({"model": ["OTHER"], "serial_number": ["A"], "date": [day(5)]})
    _, drives = labelled(rows(("A", [day(i) for i in range(10)])), other)
    assert drives.row(0, named=True)["fail_date"] is None


# --- stage on synthetic data -----------------------------------------------------------


def two_quarter_cfg(cfg):
    cfg.update(start_quarter="2025Q4", end_quarter="2026Q1", models=[MODEL_LBA, MODEL_NO_LBA])
    return cfg


def test_labels_stage_on_synthetic_pipeline(cfg, fleet_fetch):
    two_quarter_cfg(cfg)
    fleet = SyntheticFleet(cfg["seed"], "2025Q4", "2026Q1")
    results = run_data(cfg, stages=("sources", "audit", "ingest", "labels"), fetch=fleet_fetch(fleet))
    assert results["labels"] == "built"
    storage = Storage.from_config(cfg)
    assert "_synthetic" in str(storage.run_dir)
    drives = pl.read_parquet(storage.run_dir / "labels" / "drives.parquet")
    assert set(drives["data_source"]) == {"synthetic"}
    failed = {d.serial: d.last_day for d in fleet.drives if d.failed}
    got = {r["serial_number"]: r["fail_date"] for r in drives.iter_rows(named=True) if r["fail_date"]}
    assert got == failed
    report = pl.read_csv(storage.run_dir / "labels" / "report.csv")
    assert set(report["horizon"]) == {7, 14, 30}
    assert report.filter(pl.col("horizon") == 7)["positives"].sum() > 0


def test_horizon_change_reruns_labels_and_downstream(cfg, fleet_fetch):
    two_quarter_cfg(cfg)
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2025Q4", "2026Q1"))
    storage = Storage.from_config(cfg)

    def downstream(work, storage):
        (work / "features.csv").write_text("x")

    def stages(c):
        src = sources_stage(c)
        storage.run_stages([src], c)
        chain = label_split_stages(c, read_sources(storage), c["models"], ["2025Q4", "2026Q1"], fetch=fetch)
        features = Stage("features", CACHE, ["features.csv"], downstream, keys=[], deps=["splits"])
        return [src, *chain, features]

    first = storage.run_stages(stages(cfg), cfg)
    assert {r for n, r in first.items() if n != "sources"} == {"built"}
    changed = dict(cfg, horizons=[7, 14])
    second = storage.run_stages(stages(changed), changed)
    rerun = {n for n, r in second.items() if r != "skipped"}
    assert rerun == {"labels", "splits", "features"}      # sources, audit, ingest skipped
    report = pl.read_csv(storage.run_dir / "labels" / "report.csv")
    assert set(report["horizon"]) == {7, 14}
