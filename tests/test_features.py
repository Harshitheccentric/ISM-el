import importlib.util
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from diskctx.config import ConfigError, check_config, quarter_days
from diskctx.features import (
    GROUPS,
    POOLED,
    Spec,
    base_rates,
    check_feature_budget,
    compute_features,
    connect,
    feature_stages,
    keep_samples,
    load_pod_tables,
    rate_of,
    read_features,
    row_hash_sql,
    sample_r,
    smart_set,
    study_columns,
)
from diskctx.ingest import model_dir
from diskctx.labels import build_drives, first_failures, label_rows
from diskctx.pipeline import label_split_stages, run_data
from diskctx.sources import read_sources, sources_stage
from diskctx.splits import hash_unit, last_records, split_rows, study_rows
from diskctx.status import BudgetError
from diskctx.storage import CACHE, DURABLE, Stage, Storage
from diskctx.synthetic import MODEL_LBA, MODEL_NO_LBA, SyntheticFleet

SMART = (5, 9, 187, 188, 194, 197, 198, 241, 242)
S_IDS = [5, 9, 187, 188, 194, 197, 198]
SCHEMA = {"serial_number": pl.Utf8, "date": pl.Date, "capacity_bytes": pl.Int64, "pod_key": pl.Utf8,
          "pod_slot_num": pl.Utf8, **{f"smart_{i}_raw": pl.Int64 for i in SMART}}
CAP = 14_000_000_000_000
D0 = date(2025, 10, 1)                       # day 92 = 2026-01-01, the start of the next quarter
POD_A, POD_B = "sac0|000|1154|02", "sac0|000|1154|13"      # same vault
POD_C = "sac0|000|2017|02"                                   # same cluster, other vault
POD_D = "sac0|001|3000|01"                                   # other cluster
TB_PER_DAY = 2 * 10**9 * 512 / 1e12          # the default 241 step: 1.024 TB per day


def day(n):
    return D0 + timedelta(days=n)


def reading(serial, n, pod=POD_A, slot="0003", **smart):
    """One drive-day row. SMART defaults are a healthy drive's; override with smart_<id>=value."""
    values = {5: 0, 9: 24000 + 24 * n, 187: 0, 188: 0, 194: 30, 197: 0, 198: 0,
              241: 10**10 + n * 2 * 10**9, 242: 10**10 + n * 10**9}
    values.update({int(k.split("_")[1]): v for k, v in smart.items()})
    return {"serial_number": serial, "date": day(n), "capacity_bytes": CAP, "pod_key": pod, "pod_slot_num": slot,
            **{f"smart_{i}_raw": values[i] for i in SMART}}


def frame(rows):
    return pl.DataFrame(rows, schema=SCHEMA)


def all_columns(settings, ids=S_IDS):
    return study_columns("P", list(GROUPS), list(ids), settings)


def run(rows, cfg, pods=None, want=None, periods=None):
    """Features of every group (S, W, N; and P when pod tables are given)."""
    want = rows.select("serial_number", "date") if want is None else want
    columns = all_columns(cfg["features"]) if pods else study_columns("W", ["S", "W", "N"], S_IDS, cfg["features"])
    return compute_features(rows, want, cfg["features"], columns, cfg["resources"], pods=pods, model="M",
                            periods=periods)


def at(out, serial, n):
    return out.filter((pl.col("serial_number") == serial) & (pl.col("date") == day(n))).row(0, named=True)


def pod_tables(rows, events, other=(), train=(0, 200)):
    """pod_days, pod_models, events and rates from model M's drive rows, failure events
    [(serial, day, pod, model)] and other-model drives [(model, pod, drives)] present on
    every day. The base rates come from the days `train`."""
    r = rows.with_columns(pl.lit("M").alias("model"))
    mix_schema = {"date": pl.Date, "pod_key": pl.Utf8, "model": pl.Utf8, "drives": pl.Int64}
    mix = r.group_by("date", "pod_key", "model").agg(pl.len().cast(pl.Int64).alias("drives"))
    others = pl.DataFrame([{"date": d, "pod_key": p, "model": m, "drives": n}
                           for d in r["date"].unique() for m, p, n in other], schema=mix_schema)
    ev = pl.DataFrame([{"serial_number": s, "date": day(n), "pod_key": p, "model": m} for s, n, p, m in events],
                      schema={"serial_number": pl.Utf8, "date": pl.Date, "pod_key": pl.Utf8, "model": pl.Utf8})
    fails = ev.group_by("date", "pod_key", "model").agg(pl.len().cast(pl.Int64).alias("failures"))
    models = (pl.concat([mix.select(list(mix_schema)), others]).join(fails, on=["date", "pod_key", "model"], how="full",
                                                                       coalesce=True)
              .with_columns(pl.col("drives").fill_null(0), pl.col("failures").fill_null(0)))
    days = (rows.group_by("date", "pod_key")
            .agg(pl.col("smart_194_raw").mean().alias("temp_mean"), pl.lit(1.5).alias("tb_written_mean"),
                 pl.lit(0.5).alias("tb_read_mean")))
    return {"pod_days": days, "pod_models": models, "events": ev,
            "rates": base_rates(models, day(train[0]), day(train[1]))}


# --- 1. past-only, the whole registry -------------------------------------------------------


def random_world(seed, n_days=160):
    """Drive rows of model M (gaps, nulls, a reset, a pod move), other drives' events and
    other-model drives, over n_days (across the quarter boundary at day 92)."""
    rng = random.Random(seed)
    pods = [POD_A, POD_B, POD_C, POD_D]
    rows = []
    for s in range(8):
        serial, pod = f"D{s}", pods[s % 4]
        counters = {i: rng.randint(0, 5) for i in (5, 187, 188, 197, 198)}
        lba = {241: rng.randint(10**9, 10**11), 242: rng.randint(10**9, 10**11)}
        for n in range(n_days):
            for i in counters:
                counters[i] += rng.choice((0, 0, 0, 1, 2)) - (1 if i == 197 and counters[i] and rng.random() < 0.1 else 0)
            lba[241] += rng.randint(0, 4 * 10**9)
            lba[242] += rng.randint(0, 8 * 10**9)
            if s == 1 and n == 70:
                lba[241] = rng.randint(0, 10**6)                    # counter reset
            if s == 2 and n == 50:
                pod = POD_C                                         # moves to another vault
            if rng.random() < 0.1 or (s == 3 and 60 <= n < 100):    # missing rows; a 40-day silence
                continue
            values = {f"smart_{i}": v for i, v in counters.items()}
            values.update(smart_241=lba[241], smart_242=lba[242], smart_194=rng.randint(22, 45),
                          smart_9=24 * (300 + n))
            for k in list(values):
                if rng.random() < 0.08:
                    values[k] = None                                # a column silent on one day
            rows.append(reading(serial, n, pod=pod, slot=f"{rng.randint(0, 59):04d}", **values))
    events = [(f"X{i}", rng.randrange(n_days), rng.choice(pods), rng.choice(("M", "OTHER"))) for i in range(40)]
    events += [("D5", rng.randrange(n_days), pods[1], "M")]                # one of the drives' own events
    other = [("OTHER", p, rng.randint(1, 30)) for p in pods]
    return frame(rows), events, other


def splice(old, new, t):
    """Tables of `old` before day t, `new` from day t on."""
    return pl.concat([old.filter(pl.col("date") < day(t)), new.filter(pl.col("date") >= day(t))])


def test_past_only_for_every_group(cfg):
    rows, events, other = random_world(1)
    pods = pod_tables(rows, events, other)
    columns = all_columns(cfg["features"])
    by_group = {g: list(GROUPS[g](Spec(S_IDS, cfg["features"]))) for g in GROUPS}
    assert set(by_group) == {"S", "W", "N", "P"} and set(columns) == {c for cols in by_group.values() for c in cols}
    base, _ = run(rows, cfg, pods=pods)
    for group, cols in by_group.items():                       # every group yields values to compare
        assert all(base[c].drop_nulls().len() > 0 for c in cols), group

    rows2, events2, other2 = random_world(2)                   # different values, nulls, pods, events
    pods2 = pod_tables(rows2, events2, other2)
    want = rows.select("serial_number", "date")
    for t in (1, 31, 51, 71, 92, 93, 120, 158):
        changed = {name: splice(pods[name], pods2[name], t) for name in ("pod_days", "pod_models", "events")}
        changed["rates"] = pods["rates"]                       # fit once on the train window
        out, _ = run(splice(rows, rows2, t), cfg, pods=changed, want=want)
        before = pl.col("date") <= day(t)
        assert out.filter(before).equals(base.filter(before)), t
        assert not out.filter(~before).equals(base.filter(~before)), t     # later features did change


# --- 2./3. deltas, gaps, resets -------------------------------------------------------------


def test_delta_across_a_gap_is_a_per_day_rate(cfg):
    rows = [reading("G", n, smart_5=2 * n, smart_197=None if 3 <= n <= 6 else n)
            for n in [*range(0, 10), *range(13, 21)]]                     # days 10-12 missing
    out, _ = run(frame(rows), cfg)
    assert at(out, "G", 14)["w_tbw_1d"] == pytest.approx(TB_PER_DAY)       # (day 13 - day 9) / 4 days
    assert at(out, "G", 14)["s_smart_5_d7"] == pytest.approx(2.0)          # day 13 vs day 6, per day
    assert at(out, "G", 13)["s_smart_5"] == 18                             # carried from day 9
    g6 = at(out, "G", 6)
    assert g6["s_smart_197"] == 2                                          # last reading: day 2
    assert g6["s_smart_197_d7"] == pytest.approx(1.0)                      # no reading 7 days back: from day 0
    assert all(v is None for k, v in at(out, "G", 0).items() if k not in ("serial_number", "date"))
    assert at(out, "G", 1)["s_smart_5_d7"] is None                         # only one reading so far
    later = out.filter(pl.col("date") >= day(2))
    for col in ("s_smart_5_d7", "s_smart_5_d30", "s_smart_197_d7", "w_tbw_mean7", "w_tbw_max30", "w_tbw_1d"):
        assert later[col].null_count() == 0, col                           # an earlier reading exists


def test_reset_gives_a_null_delta_and_is_counted(cfg):
    rows = [reading("R", n, smart_241=(10**10 + n * 2 * 10**9) if n < 20 else (n - 20) * 2 * 10**9,
                    smart_197=5 if n < 10 else 3) for n in range(40)]
    out, resets = run(frame(rows), cfg)
    assert resets == {"smart_241_raw": 1, "smart_242_raw": 0}
    assert at(out, "R", 21)["w_tbw_1d"] is None                            # day 20 vs 19: reset
    assert at(out, "R", 21)["w_tbw_mean7"] is None                         # crosses the reset
    assert at(out, "R", 22)["w_tbw_1d"] == pytest.approx(TB_PER_DAY)
    assert at(out, "R", 29)["w_tbw_mean7"] == pytest.approx(TB_PER_DAY)    # day 28 vs 21, after it
    assert at(out, "R", 21)["w_tbr_1d"] is not None                        # 242 did not reset
    assert at(out, "R", 11)["s_smart_197_d7"] == pytest.approx(-2 / 7)     # 197 may fall: kept


def test_age_from_smart_9_and_fill_phase_from_lifetime_241(cfg):
    rows = [reading("N", n, smart_241=n * 10**10) for n in range(6)]                  # 5.12 TB per day
    rows += [reading("B", n, smart_9=n, smart_241=n * 1000, smart_242=0) for n in range(4)]   # brand new
    out, _ = run(frame(rows), cfg)
    assert at(out, "N", 1)["w_age_days"] == 1000                           # 24,000 h, though first seen day 0
    fills = {n: at(out, "N", n + 1)["w_lifetime_fills"] for n in range(5)}
    assert fills[2] == pytest.approx(2 * 10**10 * 512 / CAP)
    assert [at(out, "N", n + 1)["w_fill_phase"] for n in range(5)] == [1, 1, 1, 0, 0]    # 241 x 512 >= 14 TB
    ratio = at(out, "B", 2)["w_tbw_recent_ratio"]                          # age ~0 h, lifetime ~0
    assert ratio is not None and math.isfinite(ratio)
    assert ratio == pytest.approx(1000 * 512 / 1e12 / 0.01, rel=1e-5)     # both floors in use


def test_n_and_recent_ratio_use_the_floor(cfg):
    rows = [reading("Q", n, smart_5=n, smart_241=10**10, smart_242=10**10) for n in range(10)]  # no workload
    out, _ = run(frame(rows), cfg)
    q = at(out, "Q", 9)
    assert q["n_5_per_tbw7"] == pytest.approx(1 / 0.01) and q["n_5_per_tb7"] == pytest.approx(1 / 0.01)
    assert q["w_tbw_recent_ratio"] == 0


# --- 4./5./15. pods -------------------------------------------------------------------------


def pod_world():
    """X and Y in pod A; Z in pod A until day 19, then pod C; W in pod B (same vault), V in
    pod C (same cluster), U in pod D (other cluster). Pod A is at 30 C, pod C at 40 C."""
    rows = []
    for n in range(40):
        rows += [reading("X", n, slot="0007"), reading("Y", n), reading("W", n, pod=POD_B),
                 reading("V", n, pod=POD_C, smart_194=40), reading("U", n, pod=POD_D)]
        rows.append(reading("Z", n, smart_194=30) if n < 20 else reading("Z", n, pod=POD_C, smart_194=40))
    events = [("X", 25, POD_A, "M"),        # the drive's own event: excluded from its counts
              ("Y", 27, POD_A, "M"), ("W", 20, POD_B, "M"), ("V", 10, POD_C, "M"), ("U", 22, POD_D, "M"),
              ("Y0", 0, POD_A, "OTHER"),    # outside [t-30, t-1] for t = 31
              ("Y1", 31, POD_A, "OTHER")]   # on day t itself
    return frame(rows), events, [("OTHER", POD_A, 10), ("OTHER", POD_C, 50)]


def test_pod_counts_exclude_the_drive_and_follow_its_pod_as_of_yesterday(cfg):
    rows, events, other = pod_world()
    out, _ = run(rows, cfg, pods=pod_tables(rows, events, other))
    x = at(out, "X", 31)
    assert (x["p_fail_pod30"], x["p_fail_vault30"], x["p_fail_cluster30"]) == (1, 2, 3)
    assert at(out, "Y", 31)["p_fail_pod30"] == 1                    # Y's own event excluded, X's counted
    assert at(out, "Y", 28)["p_fail_pod30"] == 2                    # Y0 (day 0) and X (day 25); not Y's own
    assert x["p_slot"] == 7 and x["p_pod_temp_mean"] == pytest.approx(30)
    assert at(out, "Z", 20)["p_pod_temp_mean"] == pytest.approx(30)  # last reading day 19: still pod A
    assert at(out, "Z", 21)["p_pod_temp_mean"] == pytest.approx(40)  # from day 20 on: pod C
    assert at(out, "Z", 21)["p_temp_minus_pod"] == pytest.approx(0)
    assert at(out, "Z", 20)["p_fail_vault30"] == 1                   # days -10..19 in pod A's vault: Y0 only


def test_expected_counts_and_smoothed_excess(cfg):
    rows, events, other = pod_world()
    pods = pod_tables(rows, events, other, train=(0, 39))
    out, _ = run(rows, cfg, pods=pods)
    rates = dict(zip(pods["rates"]["model"], pods["rates"]["rate"]))
    window = pods["pod_models"].filter(pl.col("date").is_between(day(1), day(30)))
    for unit, prefix in (("pod", POD_A), ("vault", "sac0|000|1154|"), ("cluster", "sac0|000|")):
        units = window.filter(pl.col("pod_key").str.starts_with(prefix))
        expected = sum(d * rates[m] for d, m in zip(units["drives"], units["model"])) - 30 * rates["M"]
        x = at(out, "X", 31)
        assert x[f"p_expected_{unit}30"] == pytest.approx(expected, rel=1e-5), unit
        assert x[f"p_excess_{unit}30"] == pytest.approx((x[f"p_fail_{unit}30"] + 1) / (expected + 1), rel=1e-5)


def test_base_rates_come_from_the_train_window_only():
    rng = random.Random(3)
    rows = [{"date": day(n), "pod_key": p, "model": m, "drives": rng.randint(1, 60), "failures": rng.randint(0, 2)}
            for n in range(100) for p in (POD_A, POD_C) for m in ("M", "OTHER")]
    pm = pl.DataFrame(rows)
    train = base_rates(pm, day(0), day(49))
    later = pm.with_columns(pl.when(pl.col("date") > day(49)).then(pl.col("failures") * 7 + 3)
                            .otherwise(pl.col("failures")).alias("failures"))
    assert base_rates(later, day(0), day(49)).equals(train)              # val/test failures perturbed
    earlier = pm.with_columns(pl.when(pl.col("date") == day(10)).then(pl.col("failures") + 5)
                              .otherwise(pl.col("failures")).alias("failures"))
    assert not base_rates(earlier, day(0), day(49)).equals(train)
    assert POOLED in train["model"].to_list()
    assert rate_of(train, "NEW MODEL") == rate_of(train, POOLED)


# --- 6./8. SMART sets, Study W ----------------------------------------------------------------


def test_smart_sets_follow_coverage():
    days = pl.DataFrame([{"model": m, "date": day(n), "rows": rows, "nonnull_smart_197_raw": nn,
                          "nonnull_smart_5_raw": rows}
                         for n in range(30) for m, rows, nn in (("A", 100, 95 if n < 20 else 0),
                                                                ("B", 1000, 949 if n < 20 else 1000))])
    ids, shares = smart_set(days, "A", day(0), day(19), [5, 197], 0.95)
    assert ids == [5, 197] and shares[197] == pytest.approx(0.95)         # exactly 95%: in
    assert smart_set(days, "B", day(0), day(19), [5, 197], 0.95)[0] == [5]   # 94.9%: out
    assert smart_set(days, "A", day(0), day(29), [5, 197], 0.95)[0] == [5]   # only the window counts
    cols = study_columns("W", ["S", "W", "N"], [5, 9, 194, 198], {**SETTINGS})
    assert "s_smart_187" not in cols and "n_5_per_tbw7" in cols and "n_187_per_tbw7" not in cols


def test_study_w_refuses_pod_features(cfg):
    with pytest.raises(ValueError, match="P"):
        study_columns("W", ["S", "W", "N", "P"], S_IDS, cfg["features"])
    bad = dict(cfg, studies=dict(cfg["studies"], W=dict(cfg["studies"]["W"], groups=["S", "W", "N", "P"])))
    with pytest.raises(ConfigError, match="W"):
        check_config(bad)
    assert not any(c.startswith("p_") for c in study_columns("W", ["S", "W", "N"], S_IDS, cfg["features"]))


# --- 10./11. quarter by quarter == the whole history ------------------------------------------


def test_quarters_with_warmup_equal_one_pass(cfg):
    rows, events, other = random_world(4)
    pods = pod_tables(rows, events, other)
    q4, q1 = quarter_days("2025Q4"), quarter_days("2026Q1")
    assert day(92) == q1[0]
    whole, resets_whole = run(rows, cfg, pods=pods)
    split, resets_split = run(rows, cfg, pods=pods, periods=[(q4[0], q4[-1]), (q1[0], q1[-1])])
    assert split.equals(whole)
    assert resets_split == resets_whole and resets_whole["smart_241_raw"] >= 1
    silent = split.filter((pl.col("serial_number") == "D3") & pl.col("date").is_between(day(93), day(99)))
    assert silent["s_smart_5"].null_count() == 0                    # silent 40 days across the boundary


def test_split_rows_by_quarter_equal_whole_history():
    def rows_of(serial, first, last):
        return [{"model": "MA", "serial_number": serial, "date": day(n)} for n in range(first, last + 1)]

    rows = pl.DataFrame(rows_of("A", 0, 180) + rows_of("B", 0, 80) + rows_of("B", 100, 180) + rows_of("C", 0, 95)
                        + rows_of("F", 0, 120) + rows_of("E", 10, 140),
                        schema={"model": pl.Utf8, "serial_number": pl.Utf8, "date": pl.Date})
    events = pl.DataFrame({"model": ["MA"], "serial_number": ["F"], "date": [day(121)]})
    drives = build_drives([rows], first_failures(events))
    windows = pl.DataFrame({"study": ["P"] * 3, "model": ["MA"] * 3, "split": ["train", "val", "test"],
                            "start": [day(0), day(100), day(140)], "end": [day(99), day(139), day(180)],
                            "note": [""] * 3})
    for split in ("train", "val", "test"):
        assignment = pl.DataFrame({"model": ["MA"] * 5, "serial_number": list("ABCFE"), "split": [split] * 5,
                                   "keep": [True] * 5})
        seen = last_records(rows, windows, split)
        for n in (7, 30):
            whole = split_rows(label_rows(rows, drives, [n]), assignment, windows, split, n, "S", ["S"])
            parts = [split_rows(label_rows(rows.filter(pl.col("date").is_between(a, b)), drives, [n]),
                                assignment, windows, split, n, "S", ["S"], last_seen=seen)
                     for a, b in ((day(0), day(91)), (day(92), day(181)))]
            assert pl.concat(parts).sort("serial_number", "date").equals(whole), (split, n)
            assert whole.height > 0


# --- 14. train sampling -----------------------------------------------------------------------


def sample_frame():
    rows = []
    for split, serials in (("train", [f"H{i}" for i in range(40)] + ["F"]), ("val", ["V", "G"]), ("test", ["T"])):
        for s in serials:
            fail = day(150) if s in ("F", "G") else None
            for n in range(0, 140):
                rows.append({"study": "W", "split": split, "model": "M", "serial_number": s, "date": day(n),
                             "fail_date": fail, "y_7": 0, "in_7": True})
    return pl.DataFrame(rows, schema={"study": pl.Utf8, "split": pl.Utf8, "model": pl.Utf8, "serial_number": pl.Utf8,
                                      "date": pl.Date, "fail_date": pl.Date, "y_7": pl.Int8, "in_7": pl.Boolean})


def test_train_sampling_keeps_near_failure_rows_and_never_touches_val_test(cfg):
    samples = sample_frame()
    con = connect(cfg["resources"])
    keep_samples(con, samples, {"W": 0.3}, 42, 60)
    out = con.execute("SELECT * FROM samples ORDER BY split, serial_number, date").pl()
    again = connect(cfg["resources"])
    keep_samples(again, samples, {"W": 0.3}, 42, 60)
    assert again.execute("SELECT * FROM samples ORDER BY split, serial_number, date").pl().equals(out)   # seeded

    for split in ("val", "test"):                                          # complete, weight 1
        assert out.filter(pl.col("split") == split).height == samples.filter(pl.col("split") == split).height
        assert (out.filter(pl.col("split") == split)["sample_weight"] == 1).all()
    train = out.filter(pl.col("split") == "train")
    near = train.filter(pl.col("serial_number") == "F", (pl.col("fail_date") - pl.col("date")).dt.total_days() <= 60)
    assert near.height == 50 and (near["sample_weight"] == 1).all()        # days 90-139: all kept
    expected = {(s, d) for s, d, f in zip(samples["serial_number"], samples["date"], samples["fail_date"])
                if s.startswith("H") or (s == "F" and (f - d).days > 60)}
    expected = {(s, d) for s, d in expected if hash_unit(42, "rows", s, d.isoformat()) < 0.3}
    rest = train.filter(~((pl.col("serial_number") == "F") & ((pl.col("fail_date") - pl.col("date")).dt.total_days() <= 60)))
    assert set(zip(rest["serial_number"], rest["date"])) == expected
    assert rest["sample_weight"].to_list() == [pytest.approx(1 / 0.3)] * rest.height
    assert abs(len(expected) / (40 * 140 + 90) - 0.3) < 0.03
    want = con.execute("SELECT count(*) FROM want").fetchone()[0]
    assert want == out.select("serial_number", "date").unique().height

    probe = [("H1", day(3)), ("ZX9", day(100))]
    for s, d in probe:                                                     # the same hash in DuckDB
        sql = row_hash_sql(42, f"'{s}'", f"DATE '{d}'")
        assert con.execute(f"SELECT {sql}").fetchone()[0] == hash_unit(42, "rows", s, d.isoformat())


def test_sample_rate_caps_the_train_rows():
    assert sample_r(100, 1000, 400) == pytest.approx(0.3)
    assert sample_r(100, 1000, 10**6) == 1.0
    assert 100 + sample_r(5000, 10**7, 15 * 10**6) * 10**7 <= 15 * 10**6 + 5000
    with pytest.raises(ValueError, match="max_rows"):
        sample_r(500, 1000, 400)


# --- 9./12. the stages on the synthetic pipeline ----------------------------------------------


def synthetic_cfg(cfg):
    cfg.update(start_quarter="2025Q2", end_quarter="2026Q1", models=[MODEL_LBA, MODEL_NO_LBA])
    cfg["studies"] = {
        "W": {"groups": ["S", "W", "N"], "val_quarters": 1, "test_quarters": 1, "min_quarter_failures": 1,
              "models": {MODEL_LBA: ["2025Q2", "2026Q1"], MODEL_NO_LBA: ["2025Q2", "2026Q1"]}},
        "P": {"groups": ["S", "W", "N", "P"], "start": "2025-07-01", "train": ["2025-08-01", "2025-10-31"],
              "val": ["2025-11-01", "2025-12-31"], "test": ["2026-01-01", "2026-03-31"]},
    }
    return cfg


def test_feature_stages_on_synthetic_pipeline(cfg, fleet_fetch):
    synthetic_cfg(cfg)
    fetch = fleet_fetch(SyntheticFleet(cfg["seed"], "2025Q2", "2026Q1", n_drives=96))   # failures in every W split
    results = run_data(cfg, stages=("sources", "ingest", "labels", "splits", "features"), fetch=fetch)
    storage = Storage.from_config(cfg)
    assert "_synthetic" in str(storage.run_dir)
    model_stages = [n for n in results if n.startswith("features_") and n not in ("features_sampling",
                                                                                   "features_rates")]
    assert results["features_sampling"] == "built" and results["features_rates"] == "built" and model_stages
    sampling = pl.read_csv(storage.run_dir / "features" / "sampling.csv")
    assert sampling.filter(pl.col("split") == "train")["r"].to_list() == [1.0] * sampling.filter(
        pl.col("split") == "train").height
    assert sampling.filter(pl.col("split") != "train")["near_failure_rows"].null_count() == \
        sampling.filter(pl.col("split") != "train").height                # no label counts on val/test

    full = {}
    for study, groups in (("W", ["S", "W", "N"]), ("P", ["S", "W", "N", "P"])):
        windows = pl.read_csv(storage.run_dir / "splits" / "windows.csv").filter(
            (pl.col("study") == study) & (pl.col("split") != "skipped"))
        assert windows.height
        for model in windows["model"].unique():
            folder = storage.run_dir / "features" / study / model_dir(model)
            about = json.loads((folder / "columns.json").read_text())
            ids = about["smart_set"]
            w = windows.filter(pl.col("model") == model)
            assert about["coverage_window"] == [w.filter(pl.col("split") == "train")["start"][0],
                                                w.filter(pl.col("split") == "val")["end"][0]]   # never test
            if model == MODEL_LBA:
                assert 197 not in ids                                     # empty 197 on this model string
            else:
                assert 197 in ids
            names = list(study_columns(study, groups, ids, cfg["features"]))
            for split in ("train", "val", "test"):
                f = read_features(storage, study, model, split)
                full[(study, model, split)] = f
                assert f.columns[:4] == ["model", "serial_number", "date", "fail_date"]
                assert [c for c in f.columns if c in names] == names and f.columns[-1] == "data_source"
                assert set(f["data_source"]) <= {"synthetic"} and (f["sample_weight"] == 1).all()
                for n in cfg["horizons"]:
                    rows = study_rows(storage, cfg, study, split, n, "S").filter(pl.col("model") == model)
                    flagged = f.filter(pl.col(f"in_{n}")).select("serial_number", "date", f"y_{n}").sort(
                        "serial_number", "date")
                    assert flagged.equals(rows.select("serial_number", "date", f"y_{n}")), (study, model, split, n)
    assert any(f.height for f in full.values())
    assert any(not f.filter(pl.col("fail_date").is_not_null()).is_empty() for f in full.values())
    assert any(c.startswith("p_") for c in full[next(k for k in full if k[0] == "P")].columns)
    assert not any(c.startswith("p_") for c in full[next(k for k in full if k[0] == "W")].columns)

    # A feature setting reruns the features stages and what follows them, nothing else.
    def downstream(work, storage):
        (work / "models.txt").write_text("x")

    def stages(c):
        src = sources_stage(c)
        storage.run_stages([src], c)
        chain = label_split_stages(c, read_sources(storage), c["models"], ["2025Q2", "2025Q3", "2025Q4", "2026Q1"],
                                   fetch=fetch)
        features = feature_stages(c, storage, c["models"])
        later = Stage("models", CACHE, ["models.txt"], downstream, keys=[], deps=[s.name for s in features])
        return [src, *chain, *features, later]

    storage.run_stages(stages(cfg), cfg)
    train = sampling.filter(pl.col("split") == "train").group_by("study").agg(
        pl.col("near_failure_rows").sum(), pl.col("other_rows").sum())
    max_rows = int(min(n + 0.4 * o for n, o in zip(train["near_failure_rows"], train["other_rows"])))
    assert max_rows > train["near_failure_rows"].max()                  # so 0 < r < 1 in both studies
    changed = dict(cfg, features=dict(cfg["features"], train_sample={"max_rows": max_rows, "near_failure_days": 60}))
    second = storage.run_stages(stages(changed), changed)
    rerun = {n for n, r in second.items() if r != "skipped"}
    assert rerun == {*model_stages, "features_sampling", "models"}           # rates: no feature setting
    far_before = far_after = 0
    for (study, model, split), before in full.items():
        after = read_features(storage, study, model, split)
        if split != "train":
            assert after.equals(before)                                         # val/test complete
            continue
        near = (pl.col("fail_date") - pl.col("date")).dt.total_days() <= 60
        assert after.filter(near).equals(before.filter(near))                   # every row near a failure
        far = ~near | pl.col("fail_date").is_null()
        assert (after.filter(far)["sample_weight"] > 1).all()                   # 1 / r
        assert after.filter(far).join(before, on=["serial_number", "date"], how="anti").is_empty()
        far_before += before.filter(far).height
        far_after += after.filter(far).height
    assert 0 < far_after < 0.6 * far_before

    moved = dict(changed, resources={"duckdb_memory_limit": "2GB", "duckdb_threads": 1})
    assert set(storage.run_stages(stages(moved), moved).values()) == {"skipped"}   # resources are not hashed


def test_pod_tables_load_after_a_quarter_without_pod_fields(cfg):
    """Before the pod fields (2023Q2 on the real data), pod_days is empty and its pod_key
    column was written as an integer; reading it with the next quarter must still work."""
    storage = Storage.from_config(cfg)
    base = {q: storage.path(DURABLE, f"audit/quarters/{q}") for q in ("2025Q4", "2026Q1")}
    for path in base.values():
        path.mkdir(parents=True)
    empty = pa.table({"date": pa.array([], pa.date32()), "pod_key": pa.array([], pa.int32()),
                      "temp_mean": pa.array([], pa.float64()), "tb_written_mean": pa.array([], pa.float64()),
                      "tb_read_mean": pa.array([], pa.float64())})
    pq.write_table(empty, base["2025Q4"] / "pod_days.parquet")
    rows, events, other = pod_world()
    pods = pod_tables(rows.with_columns(pl.col("date") + timedelta(days=92)), [], other)
    pods["pod_days"].write_parquet(base["2026Q1"] / "pod_days.parquet")
    pods["pod_models"].write_parquet(base["2026Q1"] / "pod_models.parquet")
    ev = pl.DataFrame({"serial_number": ["X", "Q"], "model": ["M", "OTHER"], "date": [day(100), day(101)],
                       "pod_key": [POD_A, POD_A]})
    ev.write_parquet(base["2026Q1"] / "events.parquet")
    ev.head(0).write_parquet(base["2025Q4"] / "events.parquet")
    con = connect(cfg["resources"])
    load_pod_tables(con, storage, "2026Q1", day(60), "M")
    assert con.execute("SELECT count(*) FROM pod_days").fetchone()[0] == pods["pod_days"].height
    assert con.execute("SELECT count(*) FROM pod_models").fetchone()[0] == pods["pod_models"].height
    assert con.execute("SELECT serial_number FROM events").fetchall() == [("X",)]      # this model only
    assert con.execute("SELECT typeof(pod_key) FROM pod_days LIMIT 1").fetchone()[0] == "VARCHAR"


def test_feature_budget_stops_with_a_clear_message(cfg, tmp_path):
    storage = Storage.from_config(cfg)
    need = check_feature_budget(storage, "M", 1000, dict(cfg["budget"], default_feature_bytes_per_row=50),
                                local_free=10**9, root_free=10**9, shared_disk=False)
    assert need == 50_000
    with pytest.raises(BudgetError, match="ROOT"):
        check_feature_budget(storage, "M", 1000, dict(cfg["budget"], default_feature_bytes_per_row=50),
                             local_free=10**9, root_free=50_000, shared_disk=False)
    with pytest.raises(BudgetError, match="local disk"):
        check_feature_budget(storage, "M", 1000, dict(cfg["budget"], default_feature_bytes_per_row=50),
                             local_free=100_050, root_free=10**9, shared_disk=True)     # built locally + ROOT


# --- 13. the missingness diagnostic -----------------------------------------------------------


def load_tool():
    path = Path(__file__).resolve().parents[1] / "tools" / "diagnostics" / "feature_missingness.py"
    spec = importlib.util.spec_from_file_location("feature_missingness", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missingness_diagnostic_refuses_test_and_counts_nulls():
    tool = load_tool()
    with pytest.raises(ValueError, match="test"):
        tool.check_splits(["train", "test"])
    tool.check_splits(["train", "val"])
    f = pl.DataFrame({"date": [day(n) for n in range(10)] * 2,
                      "fail_date": [day(10)] * 10 + [None] * 10,
                      "a": [1.0] * 3 + [None] * 7 + [1.0] * 9 + [None],
                      "b": [1.0] * 20})
    rows = {r["feature"]: r for r in tool.missingness(f.lazy(), ["a", "b"], days=7)}
    assert (rows["a"]["pre_rows"], rows["a"]["pre_null"], rows["a"]["healthy_null"]) == (7, 7, 1)
    assert tool.rate_ratio(rows["a"]["pre_rate"], rows["a"]["healthy_rate"]) == pytest.approx(10)
    assert tool.flagged(10, 2) and not tool.flagged(1.5, 2) and tool.flagged(0.4, 2)
    assert tool.rate_ratio(rows["b"]["pre_rate"], rows["b"]["healthy_rate"]) is None


SETTINGS = {"s_smart_ids": S_IDS, "error_counters": [5, 187, 188, 197, 198], "min_coverage": 0.95,
            "windows": [7, 30], "lba_bytes": 512, "fill_phase_max_fills": 1.0, "n_floor_tb_per_day": 0.01,
            "pod_window_days": 30, "excess_smoothing": 1, "train_sample": {"max_rows": 100, "near_failure_days": 60}}
