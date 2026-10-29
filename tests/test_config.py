from pathlib import Path

import pytest

from diskctx.config import (
    ConfigError,
    check_config,
    config_hash,
    dropped_days,
    load_config,
    quarter_days,
    quarters_between,
)

CONFIGS = Path(__file__).resolve().parents[1] / "configs"


@pytest.mark.parametrize("mode", ["smoke", "full"])
def test_repo_configs_load(mode):
    cfg = load_config(CONFIGS / f"{mode}.yaml")
    assert cfg["mode"] == mode
    assert cfg["data_source"] == "backblaze"


def test_local_root_env_var_moves_data_out_of_the_repo(tmp_path, monkeypatch):
    monkeypatch.delenv("DISKCTX_LOCAL_ROOT", raising=False)
    cfg = load_config(CONFIGS / "smoke.yaml")
    assert (cfg["root"], cfg["local_dir"]) == ("data/root", "data/local")      # config fallback

    monkeypatch.setenv("DISKCTX_LOCAL_ROOT", str(tmp_path / "diskctx_data"))
    cfg = load_config(CONFIGS / "smoke.yaml")
    assert cfg["root"] == str(tmp_path / "diskctx_data" / "root")
    assert cfg["local_dir"] == str(tmp_path / "diskctx_data" / "local")

    cfg = load_config(CONFIGS / "smoke.yaml", root="/content/drive/x")         # override wins
    assert cfg["root"] == "/content/drive/x"
    assert config_hash(cfg) == config_hash(load_config(CONFIGS / "smoke.yaml"))


def test_full_config_covers_2016_to_2026():
    cfg = load_config(CONFIGS / "full.yaml")
    quarters = quarters_between(cfg["start_quarter"], cfg["end_quarter"])
    assert quarters[0] == "2016Q1"
    assert quarters[-1] == "2026Q2"
    assert len(quarters) == 42


def test_overrides_apply(tmp_path):
    cfg = load_config(CONFIGS / "smoke.yaml", root=str(tmp_path))
    assert cfg["root"] == str(tmp_path)


def test_hash_is_stable_and_ignores_paths(cfg):
    h = config_hash(cfg)
    assert h == config_hash(dict(cfg))
    moved = dict(cfg, root="/somewhere/else", local_dir="/content/x")
    assert config_hash(moved) == h


def test_hash_changes_with_a_knob(cfg):
    assert config_hash(dict(cfg, seed=7)) != config_hash(cfg)


def test_hash_of_subset_ignores_other_keys(cfg):
    keys = ["start_quarter", "end_quarter"]
    assert config_hash(dict(cfg, seed=7), keys) == config_hash(cfg, keys)
    assert config_hash(dict(cfg, end_quarter="2026Q2"), keys) != config_hash(cfg, keys)


def test_hash_does_not_depend_on_key_order(cfg):
    reordered = dict(reversed(list(cfg.items())))
    assert config_hash(reordered) == config_hash(cfg)


@pytest.mark.parametrize(
    "change",
    [
        {"mode": "medium"},
        {"data_source": "kaggle"},
        {"start_quarter": "2026-Q1"},
        {"start_quarter": "2026Q2", "end_quarter": "2026Q1"},
        {"models": "ST4000DM000"},
        {"horizons": [7, 0]},
        {"seed": "42"},
        {"budget": {"margin_bytes": 1}},
    ],
)
def test_bad_values_rejected(cfg, change):
    with pytest.raises(ConfigError):
        check_config(dict(cfg, **change))


def test_missing_key_rejected(cfg):
    del cfg["horizons"]
    with pytest.raises(ConfigError, match="horizons"):
        check_config(cfg)


def test_quarters_between_crosses_year():
    assert quarters_between("2025Q3", "2026Q2") == ["2025Q3", "2025Q4", "2026Q1", "2026Q2"]
    assert quarters_between("2026Q1", "2026Q1") == ["2026Q1"]


def test_quarter_days():
    q1 = quarter_days("2026Q1")
    assert (q1[0].isoformat(), q1[-1].isoformat(), len(q1)) == ("2026-01-01", "2026-03-31", 90)
    assert len(quarter_days("2024Q1")) == 91  # leap year
    assert quarter_days("2025Q4")[-1].isoformat() == "2025-12-31"


# --- splits and studies (M3) -----------------------------------------------------------


def test_split_shares_must_sum_to_one(cfg):
    bad = dict(cfg, splits=dict(cfg["splits"], shares={"train": 0.5, "val": 0.2, "test": 0.2}))
    with pytest.raises(ConfigError, match="sum to 1"):
        check_config(bad)


def test_dropped_days_need_a_date_and_evidence(cfg):
    with pytest.raises(ConfigError, match="evidence"):
        check_config(dict(cfg, dropped_days={"2022-12-10": ""}))
    with pytest.raises(ConfigError, match="date"):
        check_config(dict(cfg, dropped_days={"12/10/22": "copy"}))
    assert dropped_days(dict(cfg, dropped_days={"2022-12-10": "copy"}), "2022Q4") == ["2022-12-10"]
    assert dropped_days(dict(cfg, dropped_days={"2022-12-10": "copy"}), "2023Q1") == []
    with pytest.raises(ConfigError, match="smart_missing_days"):
        check_config(dict(cfg, smart_missing_days={"2019-06-17": " "}))
    with pytest.raises(ConfigError, match="smart_missing_days"):
        check_config(dict(cfg, smart_missing_days=["2019-06-17"]))
    check_config(dict(cfg, smart_missing_days={"2019-06-17": "scrambled SMART values"}))


def test_min_failed_drives_needs_every_split(cfg):
    bad = dict(cfg, splits=dict(cfg["splits"], min_failed_drives={"train": 30, "val": 1}))
    with pytest.raises(ConfigError, match="min_failed_drives"):
        check_config(bad)


def test_study_groups_and_windows_are_checked(cfg):
    studies = dict(cfg["studies"], W=dict(cfg["studies"]["W"], groups=["S", "X"]))
    with pytest.raises(ConfigError, match="groups"):
        check_config(dict(cfg, studies=studies))
    p = dict(cfg["studies"]["P"], val=["2026-01-20", "2026-02-20"])          # overlaps train
    with pytest.raises(ConfigError, match="P"):
        check_config(dict(cfg, studies=dict(cfg["studies"], P=p)))
    w = dict(cfg["studies"]["W"], models={"SYNTH HDD14TEY": ["2026Q2", "2026Q1"]})
    with pytest.raises(ConfigError, match="W"):
        check_config(dict(cfg, studies=dict(cfg["studies"], W=w)))


def test_full_config_studies():
    cfg = load_config(CONFIGS / "full.yaml")
    assert cfg["splits"]["shares"] == {"train": 0.5, "val": 0.2, "test": 0.3}
    assert cfg["splits"]["min_failed_drives"] == {"train": 30, "val": 1, "test": 1}
    assert set(cfg["studies"]["W"]["models"]) == set(cfg["models"])
    assert str(cfg["studies"]["P"]["train"][0]) == "2023-08-01"
    assert str(cfg["studies"]["P"]["test"][1]) == "2026-06-30"
