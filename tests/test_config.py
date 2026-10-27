from pathlib import Path

import pytest

from diskctx.config import (
    ConfigError,
    check_config,
    config_hash,
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
