"""Load and check a YAML config, and hash it for done-markers.

Every knob lives in configs/*.yaml. A stage's done-marker stores the hash of the
config keys that stage depends on, so changing an unrelated knob does not force a rerun.
"""

import hashlib
import json
import os
import re
from datetime import date, timedelta
from pathlib import Path

import yaml

LOCAL_ROOT_ENV = "DISKCTX_LOCAL_ROOT"

MODES = ("smoke", "full")
DATA_SOURCES = ("backblaze", "synthetic")

# Keys that only say *where* things go; they never change a result, so they are not hashed.
PATH_KEYS = ("root", "local_dir")

REQUIRED_KEYS = (
    "mode",
    "data_source",
    "root",
    "local_dir",
    "start_quarter",
    "end_quarter",
    "models",
    "horizons",
    "seed",
    "smart_ids",
    "selection",
    "budget",
)

SELECTION_KEYS = ("min_failures", "min_coverage", "max_models", "boot_share")

BUDGET_KEYS = (
    "margin_bytes",
    "default_zip_bytes",
    "default_day_bytes",
    "default_parquet_bytes",
)

QUARTER_RE = re.compile(r"^(\d{4})Q([1-4])$")


class ConfigError(ValueError):
    pass


def load_config(path, **overrides):
    """Read a YAML config, apply overrides (e.g. root=...), check it, return a dict.

    If the env var DISKCTX_LOCAL_ROOT is set (e.g. ~/diskctx_data), root and local_dir
    become <it>/root and <it>/local, so local data lives outside the repo. Otherwise the
    config's paths are used. Explicit overrides (the notebook's Drive ROOT) win over both.
    """
    with open(path) as f:
        cfg = yaml.safe_load(f)
    local_root = os.environ.get(LOCAL_ROOT_ENV)
    if local_root:
        base = Path(local_root).expanduser()
        cfg["root"] = str(base / "root")
        cfg["local_dir"] = str(base / "local")
    cfg.update(overrides)
    check_config(cfg)
    return cfg


def check_config(cfg):
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        raise ConfigError(f"config is missing keys: {missing}")
    if cfg["mode"] not in MODES:
        raise ConfigError(f"mode must be one of {MODES}, got {cfg['mode']!r}")
    if cfg["data_source"] not in DATA_SOURCES:
        raise ConfigError(f"data_source must be one of {DATA_SOURCES}, got {cfg['data_source']!r}")
    start = parse_quarter(cfg["start_quarter"])
    end = parse_quarter(cfg["end_quarter"])
    if start > end:
        raise ConfigError(f"start_quarter {cfg['start_quarter']} is after end_quarter {cfg['end_quarter']}")
    if not isinstance(cfg["models"], list):
        raise ConfigError("models must be a list (empty = let the audit select)")
    horizons = cfg["horizons"]
    if not horizons or not all(isinstance(h, int) and h > 0 for h in horizons):
        raise ConfigError(f"horizons must be positive integers, got {horizons!r}")
    if not isinstance(cfg["seed"], int):
        raise ConfigError(f"seed must be an integer, got {cfg['seed']!r}")
    smart_ids = cfg["smart_ids"]
    if not smart_ids or not all(isinstance(i, int) and i > 0 for i in smart_ids):
        raise ConfigError(f"smart_ids must be positive integers, got {smart_ids!r}")
    missing_selection = [k for k in SELECTION_KEYS if k not in cfg["selection"]]
    if missing_selection:
        raise ConfigError(f"selection is missing keys: {missing_selection}")
    missing_budget = [k for k in BUDGET_KEYS if k not in cfg["budget"]]
    if missing_budget:
        raise ConfigError(f"budget is missing keys: {missing_budget}")


def parse_quarter(text):
    """'2026Q1' -> (2026, 1)."""
    m = QUARTER_RE.match(str(text))
    if not m:
        raise ConfigError(f"quarter must look like 2026Q1, got {text!r}")
    return int(m.group(1)), int(m.group(2))


def quarters_between(start, end):
    """All quarters from start to end inclusive, as strings: ['2025Q4', '2026Q1']."""
    year, q = parse_quarter(start)
    end_year, end_q = parse_quarter(end)
    out = []
    while (year, q) <= (end_year, end_q):
        out.append(f"{year}Q{q}")
        year, q = (year + 1, 1) if q == 4 else (year, q + 1)
    return out


def config_hash(cfg, keys=None):
    """Short sha256 of the config (or of only `keys`), ignoring path keys.

    Uses sorted-key JSON so the same settings always give the same hash.
    """
    if keys is None:
        keys = [k for k in cfg if k not in PATH_KEYS]
    subset = {k: cfg[k] for k in sorted(keys)}
    text = json.dumps(subset, sort_keys=True, default=str)
    return hashlib.sha256(text.encode()).hexdigest()[:16]



def quarter_days(quarter):
    """Every date in a quarter: '2026Q1' -> [date(2026,1,1), ..., date(2026,3,31)]."""
    year, q = parse_quarter(quarter)
    day = date(year, 3 * q - 2, 1)
    end = date(year + 1, 1, 1) if q == 4 else date(year, 3 * q + 1, 1)
    days = []
    while day < end:
        days.append(day)
        day += timedelta(days=1)
    return days
