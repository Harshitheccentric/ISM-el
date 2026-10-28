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
    "dropped_days",
    "splits",
    "studies",
)

SELECTION_KEYS = ("min_failures", "min_coverage", "max_models", "boot_share")

BUDGET_KEYS = (
    "margin_bytes",
    "default_zip_bytes",
    "default_day_bytes",
    "default_parquet_bytes",
)

SPLITS = ("train", "val", "test")
GROUPS = ("S", "W", "N", "P")          # feature groups: SMART, workload, normalized, pod
STUDIES = ("W", "P")
SPLITS_KEYS = ("shares", "healthy_per_failed", "min_failed_drives", "min_test_failures")
W_KEYS = ("groups", "val_quarters", "test_quarters", "min_quarter_failures", "models")
P_KEYS = ("groups", "start", *SPLITS)

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
    check_dropped_days(cfg["dropped_days"])
    check_splits(cfg["splits"])
    check_studies(cfg["studies"])


def check_dropped_days(days):
    """{day: evidence}: day files left out as a whole (the day counts as missing)."""
    if not isinstance(days, dict):
        raise ConfigError("dropped_days must be a mapping {YYYY-MM-DD: evidence} (may be empty)")
    for day, why in days.items():
        try:
            parse_date(day)
        except ValueError:
            raise ConfigError(f"dropped_days: {day!r} is not a YYYY-MM-DD date") from None
        if not isinstance(why, str) or not why.strip():
            raise ConfigError(f"dropped_days: {day} needs the evidence as text")


def dropped_days(cfg, quarter):
    """The day files of `quarter` that the config leaves out, as 'YYYY-MM-DD' strings."""
    in_quarter = set(quarter_days(quarter))
    return sorted(parse_date(d).isoformat() for d in cfg["dropped_days"] if parse_date(d) in in_quarter)


def dropped_param(cfg, quarter):
    """Stage params for a quarter's dropped days. Empty when there are none, so adding a
    dropped day reruns only its own quarter's stages."""
    days = dropped_days(cfg, quarter)
    return {"dropped_days": days} if days else {}


def check_splits(splits):
    missing = [k for k in SPLITS_KEYS if k not in splits]
    if missing:
        raise ConfigError(f"splits is missing keys: {missing}")
    shares = splits["shares"]
    if set(shares) != set(SPLITS) or not all(v > 0 for v in shares.values()):
        raise ConfigError(f"splits.shares needs positive train, val and test, got {shares!r}")
    if abs(sum(shares.values()) - 1) > 1e-9:
        raise ConfigError(f"splits.shares must sum to 1, got {shares!r}")
    if not isinstance(splits["healthy_per_failed"], int) or splits["healthy_per_failed"] < 1:
        raise ConfigError("splits.healthy_per_failed must be a positive integer")
    need = splits["min_failed_drives"]
    if set(need) != set(SPLITS) or not all(isinstance(v, int) and v >= 0 for v in need.values()):
        raise ConfigError(f"splits.min_failed_drives needs non-negative integers for train, val and test, "
                          f"got {need!r}")
    if not isinstance(splits["min_test_failures"], int) or splits["min_test_failures"] < 0:
        raise ConfigError("splits.min_test_failures must be a non-negative integer")


def check_studies(studies):
    """W: per model [first quarter, last quarter]; P: fixed dates. Both are optional."""
    unknown = [k for k in studies if k not in STUDIES]
    if unknown:
        raise ConfigError(f"unknown studies {unknown}; choose from {STUDIES}")
    for name, study in studies.items():
        keys = W_KEYS if name == "W" else P_KEYS
        missing = [k for k in keys if k not in study]
        if missing:
            raise ConfigError(f"study {name} is missing keys: {missing}")
        groups = study["groups"]
        if "S" not in groups or not set(groups) <= set(GROUPS):
            raise ConfigError(f"study {name} groups must include S and come from {GROUPS}, got {groups!r}")
    if "W" in studies:
        w = studies["W"]
        for k in ("val_quarters", "test_quarters"):
            if not isinstance(w[k], int) or w[k] < 1:
                raise ConfigError(f"study W {k} must be a positive integer")
        for model, span in w["models"].items():
            if len(span) != 2 or parse_quarter(span[0]) > parse_quarter(span[1]):
                raise ConfigError(f"study W window for {model} must be [first, last] quarter, got {span!r}")
    if "P" in studies:
        p = studies["P"]
        windows = [[parse_date(d) for d in p[s]] for s in SPLITS]
        if parse_date(p["start"]) > windows[0][0] or any(a > b for a, b in windows):
            raise ConfigError(f"study P windows must start after its start date and run forward: {p!r}")
        if not (windows[0][1] < windows[1][0] and windows[1][1] < windows[2][0]):
            raise ConfigError(f"study P train, val and test windows must not overlap: {p!r}")


def parse_date(value):
    """A YAML date (date object) or 'YYYY-MM-DD' text -> date."""
    return value if isinstance(value, date) else date.fromisoformat(str(value))


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


def previous_quarter(quarter):
    """'2026Q1' -> '2025Q4'"""
    year, q = parse_quarter(quarter)
    return f"{year - 1}Q4" if q == 1 else f"{year}Q{q - 1}"


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
