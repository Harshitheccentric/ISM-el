"""Status table, download log, and the disk budget guard.

The download log (durable, ROOT/<mode>/logs/download_log.csv) records measured bytes per
quarter: zip size, largest single day CSV, and Parquet written. The budget guard uses
those measurements to decide whether the next quarter fits, and stops with a clear
message if it does not. The estimate errs high: max(config default, 1.5 x the largest
value measured so far).
"""

import csv
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from diskctx.storage import DURABLE, stage_hashes

SELECTION_FILE = "audit/selection.json"
SOURCES_FILE = "sources/sources.csv"

LOG_COLUMNS = ["quarter", "zip_bytes", "largest_day_bytes", "parquet_bytes", "data_source", "timestamp"]
SIZE_COLUMNS = ["zip_bytes", "largest_day_bytes", "parquet_bytes"]


class BudgetError(RuntimeError):
    pass


# --- download log ----------------------------------------------------------------


def download_log_path(storage):
    return storage.run_dir / "logs" / "download_log.csv"


def read_download_log(storage):
    """{quarter: {zip_bytes, largest_day_bytes, parquet_bytes, ...}} with sizes as ints."""
    path = download_log_path(storage)
    if not path.exists():
        return {}
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    log = {}
    for row in rows:
        for col in SIZE_COLUMNS:
            row[col] = int(row[col])
        log[row["quarter"]] = row
    return log


def log_download(storage, quarter, zip_bytes, largest_day_bytes, parquet_bytes):
    """Record measured bytes for a quarter (replaces an earlier row for the same quarter).

    A size passed as None keeps the earlier value (0 if none): the audit (pass 1)
    writes no Parquet, so it must not wipe the Parquet bytes measured by ingest.
    """
    log = read_download_log(storage)
    old = log.get(quarter, {})
    sizes = dict(zip(SIZE_COLUMNS, (zip_bytes, largest_day_bytes, parquet_bytes)))
    sizes = {c: int(v) if v is not None else old.get(c, 0) for c, v in sizes.items()}
    log[quarter] = {
        "quarter": quarter,
        **sizes,
        "data_source": storage.data_source,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    path = download_log_path(storage)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS)
        writer.writeheader()
        for q in sorted(log):
            writer.writerow(log[q])


# --- budget guard ------------------------------------------------------------------


def existing_parent(path):
    """The path itself, or its nearest parent that exists (folders may not exist yet)."""
    path = Path(path).resolve()
    while not path.exists():
        path = path.parent
    return path


def free_bytes(path):
    return shutil.disk_usage(existing_parent(path)).free


def same_disk(a, b):
    return os.stat(existing_parent(a)).st_dev == os.stat(existing_parent(b)).st_dev


DEFAULT_KEYS = {
    "zip_bytes": "default_zip_bytes",
    "largest_day_bytes": "default_day_bytes",
    "parquet_bytes": "default_parquet_bytes",
}
SAFETY_FACTOR = 1.5


def estimate_sizes(log, budget):
    """Expected bytes for the next quarter. Errs high on purpose.

    Each size is max(config default, 1.5 x the largest value measured so far), never the
    latest or the average, so a small old quarter cannot lower the estimate for a big
    new one.
    """
    sizes = {}
    for col, key in DEFAULT_KEYS.items():
        largest = max((row[col] for row in log.values()), default=0)
        sizes[col] = max(budget[key], int(SAFETY_FACTOR * largest))
    return sizes


def check_budget(storage, quarter, budget, local_free=None, root_free=None, shared_disk=None):
    """Raise BudgetError if `quarter` would not fit. Returns the estimate used.

    Local disk holds, at peak: the zip, one day CSV, and the quarter's Parquet before it
    is copied to ROOT. ROOT receives the Parquet. If both are the same disk (a local run),
    the two needs are added together. The free-space and shared-disk arguments are
    measured when left as None (tests pass them in).
    """
    sizes = estimate_sizes(read_download_log(storage), budget)
    source = "max(config default, 1.5 x largest measured)"
    margin = budget["margin_bytes"]
    need_local = sizes["zip_bytes"] + sizes["largest_day_bytes"] + sizes["parquet_bytes"]
    need_root = sizes["parquet_bytes"]
    if local_free is None:
        local_free = free_bytes(storage.work_dir)
    if root_free is None:
        root_free = free_bytes(storage.run_dir)

    if shared_disk is None:
        shared_disk = same_disk(storage.work_dir, storage.run_dir)
    if shared_disk:
        need_local += need_root
        need_root = 0

    if local_free < need_local + margin:
        raise BudgetError(
            f"Stopping before {quarter}: local disk has {gb(local_free)} free, needs "
            f"{gb(need_local)} (zip + largest day + Parquet, from {source}) plus "
            f"{gb(margin)} margin. Free space under {storage.work_dir} and rerun."
        )
    if root_free < need_root + margin:
        raise BudgetError(
            f"Stopping before {quarter}: ROOT has {gb(root_free)} free, needs "
            f"{gb(need_root)} Parquet (from {source}) plus {gb(margin)} margin. "
            f"Free space in {storage.run_dir} (e.g. on Google Drive) and rerun; "
            f"finished quarters are kept and will be skipped."
        )
    return sizes


# --- status table --------------------------------------------------------------------


def gb(n):
    return f"{n / 1e9:.2f} GB"


def stage_rows(storage, stages, cfg):
    """One row per stage: name, tier, state, finished (marker time)."""
    hashes = stage_hashes(stages, cfg)
    rows = []
    for stage in stages:
        marker = storage.read_marker(stage.name)
        rows.append({
            "stage": stage.name,
            "tier": stage.tier,
            "state": storage.stage_state(stage, hashes[stage.name]),
            "finished": marker["timestamp"] if marker else "",
        })
    return rows


def format_table(rows):
    """Plain-text table from a list of dicts with the same keys."""
    if not rows:
        return "(none)"
    cols = list(rows[0])
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    lines = ["  ".join(c.ljust(widths[c]) for c in cols)]
    lines.append("  ".join("-" * widths[c] for c in cols))
    for r in rows:
        lines.append("  ".join(str(r[c]).ljust(widths[c]) for c in cols))
    return "\n".join(lines)


def selection_lines(storage, cfg):
    """Which models the run uses, and why.

    A non-empty `models:` in the config overrides the audit. Otherwise the audit's
    choice is read from audit/selection.json: {"models": [{"model": ..., <reasons>}]},
    where the reasons (failures, coverage, window) are written by the select stage.
    """
    if cfg["models"]:
        return [f"models: config allowlist (overrides audit): {', '.join(cfg['models'])}"]
    path = storage.path(DURABLE, SELECTION_FILE)
    if not path.exists():
        return ["models: not selected yet (audit has not run)"]
    selection = json.loads(path.read_text())
    lines = [f"models: selected by audit ({SELECTION_FILE})", format_table(selection["models"])]
    if selection.get("warning"):
        lines.append(f"WARNING: {selection['warning']}")
    return lines


def sources_lines(storage):
    """Count of listed / derived / missing quarters, and which quarters are missing."""
    path = storage.path(DURABLE, SOURCES_FILE)
    if not path.exists():
        return ["sources: not resolved yet"]
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    counts = {kind: sum(r["kind"] == kind for r in rows) for kind in ("listed", "derived", "missing")}
    missing = [r["quarter"] for r in rows if r["kind"] == "missing"]
    return [
        "sources: " + "  ".join(f"{k} {n}" for k, n in counts.items()),
        f"missing quarters (skipped): {', '.join(missing) if missing else 'none'}",
    ]


def status_report(storage, stages, cfg):
    """Free space, model selection, stage states, and measured bytes per quarter, as text."""
    log = read_download_log(storage)
    size_rows = [
        {"quarter": q, **{c: gb(log[q][c]) for c in SIZE_COLUMNS}} for q in sorted(log)
    ]
    return "\n".join([
        f"mode: {storage.mode}   data_source: {storage.data_source}",
        f"free ROOT  ({storage.run_dir}): {gb(free_bytes(storage.run_dir))}",
        f"free local ({storage.work_dir}): {gb(free_bytes(storage.work_dir))}",
        "",
        *sources_lines(storage),
        *selection_lines(storage, cfg),
        "",
        format_table(stage_rows(storage, stages, cfg)),
        "",
        "measured bytes per quarter:",
        format_table(size_rows),
    ])
