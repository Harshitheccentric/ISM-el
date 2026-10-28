"""Storage tiers, done-markers, the stage pattern, and the results CSV.

Two tiers:
- durable: copied to ROOT, survives a Colab reset (audit tables, Parquet, splits,
  models, results).
- cache: local disk only, rebuilt when missing (feature tables).

Stage pattern: build into a fresh local work folder -> copy each output to its tier ->
verify the copy -> write the done-marker LAST. A crash at any point before the marker
leaves no marker, so the stage simply runs again.

The marker records every output file's path, size, and a sha256 (or the row count for
Parquet, read from the file footer). A stage is skipped only if its marker exists, the
hash matches, and every recorded file is still there with the same size and checksum /
row count. Otherwise it is rebuilt from durable inputs, never failed: this covers a new
Colab session that wiped the cache, and a half-copied file on Drive.

Hash propagation: a stage's effective hash = hash(its own config keys + the effective
hashes of the stages it depends on). Changing an upstream setting (e.g. the model list)
therefore reruns every downstream stage, while unrelated stages are skipped.

Layout (MODE smoke and full never share a folder; synthetic runs live under _synthetic):
    ROOT/<mode>/                     durable outputs, _markers/, results/results.csv
    ROOT/_synthetic/<mode>/          same, for synthetic data
    LOCAL/<mode>/cache/              cache outputs
    LOCAL/<mode>/work/<stage>/       scratch while a stage builds
    LOCAL/<mode>/downloads/          quarter zips (deleted after use unless kept)
"""

import csv
import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pyarrow.parquet as pq

from diskctx.config import config_hash

DURABLE = "durable"
CACHE = "cache"


class StageError(RuntimeError):
    pass


@dataclass
class Stage:
    """One pipeline step.

    name:    unique stage name, used for the marker file.
    tier:    DURABLE or CACHE.
    outputs: file or folder names, relative to the tier folder.
    build:   build(work_dir, storage) writes every output into work_dir. It reads its
             inputs from storage.path(...). It may return a small dict of facts about
             the build; it is stored in the marker as "info".
    keys:    config keys this stage depends on (None = the whole config).
    deps:    names of upstream stages whose outputs it reads.
    params:  values that belong to this stage only and are hashed with its keys, e.g.
             its quarter, its URL, or the resolved model list.
    """

    name: str
    tier: str
    outputs: list
    build: Callable
    keys: list = None
    deps: list = field(default_factory=list)
    params: dict = field(default_factory=dict)


def stage_hashes(stages, cfg):
    """{stage name: effective hash}. Stages must be listed after the stages they depend on."""
    hashes = {}
    for stage in stages:
        unknown = [d for d in stage.deps if d not in hashes]
        if unknown:
            raise StageError(f"stage {stage.name!r} depends on {unknown}, which must come earlier")
        parts = {
            "own": config_hash(cfg, stage.keys),
            "deps": {d: hashes[d] for d in sorted(stage.deps)},
        }
        if stage.params:
            parts["params"] = stage.params
        text = json.dumps(parts, sort_keys=True)
        hashes[stage.name] = hashlib.sha256(text.encode()).hexdigest()[:16]
    return hashes


class Storage:
    def __init__(self, root, local_dir, mode, data_source):
        sub = Path("_synthetic") / mode if data_source == "synthetic" else Path(mode)
        self.mode = mode
        self.data_source = data_source
        self.run_dir = Path(root) / sub
        self.cache_dir = Path(local_dir) / sub / "cache"
        self.work_dir = Path(local_dir) / sub / "work"
        self.download_dir = Path(local_dir) / sub / "downloads"   # quarter zips, local only
        self.marker_dir = self.run_dir / "_markers"
        self.results_path = self.run_dir / "results" / "results.csv"

    @classmethod
    def from_config(cls, cfg):
        return cls(cfg["root"], cfg["local_dir"], cfg["mode"], cfg["data_source"])

    def path(self, tier, name):
        """Where output `name` of a stage in `tier` lives."""
        if tier == DURABLE:
            return self.run_dir / name
        if tier == CACHE:
            return self.cache_dir / name
        raise ValueError(f"unknown tier {tier!r}")

    # --- markers -------------------------------------------------------------

    def marker_path(self, stage_name):
        return self.marker_dir / f"{stage_name}.json"

    def read_marker(self, stage_name):
        p = self.marker_path(stage_name)
        if not p.exists():
            return None
        return json.loads(p.read_text())

    def write_marker(self, stage, cfg_hash, info=None):
        marker = {
            "stage": stage.name,
            "tier": stage.tier,
            "config_hash": cfg_hash,
            "data_source": self.data_source,
            "outputs": list(stage.outputs),
            "files": self.manifest(stage),
            "info": info or {},
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        p = self.marker_path(stage.name)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(marker, indent=2))
        tmp.replace(p)

    def manifest(self, stage, base=None):
        """Entries for every file of the stage's outputs, under base (default: its tier)."""
        base = Path(base) if base else self.path(stage.tier, "")
        return manifest(base, stage.outputs)

    def stage_state(self, stage, cfg_hash):
        """pending (never run), stale (hash changed), missing (outputs gone), mismatch
        (a file differs from the marker: size, checksum or row count), or done.
        missing and mismatch are rebuilt."""
        marker = self.read_marker(stage.name)
        if marker is None:
            return "pending"
        if marker["config_hash"] != cfg_hash:
            return "stale"
        if not all(self.path(stage.tier, out).exists() for out in stage.outputs):
            return "missing"
        try:
            files = self.manifest(stage)
        except Exception:  # e.g. a truncated Parquet whose footer cannot be read
            return "mismatch"
        if files != marker.get("files"):
            return "mismatch"
        return "done"

    # --- the stage pattern ---------------------------------------------------

    def run_stage(self, stage, cfg_hash):
        """Run `stage` unless it is done. Returns 'skipped', 'built' or 'rebuilt'."""
        state = self.stage_state(stage, cfg_hash)
        if state == "done":
            return "skipped"

        # Drop the old marker first, so a crash mid-copy cannot leave a marker that
        # points at half-overwritten outputs.
        self.marker_path(stage.name).unlink(missing_ok=True)

        work = self.work_dir / stage.name
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        info = stage.build(work, self)

        for out in stage.outputs:
            src = work / out
            if not src.exists():
                raise StageError(f"stage {stage.name!r} did not write output {out!r}")
            dest = self.path(stage.tier, out)
            copy_and_verify(src, dest)

        shutil.rmtree(work)
        self.write_marker(stage, cfg_hash, info)
        return "rebuilt" if state in ("missing", "mismatch") else "built"

    def run_stages(self, stages, cfg):
        """Run stages in order with their effective hashes. Returns {name: result}."""
        hashes = stage_hashes(stages, cfg)
        results = {}
        for stage in stages:
            results[stage.name] = self.run_stage(stage, hashes[stage.name])
            print(f"{stage.name}: {results[stage.name]}", flush=True)
        return results


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def file_entry(path, rel):
    """path, bytes, and rows (Parquet, from the footer) or sha256 (any other file)."""
    entry = {"path": rel, "bytes": path.stat().st_size}
    if path.suffix == ".parquet":
        entry["rows"] = pq.ParquetFile(path).metadata.num_rows
    else:
        entry["sha256"] = sha256_file(path)
    return entry


def manifest(base, names):
    """Sorted entries for every file in the outputs `names` (files or folders) under base."""
    base = Path(base)
    entries = []
    for name in names:
        top = base / name
        files = [top] if top.is_file() else sorted(p for p in top.rglob("*") if p.is_file())
        for p in files:
            entries.append(file_entry(p, p.relative_to(base).as_posix()))
    return sorted(entries, key=lambda e: e["path"])


def copy_and_verify(src, dest):
    """Copy a file or folder to dest (replacing it) and check every file's size and
    checksum / row count match the source."""
    src, dest = Path(src), Path(dest)
    if dest.is_dir():
        shutil.rmtree(dest)
    elif dest.exists():
        dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        shutil.copytree(src, dest)
    else:
        shutil.copy2(src, dest)
    if manifest(src.parent, [src.name]) != manifest(dest.parent, [dest.name]):
        raise StageError(f"copy of {src} to {dest} does not match the source")


# --- results CSV ---------------------------------------------------------------
# One row per fit. Each row has a run_key (unique per fit) and a data_source.


def results_keys(path):
    """Set of run_keys already in the results CSV."""
    path = Path(path)
    if not path.exists():
        return set()
    with open(path, newline="") as f:
        return {row["run_key"] for row in csv.DictReader(f)}


def append_result(path, row):
    """Append one row. Returns False (and writes nothing) if its run_key is present."""
    for key in ("run_key", "data_source"):
        if key not in row:
            raise ValueError(f"results row needs {key!r}")
    path = Path(path)
    if row["run_key"] in results_keys(path):
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    if not new_file:
        with open(path, newline="") as f:
            header = next(csv.reader(f))
        if list(row) != header:
            raise ValueError(f"results row columns {list(row)} differ from file header {header}")
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row))
        if new_file:
            writer.writeheader()
        writer.writerow(row)
    return True


def read_results(path, allow_synthetic=False):
    """Read the results CSV for reporting. Refuses synthetic rows unless allowed (tests)."""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not allow_synthetic and any(r["data_source"] != "backblaze" for r in rows):
        raise ValueError(f"{path} has non-backblaze rows; synthetic results are never reported")
    return rows
