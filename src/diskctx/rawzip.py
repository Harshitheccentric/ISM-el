"""Read daily CSVs out of a quarter zip one at a time, without extracting the whole zip.

A day CSV is any member named YYYY-MM-DD.csv, at any folder depth. Everything else is
skipped: __MACOSX folders, dot-files (._2026-01-01.csv, .DS_Store), READMEs, other files.
Peak local disk = the zip + one extracted day.
"""

import re
import shutil
import zipfile
from pathlib import PurePosixPath

DAY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")


class RawZipError(RuntimeError):
    pass


def is_junk(name):
    parts = PurePosixPath(name).parts
    return any(p == "__MACOSX" or p.startswith(".") for p in parts)


def day_members(zip_path, drop=()):
    """[(date string, member name)] for every day CSV in the zip, in date order, without
    the days in `drop` (config dropped_days; each must be in the zip)."""
    days = {}
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if info.is_dir() or is_junk(info.filename):
                continue
            m = DAY_RE.match(PurePosixPath(info.filename).name)
            if not m:
                continue
            day = m.group(1)
            if day in days:
                raise RawZipError(f"{zip_path}: date {day} appears twice ({days[day]}, {info.filename})")
            days[day] = info.filename
    absent = [d for d in drop if d not in days]
    if absent:
        raise RawZipError(f"{zip_path}: dropped days {absent} are not in the zip")
    return sorted((d, m) for d, m in days.items() if d not in drop)


def extract_member(zip_path, member, dest):
    """Copy one member to dest (a file path). Returns its size in bytes."""
    with zipfile.ZipFile(zip_path) as z, z.open(member) as src, open(dest, "wb") as out:
        shutil.copyfileobj(src, out, length=1 << 20)
    return dest.stat().st_size
