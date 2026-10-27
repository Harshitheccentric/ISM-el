"""Synthetic Backblaze-like data. FOR TESTS ONLY: never report a metric computed on it.

It reproduces the quirks the real pipeline must handle:
- ID columns are zero-padded strings (serial '00S00007', cluster '031', slot '0014').
- pod_id repeats across vaults, so pod_id alone is not a pod key.
- boot drives: no SMART values and an empty pod_slot_num.
- SMART 241/242 exist for one model string only; the other has them empty. The model
  that has 241/242 has SMART 197 empty (as on the real fixture).
- SMART raw values above 2^31 (LBA counters), and a 241/242 counter reset.
- failed drives (failure=1 on their last day), censored drives (leave without a failure
  flag), and late-joining drives (fill phase, heavy writes).
- quarter zips with a nested folder and junk files (__MACOSX, dot-files, a README).

Model strings are fake ('SYNTH ...') so synthetic rows can never pass for real models.
Every value is a deterministic function of (seed, drive, day).
"""

import csv
import io
import random
import zipfile
from dataclasses import dataclass
from datetime import date

from diskctx.config import parse_quarter, quarter_days, quarters_between

DATA_SOURCE = "synthetic"

MODEL_NO_LBA = "SYNTH HDD14TA"     # no SMART 241/242
MODEL_LBA = "SYNTH HDD14TEY"       # has SMART 241/242, empty 197
MODEL_BOOT = "SYNTH BOOT VD"       # boot drive: no SMART, no slot

CAPACITY = 14000519643136
BOOT_CAPACITY = 480036847616

BASE_COLUMNS = [
    "date", "serial_number", "model", "capacity_bytes", "failure",
    "datacenter", "cluster_id", "vault_id", "pod_id", "pod_slot_num", "is_legacy_format",
]
SMART_IDS = [1, 5, 7, 9, 187, 188, 194, 197, 198, 241, 242]
COLUMNS = BASE_COLUMNS + [f"smart_{i}_{kind}" for i in SMART_IDS for kind in ("normalized", "raw")]

ERROR_COUNTERS = [5, 187, 188, 197, 198]

# (datacenter, cluster_id, vault_id, pod_id). pod_id '02' is in two vaults on purpose.
PODS = [
    ("sac0", "000", "1154", "02"),
    ("sac0", "000", "1154", "13"),
    ("ams5", "031", "2017", "02"),
    ("ams5", "031", "2017", "05"),
]

# Roles for data drives, cycled within each model.
ROLES = ["failed", "censored", "late", "reset", "active", "active"]


@dataclass
class Drive:
    serial: str
    model: str
    pod: tuple
    slot: str            # '' for boot drives
    first_day: date
    last_day: date       # last day the drive appears
    failed: bool         # failure=1 on last_day
    reset_day: date      # 241/242 counter reset, or None
    age_days: int        # age on first_day
    tb_per_day: float    # normal daily write volume


class SyntheticFleet:
    def __init__(self, seed, start_quarter, end_quarter, n_drives=24, n_boot=3):
        self.seed = seed
        self.days = [d for q in quarters_between(start_quarter, end_quarter) for d in quarter_days(q)]
        rng = random.Random(seed)
        self.drives = self._make_drives(rng, n_drives, n_boot)

    def _span_day(self, rng, lo, hi):
        """A random day between fractions lo and hi of the fleet's date span."""
        n = len(self.days)
        return self.days[rng.randint(int(lo * (n - 1)), int(hi * (n - 1)))]

    def _make_drives(self, rng, n_drives, n_boot):
        first, last = self.days[0], self.days[-1]
        drives = []
        half = n_drives // 2
        for i in range(n_drives):
            model = MODEL_NO_LBA if i < half else MODEL_LBA
            role = ROLES[(i if i < half else i - half) % len(ROLES)]
            pod = PODS[i % len(PODS)]
            d = Drive(
                serial=f"00S{i:05d}",
                model=model,
                pod=pod,
                slot=f"{i % 60:04d}",
                first_day=first,
                last_day=last,
                failed=False,
                reset_day=None,
                age_days=rng.randint(100, 1500),
                tb_per_day=rng.uniform(0.01, 0.2),
            )
            if role == "failed":
                d.last_day = self._span_day(rng, 0.4, 0.9)
                d.failed = True
            elif role == "censored":
                d.last_day = self._span_day(rng, 0.2, 0.6)
            elif role == "late":
                d.first_day = self._span_day(rng, 0.3, 0.5)
                d.age_days = 0
            elif role == "reset" and model == MODEL_LBA:
                d.reset_day = self._span_day(rng, 0.4, 0.6)
            drives.append(d)
        for j in range(n_boot):
            drives.append(Drive(
                serial=f"000b{j:012x}",
                model=MODEL_BOOT,
                pod=PODS[j % len(PODS)],
                slot="",
                first_day=first,
                last_day=last,
                failed=False,
                reset_day=None,
                age_days=0,
                tb_per_day=0.0,
            ))
        return drives

    # --- one day ------------------------------------------------------------------

    def day_rows(self, day):
        """All rows for one date, as dicts of strings ('' = empty, like the real CSVs)."""
        return [self._row(d, day) for d in self.drives if d.first_day <= day <= d.last_day]

    def _row(self, drive, day):
        dc, cluster, vault, pod = drive.pod
        row = dict.fromkeys(COLUMNS, "")
        row.update({
            "date": day.isoformat(),
            "serial_number": drive.serial,
            "model": drive.model,
            "capacity_bytes": str(BOOT_CAPACITY if drive.model == MODEL_BOOT else CAPACITY),
            "failure": "1" if drive.failed and day == drive.last_day else "0",
            "datacenter": dc,
            "cluster_id": cluster,
            "vault_id": vault,
            "pod_id": pod,
            "pod_slot_num": drive.slot,
            "is_legacy_format": "false",
        })
        if drive.model == MODEL_BOOT:
            return row

        rng = random.Random(f"{self.seed}-{drive.serial}-{day.isoformat()}")
        days_in = (day - drive.first_day).days
        smart = {
            1: 0,
            7: 0,
            9: (drive.age_days + days_in) * 24,
            194: rng.randint(22, 40),
        }
        for counter in ERROR_COUNTERS:
            smart[counter] = 0
        # Failing drives: error counters climb over their last 10 days.
        if drive.failed:
            days_left = (drive.last_day - day).days
            if days_left < 10:
                for counter in (5, 187, 197, 198):
                    smart[counter] = (10 - days_left) * rng.randint(1, 8)
        if drive.model == MODEL_LBA:
            smart[241], smart[242] = self._lba_counters(drive, day, days_in)
            del smart[197]  # empty for this model string

        for i, raw in smart.items():
            row[f"smart_{i}_normalized"] = "100"
            row[f"smart_{i}_raw"] = str(raw)
        return row

    def _lba_counters(self, drive, day, days_in):
        """Cumulative LBAs written/read (512 B each). Starts above 2^31; resets on reset_day."""
        lbas_per_day = drive.tb_per_day * 1e12 / 512
        if drive.age_days == 0 and days_in < 20:
            lbas_per_day = 14e12 / 20 / 512  # fill phase: a new drive filled in ~20 days
        if drive.reset_day is not None and day >= drive.reset_day:
            n = (day - drive.reset_day).days
            return int(n * lbas_per_day), int(n * lbas_per_day / 2)
        written = 3e10 + (drive.age_days + days_in) * lbas_per_day
        return int(written), int(written / 2)

    # --- files --------------------------------------------------------------------

    def day_csv(self, day, drop_columns=()):
        """One day's CSV text, optionally without some columns (as in older quarters)."""
        cols = [c for c in COLUMNS if c not in drop_columns]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(self.day_rows(day))
        return buf.getvalue()

    def write_quarter_zip(self, quarter, zip_path, drop_columns=()):
        """Write a Backblaze-style quarter zip. Returns the member names of the real day CSVs.

        Day CSVs sit in a nested folder; junk members mimic macOS and stray files.
        """
        year, q = parse_quarter(quarter)
        folder = f"data_Q{q}_{year}"
        fleet_days = set(self.days)
        days = [d for d in quarter_days(quarter) if d in fleet_days]
        names = []
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for day in days:
                name = f"{folder}/{day.isoformat()}.csv"
                z.writestr(name, self.day_csv(day, drop_columns))
                names.append(name)
            first = days[0].isoformat()
            z.writestr(f"__MACOSX/{folder}/._{first}.csv", b"\x00\x05\x16\x07junk")
            z.writestr(f"{folder}/.DS_Store", b"\x00\x00\x00\x01Bud1")
            z.writestr(f"{folder}/._{first}.csv", b"\x00\x05\x16\x07junk")
            z.writestr(f"{folder}/notes/README.txt", "synthetic test data\n")
        return names

