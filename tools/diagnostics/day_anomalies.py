"""Day anomalies: days on which drives' SMART 9 (power-on hours) takes an impossible step. Read-only.

Between two readings of a drive g calendar days apart, SMART 9 must grow by 0 to
24 h x g + --margin hours. For each day D: the drives with a reading on D and an earlier
one (steps), how many step into D impossibly (negative, or too large), and how many of
those revert: their next reading after D fits the reading before D, so D's values were
wrong rather than a real jump. A scrambled day shows a high share that reverts; the day
after it shows the same share stepping back, which does not revert. A day is flagged when
its impossible share >= --min-share. Days in the config's smart_missing_days are listed
with their evidence. Copied days (SMART 9 not advancing) are copied_days.py's job.

Reads the pass-2 Parquet (no downloads), one model and quarter at a time, with the
neighbouring quarters' rows so steps across quarter boundaries count.

    DISKCTX_LOCAL_ROOT=~/diskctx_data python tools/diagnostics/day_anomalies.py \
        --config configs/full.yaml

Writes --out (default results/diagnostics/): day_anomalies.csv, one row per day.
"""

import argparse
from pathlib import Path

import polars as pl

from diskctx.config import load_config, parse_date, previous_quarter, quarter_days
from diskctx.storage import DURABLE, Storage


def fits(step, days, margin):
    """A SMART 9 step that a real drive can make in `days` calendar days."""
    return (step >= 0) & (step <= 24 * days + margin)


def anomalies(rows, margin):
    """Per day: steps (drives with a reading that day and an earlier one), impossible
    steps into the day, and how many of those revert on the drive's next reading.
    rows: serial_number, date, smart_9_raw (rows without SMART 9 are skipped)."""
    r = (rows.filter(pl.col("smart_9_raw").is_not_null()).sort("serial_number", "date")
         .with_columns(prev_h=pl.col("smart_9_raw").shift(1).over("serial_number"),
                       prev_d=pl.col("date").shift(1).over("serial_number"),
                       next_h=pl.col("smart_9_raw").shift(-1).over("serial_number"),
                       next_d=pl.col("date").shift(-1).over("serial_number"))
         .filter(pl.col("prev_h").is_not_null()))
    days_in = (pl.col("date") - pl.col("prev_d")).dt.total_days()
    days_across = (pl.col("next_d") - pl.col("prev_d")).dt.total_days()
    impossible = ~fits(pl.col("smart_9_raw") - pl.col("prev_h"), days_in, margin)
    reverted = impossible & fits(pl.col("next_h") - pl.col("prev_h"), days_across, margin).fill_null(False)
    return (r.group_by("date")
            .agg(pl.len().alias("steps"), impossible.sum().alias("impossible"), reverted.sum().alias("reverted"))
            .sort("date"))


def quarter_files(model_dir):
    return {p.stem: p for p in sorted(model_dir.glob("*.parquet"))}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--margin", type=float, default=24, help="hours allowed above 24 h x calendar days")
    parser.add_argument("--min-share", type=float, default=0.05, help="flag a day at this share of impossible steps")
    parser.add_argument("--out", default="results/diagnostics")
    args = parser.parse_args()

    cfg = load_config(args.config)
    storage = Storage.from_config(cfg)
    parts = []
    for model_dir in sorted(storage.path(DURABLE, "parquet").iterdir()):
        files = quarter_files(model_dir)
        for quarter, path in files.items():
            nxt = next((q for q in files if q > quarter), None)
            near = [files[q] for q in (previous_quarter(quarter), quarter, nxt) if q in files]
            rows = pl.scan_parquet(near).select("serial_number", "date", "smart_9_raw").collect()
            days = quarter_days(quarter)
            parts.append(anomalies(rows, args.margin).filter(pl.col("date").is_between(days[0], days[-1])))
    table = (pl.concat(parts).group_by("date").agg(pl.col("steps").sum(), pl.col("impossible").sum(),
                                                   pl.col("reverted").sum())
             .with_columns((pl.col("impossible") / pl.col("steps")).round(4).alias("share"),
                           (pl.col("reverted") / pl.col("impossible")).round(4).alias("revert_share"))
             .with_columns((pl.col("share") >= args.min_share).alias("flagged"))
             .sort("date"))
    marked = {parse_date(d): why for d, why in cfg["smart_missing_days"].items()}
    table = table.with_columns(pl.col("date").map_elements(lambda d: marked.get(d, ""), return_dtype=pl.Utf8)
                               .alias("smart_missing_days"))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table.write_csv(out / "day_anomalies.csv")
    pl.Config.set_tbl_width_chars(200)
    pl.Config.set_fmt_str_lengths(40)
    flagged = table.filter(pl.col("flagged"))
    print(f"{table.height} days; {flagged.height} flagged (impossible SMART 9 steps >= {args.min_share:.0%}; "
          f"margin {args.margin} h); next highest share {table.filter(~pl.col('flagged'))['share'].max()}")
    print(flagged)
    print(out / "day_anomalies.csv")


if __name__ == "__main__":
    main()
