"""Command line.

    python -m diskctx run --config configs/full.yaml --stages sources,audit
    python -m diskctx status --config configs/full.yaml

Set DISKCTX_LOCAL_ROOT (e.g. ~/diskctx_data) to keep data outside the repo. Runs are
resumable: rerun the same command after a crash or Ctrl-C and finished stages are
skipped. The status table is printed at the end, also when a run stops early.
"""

import argparse
import sys

from diskctx.config import load_config
from diskctx.pipeline import STAGE_GROUPS, print_status, run_data
from diskctx.status import BudgetError


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m diskctx")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run pipeline stages (resumable)")
    run.add_argument("--config", required=True)
    run.add_argument("--stages", default=",".join(STAGE_GROUPS),
                     help=f"comma-separated, from: {','.join(STAGE_GROUPS)} (default: all)")
    run.add_argument("--keep-zips", action="store_true", help="keep quarter zips after use")
    status = sub.add_parser("status", help="print the status table")
    status.add_argument("--config", required=True)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    print(f"ROOT: {cfg['root']}   local: {cfg['local_dir']}")
    if args.command == "status":
        print_status(cfg)
        return 0

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = [s for s in stages if s not in STAGE_GROUPS]
    if unknown:
        parser.error(f"unknown stages {unknown}; choose from {','.join(STAGE_GROUPS)}")
    code = 0
    try:
        run_data(cfg, stages, keep_zips=args.keep_zips)   # prints each stage as it finishes
    except BudgetError as e:
        print(f"\nSTOPPED: {e}")
        code = 1
    finally:
        print()
        print_status(cfg)
    return code


if __name__ == "__main__":
    sys.exit(main())
