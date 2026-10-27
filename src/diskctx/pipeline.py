"""Run the pipeline stages in order. M2: sources -> audit (pass 1) -> select -> ingest (pass 2).

Stages are built in phases, because later stages depend on what earlier ones found:
which quarters have a zip (sources), and which models were selected (select).
A non-empty `models:` in the config skips the audit and select.
"""

from diskctx.audit import audit_stages, read_selection, select_stage
from diskctx.ingest import ingest_stages
from diskctx.sources import head_ok, read_sources, sources_stage
from diskctx.status import SELECTION_FILE, status_report
from diskctx.storage import DURABLE, Storage


def ingest_plan(cfg, storage, sources):
    """(models, quarters) for pass 2: the config allowlist, else audit/selection.json.
    None if the audit has not selected yet."""
    if cfg["models"]:
        return cfg["models"], [r["quarter"] for r in sources if r["kind"] != "missing"]
    if not storage.path(DURABLE, SELECTION_FILE).exists():
        return None
    selection = read_selection(storage)
    return [m["model"] for m in selection["models"]], selection["quarters"]


def run_data(cfg, head=head_ok, fetch=None, keep_zips=False):
    """Run the M2 data stages. Returns {stage name: 'built' | 'skipped' | 'rebuilt'}.

    head:      HEAD check for derived URLs (tests pass a fake).
    fetch:     fetch(url, dest) that puts a quarter zip at dest (default: download).
    keep_zips: keep quarter zips on local disk after use (default: delete them).
    """
    storage = Storage.from_config(cfg)
    src = sources_stage(cfg, head=head)
    results = storage.run_stages([src], cfg)
    sources = read_sources(storage)

    if not cfg["models"]:
        audits = audit_stages(cfg, sources, fetch=fetch, keep_zips=keep_zips)
        select = select_stage(cfg, sources, [a.name for a in audits])
        results.update(storage.run_stages([src, *audits, select], cfg))

    models, quarters = ingest_plan(cfg, storage, sources)
    if models:
        ingests = ingest_stages(cfg, sources, models, quarters, fetch=fetch, keep_zips=keep_zips)
        results.update(storage.run_stages(ingests, cfg))
    return results


def data_stages(cfg):
    """The M2 stages known so far, in order (for the status table)."""
    storage = Storage.from_config(cfg)
    src = sources_stage(cfg)
    if not storage.marker_path(src.name).exists():
        return [src]
    sources = read_sources(storage)
    stages = [src]
    if not cfg["models"]:
        audits = audit_stages(cfg, sources)
        stages += [*audits, select_stage(cfg, sources, [a.name for a in audits])]
    plan = ingest_plan(cfg, storage, sources)
    if plan and plan[0]:
        stages += ingest_stages(cfg, sources, *plan)
    return stages


def print_status(cfg):
    print(status_report(Storage.from_config(cfg), data_stages(cfg), cfg))
