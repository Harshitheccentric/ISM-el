"""Run the pipeline stages in order. M2: sources -> audit (pass 1) -> select -> ingest (pass 2).

Stages are built in phases, because later stages depend on what earlier ones found:
which quarters have a zip (sources), and which models were selected (select).
A non-empty `models:` in the config skips the audit and select.

Every stage is resumable: a finished stage (marker present, hash and files match) is
skipped, so rerunning after a crash or Ctrl-C continues where it stopped.
"""

from diskctx.audit import audit_stages, read_selection, select_stage
from diskctx.ingest import ingest_stages
from diskctx.sources import read_sources, sources_stage
from diskctx.status import SELECTION_FILE, status_report
from diskctx.storage import DURABLE, Storage

STAGE_GROUPS = ("sources", "audit", "select", "ingest")


def ingest_plan(cfg, storage, sources):
    """(models, quarters) for pass 2: the config allowlist, else audit/selection.json.
    None if the audit has not selected yet."""
    if cfg["models"]:
        return cfg["models"], [r["quarter"] for r in sources if r["kind"] != "missing"]
    if not storage.path(DURABLE, SELECTION_FILE).exists():
        return None
    selection = read_selection(storage)
    return [m["model"] for m in selection["models"]], selection["quarters"]


def run_data(cfg, stages=STAGE_GROUPS, head=None, fetch=None, keep_zips=False):
    """Run the M2 data stages. Returns {stage name: 'built' | 'skipped' | 'rebuilt'}.

    stages:    which groups to run, from STAGE_GROUPS. sources always runs (every later
               stage needs its quarter list); select also runs any audit stage not yet
               done, since it depends on them.
    head:      HEAD check for derived URLs (tests pass a fake).
    fetch:     fetch(url, dest) that puts a quarter zip at dest (default: download).
    keep_zips: keep quarter zips on local disk after use (default: delete them).
    """
    unknown = [s for s in stages if s not in STAGE_GROUPS]
    if unknown:
        raise ValueError(f"unknown stages {unknown}; choose from {', '.join(STAGE_GROUPS)}")
    storage = Storage.from_config(cfg)
    src = sources_stage(cfg, head=head)
    results = storage.run_stages([src], cfg)
    sources = read_sources(storage)

    if not cfg["models"] and ("audit" in stages or "select" in stages):
        audits = audit_stages(cfg, sources, fetch=fetch, keep_zips=keep_zips)
        chain = [src, *audits]
        if "select" in stages:
            chain.append(select_stage(cfg, sources, [a.name for a in audits]))
        results.update(storage.run_stages(chain, cfg))

    if "ingest" in stages:
        plan = ingest_plan(cfg, storage, sources)
        if plan is None:
            raise RuntimeError("ingest needs a model selection: run the audit and select "
                               "stages first, or set models: in the config")
        models, quarters = plan
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
