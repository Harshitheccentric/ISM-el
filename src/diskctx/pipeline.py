"""Run the pipeline stages in order. M2: sources -> audit (pass 1) -> select -> ingest (pass 2).
M3: labels (needs the audit's failure events and the pass-2 Parquet) -> splits.
M4: features (features_sampling, features_rates, one features_<model> per model).

Stages are built in phases, because later stages depend on what earlier ones found:
which quarters have a zip (sources), and which models were selected (select).
A non-empty `models:` in the config skips the audit and select.

Every stage is resumable: a finished stage (marker present, hash and files match) is
skipped, so rerunning after a crash or Ctrl-C continues where it stopped.
"""

from diskctx.audit import audit_stages, read_selection, select_stage
from diskctx.features import feature_stages
from diskctx.ingest import ingest_stages
from diskctx.labels import labels_stage
from diskctx.sources import read_sources, sources_stage
from diskctx.splits import splits_stage
from diskctx.status import SELECTION_FILE, status_report
from diskctx.storage import DURABLE, Storage

DATA_GROUPS = ("sources", "audit", "select", "ingest")
STAGE_GROUPS = (*DATA_GROUPS, "labels", "splits", "features")


def ingest_plan(cfg, storage, sources):
    """(models, quarters) for pass 2: the config allowlist, else audit/selection.json.
    None if the audit has not selected yet."""
    if cfg["models"]:
        return cfg["models"], [r["quarter"] for r in sources if r["kind"] != "missing"]
    if not storage.path(DURABLE, SELECTION_FILE).exists():
        return None
    selection = read_selection(storage)
    return [m["model"] for m in selection["models"]], selection["quarters"]


def label_split_stages(cfg, sources, models, quarters, fetch=None, keep_zips=False):
    """audit_* -> ingest_* -> labels -> splits. Labels need the failure events, so the
    audit stages are in this chain even with a model allowlist (finished ones are
    skipped)."""
    audits = audit_stages(cfg, sources, fetch=fetch, keep_zips=keep_zips)
    ingests = ingest_stages(cfg, sources, models, quarters, fetch=fetch, keep_zips=keep_zips)
    labels = labels_stage(cfg, models, quarters, [a.name for a in audits], [i.name for i in ingests])
    return [*audits, *ingests, labels, splits_stage(cfg, models)]


def run_data(cfg, stages=DATA_GROUPS, head=None, fetch=None, keep_zips=False):
    """Run pipeline stages. Returns {stage name: 'built' | 'skipped' | 'rebuilt'}.

    stages:    which groups to run, from STAGE_GROUPS (default: the M2 data stages).
               sources always runs (every later stage needs its quarter list); select
               also runs any audit stage not yet done, since it depends on them; labels,
               splits and features also run any earlier stage they depend on.
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

    if "labels" in stages or "splits" in stages or "features" in stages:
        plan = ingest_plan(cfg, storage, sources)
        if plan is None or not plan[0]:
            raise RuntimeError("labels need a model selection: run the audit and select stages "
                               "first, or set models: in the config")
        chain = label_split_stages(cfg, sources, *plan, fetch=fetch, keep_zips=keep_zips)
        if "splits" not in stages and "features" not in stages:
            chain = chain[:-1]
        for name, result in storage.run_stages([src, *chain], cfg).items():
            results.setdefault(name, result)    # keep 'built' from an earlier group
        if "features" in stages:                # built once splits/windows.csv exists
            features = feature_stages(cfg, storage, plan[0])
            for name, result in storage.run_stages([src, *chain, *features], cfg).items():
                results.setdefault(name, result)
    return results


def data_stages(cfg):
    """The stages known so far, in order (for the status table)."""
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
        chain = label_split_stages(cfg, sources, *plan)
        if not cfg["models"]:
            chain = [s for s in chain if not s.name.startswith("audit_")]   # listed above
        stages += chain
        if storage.marker_path("splits").exists():
            stages += feature_stages(cfg, storage, plan[0])
    return stages


def print_status(cfg):
    print(status_report(Storage.from_config(cfg), data_stages(cfg), cfg))
