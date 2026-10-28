# CLAUDE.md — diskctx

Context-aware disk failure prediction on public Backblaze Drive Stats.
Read `docs/HANDOFF.md` for project context and `PROGRESS.md` for where we are.

## Layout
- `src/diskctx/` — the package. One module per pipeline step (config, storage, status,
  sources, rawzip, audit, ingest, labels, splits, features, models, evaluate,
  experiments, synthetic, pipeline).
- `tests/` — pytest suite. `tests/fixtures/sample_2026-01-01.csv` is the only real data.
- `configs/` — `smoke.yaml`, `full.yaml`. Every knob lives here; hashed for done-markers.
- `tools/build_notebook.py` — builds `notebooks/diskctx_colab.ipynb` from the package source.
- `docs/data-sources.md` — the only allowed source of download URLs.

## Rules
- Never report or save a metric computed on synthetic data as a result. Synthetic data is
  for tests only.
- Every feature at date t uses only rows with date <= t-1 where the plan says past-only.
  Pod failure counts exclude the drive itself.
- No random row splits. Splits are by drive and by time.
- Work on one milestone per session. At the end: run the done-check, update PROGRESS.md
  (done, next, open questions), and tell Harsh the commit message to use. Do not commit
  unless he says so.
- If something is unclear or missing, write a TODO in PROGRESS.md and ask. Do not guess.
- Keep code simple and readable; Harsh must be able to explain every part to his mentor.
- Download URLs are listed in `docs/data-sources.md`, or derived (same base,
  `data_QN_YYYY.zip`) and confirmed by a HEAD request; a failed HEAD = missing quarter,
  logged and skipped. Drive models come only from the audit (or the config allowlist).

## Data conventions
- Read ID columns (serial_number, datacenter, cluster_id, vault_id, pod_id, pod_slot_num)
  as strings — they are zero-padded (`031`, `0028`).
- Pod key = datacenter + cluster_id + vault_id + pod_id (pod_id alone is not unique).
- SMART raw values are 64-bit ints (BIGINT, via TRY_CAST). Use raw values, not normalized.
- A column missing from a quarter becomes null, never an error.
- Dates: every row's date = the day in its file name (`YYYY-MM-DD.csv`), for every file.
  A row's own date is parsed strictly (YYYY-MM-DD or M/D/YY) only to count rows whose
  date is unreadable or not the file's day (audit quality table, status table). Dates
  never become null.
- Failure events come from the raw rows before any cleaning (`events.parquet` per
  quarter). Cleaning removes a row's features, never its failure event. Failure counts,
  selection and labels use only the events.
- Boot drives (e.g. `DELLBOSS VD`). Row rule: a row with no SMART values at all loses
  its features (its failure event is kept).
  Model rule: a model is a boot model if ≥95% of its rows have no SMART values, or ≥95%
  of its rows have an empty slot in files that "have slots" (≥1 row with a non-empty
  slot; a slot column that is empty everywhere counts as an old file). Boot
  models are dropped in every quarter (`audit/boot_models.csv`). Never drop single rows
  for an empty slot (pod fields may roll out gradually).
- Count TRY_CAST failures (non-empty → NULL) and duplicate (serial, date) rows in the audit.
- Audit and select per exact model string (MG07ACA14TA ≠ MG07ACA14TEY).
- Full 2016–2026 range must run on free Colab. Never restrict the range to fit Drive.
- Raw data is never kept. Per quarter: download zip to /content; read daily CSVs out of
  it one at a time with `zipfile` (match `*.csv`, skip `__MACOSX`/dot-files/nested
  junk); process each day, delete it; delete the zip at quarter end.
  Peak local disk = one zip + one day.
- Two passes. Pass 1 audit: every quarter in start_quarter..end_quarter → small tables,
  plus everything that needs all drives (failure events, pod-day aggregates over all
  non-boot drives, 241/242 end state carried to the next quarter). Pass 2 ingest: only
  quarters/models selected, only needed columns (~20), boot drives dropped, zstd
  Parquet per quarter. A config model allowlist may skip pass 1. Bump `AUDIT_VERSION`
  whenever pass-1 outputs change.
- Daily TB = Δ241/Δ242 × 512 B vs yesterday's counter (carried across quarters). No
  yesterday → null; negative delta = counter reset → null, counted.
- Pod coverage / P window use pod_key. The slot is only a position feature, taken from
  the last value seen ≤ t-1; slot missingness is never a feature.
- Every artifact and results row carries `data_source` (`backblaze` | `synthetic`).
  Synthetic runs write under `ROOT/_synthetic/`; reporting code refuses them.

## Splits and leakage
- Per study and model: a drive that fails inside a split window goes to that split;
  every other drive goes by seeded hash (config shares). A serial appears in one split
  only. M5 weights healthy drives by 1/share of their split for precision, F1 and MCC.
- The failure-day row is the event, not a sample: samples are days t strictly before
  the failure date; the failure-day row is in no split.
- Columns whose missingness rises before failure (slot, SMART 5/9/194/197/198) use the
  last value seen ≤ t-1.
- Embargo per horizon N: drop train and val drive-days whose (t, t+N] crosses the
  window end. Censoring is per window: a negative needs t+N <= the drive's last record
  up to the window end.
- Compute features on full drive history, then cut to the split window (warm-up).
- Downsample healthy drives by drive, on train only.

## Notebook / Colab
- One ROOT. On Colab it is a Drive folder (mount guarded by `try: import google.colab`);
  locally it is outside the repo: `DISKCTX_LOCAL_ROOT` (e.g. `~/diskctx_data`) gives
  `<it>/root` and `<it>/local`; fallback = the config paths. Marker paths are relative
  to ROOT, so a durable folder copied to Drive keeps its finished stages.
- CLI (resumable, prints the status table): `python -m diskctx run --config
  configs/full.yaml --stages sources,audit` and `python -m diskctx status --config ...`.
- Stage pattern: work on local disk → copy to ROOT → verify → write done-marker LAST
  (config hash + timestamp). Skip only if marker exists and hash matches.
- Two artifact tiers: durable (copied to ROOT: audit tables, filtered Parquet, splits,
  models, results) and cache (local only: feature tables). Marker present but cache
  output missing → rebuild from durable inputs, never fail.
- Budget guard: status table shows free ROOT and /content space plus measured bytes per
  quarter (zip, largest day, Parquet), logged in the download log. Stop with a clear
  message if the next unit would not fit.
- Results CSV: append one row per fit; skip run keys already present.
- MODE `smoke` and `full` write to separate folders.
- Pin only what the notebook installs (duckdb, polars, pyyaml). Use Colab's own numpy,
  scikit-learn, xgboost, pyarrow, scipy, shap. Write installed versions to RUN_SUMMARY.md.
- Fixed seeds everywhere.

## Done-checks
Always also run the full `pytest -q`.

| M | Scope | Command |
|---|---|---|
| M0 | skeleton | `pip install -e ".[dev]" && pytest -q` |
| M1 | config, storage tiers/resume, budget guard, status, synthetic | `pytest -q tests/test_config.py tests/test_storage.py tests/test_status.py tests/test_synthetic.py` |
| M2 | sources, rawzip, audit, select, ingest | `pytest -q tests/test_sources.py tests/test_rawzip.py tests/test_audit.py tests/test_ingest.py` |
| M3 | labels, splits | `pytest -q tests/test_labels.py tests/test_splits.py` |
| M4 | features S/W/N/P | `pytest -q tests/test_features.py` |
| M5 | models, evaluation, Exp A/B | `pytest -q tests/test_models.py tests/test_evaluate.py tests/test_experiments.py` |
| M6 | notebook builder + local dry run | `python tools/build_notebook.py && pytest -q tests/test_notebook_dryrun.py` |
| M7 | Colab smoke, then full | status table: all stages `done` under `ROOT/smoke/`, then `ROOT/full/` |

## Working style
Harsh writes briefly; keep replies short. Make only the edits asked for. Re-read a file
before changing it — he edits some directly.
