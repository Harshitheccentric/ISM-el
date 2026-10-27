# PROGRESS

## Done
- **M0 (2026-10-27)** — repo skeleton, CLAUDE.md, PROGRESS.md, README.md, .gitignore,
  pyproject.toml (pins: duckdb 1.5.6, polars 1.44.2, pyyaml 6.0.3), one import test.
  Done-check `pip install -e ".[dev]" && pytest -q` passes.
- Fixture `tests/fixtures/sample_2026-01-01.csv` re-exported and checked: 197 columns,
  36 rows (19 DELLBOSS VD boot rows, 12 TOSHIBA MG07ACA14TA, 5 TOSHIBA MG07ACA14TEY).
  TA has empty 241/242; TEY has 241/242 and empty 197.
- **M1 (2026-10-27)** — `config` (load/check YAML, quarters helpers, hash that ignores
  paths and can cover only a stage's keys), `configs/smoke.yaml` (2026Q1) and
  `configs/full.yaml` (2016Q1–2026Q2).
  `storage`: durable/cache tiers; stage pattern (build in local work dir → copy →
  verify → marker last); each stage declares `keys` and `deps`, and its effective hash =
  hash(own keys + dep hashes), so an upstream change reruns everything downstream and
  nothing else; the marker records every output file's path, bytes and sha256 (row
  count for Parquet), and the skip check verifies them, so missing or half-copied files
  are rebuilt; results CSV with run-key skip, reporting refuses synthetic rows.
  `status`: download log of measured bytes per quarter; budget guard estimate per size =
  max(config default, 1.5 × largest measured), with clear stop messages; status table
  incl. model selection (config allowlist, else `audit/selection.json`).
  `synthetic`: fake model strings, zero-padded IDs, boot drives, 241/242 per model
  string, counter reset, failed/censored/late drives, zips with nested folder and junk.
  Done-check passes (68 tests); full `pytest -q` 69 passed. Tests include: delete cache
  tier → rebuilt, durable skipped; upstream setting change → downstream rerun,
  unrelated skipped; truncated Parquet / changed bytes → caught on resume.
- **M2 (2026-10-27)** — `sources` (listed URLs, or derived + HEAD-confirmed; missing
  quarters logged in `sources/sources.csv` and the status table, never fatal),
  `rawzip` (day CSVs `YYYY-MM-DD.csv` at any depth, junk skipped, one day extracted at a
  time), `ingest` (all_varchar read + explicit projection: IDs as strings, TRY_CAST to
  BIGINT, missing column → typed NULL; row rule drops no-SMART rows; pod_key; pass 2 =
  zstd Parquet per quarter, one row group per day), `audit` (pass 1 per model × quarter
  and per model × day; boot-model rule; TRY_CAST failures and duplicate (serial, date)
  counted; select with S/W/N and P windows, ranked by (pod_ok, failures), warning if no
  selected model is pod_ok), `pipeline.run_data` (sources → audit_<Q> → select →
  ingest_<Q>; allowlist skips audit + select). One stage per quarter, so a crash
  resumes mid-range. `Stage.params` hashes per-stage values (quarter, URL, resolved model
  list): ingest reruns only if the chosen models, quarter or column config change.
  Config: `smart_ids`, `selection` (min_failures, min_coverage, max_models, boot_share).
  CLAUDE.md URL and boot-drive rules updated as decided.
  Done-check passes (32 tests); full `pytest -q` 101 passed.
- **Real-data check (2026-10-27)**
  - Sources 2016Q1–2026Q2: 9 listed, 33 derived (all HEAD 200), 0 missing.
  - 2026Q1 sizes: zip 1.31 GB, largest day CSV 136 MB, Parquet 44 MB (one selected
    model) or 497 MB (all 49 non-boot models, 30.2 M rows). Pass 1 + pass 2 took 9.4 min
    locally.
  - 2026Q1 audit: 80 models, 31 boot (DELLBOSS VD by no-SMART; 30 boot SSDs/laptop
    drives that report SMART but never have a slot), 0 cast failures, 0 duplicates.
    48 models have ≥95% 241/242 coverage. Pod coverage ≥94.97% for every non-boot model
    from 2026-01-01.
  - Selected: ST12000NM0008 only (125 failures, pod_ok). Next by failures: ST24000NM002H
    (55), ST16000NM001G (43), MG10ACA20TE (42). MG08ACA16TA (102), HUH721212ALN604 (95)
    and MG07ACA14TA (94) have no 241/242.
  - Budget defaults set to 1.5 × measured: zip 2.0 GB, day 210 MB, Parquet 750 MB.
  - The 2026Q1 zip is kept at `~/diskctx_data/local/smoke/downloads/data_Q1_2026.zip`
    for M3 checks. Outputs are in `~/diskctx_data/root/smoke/` (moved there 2026-10-27;
    all four markers still verify).
- **M2 follow-up (2026-10-27)**
  - Local data root outside the repo: `DISKCTX_LOCAL_ROOT` → `<it>/root`, `<it>/local`;
    falls back to the config paths. Overrides (notebook's Drive ROOT) win over both.
  - Markers hold only paths relative to ROOT. Test: copy ROOT elsewhere, use an empty
    local dir → every stage `done`, a rerun skips everything.
  - CLI: `python -m diskctx run --config ... --stages sources,audit[,select,ingest]`
    (resumable; sources always runs, select also runs any unfinished audit stage; prints
    each stage as it finishes and the status table at the end, also after a budget stop)
    and `python -m diskctx status --config ...`.
  - Slot reading confirmed by Harsh: a file "has slots" only if ≥1 row has a non-empty
    slot; a column that is empty everywhere is treated like an old file.
  - Full `pytest -q` 106 passed.
- **2026Q1 pod-coverage breakdown** (non-boot models, no-SMART rows dropped;
  30,242,536 rows). This is a check of the data, not a result.
  - Rows with an incomplete pod location: 23,226 (0.077%), 10,525 drives.
  - Which field: always `pod_slot_num` only. datacenter, cluster_id, vault_id and
    pod_id are never null, so `pod_key` is complete on every row.
  - By model: the 94.97% minimum is TOSHIBA MG11ACA16TE, a new model (20 drives, first
    seen 2026-03-17, 159 rows, 8 missing). Others: WUH722626ALE6L4 1.47%,
    HMS5C4040BLE640 0.50%, every other model ≤0.32%.
  - By datacenter: sac2 0.107%, iad1 0.083%, sac0 0.069%, ams5 0.069%, phx1 0.063%,
    yyz1 0.022%. No datacenter stands out.
  - By month: Jan 0.058%, Feb 0.060%, Mar 0.11%. The worst days are 2026-03-30 (0.41%),
    03-16, 03-18, 03-31 and 02-26 (0.30–0.39%).
  - New drives: 12,290 drives were first seen after Jan 1. 4,428 of them have some
    missing slot, and 4,272 are missing it on their very first day. Missing rows by
    timing: drives present on Jan 1 15,073; a new drive's first day 4,272; its days 2–7
    3,118; day 8+ 763.
  - Per drive: 5,981 drives miss 1 day, 3,070 miss 2–7 days, 188 miss 8+ days (but not
    all), and 1,286 miss every day they are seen.
  - **Failure days:** 668 of the 998 non-boot failures (67%) have an empty slot on their
    failure day; ST12000NM0008 has 77 of its 125. The slot is probably cleared when a
    drive is pulled, so "slot is null" on day t leaks the label (see M4 requirements).

## Next
- **M3** — labels and splits (requirements below). Run the full pass 1 first:
  `DISKCTX_LOCAL_ROOT=~/diskctx_data python -m diskctx run --config configs/full.yaml
  --stages sources,audit,select`. One quarter gives only one model with ≥100 failures.

## M2 requirements (decided 2026-10-27)
- Full 2016–2026 range must run on free Colab; never shrink the range to fit Drive.
- Raw data never kept. Per quarter: download zip to /content, read daily CSVs one at a
  time via `zipfile` (no full extract), process, delete the day, delete the zip at quarter
  end. Peak disk = one zip + one day. Handle nested folders/junk; missing columns → null.
- Pass 1 audit over start_quarter..end_quarter → tiny per-model × per-quarter tables
  (drives, failures, non-null share of needed SMART + pod columns, first date with pod
  coverage). Pass 2 ingest: selected quarters and models only, ~20 needed columns, boot
  drives dropped, zstd Parquet per quarter. Config model allowlist may skip pass 1.
- Download log records measured bytes per quarter (zip, largest day, Parquet).
- Select must not pick silently: write chosen models and reasons (failures, coverage,
  window) to `audit/selection.json` as `{"models": [{"model": ..., <reasons>}]}`; the
  status table shows it. A non-empty `models:` in the config overrides the choice.
- Stages declare `keys` and `deps` (select deps audit_<Q>; ingest hashes the resolved
  model list instead of select's hash, decided 2026-10-27).
- Boot drives: row rule (no SMART values) plus model rule (≥95% rows no SMART, or ≥95%
  empty slot in files that have slots, i.e. ≥1 non-empty slot); never drop a single row
  for an empty slot. Boot models → `audit/boot_models.csv`.
- `min_coverage` stays 0.95 for now (Harsh, 2026-10-27).

## M4 requirements (proposed 2026-10-27, needs Harsh's OK)
- Slot / pod features must not use day t's slot: an empty slot on the failure day
  (67% of 2026Q1 failures) leaks the label. Use the last non-empty slot seen on a day
  ≤ t-1, and never use "slot is null" as a feature. Add a leakage test.

## M3 requirements (decided 2026-10-27)
- `horizons` is a config key of the labels stage. Changing it must rerun labels and every
  stage downstream of it (features, models, results, experiments). Add a dependency test
  for this once the labels stage exists.

## Open questions
- When do pod/vault/slot fields start being populated? HANDOFF conflicts
  (Q2 2023 vs 2026 Q1). 2026Q1 has them (≥95% for every non-boot model from day 1);
  A full pass 1 answers this for earlier quarters, and the split window is set after it.
- Pod coverage counts the slot. Because only the slot is ever missing and it is missing
  around failures and installs, should P coverage use `pod_key` alone (always complete
  in 2026Q1)? Not changed yet.
- Will at least 2 exact model strings have ≥100 failures in the window, with enough in
  test for usable CIs? 2026Q1 alone gives 1, so this needs a multi-quarter audit.
- Several high-failure models lack 241/242 (MG08ACA16TA, HUH721212ALN604,
  MG07ACA14TA). They can't be used for W/N under the current rule; HANDOFF's fallback is
  to pool related models or demote a feature group.

## TODO
- Pins were chosen as the latest stable 1.x lines; duckdb 2.0.0 (2026-10-21) and
  polars 2.x (Oct 2026) were skipped as too new. Revisit before M7 if Colab needs it.
- Budget: margin 1 GB; defaults from the 2026Q1 measurement (see Done). `models: []`
  kept (audit selects).
- The shared `.venv` has diskctx installed in editable mode from the main checkout, so
  after the merge `python -m diskctx` there uses the merged code. In a worktree, run
  with `PYTHONPATH=src`.
