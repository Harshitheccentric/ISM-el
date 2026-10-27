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
  - The 2026Q1 zip is kept at `data/local/smoke/downloads/data_Q1_2026.zip` (in the m1
    worktree, gitignored) for M3 checks. Outputs are in `data/root/smoke/`.

## Next
- **M3** — labels and splits (requirements below). Run the full pass 1 (all 42
  quarters) first, or at least 2023Q2→2026Q2: one quarter gives only one model with ≥100
  failures.

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
  empty slot in files where the slot column is populated); never drop a single row for
  an empty slot. Boot models → `audit/boot_models.csv`.

## M3 requirements (decided 2026-10-27)
- `horizons` is a config key of the labels stage. Changing it must rerun labels and every
  stage downstream of it (features, models, results, experiments). Add a dependency test
  for this once the labels stage exists.

## Open questions
- When do pod/vault/slot fields start being populated? HANDOFF conflicts
  (Q2 2023 vs 2026 Q1). 2026Q1 has them (≥95% for every non-boot model from day 1);
  a full pass 1 answers this for earlier quarters, and the split window is set after it.
- Will at least 2 exact model strings have ≥100 failures in the window, with enough in
  test for usable CIs? 2026Q1 alone gives 1, so this needs a multi-quarter audit.
- **Harsh:** is "slot column exists" meant as *present in the header*, or as *populated in
  at least one row*? The code uses the second, because if legacy files carry an empty
  slot column, the first would make every model a boot model (test:
  `test_slot_column_empty_everywhere_does_not_make_boot_models`).
- Several high-failure models lack 241/242 (MG08ACA16TA, HUH721212ALN604,
  MG07ACA14TA). They can't be used for W/N under the current rule; HANDOFF's fallback is
  to pool related models or demote a feature group.

## TODO
- Pins were chosen as the latest stable 1.x lines; duckdb 2.0.0 (2026-10-21) and
  polars 2.x (Oct 2026) were skipped as too new. Revisit before M7 if Colab needs it.
- Budget: margin 1 GB; defaults from the 2026Q1 measurement (see Done). `models: []`
  kept (audit selects).
- The shared `.venv` has diskctx installed in editable mode from the main checkout; in
  this worktree run tests with `PYTHONPATH=src`.
