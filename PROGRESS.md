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
    (Counted before the audit v2 fix. With all failure events the total is 1,030.)
- **Failure-count check, 2026Q1 raw CSVs (2026-10-27, read-only).** 30,597,484 rows;
  the failure column holds only `0`/`1`. There are 1,030 failure rows from 1,030
  distinct drives, all with a pod_key. The audit v1 no-SMART row rule dropped 32 of
  them (all Seagate: ST24000NM002H 19, ST12000NM0008 4, ST8000DM002 3, ST8000NM0055 3,
  ST14000NM0138 / ST16000NM001G / ST16000NM000J 1 each). The boot-model rule, casts and
  duplicates dropped none. All 32 drives appear on earlier days, and 29 report SMART
  before failing. The first full pass 1 was stopped and is redone with audit v2.
- **Audit v2 (2026-10-27).** `AUDIT_VERSION = 2` is in each audit stage's hash, so
  finished quarters rerun. Pass 1 now also captures, from all drives:
  - `events.parquet`: failure events before any cleaning (serial, model, date, pod_key,
    vault_key, had_smart). The audit's failure counts and the selection use these.
  - `pod_days.parquet`: per pod per day over all non-boot drives (quarter-local boot
    rule): drives, drives with 241/242, mean/max SMART 194, mean/max daily TB
    written/read, resets, failures from the events, vault_key.
  - `end_state.parquet`: per drive seen on the last day, its 241/242 counters. The next
    quarter's day 1 delta uses it. If it is missing, the delta is null (not an error) and
    quality.csv records `previous_end_state found 0`. A negative delta is a reset: null,
    and counted (quality.csv `counter_reset`, pod_days `resets_*`).
  - P coverage and the P window use `pod_key_rows` (pod_key non-null). The slot is only
    a position feature.
  - The synthetic generator's fill-phase counter was not cumulative (it dropped after
    20 days); it is fixed, and counters now fall only at a planned reset.
  - New tests: a failure row with empty SMART is kept as an event; pod_days include
    non-selected models and leave out boot models; the quarter-boundary delta uses the
    carried state; a missing previous state gives null; a reset gives null and is
    counted; AUDIT_VERSION is in the hash. Full `pytest -q` 112 passed.
  - **Real 2026Q1 pass 1 (zip reused, no download):** 179 s wall time, 1.1 GB peak RAM.
    Failures 1,030 (32 without SMART). ST12000NM0008 129, MG08ACA16TA 102,
    HUH721212ALN604 95, MG07ACA14TA 94, HUH721212ALE604 86, ST24000NM002H 74.
    Selection unchanged: ST12000NM0008 (129 failures, pod_coverage 1.0).
    `pod_days.parquet` has 532,424 rows (6,079 pods, 304 vaults) and is 11.9 MB;
    `end_state.parquet` 345,224 rows, 2.7 MB; `events.parquet` 16 KB; the whole quarter
    folder is about 15 MB. Counter resets: 9 for 241, 10 for 242. Previous state not
    found (2025Q4 not audited locally), so Jan 1 deltas are null.
- **Missingness-leak check, 2026Q1** (read-only, non-boot models; null rate in %; a
  check of the data, not a feature). 1,030 failing drives; 5,909 rows in f-7..f-1.

  | column | healthy (all days) | failing f-30..f-8 | failing f-7..f-1 | failing day f |
  |---|---|---|---|---|
  | datacenter / cluster / vault / pod_id | 0 | 0 | 0 | 0 |
  | capacity_bytes, failure | 0 | 0 | 0 | 0 |
  | **pod_slot_num** | 0.068 | 7.217 | 12.405 | 67.864 |
  | **smart_5 / 9 / 194 / 198** | 0.001 | 1.126 | 2.132 | 3.107 |
  | **smart_197** | 1.731 | 4.248 | 5.517 | 6.117 |
  | smart_187 | 66.787 | 63.832 | 64.173 | 63.689 |
  | smart_188 | 66.666 | 63.376 | 63.378 | 62.913 |
  | smart_241 / 242 | 55.919 | 52.474 | 51.667 | 52.233 |

  Flagged (missingness rises before failure): `pod_slot_num` (from weeks before),
  SMART 5/9/194/198 (whole rows go silent before failure: the no-SMART days), and
  SMART 197 (part may be model mix). 187/188/241/242 are *lower* for failing drives;
  that is model mix (whole models lack them), not a leak. A per-model version belongs
  in M4.
- **Full pass 1, first run (2026-10-28):** 2016Q1–2022Q4 audited (28 quarters, about
  3–5 min each). It stopped at 2023Q1 with `BadZipFile`: the download was cut short
  (218.7 MB of 819.1 MB) and was saved as if complete, because `download` never
  compared bytes with Content-Length. The 28 finished quarters are sound: a cut-short
  zip cannot be opened (its index is at the end), and 2022Q3's smaller zip (0.79 GB)
  exactly matches the server's size and has all 92 days.
  - Fix: `download` checks bytes against Content-Length, retries 3 times, then raises
    `DownloadError` (a partial file never gets the final name). `local_zip` deletes and
    re-fetches a file that is not a readable zip, and refuses a fetch that isn't one.
    4 new tests; full `pytest -q` 116 passed.
  - Second network drop during 2023Q4 (2023Q1–Q3 had finished). Downloads now resume:
    a partial `.part` is continued with an HTTP Range request (Backblaze answers
    `Accept-Ranges: bytes`), starting over only if the server ignores the range. Up to 5
    tries per run, waiting 30/60/90/120 s between them. After that it stops but keeps
    the `.part`, so a rerun continues it. Tests: resume after a drop, resume from an
    earlier run's partial file, range ignored, give up but keep the partial. Full
    `pytest -q` 118 passed.
  - The kept smoke 2026Q1 zip is hard-linked into `local/full/downloads/` (no extra
    disk), so the full run reuses it instead of downloading 1.3 GB again.

## Next
- **Resume the full pass 1** (finished quarters are skipped; the bad 2023Q1 file is
  replaced automatically): `DISKCTX_LOCAL_ROOT=~/diskctx_data python -m diskctx run
  --config configs/full.yaml --stages sources,audit,select`.
- **M3** — labels and splits (requirements below).

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

## M3 requirements (decided 2026-10-27)
- `horizons` is a config key of the labels stage. Changing it must rerun labels and every
  stage downstream of it (features, models, results, experiments). Add a dependency test
  for this once the labels stage exists.
- Failure dates come only from the failure events (`audit/quarters/*/events.parquet`,
  built before any cleaning). Cleaning may remove a row's features, never the event.
- The failure-day row is the event, not a sample. Samples are drive-days t strictly
  before the failure date. The failure-day row gets no label and is excluded from
  train, val and test. Add a test.
- Daily TB deltas across quarters use the carried end state (see audit v2). M4 features
  for the selected models need the same rule over the pass-2 Parquet.

## M4 requirements (decided 2026-10-27)
- Slot: use the last slot seen on a day ≤ t-1. Never use slot missingness as a feature.
  Pod coverage and the P window use pod_key; the slot is only a position feature.
- Every column flagged by the missingness-leak check (pod_slot_num, SMART 5/9/194/198,
  197) must also use the last value seen on a day ≤ t-1. Add a leakage test.
- Pod features come from `pod_days.parquet` (whole pod, all non-boot models), lagged to
  ≤ t-1; pod failure counts exclude the drive itself.

## Open questions
- When do pod/vault/slot fields start being populated? HANDOFF conflicts
  (Q2 2023 vs 2026 Q1). 2026Q1 has them (≥95% for every non-boot model from day 1);
  A full pass 1 answers this for earlier quarters, and the split window is set after it.
- Durable pass-1 size for the full range: about 15 MB per quarter with pod fields
  (mostly pod_days). Older quarters without pod fields will be smaller. Check this on
  the full run.
- The carried end state is read when a quarter runs; it is not in the stage hash. If an
  earlier quarter is re-audited later, the next quarter's day-1 deltas are not redone
  automatically (rerun it by deleting its marker). Pass 1 runs quarters in
  chronological order, so a fresh run carries state into every quarter after the first
  one. Each audit marker records `info.carried_state_day1` (true/false) and
  `info.previous_quarter` (tested).
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
