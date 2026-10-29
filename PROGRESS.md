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
- **Full pass 1 complete (2026-10-28):** all 42 quarters 2016Q1–2026Q2 (9 listed, 33
  derived, 0 missing), plus select. 29,277 failure events (224 without SMART). Carried
  241/242 state was found for 41 of 42 quarters (all but 2016Q1). No duplicate rows and
  no SMART cast failures. Durable `root/full` is 209 MB. Pod fields begin 2023-07-01.
  Selected (all pod_ok, no warning): ST4000DM000 (S/W/N window 2016Q1–2025Q1, 4,920
  failures, pod 364), ST12000NM0008 (2019Q4–2026Q2, 2,611, pod 1,558), ST8000NM0055
  (2016Q4–2026Q2, 2,516, pod 994). ST4000DM000 has no failures after 2024Q3 and no
  rows after 2025Q1; it stays, and M3 handles it with per-study windows (Harsh).
- **Date fix (2026-10-28).** The audit counted 210,786 date cast failures. Two day files
  write dates as M/D/YY: `2018-02-25.csv` (100,253 rows `2/25/18`) and
  `2019-06-17.csv` (110,532 rows `6/17/19` plus one row `6/15/19`). That left 9
  failure events with a null date, and pass 2 would have nulled ~210k Parquet dates.
  - Rule (Harsh): every row's date is the day in its file's name, for every file. A
    row's own date is parsed strictly (YYYY-MM-DD or M/D/YY) only to count, per
    quarter, rows whose date is unreadable (`date_unparseable`) or not the file's day
    (`date_not_file_day`) in quality.csv. The status table shows these counts and lists
    quarters audited before the check as "not checked", never as zero.
  - Rerun: only 2018Q1, 2019Q2 and select (their markers deleted; AUDIT_VERSION
    unchanged, so the other 40 quarters keep their outputs). Their end_state,
    pod_days, days.csv and models.csv are identical before and after; only
    events.parquet changed (the 1 + 8 null dates are now filled). So 2018Q2 and 2019Q3
    did not need a rerun. The selection is identical. New counts: 2018Q1 0 / 0;
    2019Q2 0 unreadable, 1 not the file's day (the `6/15/19` row).
  - The other 40 quarters show "not checked" for the date counts. Their old
    `cast_failed date` was 0, so every row date there was readable ISO; whether any
    differs from its file's day is unknown.
  - The resumable download recovered from one real timeout during the 2019Q2 fetch.
  - Tests: an M/D/YY day file with one row dated another day and one unreadable date;
    the status table marks quarters not checked. Full `pytest -q` 120 passed.

- **Pass-2 setup (2026-10-28).**
  - Model list (Harsh), set as the `models:` allowlist in `configs/full.yaml`:
    ST4000DM000, ST12000NM0008, ST8000NM0055 (the audit's 3), ST8000DM002,
    ST14000NM001G, TOSHIBA MG08ACA16TEY (second vendor). Which models enter each study is
    decided later in config.
  - ST16000NM001G left out by Harsh's rule (every quarter from 2023Q4 must have
    pod_key coverage ≥0.95). Its coverage was 0.914 (2023Q4), 0.965 (2024Q1),
    0.831 / 0.821 / 0.878 (2024Q2–Q4), then 1.0 from 2025Q1: about 3,600 of its ~27,500
    drives had no pod location for over a year, not just the July 2023 rollout.
  - Size estimate: 224.0 M rows × 26.6 bytes/row (ST12000NM0008 2026Q1: 44.3 MB for
    1.66 M rows) ≈ 6.0 GB in total, under the 10 GB limit. By model: ST4000DM000 1.86,
    ST8000NM0055 1.23, ST12000NM0008 1.19, ST8000DM002 0.90, ST14000NM001G 0.56,
    MG08ACA16TEY 0.23 GB. Largest quarter ≈ 191 MB (2022Q4). Older quarters may compress
    differently; the download log measures each quarter.
  - Layout: one file per model per quarter, `parquet/<model>/<quarter>.parquet`
    (spaces → `_`, e.g. `TOSHIBA_MG08ACA16TEY`), so a study copies only the models and
    quarters it needs to Drive.
  - Pass 2 also counts odd row dates over every row of every day file and stores them in
    its marker (`info.date_unparseable`, `info.date_not_file_day`). The status table
    uses them for the 40 quarters audited before the date check.
  - The 2026Q1 zip is hard-linked into `local/full/downloads/` again (the audit deleted
    its link after use).
  - Full `pytest -q` 120 passed.

- **M3 (2026-10-28)** — `labels`, `splits`, pipeline stages `labels` and `splits`
  (`python -m diskctx run --config ... --stages labels,splits`).
  - `labels` stage → `labels/drives.parquet` (one row per model × serial: first and last
    record, failure date = first event, events, failure-day rows, rows after the
    failure), `report.csv` (per model × horizon: samples, positives, negatives,
    censored rows, plus drive counts) and `reappeared.csv`. `label_rows()` adds
    `y_<N>` (1 / 0 / null) and drops the failure-day row and every later row. Labels use
    calendar dates; failure dates come only from `events.parquet`, matched on
    (model, serial). Keys: `horizons`, `data_source`; deps: every `audit_*` and
    `ingest_*` stage (the audit stages are in the chain even with a model allowlist,
    because the events come from pass 1; finished ones are skipped).
  - `splits` stage → `splits/<study>/drives.parquet` (split, by = failure | hash,
    weight, keep), `windows.csv`, `summary.csv`; prints failing drives per study, model
    and split and warns for test cells under `min_test_failures`. Keys: `seed`,
    `splits`, `studies`; dep: `labels`.
  - A model enters a study only with ≥ 30 failed drives in train and ≥ 1 in val and in
    test (`splits.min_failed_drives`, Harsh). Otherwise it is left out of that study
    entirely (no drives in `drives.parquet`, a `skipped` row in `windows.csv`) and
    `summary.csv` lists it with `included = false` and the reason. `summary.csv` lists
    every model of a study, also those skipped for their window. No model trains on
    healthy drives only.
  - Config: `splits` (shares 50/20/30, `healthy_per_failed` 20, `min_failed_drives`
    30/1/1, `min_test_failures` 50)
    and `studies` (W: per-model S/W/N quarters from `audit/summary.csv`, 2 val + 2 test
    quarters, `min_quarter_failures` 10; P: 2023-07-01 start, train 2023-08-01–2025-03-31,
    val 2025-04-01–2025-09-30, test 2025-10-01–2026-06-30). Only these keys were added to
    `full.yaml`; models and ingest/audit keys are unchanged. `smoke.yaml` has Study P
    only, with short windows in 2026Q1 (plumbing, never a result).
  - `split_rows()` gives the drive-days of one split and horizon: rows inside the drive's
    split window only; a label must be decidable by the window end (the drive fails
    inside the window, or label 0 with t+N ≤ its last record up to the window end);
    train and val embargo t+N > window end. The rows never depend on the feature set
    (`S+P` in Study W raises).
  - CLAUDE.md updated (Harsh): failed-in-window drives go to the split of their failure
    date, others by seeded hash; embargo per horizon N; censoring per window.
  - Tests (test_labels 9, test_splits 16, test_config +4): gaps; failure on t+N vs
    t+N+1; failure-day row excluded; rows after a failure dropped, reappearing serial not
    merged; censored tails; failure dates only from events; no serial in two splits;
    failed drives in the split of their failure date (failures in every split); hash
    shares and seed; downsampling train only, by drive, seeded; failure summary warns
    < 50; a model short of failed drives in any split is left out with its reason; weighted precision on the test split = precision on the whole population (and
    unweighted is inflated); rows only inside the split window; embargo removes exactly
    the crossing days and test is not embargoed; cutting data at the train end leaves
    train rows and labels identical (incl. a gap across the train end); identical rows
    for every feature set; synthetic end-to-end runs; changing horizons reruns labels,
    splits and a downstream stage only. `test_cli` moved-root test now runs all stages.
  - Done-check: `pytest -q tests/test_labels.py tests/test_splits.py` 23 passed.
    Full `pytest -q` 147 passed.
- **Operator-action leak check (2026-10-28, read-only; a check of the data, not a
  result; for the paper's data-quality section).** Reproducible scripts in
  `tools/diagnostics/` (paths from the config or arguments; `--help` on each), tables in
  `results/diagnostics/`, rerun 2026-10-28 with the same numbers as below (the 8-day
  move share in (a) came from the first run only):
  `pod_moves.py` (a; also writes `pod_model_days_<Q>.csv`), `hot_pods.py` (b, raw and
  model-adjusted), `model_spike.py` (c). Run with `DISKCTX_LOCAL_ROOT=~/diskctx_data
  python tools/diagnostics/<script> --config configs/full.yaml`, (a) first.
  - **Findings:** no pod moves before failure; no quarantine pods; MG08ACA16TEY's 2025Q3
    spike is spread over 111 pods, but 96% sits in 8 vaults of ams5|031 (a vault-group
    event).
  - **(a) pod_key moves before failure, 2026Q1 zip (all non-boot models, 30.2 M rows).**
    0 moves for the 1,030 failing drives in any period: f-7..f-1 (923 drives, 5,835
    drive-days), failure day, f-30..f-8, or earlier. Healthy drives: 26 moves in 29.9 M
    drive-days (0.009 per 10k; 25 drives), 24 to another vault, all after a gap of ≥ 1
    day; 8 of them on 2026-03-31. Share of drives with a move in 8 days: failing (f-7..f)
    0%, healthy (last 8 days of the quarter) 0.003%. **Not elevated.** Failing drives are
    not moved to another pod before they fail in 2026Q1. One quarter only; a move across
    a quarter boundary is not seen.
  - **(b) Pods and vaults by failures per drive-day, pod_days 2023Q3–2026Q2.** 7,000
    pods, 352 vaults, 322.6 M drive-days, 13,257 failures (4.11 per 100k drive-days, AFR
    1.50%). Extreme = rate ≥ 3× the fleet and Poisson p < 0.001 / units (Bonferroni):
    35 pods (631 failures, 4.8%, in 0.7% of drive-days) and 14 vaults (2,381 failures,
    18%, in 4.7% of drive-days). Top vaults: sac2|000|1124 (273 failures, 5.1×),
    sac0|000|1022 (5.6×, only 398 days), sac0|000|1128 (4.3×), sac2|000|1156 (3.8×).
    Top pods: sac2|000|1124|08 (28 failures, 10.4×), sac2|000|1124|11, phx1|000|1066|19
    (9.3×). These are full pods (~60 drives, ~1,090 days); no pod averages < 20 drives,
    so there is no small spare/quarantine-pod signature.
    Model-adjusted (2026Q1 rows from (a), expected = Σ drive-days × the model's rate):
    extreme vaults drop from 6 (raw) to 2 and pods from 3 to 1. sac2|000|1124 falls from
    7.2× raw to 2.5× (model mix). Still extreme: sac2|000|1133 (16 failures vs 3.1
    expected, 5.2×), sac2|020|3018 (a small vault: 6,765 drive-days, 5 vs 0.2; two pods
    of ~4 drives each with 2 failures, the closest thing to a spare-pod pattern), pod
    phx1|000|1057|16 (8 vs 0.3). **Raw extremes are mostly model mix; a few hot units
    remain.**
  - **(c) MG08ACA16TEY 2025Q3 (215 failures).** Per quarter since 2023Q3: 18, 39, 10,
    22, 32, 18, 19, 33, **215**, 51, 26, 46. The model lives only in ams5|031 (2026Q1:
    4,885 drives in vaults 2004–2021). 2025Q3 failures are in 12 vaults and 111 pods,
    no null pod_key. By vault: 2005 33, 2007 31, 2020 30, 2004 28, 2008 23, 2017 22,
    2009 21, 2019 19 (these 8 = 96%), then 2015 5, 2010/2018/2014 1 each, 2016 0, though
    2016 has 510 of the model's drives and 2010/2014/2015/2018 have another ~1,100. By
    pod: max 5 per pod, the top 10 pods hold 42 (20%), 50 pods have exactly 1. By week:
    every week of the quarter, 1–31 per week, peaks in the weeks of 07-28 (25), 08-11 (31)
    and 09-22 (31); the worst day is 08-12 with 14. The same 8 vaults had 0–8 per quarter
    before and 0–14 after. **Not a few pods: spread evenly across pods and weeks, but
    concentrated in 8 of the model's ~13 main vaults.** That looks like a vault-group
    event (a batch, firmware, environment, or a replacement campaign), not a single bad
    pod.
  - **Decision (Harsh):** pod_key from before any move; no static hot-pod flag; rolling
    past-only pod/vault/cluster failure counts plus model-mix "excess" versions (see M4
    requirements). MG08ACA16TEY's spike falls in Study P's val window, so pooled results
    are reported with and without it (M5).

- **Pass 2 complete (2026-10-28, Harsh).** All 42 quarters, 6 models: 252 Parquet files,
  223.9 M rows, 5,310,811,337 bytes (5.31 GB) in `root/full/parquet`. Largest quarter
  2023Q3 (168.2 MB), then 2021Q4 (163.1 MB) and 2022Q2 (162.3 MB). Durable `root/full`
  also holds audit 218 MB, labels 0.7 MB, splits 0.7 MB. Largest zip 2026Q2 (2.09 GB);
  largest day CSV 2026-06-21 (140 MB).
  - Why 2026Q2's zip is 2.09 GB while 2026Q1's is 1.31 GB: the CSVs are the same size
    (12.6 vs 12.0 GB unpacked, same 180 columns, same deflate settings), but 2026Q2's
    rows are in no order (a model change on 93% of consecutive rows) while 2026Q1's are
    sorted by model and serial, so deflate packs them worse (ratio 0.167 vs 0.11). Read
    from the zips' central directories and first day files by HTTP range requests.
- **Copied day file 2022-12-10.csv (2026-10-28).** All 235,623 rows of `2022-12-10.csv`
  are dated 2022-12-11, and `2022-12-11.csv` exists. SMART 9 (power-on hours) per drive:
  2022-12-09 → file 12-10 +47 h median (two days), file 12-10 → 12-11 0 h for all
  235,212 drives, 12-07 → file 12-10 +95 h (four days). Ignoring the date column, the
  file equals `2022-12-11.csv` except 8 rows whose failure flag is 0 there and 1 on
  12-11. So it is a copy of Dec 11's snapshot; Dec 10 itself is missing.
  - Rule applied (Harsh): a copied day file is dropped and the day counts as missing.
    New config key `dropped_days: {day: evidence}` (full.yaml has 2022-12-10 with the
    evidence above; smoke `{}`). `rawzip.day_members(zip, drop)` leaves those files out
    (a listed day missing from the zip is an error); the audit records `day_dropped` in
    quality.csv and ingest records `days_dropped` in its marker. A quarter's audit and
    ingest stages get the dropped days as a param only when it has some, so only that
    quarter's stages rerun (tested). With no yesterday, Dec 11's daily TB is null.
  - Rerun: audit_2022Q4, audit_2023Q1 (marker deleted, carried state) and ingest_2022Q4,
    then labels and splits. 2022Q4 events.parquet and end_state.parquet are byte-for-byte
    the same as before (the copy had no failure flags); days.csv, models.csv and
    quality.csv lose Dec 10. 2023Q1's outputs are identical except quality.csv, which
    now has the date checks (it was audited before them). Each model's 2022Q4 Parquet =
    the old file minus its 12-10 rows. Status: 2022Q4 date_not_file_day 0 (2019Q2 keeps
    its 1 row).
  - Check over all 42 quarters (events vs Parquet, the 6 models): no Parquet date outside
    its quarter; every event date is a Parquet day; every event with SMART has a
    failure=1 row on its date and every failure=1 row has an event. Days missing from
    the Parquet: 2022-12-10 (dropped) and 2017-01-28..31 (see Open questions).
  - Tests: dropped day left out and must exist in the zip; dropped day is a missing day
    in audit (days, events, pod_days, null TB delta, quality) and ingest; only that
    quarter's stage hashes change; config check. Full `pytest -q` 151 passed.
- **Labels and splits on the full data (2026-10-28).** Study settings unchanged.
  - labels/report.csv totals (6 models): 99,819 drives, 12,801 failed; 12,716
    failure-day rows and 104,944 rows after a failure dropped (104,855 after the four
    more copied days below were dropped); 236 reappeared serials (`reappeared.csv`);
    0 events without rows. (85 failures have no failure-day row:
    their row had no SMART and was dropped by the row rule; the event is kept.)
  - Study W windows as the 10-failure rule predicts: ST4000DM000 ends 2024Q3 (train
    2016-01-01–2023-09-30, val 2023Q4–2024Q1, test 2024Q2–Q3); the other five end
    2026Q2 (val 2025Q3–Q4, test 2026Q1–Q2). Study P: ST4000DM000 skipped (no failures in
    the test window); the other five in.
  - Failed drives per split (train / val / test), every model included, no test cell
    < 50 (lowest: ST14000NM001G W test 60):

    | model | W | P |
    |---|---|---|
    | ST4000DM000 | 4,685 / 156 / 78 | skipped |
    | ST12000NM0008 | 2,093 / 243 / 274 | 840 / 270 / 385 |
    | ST8000NM0055 | 2,255 / 107 / 151 | 623 / 126 / 205 |
    | ST8000DM002 | 1,288 / 65 / 63 | 301 / 67 / 101 |
    | ST14000NM001G | 663 / 71 / 60 | 305 / 71 / 93 |
    | TOSHIBA MG08ACA16TEY | 225 / 253 / 71 | 157 / 239 / 116 |

- **Four more copied day files dropped (2026-10-28, Harsh).** A copy holds the next
  day's data under an earlier date, so a feature for day D+1 built from "yesterday"
  would see D+1 itself and break the t-1 rule.
  - `tools/diagnostics/copied_days.py` (reads the pass-2 Parquet, no downloads) flags
    every day D on which ≥ 99% of drives' SMART 9 did not advance since D-1 and says
    which file is the copy from the neighbours (D-2 → D-1 about +48 h and D → D+1 about
    +24 h: D-1 is a copy of D, i.e. it matches the next day). Days already in
    `dropped_days` are listed with their evidence. Table:
    `results/diagnostics/copied_days.csv`. Of 3,829 days, 5 were flagged (share
    unchanged 1.000; every other day ≤ 0.022): 2017-12-31, 2019-08-26, 2019-10-15 and
    2021-07-01, each a copy of the next day (+48 h before, +24 h after, failure flags 0
    in the copy, ≥ 99.99% of rows identical to the next day's file), and 2017-01-27
    (unclear, see Open questions).
  - The four are in `dropped_days` with their evidence. Reran audit and ingest for
    2017Q4, 2019Q3, 2019Q4, 2021Q3, audit 2018Q1 (marker deleted; carried state), then
    labels and splits. Events are identical in all five quarters; each Parquet = the old
    file minus the dropped day; end states identical except 2017Q4's, which now comes
    from 2017-12-30 (93,240 drives instead of the copy's 92,966). Events vs Parquet
    agree on every date in these quarters. splits/summary.csv is unchanged in every cell
    (drives, failed drives, kept healthy). The scan now flags only 2017-01-27.
- **Late January 2017, from the 2017Q1 zip (read-only, all 52 models).**
  - 2017-01-26 and 2017-01-27 hold one snapshot: SMART 9 01-25 → 01-26 is +17 h for
    every model (01-24 → 01-25 is +24 h), 01-26 → 01-27 is 0 h, 01-27 → 02-01 +150 h.
    So the snapshot is +17 h, not +48 h: it belongs to 01-26 (taken early), not 01-27.
    73,585 of 73,699 rows of 01-27 equal 01-26's; 91 drives (45 HGST HMS5C4040BLE640,
    45 HMS5C4040ALE640, 1 ST500LM012 HN) have a newer reading on 01-27 (+22 to +30 h);
    01-27 also carries 22 failure flags (01-26: 0).
  - **Decision (Harsh): keep both 01-26 and 01-27; 01-27 is a kept stale day.** This
    copy runs backwards: 01-27 repeats the past, not the future, so it cannot break the
    t-1 rule; it only makes one day's deltas stale, and it carries 22 real failure
    events. Recorded with the evidence in `configs/full.yaml` (comment above
    `dropped_days`).
  - Small files: 2017-01-28 (46 rows: 45 of the HMS5C4040BLE640 drives above + 1) is
    +18 h after 01-27, a normal step. 2017-01-29 (the same 46 drives) is 0 h after 01-28:
    a copy of 01-28. 2017-01-30 has no file. 2017-01-31 (61 rows, 60 Hitachi
    HDS722020ALA330 + 1) holds drives not seen on 01-25..01-29; they step +30 h to
    02-01. None of these drives is one of the 6 pass-2 models, so they touch only the
    audit tables. 01-28 and 01-31 are kept.
  - **2017-01-29 dropped (Harsh):** an exact copy of 01-28, added to `dropped_days`
    with its evidence. Reran audit 2017Q1; ingest 2017Q1 reran too (the dropped day is
    in its stage hash) and its Parquet is byte-for-byte unchanged, as are events and
    end state; days.csv loses 01-29's 2 rows (model × day). Labels and splits reran;
    splits/summary.csv is unchanged.
  - Of the 72,420 non-boot drives present on 01-27, 0 are missing on 02-01 without a
    failure event: nobody disappears across the gap.

- **M4 (2026-10-28/29)** — `features` (groups S, W, N, P), stages `features_sampling`,
  `features_rates` and one `features_<model>` per model (`python -m diskctx run --config
  ... --stages features`), `tools/diagnostics/feature_missingness.py`.
  - Timing rule, every feature: a feature at t uses only rows dated <= t-1. A reading of
    a column = a row where it is not null. Each feature is computed as of the drive's
    last reading a <= t-1 of its column and carried forward (a silent drive keeps its
    values). k-day per-day delta = (x_a - x_b) / (a - b) calendar days, b = the last
    reading <= a-k, else the earliest reading; null only without an earlier reading.
    A fall in 241/242 is a reset: null, counted per model and quarter (marker info).
    Error counters keep their sign (197 can fall). No "is null" / "days since" columns.
  - Registry `GROUPS` (each maps the drive's as-of table, plus pod tables for P, to
    {name: SQL}):
    - S: raw SMART the model reports (coverage >= 0.95 over the study's train + val
      windows, from days.csv; Harsh D2), plus 7/30-day deltas of 5/187/188/197/198.
      Seagates: all 7 IDs; Toshiba: 5, 9, 194, 198.
    - W: TB/day written/read over the last interval; the same / capacity; 7/30-day mean
      and max; mean7 / max(lifetime average, 0.01 TB/day), with age floored at 1 day;
      age = SMART 9 / 24; lifetime fills = 241 x 512 B / capacity; fill phase (< 1 fill).
    - N: Δerr / max(TB written/day, 0.01) and Δerr / max(TB written+read/day, 0.01)
      (Harsh D3), 7/30 days.
    - P (Study P only; `study_columns` and the config check refuse P in W): pod key =
      the last pod_key <= t-1. Pod mean TB written/read and temperature come from the
      pod's last pod-day <= t-1 (the means include the drive); also drive minus pod
      temperature and slot. Failures in pod / vault / cluster over [t-30, t-1] minus
      the drive's own events. Expected = Σ drive-days x model base rate, minus the
      drive's own days (`p_expected_*`). Excess = (observed + 1) / (expected + 1)
      (`excess_smoothing`). Base rates = failures / drive-days per model over Study P's
      train window only (`features/P/base_rates.csv`; the pooled rate for a model
      without train drive-days).
    - Columns: Seagate W 54, P 68; Toshiba W 33, P 47.
  - Pass 1 also writes `pod_models.parquet` (pod x model x day: drives, failures;
    non-boot, the pod_days boot rule). It is written only for quarters from Study P's
    start (stage param `pod_models`), so only audit 2023Q3–2026Q2 reran (Harsh D1).
  - Processing: per model, quarter by quarter, in one DuckDB connection (memory limit
    from `resources`). Input = the quarter's rows + the last 30 days of the previous
    quarter + two carried rows per drive and column (the reading before the warm-up;
    the as-of row of the last reading before the quarter). Tested equal to one pass
    over the whole history. `splits.split_rows(..., last_seen=)` (from
    `splits.last_records`) gives the same rows quarter by quarter.
  - Files: `features/<study>/<model>/{train,val,test}.parquet` (durable; CLAUDE.md tier
    line changed by Harsh's request) + `columns.json` (S set, coverage, columns per
    group). Rows = split_rows for every horizon (`in_7/14/30`), with fail_date, y_N and
    sample_weight; features are Float32. Train sampling: see the addendum under "Frozen".
  - Config: `features:` (every knob), `resources:` (DuckDB memory limit 3GB, 4 threads;
    not hashed), `budget.default_feature_bytes_per_row`; a budget guard per model.
  - Tests: test_features 18, test_audit +1. Covered:
    - past-only over the whole registry: every value on day t and later replaced
      (values, nulls, resets, pods, events);
    - a gap gives a per-day rate, and there is no NaN when an earlier reading exists;
    - a reset is null and counted; a 197 fall is kept;
    - pod counts exclude the drive, and a moved drive uses its old pod through t;
    - expected counts and the smoothed excess match a hand calculation; base rates
      ignore val/test failures;
    - SMART sets follow coverage (0.95 in, 0.949 out, window only);
    - age comes from SMART 9 and the fill phase from 241; a new drive gives a finite
      recent ratio; the floors apply;
    - Study W refuses P;
    - quarter by quarter equals one pass (including a 40-day silence across the
      boundary), and split_rows by quarter equals the whole history;
    - train sampling: seeded, val/test complete, near-failure rows kept, weights 1/r,
      the Python hash equals the DuckDB hash; r caps the rows;
    - end to end on the synthetic pipeline: one file per study/model/split, in_N rows =
      split_rows, columns = registry, coverage window = train start .. val end;
    - a features setting reruns features_* and downstream only; `resources` reruns
      nothing;
    - the budget guard stops with a clear message; pod tables load after a quarter
      without pod fields; the diagnostic refuses test.
  - Done-check: `pytest -q tests/test_features.py` 18 passed; full `pytest -q` 170 passed.
  - **Real run (local, DISKCTX_LOCAL_ROOT=~/diskctx_data):**
    - Audit 2023Q3–2026Q2 + labels + splits: 1 h 02 min, 1.7 GB peak RSS (11 downloads;
      the kept 2026Q1 zip was hard-linked). Every output is unchanged except: the new
      pod_models; quality.csv, which gained the date-check rows (these quarters were
      audited before that check); and pod_days floats, which differ by <= 7e-16
      relative (parallel sum order). labels/ and splits/ are identical. pod_models: 12
      quarters, 11.9 MB, sums equal pod_days on every pod-day checked (2023Q3, 2024Q4,
      2026Q2).
    - Sampling: W train 105,786,314 rows (653,553 within 60 days of a failure), r =
      0.1365; P train 17,328,409 (123,061), r = 0.8647. Base rates: 60 models, pooled
      7,159 failures in 164,872,626 drive-days.
    - The first features run stopped at ST12000NM0008 2023Q3: 2023Q2's empty pod_days
      has an int32 pod_key. The loader now casts each file (tested). Sampling + rates
      took about 5.5 min; the six model stages 1 h 07 min; peak RSS 4.6 GB.
    - Stored: 41,778,470 rows, 3,221,461,794 bytes (3.22 GB, 77 B/row). W train
      14,998,530 / val 2,396,928 / test 2,954,278; P train 14,999,677 / val 2,002,283 /
      test 4,426,774. By model: ST12000NM0008 11.6 M (0.87 GB, 14 min), ST8000NM0055
      9.3 M (0.71 GB, 12 min), ST8000DM002 6.2 M (0.49 GB, 9 min), ST14000NM001G 6.2 M
      (0.47 GB, 6 min), ST4000DM000 5.7 M (0.46 GB, 24 min), MG08ACA16TEY 2.8 M
      (0.22 GB, 2 min). Durable `root/full` is now 8.2 GB.
    - 241/242 resets: ST4000DM000 13,986, ST8000NM0055 10,323, ST8000DM002 7,081 (all
      but 23 / 51 / 5 of them in 2019Q2: the scrambled 2019-06-17, fixed in the next
      entry), ST12000NM0008 23, ST14000NM001G 0, MG08ACA16TEY 0.
    - Missingness (train and val only; `results/diagnostics/feature_missingness.csv`,
      1,244 study x model x split x feature rows): 576 are flagged by the 2x rule, but
      every one has both rates < 1% (456 < 0.1%). The highest pre-failure null rate
      anywhere is 0.44% (P ST12000NM0008 val p_pod_tb*_mean, 7 of 1,593 rows). Flags
      resting on >= 10 null pre-failure rows are only in W train: ST4000DM000 (<= 36 of
      32,232 rows, 0.11% vs 0.054% healthy) and ST8000NM0055 (<= 39 of 14,984, 0.26% vs
      0.105%). Before carry-forward the raw columns were at 12.4% (slot) and 2.1% (SMART
      5) vs ~0, so the rule removed the "silent before failure" signal.
  - Committed as `df81fa6`.

- **2019-06-17: scrambled SMART day (2026-10-29, Harsh: option a).**
  - The 8 failure events of that day look genuine. Each drive reported every day from
    06-01 to 06-16, has failure = 1 on 06-17, and has no row after it through
    2019-09-30 (raw 2019Q2/Q3 zips, read-only). None is on any later quarter's last day
    through 2026Q2 (end_state), and none has a second event.
  - New config key `smart_missing_days: {day: evidence}` (full.yaml: 2019-06-17; smoke:
    `{}`). On those days the rows stay (as samples, in the Parquet, in labels), and so
    do their failure events. Their SMART columns are never readings for later days;
    capacity, slot and pod_key still are (capacity is right that day). It is applied in
    `features.run_period`. A model's stage params hold the days in quarters where it
    has rows, so only those models rerun.
  - Rerun: only features_ST4000DM000, features_ST8000NM0055 and features_ST8000DM002
    (48 min, 4.7 GB peak RSS); the other 92 stages were skipped. All 49 event, label and
    split files (labels/*, splits/*.csv incl. summary.csv, splits/*/drives.parquet, every
    events.parquet) have the same sha256 as before. Every feature file has the same row
    count. Features now total 3,221,243,317 bytes.
  - 241/242 resets: ST4000DM000 13,986 / 13,958 -> 24 / 24; ST8000NM0055 10,323 / 10,366
    -> 52 / 52; ST8000DM002 7,081 / 7,075 -> 5 / 6. In 2019Q2 alone: 13,963 -> 1,
    10,272 -> 1, 7,076 -> 0 (241).
  - `tools/diagnostics/day_anomalies.py` (pass-2 Parquet, read-only) ->
    `results/diagnostics/day_anomalies.csv`, one row per day (3,824). A step between two
    readings g calendar days apart is impossible if SMART 9 falls or grows by more than
    24 h x g + 24 h (`--margin`). "Reverts" = the next reading fits the one before the
    day. Flagged (>= 5%):
    - 2019-06-17: 92.3% impossible, 99.99% revert (the scrambled day);
    - 2019-06-18: 92.3%, 0.3% revert (the step back);
    - 2017-02-01: 99.99%, 0% revert. This is the known late-January 2017 gap: the last
      reading before it is the kept stale 2017-01-27 (01-26's early snapshot), so
      01-27 -> 02-01 is about +150 h over 5 calendar days vs 144 h allowed. Not
      scrambled.
    - Every other day <= 1.8%.
  - Tests: a missing day gives no SMART readings (features equal those with the day's
    SMART set to null, the false reset gone, the row and its slot kept, nothing <= the
    day changed); a smart_missing_day reruns only the model feature stages (+
    downstream), not labels or splits, and a day without rows reruns nothing; the config
    check; the anomaly tool flags a reverting scrambled day, not the step back, not a
    real drop, not a 3-day gap. test_features 20, full `pytest -q` 172 passed.

## Next
- Harsh: decide the study settings from the labels/splits table (Done: kept as they
  are, frozen below, tag `studies-frozen`).
- M4 done (2019-06-17 handled, see Done).
- **M5** — models, evaluation, Experiments A/B (requirements below).

## Frozen before modelling (2026-10-28)
Fixed before any model is trained (git tag `studies-frozen`); not to be changed after
looking at results. Every test cell has at least 60 failed drives (900 pooled in Study P,
697 in Study W).
- **Horizons:** N = 7, 14, 30 days (`horizons`).
- **Study W windows** (S/W/N features; per model, `splits/windows.csv`): the config
  window ends at its last quarter with ≥ 10 failures; the last 2 quarters are test, the
  2 before are val, the rest train.

  | Model | Train | Val | Test |
  |---|---|---|---|
  | ST4000DM000 | 2016-01-01 – 2023-09-30 | 2023-10-01 – 2024-03-31 | 2024-04-01 – 2024-09-30 |
  | ST12000NM0008 | 2019-10-01 – 2025-06-30 | 2025-07-01 – 2025-12-31 | 2026-01-01 – 2026-06-30 |
  | ST8000NM0055 | 2016-10-01 – 2025-06-30 | 2025-07-01 – 2025-12-31 | 2026-01-01 – 2026-06-30 |
  | ST8000DM002 | 2016-04-01 – 2025-06-30 | 2025-07-01 – 2025-12-31 | 2026-01-01 – 2026-06-30 |
  | ST14000NM001G | 2020-07-01 – 2025-06-30 | 2025-07-01 – 2025-12-31 | 2026-01-01 – 2026-06-30 |
  | TOSHIBA MG08ACA16TEY | 2020-10-01 – 2025-06-30 | 2025-07-01 – 2025-12-31 | 2026-01-01 – 2026-06-30 |
- **Study P windows** (all groups S/W/N/P; same for every model): train 2023-08-01 –
  2025-03-31, val 2025-04-01 – 2025-09-30, test 2025-10-01 – 2026-06-30. Only models
  with failures in the test window; ST4000DM000 is out (none).
- **Split shares:** a drive that fails inside a split window goes to that split; every
  other drive goes by seeded hash (seed 42) with train 0.5, val 0.2, test 0.3.
- **Downsampling:** train only, by drive, 20 healthy drives per failed drive
  (`healthy_per_failed`).
- **Min-failed-drives rule:** a model enters a study only with ≥ 30 failed drives in
  train and ≥ 1 each in val and test; otherwise it is left out and listed in
  `splits/summary.csv` with the reason. Warning below 50 failed drives in a test cell.
- **Phase 1 pass criteria** (HANDOFF):
  - Experiment A passes if the 95% CI of the false-alarm relative risk excludes 1 for at
    least one model.
  - Experiment B (Study P) passes if the paired bootstrap CI of the FAR reduction, S vs
    S+W+N+P, excludes 0 on at least 2 of 3 horizons.
  - Secondary (Study W): the same test for S vs S+W+N.
- **Addendum (2026-10-28): operating points** (git tag `criteria-frozen`; the lines
  above are unchanged).
  - Primary operating point: 50% drive-level recall. 70% is reported but does not count
    for pass/fail.
  - Experiment A: XGBoost-S's threshold is set per model on val at 50% recall. On test,
    its false alarms are compared with all healthy drive-days: relative risk per
    workload decile and per pod condition. A passes if the 95% CI excludes 1 for at
    least one model at the 50% threshold.
  - Experiment B (Study P): FAR at exactly 50% drive-level recall on test, i.e. the
    point on each model's test FAR-recall curve. That is a property of the score
    ranking, not a tuned threshold. Pooled FAR = false-alarm healthy test drives ÷ all
    healthy test drives, using each model's own 50%-recall point. B passes if the paired
    bootstrap 95% CI of FAR(S) − FAR(S+W+N+P) excludes 0 on at least 2 of 3 horizons.
  - Deployment view, reported but not a criterion: thresholds set on val, applied to
    test, with the resulting test recall and FAR.
  - Bootstrap: 2,000 paired resamples of drives, stratified by model, the same
    resamples for both feature sets, percentile 95% CI, fixed seed.
  - The Study W secondary test (S vs S+W+N) uses the same rules as B.
  - Bootstrap seed: 42 (the config seed).
- **Addendum (2026-10-28, Harsh, M4): train-set sampling.** This changes the training
  data only, not evaluation; nothing had been trained yet. W train had 107 M rows,
  which won't fit XGBoost in 7 GB (laptop) or 12 GB (free Colab).
  - Train split only. Every drive-day of a failed drive within 60 days before its
    failure is kept (all positives plus the negatives near them), sample_weight 1.
  - Every other train drive-day is kept if hash_unit(seed, "rows", serial, date) < r,
    sample_weight 1/r. r per study is set so the stored train set has at most 15 M
    rows (`features.train_sample`).
  - Features are still computed over full histories. Val and test are complete
    (weight 1).
  - Result: r = 0.1365 (W) and 0.8647 (P); stored train rows 14,998,530 (W) and
    14,999,677 (P).

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
- Decided 2026-10-28 (Harsh), as built: label y_N = 1 if the failure date is in
  (t, t+N] by calendar date; a drive with no failure keeps negatives only up to N days
  before its last record (per split: its last record up to the window end); rows after
  the first failure dropped and counted, reappearing serials reported, not merged.
- Studies: W (S/W/N) per model from the start of its S/W/N window to its last quarter
  with ≥ 10 failures (reported in windows.csv), last 2 quarters test, 2 before val;
  P (all groups) fixed dates, only models with failures in the test window. Every
  feature set of a study is evaluated on the same drive-days.
- Assignment: a drive that fails inside a split window goes to that split; others by
  seeded hash (50/20/30). A drive contributes only rows inside its split window.
  Embargo per horizon N on train and val. Healthy drives downsampled 1:20 by drive,
  train only.
- A model enters a study only if it has ≥ 30 failed drives in train and ≥ 1 in val and
  in test; otherwise it is excluded from that study entirely and listed in
  `summary.csv` with the reason. No healthy-only training cells (Harsh, 2026-10-28).
- Smoke has no Study W (one quarter can't hold train, val and test); that is fine. The
  W path is tested on real data locally before M7 (Harsh, 2026-10-28).

## M4 requirements (decided 2026-10-27)
- Slot: use the last slot seen on a day ≤ t-1. Never use slot missingness as a feature.
  Pod coverage and the P window use pod_key; the slot is only a position feature.
- Every column flagged by the missingness-leak check (pod_slot_num, SMART 5/9/194/198,
  197) must also use the last value seen on a day ≤ t-1. Add a leakage test.
- Pod features come from `pod_days.parquet` (whole pod, all non-boot models), lagged to
  ≤ t-1; pod failure counts exclude the drive itself.
- A workload delta between two rows (Δ241/Δ242) is divided by the calendar days between
  them, so a gap in a drive's history gives a per-day rate, not a lump (Harsh,
  2026-10-28).
- Pod features (Harsh, 2026-10-28, after the operator-action leak check in Done):
  - Use the drive's pod_key from before any move (see Open questions for the exact rule).
  - No static hot-pod flag as a feature. Instead rolling, past-only context: failures in
    the same pod, vault and cluster (datacenter|cluster_id) over the 30 days ending t-1,
    the drive itself excluded.
  - Also an "excess" version of each: observed failures ÷ the count expected from the
    unit's model mix (Σ drive-days × the model's base rate), with per-model base rates
    from the train window only. Check (b) showed raw hot pods mostly vanish after
    model-mix adjustment, so a raw count would mostly re-learn the model.
  - For Experiment A, hot pods may be listed, using train-window data only.

## M5 requirements (decided 2026-10-28)
- TOSHIBA MG08ACA16TEY has 215 failures in 2025Q3 (its other quarters since 2023Q3:
  10–51), inside Study P's validation window. Choose alarm thresholds per model, and
  report pooled results both with and without MG08ACA16TEY.
- ST4000DM000 has no failures after 2024Q3 and no rows after 2025Q1; M3 handles it with
  per-study windows (Harsh, 2026-10-28). Its Study W window ends at 2024Q3 (rule: the
  last quarter with ≥ 10 failures). Its 2024Q4–2025Q1 retirement period may include
  data-migration reads (high 242 deltas while drives are emptied); that matters for
  Experiment A (workload deciles), so keep those quarters out of it or report them apart.
- Splits keep every failure in its own window, so val and test hold all failing drives
  but only 20% / 30% of healthy drives. FAR and recall are per drive as usual; precision,
  F1 and MCC must weight healthy drives by 1/share of their split (the `weight` column
  in `splits/<study>/drives.parquet`) so they reflect the real fleet ratio. Tested:
  weighted precision on a synthetic test split matches precision on the whole population.
- Feature files (M4): train rows are sampled (see the addendum under "Frozen"), and
  `sample_weight` is 1/r for sampled rows and 1 otherwise; val and test have weight 1.
  A model's features are the columns listed in `columns.json` for its study and groups.

## Open questions
- The carried end state is read when a quarter runs; it is not in the stage hash. If an
  earlier quarter is re-audited later, the next quarter's day-1 deltas are not redone
  automatically (rerun it by deleting its marker). Pass 1 runs quarters in
  chronological order, so a fresh run carries state into every quarter after the first
  one. Each audit marker records `info.carried_state_day1` (true/false) and
  `info.previous_quarter` (tested).
- Several high-failure models lack 241/242 (MG08ACA16TA, HUH721212ALN604,
  MG07ACA14TA). They can't be used for W/N under the current rule; HANDOFF's fallback is
  to pool related models or demote a feature group.
- ~~M4 pod_key "from before any move"~~ **Closed (Harsh, M4):** P features are keyed by
  the drive's pod_key on its last record on or before t-1 (its location as of
  yesterday). A drive that moves on day m uses its old pod through t = m (tested).
- The labels' "last record" is the last pass-2 row; days dropped by the no-SMART row
  rule do not count as records, so censoring is slightly conservative.
- ~~2019-06-17.csv has scrambled SMART values~~ **Closed (Harsh, option a, 2026-10-29):**
  its SMART values are treated as missing (`smart_missing_days`), its failure events are
  kept. See Done and Threats to validity.

## Threats to validity
- **Failure labels.** 236 serials keep reporting after their failure flag (104,855 rows
  after a failure dropped, after the copied days were removed; 104,944 before). So some
  Backblaze "failures" may be removals or redeployments, not hardware deaths; the
  label is "Backblaze flagged the drive as failed", and the paper should say so.
- **Data quality: a scrambled SMART day (rule for the paper).** One day, 2019-06-17,
  has scrambled SMART values: 92% of drives take an impossible SMART 9 step into it and
  step back the next day (`results/diagnostics/day_anomalies.csv`). Its readings are
  treated as missing (`smart_missing_days`) and its failure events are kept: the 8
  failed drives each stop reporting after 06-17.
- **Copied day files (rule for the paper).** A day file that is an exact copy of
  another day is dropped and the day counts as missing: 6 files (2017-01-29,
  2017-12-31, 2019-08-26, 2019-10-15, 2021-07-01, 2022-12-10; `dropped_days` in
  `configs/full.yaml`, `results/diagnostics/copied_days.csv`). None carried failure
  flags of its own, so no failure event is lost. One partial copy, 2017-01-27, is kept:
  it carries 22 failure events and 91 newer readings, and its stale readings are past
  data (a copy of 2017-01-26), so they cannot leak the future; only that day's deltas
  are stale. Copies are found from SMART 9 (power-on hours) not advancing between
  days; the scan covers the 6 pass-2 models, and January 2017 was checked on all
  models.

## TODO
- Pins were chosen as the latest stable 1.x lines; duckdb 2.0.0 (2026-10-21) and
  polars 2.x (Oct 2026) were skipped as too new. Revisit before M7 if Colab needs it.
- Budget: margin 1 GB; defaults from the 2026Q1 measurement (see Done). `models: []`
  kept (audit selects).
- The shared `.venv` has diskctx installed in editable mode from the main checkout, so
  after the merge `python -m diskctx` there uses the merged code. In a worktree, run
  with `PYTHONPATH=src`.
