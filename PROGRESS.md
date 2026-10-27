# PROGRESS

## Done
- **M0 (2026-10-27)** — repo skeleton, CLAUDE.md, PROGRESS.md, README.md, .gitignore,
  pyproject.toml (pins: duckdb 1.5.6, polars 1.44.2, pyyaml 6.0.3), one import test.
  Done-check `pip install -e ".[dev]" && pytest -q` passes.
- Fixture `tests/fixtures/sample_2026-01-01.csv` re-exported and checked: 197 columns,
  36 rows (19 DELLBOSS VD boot rows, 12 TOSHIBA MG07ACA14TA, 5 TOSHIBA MG07ACA14TEY).
  TA has empty 241/242; TEY has 241/242 and empty 197.

## Next
- **M1** — config + hash; storage with two tiers (durable → ROOT, cache → local only)
  and done-markers; a stage whose marker exists but cache output is missing rebuilds
  from durable inputs; results CSV with run-key skip; disk budget guard (free ROOT,
  free /content, measured bytes per quarter, stop if next unit won't fit); status table;
  synthetic generator (zero-padded IDs, boot drives, 241/242 per model string, counter
  resets, censored drives, zips with nested folders and junk files).
  Tests must include: delete the cache tier and rerun → rebuilt, durable stages skipped.

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

## Open questions
- When do pod/vault/slot fields start being populated? HANDOFF conflicts
  (Q2 2023 vs 2026 Q1). The M2 audit answers this; the split window is set after it.
- Real bytes per quarter (zip, day, Parquet) are unknown until the first real audit;
  the budget guard measures them.
- Will at least 2 exact model strings have ≥100 failures in the window, with enough in
  test for usable CIs? Decided by the M2 `select` stage.

## TODO
- **Harsh:** `docs/data-sources.md` lists only Q2 2024 → Q2 2026. For the full range,
  paste the URLs for Q1 2016 → Q1 2024 (and Q3 2026 if released). Code will not derive
  URLs. Until then the audit can only cover the listed quarters.
- Pins were chosen as the latest stable 1.x lines; duckdb 2.0.0 (2026-10-21) and
  polars 2.x (Oct 2026) were skipped as too new. Revisit before M7 if Colab needs it.
