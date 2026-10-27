# Handoff: Context-Aware Disk Failure Prediction

Oct 27, 2026 · @harsh patel

## How to use this handout

You are taking over a publication-level student research project, "Context-Aware Disk Failure Prediction". Planning, literature survey, a Phase 1 plan, a 16-slide deck and a pitch script are done; no code has been run yet. Read "Project in brief" through "Technical design" for the idea, "What exists and where it lives" before touching any artifact, and "Open items and next steps" for what to do next.

**The user:** Harsh, final-year CSE student at RV College of Engineering (RVCE), Bangalore, ID 1RV23CS095. The project is for his Information Storage Management (ISM) course (project name "ISM EL"), under a mentor.

**Working preferences:** he writes briefly and informally, wants to see built outputs rather than descriptions of them, and prefers clean, functional design over ornate design. Keep replies short. Make only the edits he asks for. Before changing a file or artifact, re-read it, because he edits some directly.

## Project in brief

The goal is a reproducible, open pipeline that predicts hard-drive failures from SMART telemetry plus workload and storage-pod context, and raises fewer false alarms than SMART-only predictors. The mentor's brief covered the research problem, the existing technique, novelty, algorithms and technologies, datasets, measurable parameters, and a paper or patent output. Harsh asked for publication-level work.

**Problem statement (as on the deck's statement slide):** To reduce false alarms in SMART-based disk failure prediction, where models cannot tell workload stress from true degradation, by developing a context-aware predictor that conditions SMART signals on workload intensity and storage-pod conditions from open Backblaze data, lowering the false-alarm rate at equal recall while extending warning lead time.

**Datasets:** Backblaze Drive Stats, Q2 2023 to the latest quarter (pod fields start Q2 2023). The 2026-01-01 file was used to check the schema. Fallback: Alibaba SSD and fail-slow (Perseus) datasets.

**Target:** a conference or journal paper. A patent is possible but risky (see "Why this idea, and what is novel").

## Why this idea, and what is novel

The original idea (SMART plus workload) is already published by industry teams, so the scope was reframed around what public data newly allows. Context-aware prediction is shown only on proprietary fleets (Microsoft, Alibaba, Meta). Workload appears as a side feature or a lifetime total, never as a normaliser for degradation. Nobody has measured how many false alarms workload explains. Backblaze's 2026 Q1 files now publish pod and slot fields, which opens neighbour context on public data.

**Four novelty claims:**

1. An open, reproducible pipeline joining SMART, workload and pod context on Backblaze data.
2. False-alarm attribution: what share of SMART-only false alarms workload explains.
3. Pod and vault context tested on public data.
4. Workload-stratified (Mondrian) conformal alarms, capping false alarms per workload band.

**Patent risk:** US 12,248,682 already covers a workload index plus diagnostics for failure prediction. Treat a patent as unlikely; the paper is the main output. Do not assert patentability without a proper search.

**Fallback if Backblaze data fails the Phase 1 gates:** Alibaba SSD and fail-slow (Perseus) datasets.

## Technical design

The design is one XGBoost scorer behind a common fit/score interface, four feature groups, drive-disjoint time-ordered splits, and drive-level metrics. Survival models, conformal alarms and other datasets plug in later without rewriting this.

**Backblaze schema facts (checked against the 2026-01-01 file; these corrected earlier assumptions):**

- Row layout: date, serial\_number, model, capacity\_bytes, failure, datacenter, cluster\_id, vault\_id, pod\_id, pod\_slot\_num, is\_legacy\_format, then SMART normalized/raw pairs.
- pod\_id is two digits (00–19) and not unique alone. The pod key is datacenter + cluster\_id + vault\_id + pod\_id.
- Read the ID columns as strings (zero-padded, e.g. 0028) and SMART raw values as 64-bit integers (LBA counters exceed 2^31). Use union\_by\_name because columns change across quarters.
- Boot drives (e.g. DELLBOSS VD) are in the daily files with empty SMART and no slot number. Drop rows with empty pod\_slot\_num or no SMART values.
- SMART 241/242 (LBAs written/read) coverage differs per exact model string, even within a family (Toshiba MG07ACA14TEY has them; MG07ACA14TA does not). Select and audit per exact model string.
- SMART 9 normalized decays to 1 on old Toshiba drives, so use raw values. SMART 197 appears empty on MG07ACA14TEY.
- Backblaze is a write-once archive: most writes come from filling new drives and rebuilds, so report false alarms separately for fill-phase and steady-state days.

**Feature groups** (each uses only past data):

| Group | Content |
| --- | --- |
| S — SMART | Raw 5, 187, 188, 197, 198, 9, 194 as the model reports them; 7- and 30-day deltas of error counters |
| W — workload | Daily TB written/read from Δ241/Δ242 × LBA size (assume 512 B); per-capacity fills per day; rolling mean/max; drive age; fill-phase flag |
| N — stress-normalized | Error-counter growth per TB written over 7/30 days; error growth ÷ workload growth (core novelty) |
| P — pod context | Pod mean/max daily TB, pod mean temperature, drive minus pod temperature, slot number, failures in same pod/vault in past 30 days excluding the drive itself |

**Labels and splits:** a drive-day is positive if the drive fails within N = 7, 14 or 30 days. Drives that leave without a failure flag are censored. Train Q2 2023 to mid 2025, validation the next two quarters, test the latest two. A serial in test never appears in train. Healthy drives are downsampled by drive on train only.

**Models:** Phase 1 uses XGBoost plus a rule baseline (any of SMART 5, 187, 188, 197, 198 non-zero). Later: random forest, Cox PH, random survival forest, LSTM/TCN, Mondrian conformal wrapper (MAPIE).

**Metrics (drive-level):** a drive is a false alarm if flagged and it does not fail within N days of its first flag. Primary: false-alarm rate (FAR) at 50% and 70% recall. Secondary: recall at FAR 0.1% and 0.5%, precision, F1, MCC, median lead time, prediction latency, bootstrap 95% CIs over test drives, 5 seeds.

## Phase 1 plan (4 weeks, about 10–12 hours a week)

Phase 1 must show, on public Backblaze data, that a meaningful share of SMART-only false alarms is explained by workload or pod conditions, and that adding that context lowers FAR at the same recall. It runs on a 16 GB laptop with no GPU. Stack: Python 3.11, DuckDB, Polars, PyArrow, scikit-learn, XGBoost, SHAP, SciPy; YAML configs plus a results CSV per run, in git.

1. **Week 1 — data audit and labels.** Ingest with an explicit schema, drop boot drives, audit per exact model string, pick 2–3 models with at least 100 failures and 95% coverage of SMART 241, 242 and pod fields. Build horizon labels (7/14/30 days) with censoring. First gate: if fewer than 2 models qualify, widen the window or demote pod features to a secondary model.
2. **Week 2 — features and baselines.** Feature registry (S, W, N, P), splits, rule baseline and XGBoost-S. Mentor review.
3. **Week 3 — the two experiments.** Experiment A: fix XGBoost-S at 50% recall (also 70%), then compute the relative risk of a false alarm per workload decile and pod condition, with bootstrap CIs and a chi-square test. Experiment B: train S, S+W, S+N, S+P, S+W+N, S+W+N+P with equal tuning budgets, 5 seeds, all three horizons, SHAP on false-alarm drives.
4. **Week 4 — evidence package.** Three figures (false-alarm relative risk by workload decile; FAR-vs-recall for S vs best context set; SHAP summary), one ablation-by-horizon table, a 4-page workshop-style draft, a tagged repo with a reproduction README. Mentor review and the go/no-go meeting.

**Pass criteria (fixed in advance):** A passes if the 95% CI of the false-alarm relative risk excludes 1 for at least one drive model. B passes if the paired bootstrap CI of the FAR reduction (S vs S+W+N+P) excludes 0 on at least 2 of 3 horizons.

**Decision rules:** both pass → full plan (survival models, two-stage gate, conformal alarms). Only A → publish a characterization result and move to conformal alarms using the attribution as strata. Only B → proceed, framing the paper around the predictor. Neither → stop the HDD track and move to the fail-slow-to-fail-stop SSD direction on Alibaba data, reusing the pipeline.

**Main risks:** too few failures per model (pool related models with a model-ID feature); 241/242 counter resets (mark invalid days); leakage via pod failure counts (1-day lag, exclude the drive itself); wide CIs (merge horizons, widen test window). The full step-by-step plan with architecture and schedule widgets is in the plan tab of the scope document (linked under "What exists and where it lives").

## What exists and where it lives

Three artifacts are published on claude.ai. Read them with the Artifact tool's read action (docs with the Docs tools), never by fetching the URL.

| Artifact | Link | Contents |
| --- | --- | --- |
| Scope document (Docs, 3 tabs) | [open](https://claude.ai/code/artifact/68e8de9e-82d2-4326-b05d-05c0b7795cb8) | Tab 1: scope document. Tab 2: industry literature survey. Tab 3: Phase 1 implementation plan with architecture and schedule widgets |
| Slide deck (Slides, 16 slides) | [open](https://claude.ai/artifact/4pA1PijaVVbfKaZc7agDXg) | The presentation for the course review |
| Pitch and Q&A (Docs) | [open](https://claude.ai/code/artifact/fb6fdd31-fbb2-4ce1-8e85-da79bf17fd78) | Timing table, per-slide speaking script for slides 1–16, Q&A answers, glossary |

**Deck order:** cover, introduction, existing solutions, problem definition, problem statement, literature survey 1 and 2 (slides 6–7), research gaps, objectives and deliverables, pipeline (flowchart), features, conclusion, references 1–4 (24 references). Slides are 1920×1080 HTML sections using an inline-style subset. The previous session kept a local working copy (deck.json plus slides/\*.html) that may no longer exist; if so, read the published deck and republish with the same URL.

**Do not overwrite the user's own edits.** He edited the statement and objectives slides directly (the deliverable "Workload-conditioned risk scoring" is his wording). The pipeline slide was edited by the previous assistant (comment removed, box 08 relabelled "Outputs"). Always re-read a slide before changing it.

**Latest change:** slides 6–7 were reformatted into three columns: Paper (APA), Paper highlights, Research gap. Long author lists use "et al." in the table; full author lists are on the reference slides. The speaker script was deliberately left unchanged and may need a light check against the new table wording.

**Constraints to respect:** keep jargon low in anything spoken; the pitch text must stay presentable live; all numbers on slides and in the script must trace to the cited papers.

## Literature base

The deck cites 24 references, all from 2023–2026, written in full APA on the four reference slides. The twelve below are the core papers shown in the literature tables (slides 6–7). Details come from the papers' own abstracts as read earlier; re-verify any number before reusing it in a paper.

| Paper | Highlights | Research gap |
| --- | --- | --- |
| Siemroth & Park (2026), IEEE Trans. Cloud Computing | Survival regression on 443k Backblaze drives; each +1 °C raises failures about 2.1% | Fleet-level statistics only; no per-drive early warning or workload context |
| Akewar et al. (2026), SMARTTalk, USENIX OSDI | Turns SMART trends into text for an LLM; about 25% better health classification | SMART only; workload and location ignored |
| Akewar et al. (2026), KORAL, IEEE IPDPS | LLM plus knowledge graphs fuse SMART, workload and environment for SSD diagnosis | SSD-only; environment joined from lab studies; no false-alarm guarantee |
| Wang et al. (2026), FailureMiner, USENIX FAST | Mines key decision rules; deployed on 350k+ SSDs at Tencent for a year | Proprietary SSD telemetry; degradation not normalised by workload |
| Zhang et al. (2025), LWCM, J. Computer Science and Technology | Relabels samples by warning window; true-positive rate up 38.7% over offline random forest | Fixes horizon labels but stays SMART-only |
| Mallik, Reddy & Sahoo (2024), Frontiers in Computer Science | Cox model on data volume and access rate; faster access lowers survival | Static lifetime workload on vendor data; no lead time or false-alarm rate |
| Ahmed & Green (2024), Neural Computing and Applications | Cost-aware deep net for rare failures plus survival analysis of SMART | SMART-only features; no workload or location context |
| Li et al. (2024), Microsoft Azure, IEEE ISSRE | Unverifiable post-mitigation labels drop accuracy about 9%; the Uptake method recovers about 5% | Fixes label noise; workload-driven false alarms untouched |
| Zhou et al. (2024), Huawei, IEEE TDSC | Two semi-supervised models; separates slow and sudden deterioration | Tackles label scarcity, not operating context |
| Zhang et al. (2024), MSFRD, USENIX ATC | Rates SSDs on four levels from serious failure to perfect health | Graded SSD alarms; no workload or pod conditioning |
| Lu et al. (2023), Perseus, USENIX FAST (Alibaba) | 248k drives; latency vs throughput finds 304 slow drives; best paper | Detects slow drives; not linked to failure prediction |
| Miller et al. (2023), Meta, IEEE/IFIP DSN | Failure rate rises with age and lifetime workload; XGBoost on SMART | Workload only as lifetime total; proprietary data; no false-alarm study |

**Other references in the deck:** Gao et al. (2023) domain adaptation; Han et al. (2023) StreamDFP; Mohapatra et al. (2023) end-of-life prediction and TFBEST transformer; Oakley et al. (2024) multi-state survival models; Vishwakarma et al. (2023) Mondrian conformal predictors for disk scrubbing (the basis for the conformal idea); Wang et al. (2026) comparison of SSD prediction methods; Wu (2023) learned predictability; Zhang et al. (2023) multi-view SSD prediction (FAST '23); Akewar et al. (2025) environmental impact on SSD (HotStorage '25); Backblaze (2026) dataset; Doyle (2026) Backblaze blog on academic use of Drive Stats.

The industry literature survey tab in the scope document holds the longer analysis, including company-authored work.

## Pitch and Q&A

The pitch document is written for live delivery with minimal jargon. It has a timing table, a script for each of slides 1–16 (what to say, what to point at, a "move on" line), and Q&A answers grouped as: problem and novelty, the data, why these models, why these metrics, and feasibility. It ends with a plain-language glossary.

**The standing answers, in one line each:**

- *Why Backblaze:* it is the only large public drive dataset with per-drive daily SMART plus pod and slot fields, so anyone can reproduce the work.
- *Why XGBoost first:* it handles missing values and rare failures well, is fast, and gives a strong, explainable baseline; the contribution is the context features, not the model.
- *Why FAR at fixed recall:* accuracy is meaningless when only a tiny fraction of drives ever fail; operators care how many healthy drives get wrongly pulled for a given detection rate.
- *Why bootstrap over drives:* drive-days from one drive are not independent, so resampling drives gives honest confidence intervals.
- *Why drive-disjoint, time-ordered splits:* random splits leak the future and the same drive into training and inflate results.
- *Why conformal alarms and survival models are later:* they reuse the Phase 1 pipeline once the core context effect is proven.

The exact wording of each answer is in the pitch document; reuse it rather than rewriting.

## Open items and next steps

Nothing is blocking. The user has not yet confirmed the date of the course review or whether the mentor wants changes to the deck. Suggested order:

1. Ask Harsh what the mentor said about the deck and the scope; adjust the deck or the plan only as he asks.
2. Check the pitch script for slides 6–7 against the new table wording (the script was not edited after the reformat).
3. Start Phase 1 week 1 if he wants to build: set up the repo (diskctx/), download Backblaze quarters from Q2 2023, and produce the audit table. This is the first gate.
4. Before any claim in a paper, re-verify each number against its source and run a proper patent search around US 12,248,682.
5. Keep the fallback (Alibaba datasets) ready in case the audit leaves fewer than 2 usable drive models.

**Known caveats:**

- The Backblaze sample inspected was the first 50 lines of the 2026-01-01 file only; coverage claims per model are from that sample until the full audit runs.
- The literature table uses "et al." for long author lists; full APA is on the reference slides.
- Some paper details (percentages, deployment sizes) come from abstracts; confirm in the full text before citing them as results.
- No experiments have been run, so no result in any artifact is a finding yet.
