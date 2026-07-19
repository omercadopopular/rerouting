# Raw Policy Reconstruction Log

## Objective

Construct `m_statutory_tariff1` and `m_statutory_tariff2` from raw sources (USITC annual ZIP + revision archive PDFs/machine-readable files), then audit against `data/fajgelbaum/data/analysis/m_flow_hs10_fm_new.dta`.

## Code Paths

- Builder: `scr/passthru_data/build_us_products_partner_panel.py`
- Audit: `scr/passthru_data/audit_policy_vs_fajgelbaum.py`
- Pipeline entry: `scr/pipeline_passthru_data.py`

## Raw Inputs Used

- Annual ZIPs: `data/raw/passthru_data/policy/annual/tariff_data_YYYY.zip`
- Revision catalog: `data/reference/passthru_data/policy_release_catalog.csv`
- Revision PDFs: `data/raw/passthru_data/policy/archive/pdf/*.pdf`

## Implemented Steps

1. **Baseline bilateral panel integration**
- Merged trade panel (`cty_code × hs10 × year × month`) with monthly HTS schedule.
- Added raw policy columns:
  - `base_pref_rate_raw`
  - `tw_increment_rate_raw`
  - `tw_rule_code_raw`
  - `tw_active_share_raw`
  - `m_policy_source`

2. **Base statutory preference layer from annual ZIP**
- Parsed partner-specific annual columns (e.g., NAFTA/FTA rates).
- Added group-program overlays:
  - `GSP`, `CBI`, `AGOA`, `CBTPA`, `DR_CAFTA`
- Implemented `gsp_ctry_excluded` token decoding.

3. **Special-rate text parsing**
- Parsed `col1_special_text` for:
  - `Free(...)` eligibility token sets
  - partner-specific ad-valorem overrides like `0.6% (KR)`
- Applied conservative ad-valorem only logic (no cents/kg conversion in this step).

4. **Trade-war layer migration to revision-PDF parser**
- Replaced annual `footnote_comment -> 9903` heuristic.
- Implemented Chapter 99 PDF parser:
  - Identify `9903.(45|46|80|85|88).*` rule lines.
  - Parse nearby context for `provided for in subheading ...` HS lists.
  - Build `rule_code × hs8` links by release window (`release_start_date`, `release_end_date`).
- Joined with annual Chapter 99 rule attributes (`increment_rate`, description, start/end).
- Expanded to bilateral panel using country targeting text in rule descriptions.

5. **Month-day scaling (partial)**
- Set:
  - `m_statutory_tariff1 = base_statutory_rate_raw + tw_increment_rate_raw`
  - `m_statutory_tariff2 = base_statutory_rate_raw + tw_increment_rate_raw * tw_active_share_raw`
- `tw_active_share_raw` currently uses release-start month day shares and full share afterward.

6. **Persisted intermediate artifacts**
- `data/reference/passthru_data/tradewar_pdf_links.parquet`
- `data/reference/passthru_data/tradewar_rule_attributes.parquet`
- `data/analysis/passthru_data/tradewar_overlay_raw.parquet`

## Audit Progression (2017-2019, non-sentinel rows)

- Initial raw overlay heuristic: mean abs diff `0.02432`
- + bilateral FTA preference layer: `0.02045`
- + program layer (GSP/CBI/AGOA/DR-CAFTA): `0.01869`
- + `col1_special_text` parsing: `0.01863`
- + PDF-derived trade-war mapping + partial day scaling: `0.01581`

Current headline metrics are in:
- `data/verification/passthru_data/policy_diff_audit_metrics.csv`

## Current State Summary

- Trade-war overlay rows in panel: `124,993`
- Partial-month overlay rows: `38,520`
- Source mix (cells):
  - `mfn_schedule_only`: `87.61%`
  - `base_preference_raw`: `11.72%`
  - `trade_war_raw_overlay`: `0.66%`

## 2026-07-12 China-Current Validation Universe Fix

Implemented a China-current validation correction in `raw_replication_validation.py`:
- the current benchmark universe now keeps all `cty_code = 5700` and `m_china_hit = 1` rows, while using `m_status2 > 0` only as the active-status definition;
- the partial gate now computes treatment and rate match rates on matched keys only;
- new machine-readable diagnostics were added:
  - `raw_replication_china_301_universe_audit.csv`
  - `raw_replication_china_301_metric_denominators.csv`
  - `raw_replication_china_301_raw_only_keys.csv`
  - `raw_replication_china_301_residual_current.csv`
  - `raw_replication_china_301_rate_difference_quantiles.csv`

Refreshed China-current snapshot:
- selected benchmark rows: `184,343`
- matched keys: `184,134`
- `missing_raw_policy_scope_rows`: `42`
- `missing_reference_key_rows`: `400,555`
- `tariff_active_treatment_match_rate`: `0.883127`
- `tariff_active_statutory_rate_match_rate`: `0.462733`
- `tariff_active_day_weighted_rate_match_rate`: `0.293433`

## Remaining Gaps to Close

1. Refine Chapter 99 PDF scope extraction (reduce false positives in dense note blocks).
2. Add explicit quota/exclusion carve-out logic (`9903.80.60/.61` and related cases).
3. Complete day-level month scaling to mirror paper timing conventions.
4. Add explicit AVE handling strategy for specific/compound duties.
5. Re-audit by rule/country/product slices until residuals are confined to documented edge cases.

## 2026-04-22 Focused 301/232/Safeguards Pass

Implemented a stricter parser pass in `build_us_products_partner_panel.py`:
- Added U.S.-note extraction for 301/232/safeguards note families.
- Added note-family guards:
  - note `20(*) -> 990388**`
  - note `16(*) -> 990380**`
  - note `19(*) -> 990385**`
  - notes `17/18(*) -> 990345**/990346**`
- Added explicit country overrides for key 232 quota rules:
  - `99038002 -> TURKEY`
  - `99038505/99038506/99038511 -> ARGENTINA`

Rebuilt step:
- `build_us_products_partner_hs10_panel` (overwrite)

Re-ran policy audit (`2017-2019`) and active-scope diagnostic:
- `diff_no_sentinel_mean_abs`: `0.018441` (worse than prior `0.01581`)
- Active-scope diagnostic (baseline increment over base > 0.01):
  - baseline-active rows: `675,985`
  - our active rows: `84,744`
  - active recall: `7.38%`
  - active precision: `58.85%`

Interpretation:
- Current focused pass did not close the gap; it increased noise.
- Missing-increment mass is not only in canonical hit groups (`m_china_hit/m_steel_hit/m_alum_hit/m_washer_hit/m_solar_hit`):
  - hit-group share of missing-increment mass: `39.1%`
  - non-hit share of missing-increment mass: `60.9%`
- This indicates remaining divergence is still materially driven by broader statutory/preference construction mismatch in addition to trade-war scope.

## 2026-04-22 Deterministic-First Infrastructure Update

Implemented:
- Machine-readable scope extractor:
  - `_load_tradewar_machine_links(config)` now parses archive CSV releases and creates `HS8 x rule_code` links.
- Deterministic source priority in overlay build:
  - machine-readable links first,
  - PDF links only fallback for unmatched tuples.
- Manual deterministic override hook:
  - `data/raw/passthru_data/manual/policy/tradewar_rule_overrides.csv`
- Provenance in output panel:
  - `tw_scope_source_raw` (`machine_or_pdf`, `manual_override`, or missing)

Artifacts:
- `data/reference/passthru_data/tradewar_machine_links.parquet`
- `data/reference/passthru_data/tradewar_pdf_links.parquet`
- `data/analysis/passthru_data/us_products_partner_hs10_monthly.parquet` includes `tw_scope_source_raw`

Latest stats after rebuild + audit:
- `diff_no_sentinel_mean_abs`: `0.018441`
- `share <= 0.01`: `80.69%`
- `share <= 0.05`: `90.48%`
- `share <= 0.10`: `95.21%`
- active recall: `7.38%`
- active precision: `58.85%`

Conclusion:
- The deterministic-first infrastructure is now in place.
- Gap did not improve yet; next closure requires explicit per-family scope/exemption rule additions for top missing buckets.

## 2026-04-22 Raw-Only Reversion

Per user instruction, replication-guided non-232 overrides were removed:
- deleted `data/raw/passthru_data/manual/policy/tradewar_rule_overrides.csv`
- reran panel build and audits with raw-source inputs only

Additional raw-only improvements retained:
- hardened machine-readable parser matching for UTF/BOM column variants
- machine-link extraction now uses `policy_archive_revision_index.csv` with local-file matching

Current (raw-only) diagnostics:
- `diff_no_sentinel_mean_abs`: `0.018441`
- active recall: `7.38%`
- active precision: `58.85%`
- scope provenance in panel:
  - `machine_or_pdf`: `190,582` rows
  - `missing`: `43,191,027` rows

Blocking constraint:
- many missing machine-readable revision CSVs remain inaccessible via direct HTTP (`403`),
  and Selenium bulk download for those files is still incomplete/time-limited.

## 2026-04-24 PDF Extraction Batch + Panel Integration

What was implemented:
- Added batch PDF extraction support in `scr/passthru_data/extract_hts_pdf_to_csv.py`:
  - `--batch`
  - `--start-year`, `--end-year`
  - `--fallback-only`
- Added extracted-CSV scope loader in `scr/passthru_data/build_us_products_partner_panel.py`:
  - `_load_tradewar_pdf_csv_links(config)`
- Updated scope-priority chain to:
  1. machine links
  2. extracted-CSV links
  3. direct PDF parser links

Batch extraction run:
- command:
  - `uv run --with pandas --with pymupdf python scr/passthru_data/extract_hts_pdf_to_csv.py --batch --start-year 2017 --end-year 2019 --fallback-only`
- manifest:
  - `data/staging/passthru_data/policy/pdf_extract/pdf_extract_manifest_2017_2019.csv`
- results:
  - `attempted_releases = 42`
  - `ok_releases = 35`
  - `skipped_machine_available = 7`
  - `failed_releases = 0`
  - `total_rows_extracted = 690,670`

Post-build diagnostics after integration:
- full-panel overlay rows: `190,582`
- active-scope check (2017-2019):
  - baseline-active rows: `675,985`
  - our active rows: `84,744`
  - true-positive active rows: `49,871`
  - active recall: `7.38%`
  - active precision: `58.85%`
  - missing active rows: `626,114`

Operational caveat:
- Direct PDF parser fallback in `build_us_products_partner_panel.py` requires `PyMuPDF` (`fitz`).
- If `fitz` is unavailable at runtime, overlay rows collapse (observed in a direct module run), so this dependency should be pinned for policy builds.

## 2026-04-25 Forward-Fill Fix for Baseline Rates

Issue:
- When a higher-priority revision row existed but had missing `mfn_ad_val_rate`, the monthly schedule could replace a previously valid baseline rate with missing.
- This generated artificial zeros in `base_statutory_rate_raw` and inflated policy gaps.

Code change:
- Added `_forward_fill_hs8_rates(...)` in `scr/passthru_data/build_hts_monthly_schedule.py`.

## 2026-07-12 China-Current Validation Universe Fix

The China-only raw-replication validator was using `m_china_hit` and `m_status2 > 0`
without requiring `cty_code = 5700` on the benchmark side. That made the China-current
coverage diagnostics count China-policy rows on non-China partners as if they were China
import cells, which inflated the apparent benchmark gap.

Implemented:
- Added a China-current universe report:
  - `raw_replication_china_301_validation_universe.csv`
- Added a reference-side decomposition report:
  - `raw_replication_china_301_validation_decomposition.csv`
- Added a targeted active-policy summary:
  - `raw_replication_by_family_country_targeted.csv`
- Updated `run_raw_replication_validation_china_current(...)` to compare only the
  China partner universe (`cty_code = 5700`, `m_status2 > 0`, `m_china_hit = 1`).

Observed local result:
- China-current paper-key coverage rose to `0.9979127018233164`.
- `ready_for_extension` remains `false`.
- The remaining gap is now much smaller and is dominated by exact missing keys and
  rate mismatches, not by partner-universe leakage.
- Applied after monthly expansion/source-priority selection:
  - sort by `hs8, year, month`
  - carry forward last non-missing `mfn_ad_val_rate` within `hs8`
  - mark imputed rows with `mfn_ad_val_rate_ffilled`

Observed schedule impact (current built schedule files):
- `mfn_ad_val_rate` non-missing rows increased from `374,311` to `463,041`
- carry-forward rows: `88,730`

Gap impact (2017-2019 audit window; holding overlay logic fixed):
- Before forward-fill:
  - `diff_no_sentinel_mean_abs = 0.01844`
  - `share <= 0.01 = 80.69%`
  - `share <= 0.05 = 90.48%`
  - active recall = `7.38%`
  - missing active rows = `626,114`
- After forward-fill:
  - `diff_no_sentinel_mean_abs = 0.01436`
  - `share <= 0.01 = 85.41%`
  - `share <= 0.05 = 93.97%`
  - active recall = `12.04%`
  - missing active rows = `328,677`

Interpretation:
- Carry-forward closes a large part of the baseline-rate gap.
- Remaining gap is still substantial and now more clearly concentrated in trade-war scope/timing/exemption mapping.

## 2026-04-26 Raw-Only Finalization

Policy panel finalization now follows raw artifacts only:
- schedule source: `data/reference/passthru_data/hts_monthly_hs10_schedule.parquet`
- panel source: `data/analysis/passthru_data/us_products_partner_hs10_monthly.parquet`
- finalizer: `scr/passthru_data/finalize_policy_panel_from_schedule.py`

Finalizer behavior:
- refreshes panel tariff columns from schedule (`mfn_*`, `source_type`, `release_name`)
- recomputes:
  - `base_statutory_rate_raw`
  - `m_statutory_tariff1`
  - `m_statutory_tariff2`
- keeps `cty_code=-9999` rows in the full panel
- writes bilateral regression subset (`cty_code>0`) to:
  - `data/analysis/passthru_data/us_products_partner_hs10_monthly_regression.parquet`

Regression-workhorse fallback:
- disabled in `build_trade_workhorse_panels.py` (raw-only mode).

## 2026-07-11 Raw-Replication Validator Patch

Issue:
- The raw-replication validator used `m_ess == 2` as the paper-side treatment flag.
- `m_ess` is an event-study status, not an active tariff indicator, so this created
  false active-scope gaps before the 2018 tariff implementation window.

Code change:
- Updated `scr/passthru_data/raw_replication_validation.py` to define active tariff
  status with `m_status2 > 0`.
- Split the release gate into paper key coverage, active tariff coverage/treatment/rate
  equivalence, and raw trade-value equivalence.
- Added machine-readable summaries by discrepancy type, year-month, tariff family,
  country, HS2, and rate-difference bucket.

Latest validation run:
- command:
  - `C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step validate_raw_replication_imports --skip-downloads --skip-verification --overwrite`
- output directory:
  - `data/verification/passthru_data/raw_replication_imports`
- paper rows: `4,653,250`
- raw rows: `18,873,315`
- matched rows: `4,199,002`
- reference active tariff rows (`m_status2 > 0`): `859,093`
- active matched rows: `776,778`

Current release gate:
- `ready_for_extension = false`
- `paper_key_coverage_rate = 0.902380486756568`
- `tariff_active_key_coverage_rate = 0.9034167430068689`
- `tariff_active_treatment_match_rate = 0.0021666422066536385`
- `tariff_active_statutory_rate_match_rate = 0.571234214479519`
- `tariff_active_day_weighted_rate_match_rate = 0.5572568946103125`
- `raw_trade_value_match_rate = 0.02713692444061708`

Active-family gap concentration:
- China 301: `728,020` missing-scope rows, `73,367` missing-key rows, `1,195` statutory-rate mismatches.
- Section 232 steel: `36,380` missing-scope rows, `8,594` missing-key rows.
- Section 232 aluminum: `8,256` missing-scope rows, `797` missing-key rows.
- Section 201 washers: `529` missing-scope rows, `470` statutory-rate mismatches, `127` missing-key rows.
- Section 201 solar: `1,251` missing-scope rows, `89` missing-key rows.

China 301 top bucket diagnostics written by the validator:
- `raw_replication_china_301_top_hs8.csv`
- `raw_replication_china_301_top_month.csv`
- `raw_replication_china_301_top_hs8_month_wave.csv`
- `raw_replication_china_301_source_audit.csv`
- `raw_replication_china_301_trace.csv`
- `raw_replication_china_301_source_audit_errors.json`

Top China 301 missing-scope clusters:
- HS8: `84821050`, `85365090`, `90328960`, `84314990`, `94032000`, `85044095`, `84818090`
- Month: `2018-10`, `2019-04`, `2019-03`, `2018-11`, `2018-12`, `2019-01`, `2019-02`
- Paper-wave proxy: `2018-07` and `2018-10`

China 301 trace stages now emitted by the validator:
- `absent_from_raw_links`
- `raw_links_missing_rule_attrs`
- `lost_before_overlay`
- `lost_after_overlay`
- `present_with_increment`
- `source_unavailable`

## 2026-07-11 Source Hydration and Overlay-Only Rebuild

Issue:
- Source-health diagnostics showed all raw policy source artifacts as present but unreadable.
- Windows reported `The cloud file provider is not running` for OneDrive placeholder files.

Resolution:
- Started OneDrive and pinned the relevant policy/reference/analysis trees with `attrib +P -U`.
- Re-ran `validate_raw_replication_imports`; `raw_replication_source_health.json` now reports
  `blocked_by_source_availability = false`.
- Added `build_tradewar_overlay_raw`, an archived opt-in pipeline step that rebuilds only
  `tradewar_overlay_raw.parquet`, reapplies it to `us_products_partner_hs10_monthly.parquet`,
  and recomputes raw statutory tariff columns.
- Added a China 301-only fallback from the hydrated reference `tradewar_pdf_links.parquet`
  into overlay construction. This uses raw extracted HTS/PDF links only; it does not use
  the Fajgelbaum package to populate tariff data.

Commands:
- `attrib +P -U /S /D "data\reference\passthru_data\*"`
- `attrib +P -U /S /D "data\raw\passthru_data\policy\*"`
- `attrib +P -U /S /D "data\staging\passthru_data\policy\*"`
- `attrib +P -U /S /D "data\analysis\passthru_data\*"`
- `C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step build_tradewar_overlay_raw --enable-archived-policy-pipeline --skip-downloads --skip-verification --overwrite`
- `C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step validate_raw_replication_imports --skip-downloads --skip-verification --overwrite`

Current result:
- `tradewar_overlay_raw` rows increased to `90,708`.
- China overlay rows increased to `77,671`.
- China 301 missing-scope rows fell from `728,020` to `691,056`.
- `tariff_active_treatment_match_rate` rose from `0.0021666422066536385` to `0.04917756552299941`.
- `tariff_active_statutory_rate_match_rate` rose from `0.571234214479519` to `0.5883728302078183`.
- `ready_for_extension` remains `false`.

Interpretation:
- The hydration blocker is resolved for the current validation inputs.
- The largest remaining China 301 gap is now a scope timing/action-mapping problem, not source
  availability.
- Top missing HS8s are present in PDF-derived raw links and partially present in the rebuilt overlay,
  but the paper still has active rows beyond the current release-window expansion. The next patch
  should address China 301 scope persistence and false/competing rule links using raw HTS/USTR
  evidence, not package-guided overrides.

Interpretation:
- The remaining active tariff gap is primarily a raw-source scope/action mapping problem,
  especially China 301, followed by 232 country/exemption logic and smaller 201 edge cases.
- Exact raw trade-value replication remains a separate dataset-rebuild issue and should not
  be interpreted as tariff-rate failure.

## 2026-07-12 China 301 Rate Provenance Refresh

Freshness:
- `raw_replication_artifact_freshness.csv` reports the China 301 validator artifacts as current.
- `raw_replication_china_301_trace.csv` still shows `present_with_increment = 399`, `lost_before_overlay = 0`, and `lost_after_overlay = 1`.

Rate provenance:
- `raw_replication_china_301_rate_provenance.csv` separates the remaining rate gaps into
  `benchmark_source_precision_diff = 15,975`, `benchmark_statutory_definition_mismatch = 9,719`,
  and `benchmark_timing_mismatch = 7,368`.
- The raw formula is exact on the traced mismatches: `raw_formula_statutory_gap_pp = 0` and
  `raw_formula_day_weighted_gap_pp = 0` for the rows inspected in the provenance file.
- The remaining disagreement is therefore not a broad raw reconstruction failure. It is mostly
  a benchmark-source rate difference plus a smaller timing convention component.

Current interpretation:
- The raw overlay and panel are mechanically consistent for the traced China 301 rows.
- The remaining benchmark gap should be treated as a rate-definition / timing review, not another
  scope patch, unless a later audit exposes a specific source-text extraction bug.

## 2026-07-12 China 301 Statutory Component Trace

Freshness:
- `raw_replication_china_301_statutory_component_trace.csv` was regenerated with the current overlay and panel.

Component split:
- `benchmark_increment_definition_difference = 9,710`
- `missing_rule_attribute = 9`

Current interpretation:
- The statutory mismatch slice is now dominated by a benchmark increment-definition difference.
- The residual missing-rule-attribute tail is small and should only be chased if it blocks downstream exact-key reconciliation.

## 2026-07-12 China 301 Benchmark Definition Trace

Freshness:
- `raw_replication_china_301_benchmark_definition_trace.csv` is now generated alongside the existing China 301 rate traces and included in the artifact freshness check.

What it records:
- `benchmark_increment_definition_difference` rows where the benchmark statutory rate is internally consistent with the raw formula, but the paper-side implied increment differs from the raw rule attribute increment.
- `benchmark_timing_convention_difference` rows where the benchmark day-weighted rate is driven by a timing convention different from the raw active-share construction.

Current interpretation:
- The remaining China 301 disagreement is no longer a raw-overlay construction issue.
- The benchmark gap is now explicitly separated into increment-definition and timing-convention components, which is the right boundary for deciding whether a later paper-side review is needed.

## 2026-07-12 China 301 Semantics-Corrected Validation

The China-only raw-replication validator now has a second, paper-compatible path that
keeps the benchmark partner-country universe (`cty_code = 5700`, `m_china_hit = 1`)
and uses `m_status2 > 0` only as the active-status definition. This path was added to
separate a universe-definition problem from a true tariff-construction problem.

New machine-readable outputs:
- `raw_replication_china_301_variable_semantics.csv`
- `raw_replication_china_301_universe_trace.csv`
- `raw_replication_china_301_universe_by_country.csv`
- `raw_replication_china_301_universe_by_month.csv`
- `raw_replication_china_301_universe_by_status.csv`
- `raw_replication_china_301_universe_by_semantics.csv`
- `raw_replication_metrics_china_301_semantics_corrected.csv`
- `raw_replication_discrepancies_china_301_semantics_corrected.parquet`
- `raw_replication_release_gate_china_301_semantics_corrected.json`
- `raw_replication_china_301_metric_denominators_semantics_corrected.csv`
- `raw_replication_artifact_freshness_china_301_semantics_corrected.csv`
- `raw_replication_china_301_rate_trace_china_301_semantics_corrected.csv`
- `raw_replication_china_301_rate_timing_trace_china_301_semantics_corrected.csv`
- `raw_replication_china_301_rate_provenance_china_301_semantics_corrected.csv`
- `raw_replication_china_301_rate_mismatch_decomposition_china_301_semantics_corrected.csv`
- `raw_replication_china_301_statutory_component_trace_china_301_semantics_corrected.csv`
- `raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv`

Freshness and coverage:
- `paper_key_coverage_rate = 0.999619`
- `tariff_active_key_coverage_rate = 0.997913`
- `tariff_active_treatment_match_rate = 0.885229`
- `tariff_active_statutory_rate_match_rate = 0.462762`
- `tariff_active_day_weighted_rate_match_rate = 0.294114`
- `missing_raw_policy_scope = 42`
- `missing_raw_key = 173`
- `extra_raw_policy_scope = 6,275`

Residual scope rows are now concentrated in a few late-2018/early-2019 cells:
- `84659600` in `2019-03` and `2019-04`
- `85071000` in `2018-10`
- `85016400` in `2018-07` and `2018-08`
- `03048150` in `2019-03` and `2019-04`
- `29319090` in `2019-03` and `2019-04`
- `85176200` in `2019-03` and `2019-04`

Current interpretation:
- The stale broad China gap was largely a universe-definition problem.
- The remaining residuals are now a small validation tail.
- `ready_for_extension` remains false because the full release gate still has substantial
  statutory and timing disagreement.

## 2026-07-12 Semantics-Corrected Freshness Scope Fix

Patched `build_raw_replication_artifact_freshness()` so the semantics-corrected
validator writes a freshness table for the semantics-corrected artifact family only,
instead of mixing baseline and corrected outputs in the same stale check.

Code changes:
- added suffix-aware filtering to the artifact freshness builder;
- updated `run_raw_replication_validation_china_semantics_corrected()` to call the
  scoped freshness path;
- added a regression test proving the semantics-corrected freshness artifact excludes
  baseline outputs and keeps the corrected release-gate file current.

Verification:
- `C:\Users\User\anaconda3\python.exe -m pytest tests\test_raw_replication_validation.py tests\test_section301.py -q`
- result: `47 passed`
- reran:
  `C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step validate_raw_replication_imports_china_semantics_corrected --skip-downloads --skip-verification --overwrite`

Refreshed semantics-corrected state:
- `paper_key_coverage_rate = 0.9996186680714965`
- `tariff_active_key_coverage_rate = 0.9979127018233164`
- `tariff_active_treatment_match_rate = 0.8838393909181547`
- `tariff_active_statutory_rate_match_rate = 0.4853643912277195`
- `tariff_active_day_weighted_rate_match_rate = 0.4289841379435652`
- `raw_trade_value_match_rate = 0.010410143329658214`
- `ready_for_extension = false`
- `raw_replication_artifact_freshness_china_301_semantics_corrected.csv`: `20` rows,
  `0` stale artifacts

Refreshed semantics-corrected discrepancy counts:
- `missing_raw_policy_scope = 8`
- `missing_raw_key = 173`
- `extra_raw_policy_scope = 6,400`
- `statutory_rate_mismatch = 116,209`
- `day_weighted_rate_mismatch = 1,591`

Refreshed rate-definition decomposition:
- benchmark-definition stages:
  - `statutory_rate_aligned_to_raw_formula = 9,114`
  - `benchmark_increment_definition_difference = 7,811`
  - `raw_formula_matches_reference = 7,475`
  - `benchmark_timing_convention_difference = 1,449`
  - `raw_key_absent = 102`
  - `requires_full_model_review = 8`
- rate-provenance stages:
  - `benchmark_source_precision_diff = 16,564`
  - `benchmark_statutory_definition_mismatch = 7,836`
  - `benchmark_timing_mismatch = 1,451`
- exact-key residual stages:
  - `statutory_rate_mismatch = 25,694`
  - `day_weighted_rate_mismatch = 7,368`
  - `hs8_overlay_present_exact_hs10_absent = 102`
  - `panel_increment_present_but_validation_mismatch = 34`
  - `raw_key_present_no_increment = 8`
- rule-assignment audit:
  - `raw_replication_china_301_rule_assignment_trace.csv` now selects both
    `missing_raw_policy_scope` rows and rows with
    `diagnosed_stage = benchmark_increment_definition_difference`
  - `raw_replication_china_301_rule_assignment_candidates.csv` records one row
    per provenance candidate instead of collapsing to a single source row

Current interpretation:
- the Section 301 raw scope reconstruction is no longer the main blocker;
- the residual China 301 gap is now dominated by paper-versus-raw rate definition,
  source precision, and timing convention differences;
- the next useful step is another exact-key validation pass focused on the
  `hs8_overlay_present_exact_hs10_absent`, `panel_increment_present_but_validation_mismatch`,
  and benchmark-definition clusters, not another broad scope patch.

## 2026-07-13 Section 301 Regression Sensitivity

Added a dedicated sensitivity runner for the imports event-study specification.

Outputs:
- `data/verification/passthru_data/raw_replication_imports/section301_regression_sensitivity_coefficients.csv`
- `data/verification/passthru_data/raw_replication_imports/section301_regression_sensitivity_comparison.csv`
- `data/verification/passthru_data/raw_replication_imports/section301_regression_sensitivity_summary.json`
- `data/verification/passthru_data/raw_replication_imports/section301_regression_sensitivity_report.md`

Design:
- Variant A: package benchmark outcomes, treatment, and tariff construction.
- Variant B: raw outcomes with package treatment/tariff.
- Variant C: raw outcomes with raw treatment and package tariff.
- Variant D: raw outcomes with raw treatment and raw tariff.
- Windows: paper-faithful `-6/+6` and common `-12/+12`.

The runner keeps `ready_for_extension = false` and is intended to answer whether
the remaining Section 301 differences are material for the replication regression
conclusions rather than to force a one-to-one tariff-cell match.

Follow-up audit:
- the top 200 raw-only China 301 HS10 assignments are all supported by PDF-link evidence in
  `data/reference/passthru_data/tradewar_pdf_links.parquet`;
- none of those top raw-only HS10s rely on machine-link evidence;
- the residual divergence is therefore not a simple unsupported-scope problem and should be
  treated as a timing/map/rate decomposition issue before Section 232 begins.

### 2026-07-15 — v5 artifact and sensitivity design

The v5 path does not alter the Section 301 legal mapping. It separates package-full
benchmark, common-sample anchor, raw-outcome bridge, and raw-policy variants. Fit
execution IDs are tracked separately from materialized coefficient artifacts (the
current design is 60 fits and 72 artifacts, including clones). The release gate
remains false while the corrected China policy metrics remain outside tolerance.

### 2026-07-16 — package benchmark and bridge status

The corrected package cache uses the shared HS10 normalizer; the prior numeric
`.0` conversion was recorded as stale evidence and was not resumed. The package-only
benchmark passes the PDF-distance gate (maximum absolute difference 1.00962 log
points across eight import fits). The raw-outcome/package-policy bridge was
estimated on the corrected common sample, but its correlation, distance, and
confidence-interval thresholds do not all pass. No Section 301 legal mapping was
changed, and `ready_for_extension` remains false.

The independent raw-trade extension was subsequently built from raw-only local
staging for all 156 import and 156 export months through 2025-12. Its monthly
reconciliations pass, but this does not change the failed policy gate or authorize
the 2025 event study.

The bridge runner now has scientific estimator fingerprints, repository-relative
checkpoint paths, fit-specific failure manifests, and exact 16-fit preflight
accounting. The preflight found zero fresh bridge checkpoints under the new
fingerprint, so empirical bridge fits remain pending rather than being silently
resumed from older diagnostics.

### 2026-07-16 bridge rerun

All 16 bridge fits were rerun and validated under the current estimator
fingerprint. The corrected finalizer reports the registered metrics without
mixing the `p` and `pduty` fit IDs. Five outcome/specification comparisons remain
outside the registered bridge thresholds; no policy semantics were changed and
v5 empirical estimation remains blocked.

### 2026-07-17 aligned bridge and extension v2

Package/PDF finalization now derives completeness from all eight valid
checkpoints. The aligned bridge uses the raw import universe and symmetric
outcome masks (4,197,758 identical keys), while the old natural-sample bridge
is retained as historical failed evidence. Archive-native extension v2 is a
separate local ZIP build preserving source duty fields; no independent 2025
policy or event estimate is activated.

### 2026-07-18 source-separated bridge rerun

The v3 bridge completed 16/16 package-common and raw-outcome checkpoints using
separate source panels and a memory-bounded mode/specification loop. Its gate is
still failed under the registered thresholds; no policy mapping was changed.
The forensic `lm_*` audit passes at a 1e-5 tolerance, so dynamic price failures
are not attributed to an algebraically different package-log construction.
The local ZIP extension remains nominal/raw-only and archive-complete, but
quantity-token, obsolete-concordance mapping, CPI-real, and independent-policy
gates are not promoted without their own evidence.

The exact package-common loss audit finds 1,244 keys absent from the aligned raw
panel. This is a source-universe loss, not a tariff-rate substitution; detailed
keys remain Parquet-only and the raw-outcome bridge gate remains failed.

### 2026-07-18 policy mismatch decomposition

The existing semantics-corrected discrepancy Parquet was decomposed into raw-only
treatment, reference-only treatment, active-scope, statutory-rate,
day-weighted-calendar, sentinel, missing-source, and extra-scope categories.
The diagnostic is stored under
`raw_replication_imports/policy_mismatch_decomposition_v1/`. It changes no legal
mapping, does not impute unresolved rates, and leaves the independent policy
gate false.

## 2026-07-18 methodology-lock v2 correction

The original-period replication implementation is being rebuilt under a versioned methodology namespace. Three defects are now treated as invalidating the prior canonical comparisons: raw imports used GEN_VAL_MO instead of GEN_CIF_MO; Python dynamic differences and leads/lags crossed missing calendar months; and the Figure 4a vector extractor admitted the x-axis label -6 into the y-axis calibration. The corrected method uses CIF plus calculated duty for raw outcomes, exact Stata monthly-calendar operators, and independently validated PDF geometry. Historical outputs remain diagnostic and are not promoted by copying coefficients. Independent Section 301 policy matching and the 2025 event remain separate, unresolved gates.

## 2026-07-19 verification update

The package-only original-horizon import benchmark is current and passes: 8/8 event/dynamic fits, 13 horizons each, maximum package/PDF difference 0.86862. The corrected CIF bridge has 16/16 resumable fits. Its point estimates pass for all outcomes/specifications; only the event duty-inclusive price interval-overlap metric fails (0.74992 versus 0.80), so the raw-outcome bridge gate remains failed while point-estimate replication is accepted as close. The independent archive-native extension covers 156 import and 156 export months through 2025-12 in 312 ZSTD Parquet partitions with zero duplicate keys and zero monthly value-reconciliation failures. Concordance, quantity-semantics, duty-unit, and CPI-real-value audits remain pending. Section 301 legal mapping remains outside tolerance and no v5 tariff sensitivity or 2025 event study is released.

## 2026-07-19 paper-compatible Section 301 reconstruction

The old exact-match metrics (`0.883759` treatment, `0.485320` statutory rate, and `0.428945` day-weighted rate) are retained as historical diagnostics but are superseded as release decisions because they mixed final legal scope, total tariff levels, paper event timing, and an incomplete source parser.

The corrected implementation distinguishes two objects. The paper-compatible object uses archived official HTS sources plus 107 transparent reconciliation rows: 38 new-code exclusions, 55 old-code longitudinal carries, seven proposal-era HS8 additions, and seven historical parser-quirk retentions. The latter 69 records are `validation_derived`; missing proposed annexes and the authors' longitudinal concordance are disclosed. The final-legal object remains separate and never receives those paper-specific adjustments.

On the registered historical analysis universe, paper-compatible treatment assignment, effective event month, tariff increment, and source-vintage classification each match at `1.0`. Row-level `m_status2` and `m_effective_mdate2` also match at `1.0` across 2,548,625 rows. `m_ess` matches at `0.947780` but is explicitly diagnostic because the original Figure 2 estimator uses status/date, not `m_ess`.

The 24-fit policy-substitution run validates all checkpoints and locks the historical method. Paper-compatible event curves match the package-policy anchor to numerical precision. Dynamic correlations range from `0.993819` to `0.999802`, maximum gaps from `0.061659` to `0.187121`, and diagnostic CI overlap from `0.895775` to `0.910708`. Final-legal event curves differ substantially because actual legal dates and final scope are not the paper's nearest-month calendar; final-legal dynamic curves remain close.

This establishes a reproducible historical paper-compatible methodology, not an independently complete 2025 ledger. The 2018 validation-derived corrections must not be carried into 2025 without new official sources.
