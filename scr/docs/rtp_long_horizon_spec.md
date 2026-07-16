---
document_id: rtp_long_horizon_spec
document_version: 1
status: active_development
research_design: long_horizon_us_import_tariff_response
outcome_source: us_census_monthly_import_detail
outcome_cutoff_rule: latest_month_discovered_from_census_bulk_archive
current_cached_outcome_cutoff: 2025-12
benchmark_source: fajgelbaum_goldberg_kennedy_khandelwal_2019_replication_package
primary_unit: country_of_origin_hs10_month
policy_ledger_status: blocked_pending_public_source_reconciliation
output_root: data/analysis/passthru_data/trade_regressions/rtp_long_horizon
---

# Long-Horizon RTP Import Pipeline

## Research Objective

Replicate the import event-study design in Fajgelbaum, Goldberg, Kennedy, and
Khandelwal (2019), retain its original 2018 treatment definition, and extend the
response horizon using current U.S. Census import outcomes. A separate public-source
tariff ledger is required before post-2019 duty-inclusive outcomes or new-policy event
studies are released.

## Pipeline Registry

| step_id | executable | inputs | outputs | release_gate |
| --- | --- | --- | --- | --- |
| treatment_crosswalk | `build_rtp_long_horizon_panel` | Fajgelbaum import panel | `2018_treatment_crosswalk.parquet` | package key uniqueness |
| raw_replication_validation | `validate_raw_replication_imports` | raw bilateral policy panel, package import panel | cell discrepancies and release gate | exact original treatment/timing/rate reconciliation |
| frozen_panel | `build_rtp_long_horizon_panel` | crosswalk, Census import panel | frozen long-horizon panel | Census key uniqueness |
| original_event | `run_rtp_long_horizon_2018_event` | frozen panel | monthly coefficients | original-window benchmark |
| tariff_ledger | future policy builder | official HTS, USTR, White House, Federal Register | `public_tariff_ledger_hs10_monthly.parquet` | scope/timing reconciliation |
| duty_outcomes | future policy builder | frozen panel, tariff ledger | duty-inclusive outcomes | current complete ledger |
| 2025_event | future policy builder | validated ledger, Census outcomes | Feb-2025 event study | 2025 scope/stacking validation |

## 2018 Benchmark Design

### Treatment Definition

The original treatment crosswalk is extracted from the package, keyed by
`cty_code`, `hs10`. It records `m_ess`, the first `m_effective_mdate2`, and each
original wave flag. It is used only to reproduce the published 2018 experiment; it is
not treated as a post-2019 tariff source.

| policy family | included | rationale | treatment source |
| --- | --- | --- | --- |
| Section 201 solar safeguards | yes | original pooled 2018 import treatment | package crosswalk |
| Section 201 washers safeguards | yes | original pooled 2018 import treatment | package crosswalk |
| Section 232 steel and aluminum | yes | original pooled 2018 import treatment | package crosswalk |
| China Section 301 Lists 1-4A | yes | original pooled 2018 import treatment | package crosswalk |

### Estimators

| estimator_id | sample | event window | outcomes | fixed effects | clusters |
| --- | --- | --- | --- | --- | --- |
| original_monthly | frozen package universe | `-12` through latest month | value, quantity, unit value | country-HS10, country-month, HS10-month | HS8, country |
| original_comparison | frozen package universe | `-12/+12` | value, quantity, unit value, duty-inclusive after ledger validation | country-HS10, HS10-month | HS8, country |
| expanded_robustness | all current eligible pairs | same as corresponding primary estimator | same | documented separately | HS8, country |

The frozen universe is primary because it prevents post-2018 product-code entry and
exit from changing the paper's treatment composition. The expanded universe is a
separate robustness result, never a replacement.

## Public Tariff Ledger

## Raw Reconstruction Validation

The raw reconstruction is a required, independent validation leg. It rebuilds the
2017-2019 import-policy panel from public sources and compares it to the package on
the country-HS10-month key. Package fields are read only as a golden reference and
never used to populate the raw panel.

The validator writes:

| artifact | purpose |
| --- | --- |
| `raw_replication_metrics.csv` | key, treatment, statutory-rate, and day-weighted-rate match counts |
| `raw_replication_discrepancies.parquet` | all nonmatching cells with a deterministic discrepancy type |
| `raw_replication_release_gate.json` | explicit `ready_for_extension` decision |
| `raw_replication_by_type.csv` | discrepancy counts by class |
| `raw_replication_by_family.csv` | active policy discrepancies by original 2018 tariff family |
| `raw_replication_by_year_month.csv` | discrepancy timing diagnostics |
| `raw_replication_by_country.csv` | top active policy discrepancies by country |
| `raw_replication_by_hs2.csv` | top active policy discrepancies by HS2 |
| `raw_replication_by_rate_bucket.csv` | active policy discrepancies by statutory-rate gap bucket |
| `raw_replication_china_301_top_hs8.csv` | China 301 missing-scope rows grouped by HS8 |
| `raw_replication_china_301_top_month.csv` | China 301 missing-scope rows grouped by month |
| `raw_replication_china_301_top_hs8_month_wave.csv` | China 301 missing-scope rows grouped by HS8, month, and paper-wave proxy |
| `raw_replication_china_301_source_audit.csv` | comparison of top China 301 missing buckets against source layers and final overlay |
| `raw_replication_china_301_trace.csv` | same China 301 trace with explicit stage classification across raw links, overlay, and final panel |
| `raw_replication_china_301_source_audit_errors.json` | source-layer load status for the China 301 audit |
| `raw_replication_china_301_validation_universe.csv` | benchmark-universe decomposition for the China-current validation path |
| `raw_replication_china_301_validation_decomposition.csv` | reference-side decomposition showing China-partner versus non-China-partner China-policy rows |
| `raw_replication_china_301_universe_audit.csv` | China-current universe audit for the full reference population |
| `raw_replication_china_301_metric_denominators.csv` | denominator breakdown used for the China-current gate |
| `raw_replication_china_301_raw_only_keys.csv` | raw-only China keys after the universe fix |
| `raw_replication_china_301_residual_current.csv` | exact-key residual mismatches after the universe fix |
| `raw_replication_china_301_rate_difference_quantiles.csv` | statutory and day-weighted gap quantiles in percentage points |
| `raw_replication_china_301_rate_provenance.csv` | row-level provenance for remaining statutory/day-weighted rate gaps |
| `raw_replication_china_301_rate_mismatch_decomposition.csv` | grouped decomposition of the remaining China rate gaps |
| `raw_replication_china_301_statutory_component_trace.csv` | component-level decomposition of remaining China statutory mismatches |
| `raw_replication_china_301_statutory_component_summary.csv` | grouped summary of the statutory component trace |
| `raw_replication_china_301_statutory_component_top_clusters.csv` | top statutory mismatch clusters for quick review |
| `raw_replication_china_301_benchmark_definition_trace.csv` | benchmark-vs-raw statutory and day-weighted definition audit |
| `raw_replication_china_301_benchmark_definition_by_rule.csv` | benchmark-definition summary by rule |
| `raw_replication_china_301_benchmark_definition_by_month.csv` | benchmark-definition summary by month |
| `raw_replication_china_301_benchmark_definition_by_stage.csv` | benchmark-definition summary by diagnosed stage |
| `raw_replication_china_301_benchmark_definition_quantiles.csv` | percentage-point quantiles for benchmark-definition gaps |
| `raw_replication_by_family_country_targeted.csv` | China-current active-policy discrepancy summary after partner-country filtering |
| `raw_replication_source_health.csv` | machine-readable health check for each expected raw tariff source artifact |
| `raw_replication_source_health.json` | artifact-level source-availability summary used to block the China 301 audit when hydrated files are unavailable |
| `raw_replication_china_301_variable_semantics.csv` | paper-side variable semantics map used by the corrected China-only validator |
| `raw_replication_china_301_universe_trace.csv` | full benchmark-universe decomposition for the corrected China-only validator |
| `raw_replication_china_301_universe_by_country.csv` | corrected-universe counts by country |
| `raw_replication_china_301_universe_by_month.csv` | corrected-universe counts by month |
| `raw_replication_china_301_universe_by_status.csv` | corrected-universe counts by benchmark status |
| `raw_replication_china_301_universe_by_semantics.csv` | corrected-universe counts by paper-side semantics class |
| `raw_replication_metrics_china_301_semantics_corrected.csv` | corrected China-only match metrics on the paper-compatible universe |
| `raw_replication_discrepancies_china_301_semantics_corrected.parquet` | corrected China-only discrepancy rows |
| `raw_replication_release_gate_china_301_semantics_corrected.json` | corrected China-only release gate output |
| `raw_replication_china_301_metric_denominators_semantics_corrected.csv` | denominator breakdown for the corrected China-only gate |
| `raw_replication_artifact_freshness_china_301_semantics_corrected.csv` | freshness check for the corrected China-only validation outputs |
| `raw_replication_china_301_rate_trace_china_301_semantics_corrected.csv` | corrected China 301 exact-key rate trace |
| `raw_replication_china_301_rate_timing_trace_china_301_semantics_corrected.csv` | corrected China 301 rate/timing trace |
| `raw_replication_china_301_rate_provenance_china_301_semantics_corrected.csv` | corrected China 301 rate provenance trace |
| `raw_replication_china_301_rate_mismatch_decomposition_china_301_semantics_corrected.csv` | corrected China 301 rate mismatch decomposition |
| `raw_replication_china_301_statutory_component_trace_china_301_semantics_corrected.csv` | corrected China 301 statutory-component trace |
| `raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv` | corrected China 301 benchmark-definition trace |

Discrepancies are classified as missing raw/reference keys, missing raw policy scope,
extra raw policy scope, non-ad-valorem/sentinel duty, statutory-rate mismatch,
day-weighted-rate mismatch, or trade-value mismatch. Active tariff status is defined
by the paper's time-varying tariff status field (`m_status2 > 0`), not by event-study
status (`m_ess`). The public 2020-2025 extension remains blocked until original
scope/timing and both tariff fields match exactly in the estimation sample or residual
rows are documented as non-ad-valorem or source-ambiguous.

### Current Raw-Replication Gate

Latest local run: `2026-07-12`, via `validate_raw_replication_imports_china_301_semantics_corrected`.

| gate field | value | interpretation |
| --- | ---: | --- |
| `paper_key_coverage_rate` | `0.999619` | paper-compatible China universe is almost fully covered |
| `tariff_active_key_coverage_rate` | `0.997913` | active China keys are almost fully covered |
| `tariff_active_treatment_match_rate` | `0.883839` | most active treatment flags now align |
| `tariff_active_statutory_rate_match_rate` | `0.485364` | statutory rate agreement is still partial |
| `tariff_active_day_weighted_rate_match_rate` | `0.428984` | day-weighted rate agreement improved but still trails |
| `raw_trade_value_match_rate` | `0.010410` | exact trade values are still largely mismatched |

The corrected China-only validation path explicitly keeps the benchmark partner-country
filter (`cty_code = 5700` and `m_china_hit = 1`) and uses `m_status2 > 0` only as the
active-status definition. That removes the earlier universe inflation from China-policy
rows on non-China partners. On the refreshed run, `missing_raw_policy_scope` falls to
`8` rows, `missing_raw_key` is `173`, and the remaining exact-key residuals are no
longer dominated by broad Section 301 scope loss.

The refreshed exact-key trace now shows:
- `399` China 301 `hs8 x month` buckets as `present_with_increment`
- `0` buckets `lost_before_overlay`
- `1` bucket `lost_after_overlay`, which is `22042150` in `2019-01` and is now
  diagnosed as `no_raw_trade_key` rather than an overlay failure

The remaining China 301 gap is therefore mostly a rate-definition problem, not a scope
construction problem. The corrected benchmark-definition trace splits the active China
rate residuals into:
- `9,114` rows: `statutory_rate_aligned_to_raw_formula`
- `7,811` rows: `benchmark_increment_definition_difference`
- `7,475` rows: `raw_formula_matches_reference`
- `1,449` rows: `benchmark_timing_convention_difference`
- `102` rows: `raw_key_absent`

The rule-assignment audit now retains both the raw-scope rows and the
benchmark increment-definition disagreements, and it writes a separate
candidate-provenance file so source context remains auditable.

That is the current release blocker for any long-horizon duty-inclusive extension.

The rate-provenance, statutory-component, and benchmark-definition traces remain the
next diagnostic layers. They are still useful for separating source-precision,
statutory-definition, and timing-convention differences, but they should no longer be
read as evidence that the raw 301 scope reconstruction is broadly broken.

### Required Schema

`public_tariff_ledger_hs10_monthly.parquet` must have one row per
`cty_code x hs10 x year x month` and at least:

| field | definition |
| --- | --- |
| `applicable_total_ad_valorem_duty` | legally applicable stacked ad-valorem duty |
| `is_non_ad_valorem` | specific or compound duty cannot enter price outcome |
| `is_unresolved` | scope, date, or stacking cannot be verified |
| `source_url` | first-party legal or schedule source |
| `policy_panel_version` | immutable ledger release identifier |
| component columns | baseline duty and each applicable Section 201, 232, 301, and IEEPA component |

### Included Tariff Families

| policy family | included | rationale | required treatment |
| --- | --- | --- | --- |
| Section 201 safeguards | yes | preserves original paper policy environment | rates, quota thresholds, expiry/extensions |
| Section 232 metals and derivatives | yes | original treatment and later exemptions, derivative scope, and 2025 rate changes | country scope, exclusions, quota/TRQ status, day weighting |
| China Section 301 | yes | core China trade-war policy lifecycle | Lists 1-4A, rate changes, exclusions/restorations, four-year review |
| China/Hong Kong IEEPA | yes | broad 2025 China duty shock | effective dates, rate modifications, Chapter 99 exclusions |
| Section 232 autos, parts, and other active China-covered actions | yes | material China duties active by the outcome cutoff | HTS annex scope and non-cumulation rules |

### Recorded But Not Added To HS10 Tariff Rates

| policy | treatment | rationale |
| --- | --- | --- |
| de minimis changes | coverage flag | entry-level treatment is not a standard HS10 ad-valorem duty |
| UFLPA enforcement | restriction flag | non-tariff import restriction |
| antidumping/countervailing duties | exclusion flag | order/importer-specific and excluded from RTP construction |
| quotas and importer offsets | flag or separate quantity rule | not a universal ad-valorem rate |
| vessel fees | excluded | not an imported-product tariff |

### Source And Validation Rules

1. Use official USITC HTS releases/revisions for tariff-line and Chapter 99 implementation.
2. Use USTR, White House proclamations, and Federal Register notices for authority, dates, and annexes.
3. Store legal source URLs and raw-file checksums with every parsed action.
4. Validate against the Fajgelbaum 2017-2019 panel by country-HS10-month. Report scope, timing, rate, exemption, and non-ad-valorem discrepancies separately.
5. Do not release tariff-based estimates unless the ledger reaches the Census cutoff and has no unresolved cells in the estimation sample.

## Duty-Inclusive Outcomes

For valid observations only:

```text
duty_inclusive_unit_value = (import_value / quantity) * (1 + applicable_total_ad_valorem_duty)
```

Observations with non-ad-valorem duties, unresolved scope, missing quantities, or unknown
stacking remain missing and are reported in event-month coverage diagnostics. They are never
assigned a zero tariff.

## 2025 Comparison Event

The new event is the first China IEEPA duty effective in **February 2025**. The comparison
window is monthly `-12/+12`, starting in January 2024. The available Census panel ends in
December 2025, so horizons after `+10` are marked unavailable rather than imputed or binned.

The February 2025 duty is broad across China, so country-month fixed effects would absorb its
variation. The comparison estimator therefore uses country-HS10 and HS10-month fixed effects,
with non-China observations of the same HS10 as controls. The original 2018 event is re-run
under this same paired-comparison specification, while its paper-faithful estimate remains
separately reported.

## Commands

```powershell
# Build the frozen package-treatment/Census-outcome panel.
C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step build_rtp_long_horizon_panel --skip-downloads --skip-verification --overwrite

# Validate the raw 2017-2019 reconstruction before extending public tariffs.
C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step validate_raw_replication_imports --skip-downloads --skip-verification --overwrite

# Build the China 301 trace directly from saved artifacts.
C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step build_china_301_trace --skip-downloads --skip-verification --overwrite

# Estimate all available monthly original-event horizons.
C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step run_rtp_long_horizon_2018_event --skip-downloads --skip-verification --overwrite

# Available only after public_tariff_ledger_hs10_monthly.parquet passes its validation gate.
C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step build_rtp_2025_ieepa_event_panel --skip-downloads --skip-verification --overwrite
```

## Section 301 Sensitivity Addendum

The Section 301 regression-relevance check now runs as a separate sensitivity step:

```powershell
C:\Users\User\anaconda3\python.exe scr\pipeline_passthru_data.py --only-step run_section301_regression_sensitivity --skip-downloads --skip-verification --overwrite
```

It compares four source combinations:
- A: package benchmark;
- B: raw outcomes with package treatment/tariff;
- C: raw outcomes with raw treatment and package tariff;
- D: raw outcomes with raw treatment and raw tariff.

The comparison is reported for both the paper-faithful `-6/+6` window and a
common `-12/+12` window. The output is diagnostic only and does not change the
release gate.

The current residual audit shows that the top raw-only China 301 HS10 products are
present in the PDF link table, so the remaining disagreement is not just missing
scope extraction. Treat subsequent Section 301 work as a map/timing/rate
decomposition problem, not another broad tariff-scope pass.

## Release Checklist

- [ ] Census source inventory records the resolved latest month.
- [ ] Package treatment crosswalk has unique country-HS10 keys.
- [ ] Original paper-window coefficients pass the benchmark comparison.
- [ ] Public tariff ledger reaches the Census cutoff.
- [ ] Section 201/232/301/IEEPA scopes, exemptions, and stacking pass reconciliation.
- [ ] Duty-inclusive coverage is reported by event month.
- [ ] February-2025 `-12/+12` results label unavailable right-tail horizons.

### 2026-07-15 v5 status

Before extending the horizon, the package-full import benchmark must be stable and
the v5 Section 301 artifact grid must finalize without a `current_fit.json` marker.
The package/PDF gate is 1.10 log points; the raw-outcome bridge targets correlation
0.95, RMSE 1.25, maximum difference 2.50, and confidence-interval overlap 0.80.
These are diagnostics only: the policy reconstruction gate remains unchanged and
no 2025 ledger or event-study work is activated by default.

### 2026-07-16 extension status

The raw-only extension now covers all 312 locally available flow-months through
2025-12 in ZSTD Parquet, with zero monthly reconciliation failures. It contains
no package tariff fields; legal-policy and event-study gates remain false.

Archive-native extension v2 is currently limited to a December 2025 smoke build;
the staging-projection extension remains the broader 312-partition artifact until
the complete ZIP audit and concordance-vintage checks pass.

The 16-fit raw-outcome bridge has now completed with reproducible checkpoints,
but its registered outcome gate remains failed. The archive-native validator is
still running across the full local ZIP set; the 312-partition staging projection
must not be described as archive-validated until that scan and the concordance
audit are complete.
