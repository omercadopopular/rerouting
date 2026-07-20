# Master Trade-Rerouting Pipeline

> Canonical long-horizon RTP documentation: [`rtp_long_horizon_spec.md`](rtp_long_horizon_spec.md).
> It is the machine-readable source for the 2018 replication, public tariff ledger, and 2025 comparison event.

## Objective and Estimands

This pipeline implements a production empirical path that:

1. Builds trade flows from raw Census files (extended panel through latest available months).
2. Builds a raw-source China Section 301 policy layer for current-window import identification.
3. Constructs import analysis panels at HS10 and HS6 levels.
4. Replicates baseline RTP-style regressions.
5. Merges rerouted-share controls and re-estimates controlled regressions.

Primary estimands:

- Baseline tariff-response event-study coefficients and dynamic lead-lag coefficients.
- Controlled extensions with rerouting interactions:
  - `treated x reroute_share_t`
  - `treated x reroute_share_init_2017`

The rerouting controls remain a legacy extension and are not part of the current Section 301 build.

## Current U.S. Import Extension

The current-data workflow uses the raw bilateral HTS policy panel and never carries package shock
values beyond the published paper window. `build_section301_import_panel` writes a China-only HS10
panel with raw statutory rates, Section 301 increments, effective-month shares, treatment status, and
provenance. It also writes `section301_action_registry.parquet` and a coverage report identifying policy
actions whose effective month has no observed raw policy scope.

Run the archived raw-policy steps first, then:

1. `python scr/pipeline_passthru_data.py --only-step build_section301_import_panel --enable-archived-policy-pipeline --skip-downloads --skip-verification --overwrite`
2. `python scr/pipeline_passthru_data.py --only-step build_trade_workhorse_panels --trade-flow imports --analysis-window current --skip-downloads --skip-verification --overwrite`
3. `python scr/pipeline_passthru_data.py --only-step run_trade_regressions --trade-flow imports --analysis-window current --skip-downloads --skip-verification --overwrite`

Current-window artifacts are written below `trade_regressions/workhorse/current`,
`trade_regressions/tables/current`, and `trade_regressions/charts/current`; benchmark artifacts are left
unchanged. The Census importer continues to discover available monthly files and records the resolved
source window in its inventory and staging metadata.

## Data Map and Lineage

- Raw trade flow panel:
  - `data/analysis/passthru_data/m_flow_hs10_fm_new.parquet`
- Fajgelbaum package shock source:
  - `data/fajgelbaum/data/analysis/m_flow_hs10_fm_new.dta`
- Rerouted shares source:
  - `data/rerouted_shares/data_share_rerouted.dta`

Constructed outputs:

- HS10 imports + package shocks:
  - `data/analysis/passthru_data/imports_hs10_raw_package_shocks.parquet`
- HS6 imports + package shocks:
  - `data/analysis/passthru_data/imports_hs6_raw_package_shocks.parquet`
- HS6 imports + rerouting controls:
  - `data/analysis/passthru_data/imports_hs6_raw_package_shocks_rerouting.parquet`

## Equation Mapping (Model to Empirics)

Reference manuscript: `main.pdf`.

- Eq. (6) in manuscript (rerouting response to tariffs):
  - Implemented analog: `dl_reroute ~ dl_tarf | hs6 + time`.
  - Output: `rerouting_outcome_hs6_diff_regression.csv`.

- Eq. (7) in manuscript (pass-through with rerouting interaction):
  - Implemented analog: RTP import response models augmented with rerouting-treatment interactions.
  - Controlled RHS adds:
    - `reroute_treated_t`
    - `reroute_treated_init`
  - FE structure remains RTP-style within implemented panel definitions.

- Eq. (8) in manuscript (welfare sufficient-statistics decomposition):
  - Treated as an accounting extension; not part of causal core regressions in this production path.

## Regression Specs Run Here

Baseline and controlled regressions are estimated for imports:

- Event-study (`val` outcome):
  - Baseline: RTP event terms + RTP controls with FE.
  - Controlled: baseline + rerouting interaction controls.

- Dynamic lead-lag (`val` outcome):
  - Baseline: RTP dynamic tariff-change terms with FE.
  - Controlled: baseline + rerouting interaction controls.

Outputs:

- `data/analysis/passthru_data/trade_regressions/rerouting_extension/imports_event_val_baseline_vs_rerouting_controls.csv`
- `data/analysis/passthru_data/trade_regressions/rerouting_extension/imports_dynamic_val_baseline_vs_rerouting_controls.csv`

## Archived Subpipeline (Raw Tariff Reconstruction)

The raw HTS machine/PDF tariff reconstruction branch is archived from the default production run.

- Archived steps:
  - `download_policy_sources`
  - `download_policy_updates`
  - `build_hts_monthly_schedule`
  - `build_us_products_partner_hs10_panel`

- These run only when `--enable-archived-policy-pipeline` is explicitly set.

## Runbook

1. Build trade panels:
   - `python scr/pipeline_passthru_data.py --only-step build_trade_panels --skip-downloads --skip-verification --overwrite`
2. Build imports with package shocks:
   - `python scr/pipeline_passthru_data.py --only-step build_imports_with_package_shocks --skip-downloads --skip-verification --overwrite`
3. Merge rerouting controls:
   - `python scr/pipeline_passthru_data.py --only-step build_rerouting_controls --skip-downloads --skip-verification --overwrite`
4. Run rerouting-extended regressions:
   - `python scr/pipeline_passthru_data.py --only-step run_rerouting_regressions --skip-downloads --skip-verification --overwrite`

### 2026-07-16 replication extension status

The package-only import benchmark is current and passes its PDF gate. The raw
trade extension is independently materialized through 2025-12 for both flows;
the raw-outcome bridge and independent tariff-policy gates remain separate and
are not cleared by the extension build.

The current raw-trade extension is a staging projection with a stratified
archive-level smoke audit. Full ZIP validation, concordance-vintage validation,
and duty-inclusive extension coverage remain separate prerequisites for a recent
tariff/event analysis.

The package/common raw-outcome bridge has completed all 16 resumable fits but
fails its registered CI, correlation, or maximum-distance checks for five
specification/outcome pairs. This keeps Section 301 v5 and the 2025 event study
blocked even while archive-level extension validation proceeds independently.

### 2026-07-18 current gates

The package-only import benchmark is current and passes the frozen PDF gate.
The independent extension covers every local import/export month through
2025-12 and reconciles to raw-only staging; it contains no package policy
variables. The aligned bridge is complete but fails three registered outcome
comparisons. Section 301 v5 and the 2025 event remain explicitly blocked until
the raw-outcome and independent-policy gates pass.

The corrected v4 rerun reduces the remaining failures to event/value confidence
interval overlap, event/pre-duty-price confidence interval overlap, and
dynamic/pre-duty-price Pearson correlation plus interval overlap. The realized
duty formula now passes both duty-price comparisons. These remain bridge-gate
failures, not reasons to alter policy semantics.

## 2026-07-18 methodology-lock v2 correction

The original-period replication implementation is being rebuilt under a versioned methodology namespace. Three defects are now treated as invalidating the prior canonical comparisons: raw imports used GEN_VAL_MO instead of GEN_CIF_MO; Python dynamic differences and leads/lags crossed missing calendar months; and the Figure 4a vector extractor admitted the x-axis label -6 into the y-axis calibration. The corrected method uses CIF plus calculated duty for raw outcomes, exact Stata monthly-calendar operators, and independently validated PDF geometry. Historical outputs remain diagnostic and are not promoted by copying coefficients. Independent Section 301 policy matching and the 2025 event remain separate, unresolved gates.

## 2026-07-19 verification update

The package-only original-horizon import benchmark is current and passes: 8/8 event/dynamic fits, 13 horizons each, maximum package/PDF difference 0.86862. The corrected CIF bridge has 16/16 resumable fits. Its point estimates pass for all outcomes/specifications; only the event duty-inclusive price interval-overlap metric fails (0.74992 versus 0.80), so the raw-outcome bridge gate remains failed while point-estimate replication is accepted as close. The independent archive-native extension covers 156 import and 156 export months through 2025-12 in 312 ZSTD Parquet partitions with zero duplicate keys and zero monthly value-reconciliation failures. Concordance, quantity-semantics, duty-unit, and CPI-real-value audits remain pending. Section 301 legal mapping remains outside tolerance and no v5 tariff sensitivity or 2025 event study is released.

## 2026-07-19 locked historical import methodology

The historical import replication now has three completed layers: the package estimator versus Figures 2/4a, raw Census outcomes versus package-policy common-sample estimates, and reconstructed paper-compatible Section 301 policy versus the package-policy anchor. The paper-compatible substitution grid passes all eight point-estimate comparisons; event curves are numerically identical and dynamic maximum gaps remain below 0.188 log points.

This lock is deliberately narrower than a claim that final legal dates reproduce the paper. The independent final-legal schedule is a separate diagnostic and the versioned 2025 legal ledger remains a prerequisite for any new-administration event study. The next phase may reuse the estimator and raw trade schema, but must construct and verify a new official 2025 product/date/rate/exclusion/stacking ledger before estimation.

## 2026-07-20 pooled policy reconstruction boundary

The source-only pooled layer now attempts all five historical policy families
without importing package policy values. It keeps family increments, legal
day-weighting, and paper-compatible months distinct. The local source ledger is
incomplete for solar and the principal 232 scopes, so the resulting panel is a
diagnostic `built_partial` artifact. It is not a replacement for the locked
package-policy replication and does not authorize Section 301 v5 or the 2025
event study.

## 2026-07-20 pooled family correction

The pooled source layer now uses exact Chapter-99 family codes: `99034501/02/06`
are washers and `99034522/25` are solar. A local byte-level parser expands the
principal 232 scopes from archived HTS notes 16 and 19 and recovers the note 17
washer-parts scope. Solar, washer, and aluminum source scopes are complete;
steel's qualifying-contract `99038061` and China `99038809/15/16` remain
unresolved. The pooled panel is still diagnostic and independent policy release
remains blocked. The package-only historical estimator and raw-trade extension
gates are unaffected by this independent-policy status.
