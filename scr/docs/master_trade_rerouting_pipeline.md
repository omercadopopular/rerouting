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
