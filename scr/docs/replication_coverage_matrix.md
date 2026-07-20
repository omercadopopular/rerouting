# Replication coverage and readiness

Generated from canonical manifests on 2026-07-20.

| Track | Status | Evidence |
|---|---|---|
| package_import_pdf | `passed` | 8 fits and frozen PDF comparison are complete |
| package_common_sample | `complete` | 4197758 aligned import rows |
| raw_outcome_point_estimates | `passed` | correlation, RMSE, maximum gap, and sign agreement |
| raw_outcome_inference_diagnostic | `failed` | event duty-inclusive-price CI overlap remains below 0.80 |
| raw_trade_archive_ingestion | `passed` | 312 archive flow-months |
| raw_trade_staging_reconciliation | `passed` | 312 archive-native partitions; 0 failures |
| raw_trade_quantity_semantics | `passed` | source fixed-width quantity token audit |
| raw_trade_duty_preservation | `pending` | duty fields are present; units and source semantics require review |
| raw_trade_concordance | `pending` | pending_obsolete_mapping_parse |
| raw_trade_real_values | `not_required_for_replication` | nominal values are canonical; local CPI data are preserved for future analysis |
| historical_section301_policy | `passed` | paper-compatible Section 301 source-vintage assignment and substitution curves |
| historical_pooled_201_232_301_policy | `failed` | independent pooled family scope/rate/timing comparison; not promoted while rate or calendar diagnostics fail |
| pooled_policy_v2_preflight | `blocked_missing_data` | fail-closed source-qualified reconstruction; solar, washer, and aluminum quota/product semantics remain unresolved |
| independent_section301_legal_variable | `passed` | Section 301 scope/date/increment diagnostic; total-rate legacy metrics are superseded and legal-calendar curves are not expected to match paper timing |
| forward_2025_policy_ledger | `failed` | 2025 official ledger sources remain incomplete |

Package import replication covers Figures 2 and 4a only. Figure 4b exports and other tables remain outside the package gate.

The Section 301 paper-compatible policy diagnostic is distinct from the independent pooled 201/232/301 policy gate. The pooled gate remains failed until independently sourced family rates and calendars reproduce the package policy fields on the paper sample. CPI inputs remain in place for future work but are not required for the original nominal replication.
