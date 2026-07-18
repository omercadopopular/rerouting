# Replication coverage and readiness

Generated from canonical manifests on 2026-07-18.

| Track | Status | Evidence |
|---|---|---|
| package_import_pdf | `passed` | 8 fits and frozen PDF comparison are complete |
| package_common_sample | `complete` | 4197758 aligned import rows |
| raw_outcome_point_estimate | `passed` | v4 realized-duty bridge passes all point-estimate comparisons; inference failures remain |
| raw_outcome_inference | `failed` | event/value and event/pre-duty CI overlap narrowly fail; dynamic/pre-duty Pearson and CI fail |
| raw_trade_archive_ingestion | `pending` | staging projection is complete; independent ZIP-native reparse remains pending |
| raw_trade_staging_reconciliation | `passed` | 312/312 monthly comparisons against archive-native staging |
| raw_trade_quantity_semantics | `pending` | source fixed-width quantity token audit |
| raw_trade_duty_preservation | `pending` | v4 preserves dut_val_mo and cal_dut_mo; cross-vintage ZIP audit remains |
| raw_trade_concordance | `pending` | pending_obsolete_mapping_parse |
| raw_trade_outcome_extension | `pending` | 156 nominal outcome partitions built; ZIP/concordance/CPI gates remain |
| raw_trade_real_values | `pending` | nominal extension is canonical; CPI real-value build not run |
| independent_policy | `failed` | 2025 ledger sources remain incomplete |

Package import replication covers Figures 2 and 4a only. Figure 4b exports and other tables remain outside the package gate.

Section 301 v5 and the 2025 event remain blocked until the raw-outcome inference and independent-policy gates pass.
