# Replication coverage and readiness

Generated from canonical manifests on 2026-07-19.

| Track | Status | Evidence |
|---|---|---|
| package_import_pdf | `passed` | 8 fits and frozen PDF comparison are complete |
| package_common_sample | `complete` | 4197758 aligned import rows |
| raw_outcome_bridge | `failed` | registered thresholds are unchanged |
| raw_trade_archive_ingestion | `passed` | 312 archives |
| raw_trade_staging_reconciliation | `passed` | 312 archive-native partitions; 0 failures |
| raw_trade_quantity_semantics | `pending` | source fixed-width quantity token audit |
| raw_trade_duty_preservation | `pending` | duty fields are present; units and source semantics require review |
| raw_trade_concordance | `pending` | native audit absent |
| raw_trade_real_values | `pending` | nominal extension is canonical; CPI real-value build not run |
| independent_policy | `failed` | 2025 ledger sources remain incomplete |

Package import replication covers Figures 2 and 4a only. Figure 4b exports and other tables remain outside the package gate.

Section 301 v5 and the 2025 event remain blocked until the raw-outcome and independent-policy gates pass.
