# Replication coverage and readiness

Generated from canonical manifests on 2026-07-18.

| Track | Status | Evidence |
|---|---|---|
| package_import_pdf | `passed` | 8 fits and frozen PDF comparison are complete |
| package_common_sample | `complete` | 4197758 aligned import rows |
| raw_outcome_bridge | `failed` | realized-duty correction fixes the duty-price point miss; event CI and other price/value gates remain failed |
| raw_trade_archive_ingestion | `passed` | 312 archives |
| raw_trade_staging_reconciliation | `passed` | 312/312 monthly comparisons |
| raw_trade_quantity_semantics | `pending` | source fixed-width quantity token audit |
| raw_trade_duty_preservation | `pending` | 2018 layout confirms dutiable-value versus calculated-duty semantics; cross-vintage audit remains |
| raw_trade_concordance | `pending` | pending_obsolete_mapping_parse |
| raw_trade_real_values | `pending` | nominal extension is canonical; CPI real-value build not run |
| independent_policy | `failed` | 2025 ledger sources remain incomplete |

Package import replication covers Figures 2 and 4a only. Figure 4b exports and other tables remain outside the package gate.

Section 301 v5 and the 2025 event remain blocked until the raw-outcome and independent-policy gates pass.
