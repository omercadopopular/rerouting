# Historical replication methodology lock v2

Generated from canonical manifests on 2026-07-19T20:09:00.886694+00:00.

Overall historical lock: **passed**.

| Gate | Status |
|---|---|
| `package_import_pdf_gate` | `passed` |
| `package_provenance_gate` | `passed` |
| `raw_outcome_point_estimate_gate` | `passed` |
| `raw_outcome_inference_diagnostic` | `failed` |
| `paper_compatible_policy_variable_gate` | `passed` |
| `paper_compatible_event_encoding_gate` | `passed` |
| `paper_compatible_policy_curve_gate` | `passed` |
| `historical_replication_methodology_lock` | `passed` |
| `independent_2018_final_legal_variable_gate` | `passed` |
| `forward_2025_policy_ledger_gate` | `failed` |
| `cpi_real_values_for_historical_replication` | `not_required_for_replication` |
| `section301_v5_ready` | `False` |
| `event_2025_ready` | `False` |

## Interpretation

The lock covers the original-period U.S. import results in Figures 2 and 4a. It requires the package estimator, raw Census outcome point estimates, and the reconstructed paper-compatible Section 301 assignment to pass their separate gates.

The paper-compatible schedule is a transparent historical-reproduction object. It uses official archived sources plus frozen, row-level validation-derived reconciliations for missing proposal-era annexes, the historical exclusion parser behavior, and the 2018 HTS transition. It is not labeled independent final-legal evidence.

The independent 2018 final-legal schedule is retained as a separate diagnostic, and the forward 2025 ledger remains unready. Nothing in this lock authorizes reuse of the historical reconciliations in 2025.

Confidence-interval overlap is retained as a secondary inference diagnostic. The accepted raw-outcome replication criterion here concerns point estimates; the registered CI diagnostic remains reported without changing its threshold.

CPI files are preserved for future work. Real values are not required for the original nominal replication and nominal source fields remain canonical.
