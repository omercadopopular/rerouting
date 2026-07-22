# Replication Results and Decision Record

## Package estimator benchmark

Eight package-only import fits were validated: event and dynamic specifications for value, quantity, pre-duty unit value, and duty-inclusive unit value. Each has thirteen horizons. Across 104 aligned PDF reference points, the maximum absolute difference is 1.009620 log points, below the registered 1.10 threshold. This validates the Python implementation of Figures 2 and 4a, not every empirical result in the paper. Export Figure 4b is outside this gate.

## Independent policy substitution

The fixed raw-outcome sample contains 4,197,758 observations. Replacing the authors’ policy with the independent paper-clock MFN+201+232+301 construction produces the following point-estimate diagnostics relative to the package-policy anchor on the same raw sample.

| Specification | Outcome | Correlation | RMSE | Maximum absolute difference | Registered point gate |
|---|---:|---:|---:|---:|---|
| Event | Value | 0.9994 | 1.0727 | 1.9403 | Pass |
| Event | Quantity | 0.9984 | 1.5650 | 2.6478 | **Fail narrowly** |
| Event | Pre-duty price | 0.9703 | 0.5280 | 1.4191 | Pass |
| Event | Duty-inclusive price | 0.9979 | 0.4691 | 1.4172 | Pass |
| Dynamic | Value | 0.9986 | 0.1203 | 0.2338 | Pass |
| Dynamic | Quantity | 0.9970 | 0.1663 | 0.2989 | Pass |
| Dynamic | Pre-duty price | 0.9599 | 0.0307 | 0.0595 | Pass |
| Dynamic | Duty-inclusive price | 0.9978 | 0.0404 | 0.0891 | Pass |

The thresholds are correlation at least 0.95, RMSE at most 1.25, maximum pointwise difference at most 2.50, and post-treatment sign agreement at least 0.50. Event quantity exceeds the RMSE threshold by 0.315 and the maximum-difference threshold by 0.148. It remains highly correlated and has complete post-treatment sign agreement.

## Locked decision

The methodology is accepted for the historical replication with a disclosed event-quantity exception. This is recorded as `accepted_with_disclosed_quantity_exception`; it is not rewritten as a formal all-specification pass. Point estimates are the primary decision criterion requested for this stage. Confidence intervals remain plotted and available, but they are not used to conceal or reverse the registered point-estimate result.

The independent policy construction is sufficiently close to lock the historical methodology and develop the separate raw-trade extension. It is not yet authority to estimate a 2025 tariff event: later legal actions need their own versioned source ledger, product scope, rates, exclusions, and stacking rules.
