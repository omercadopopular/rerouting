# Pooled historical policy replication: v2 results

Date: 2026-07-21

## Scope and source boundary

This exercise reconstructs the historical import-policy object from local HTS,
Chapter-99, and archived notice sources.  It is distinct from the authors'
replication package.  The package is opened only on the validation side, to
compare treatment, dates, and increments.  It is not used to populate the
independent policy panel.

The raw outcome regressions use the frozen common-sample artifact
`data/verification/passthru_data/trade_regressions/package_benchmark_v5/common_sample_v5_cif/raw_outcomes_package_policy_cif.parquet`.
Thus the outcome source is reconstructed Census data with package outcomes
held fixed for this policy test; the policy source is the independent pooled
panel.  This isolates policy assignment from the separate raw-outcome bridge.

## Construction steps

1. Read locally archived rule attributes and source-qualified HS8 scope links.
2. Normalize HS codes to fixed-width strings and preserve source rule codes.
3. Expand positive-rate rules over their source-derived effective dates and
   compute monthly active shares using the paper arithmetic: the effective day
   is excluded, so the initial-month share is
   `(days_in_month - effective_day) / days_in_month`.
4. Keep two policy objects:
   - `paper_compatible`: nearest-full-month event dates and product-level
     treatment coding.  The principal 201/232 schedules are retained; the
     appendix's small threshold-only quota set is not separately fabricated.
     Where monthly HS10 data cannot identify an entry's quota tier, the listed
     principal rate is used as a deterministic paper convention and the
     unresolved legal limitation is recorded.
   - `independent_legal`: partner-specific legal dates and source schedules;
     unresolved quota allocation remains a legal blocker and is never filled
     with zero.
5. Join the independent policy fields to the raw-outcome common sample.  For
   event studies, status `2` denotes an active treated month; status `1`
   denotes a product that is eventually treated but is not active in that
   month.  For the paper object, Section 301 activation is status `2` only for
   China, while universal 201/232 activation may be status `2` for all
   partners.  This mirrors the package's bilateral status convention.
6. Run Figures 2 and 4a specifications with the existing FE, cluster, outcome,
   singleton, and dynamic first-difference code.  The package-only benchmark
   remains the authoritative estimator replication; these are independent
   policy-substitution diagnostics.

## Variable-level validation

Artifact: `data/verification/passthru_data/raw_replication_imports/pooled_policy_replication_v2/pooled_policy_v2_validation_summary.csv`.

On 4,199,002 package-overlap rows, the paper-compatible object gives:

| metric | value |
|---|---:|
| treatment match | 0.997125 |
| trade-weighted treatment match | 0.997512 |
| exact paper month | 0.961687 |
| increment MAE | 0.003964 |
| trade-weighted increment MAE | 0.002705 |
| increment within 10 bp | 0.989648 |
| day-weighted increment MAE | 0.005528 |

The registered paper-compatible variable gate is therefore passed.  The total
tariff-level difference is reported separately because the independently parsed
2017 MFN baseline is not identical to the package baseline (and is missing for
315,854 overlap rows); it is not silently replaced with package values.

The independent legal gate remains false.  Legal effective-month agreement is
0.741321, and quota-tier allocation cannot be identified from monthly HS10
totals.

## Regression diagnostics

Canonical coefficients, metrics, and plots are under:

`data/verification/passthru_data/raw_replication_imports/pooled_policy_replication_v2/regressions/`

The complete 16-fit grid (paper/legal × event/dynamic × four outcomes) was
materialized on the common sample.  The paper-calendar event curves have high
shape correlation with the package benchmark (0.984–0.999), but value and
quantity point-estimate RMSEs are 3.39 and 3.62 log points respectively when
compared with the full package benchmark.  On the package/common anchor, price
RMSE is 0.73 and duty-inclusive-price RMSE is 0.91, while value and quantity
remain about 3.4–3.7 log points apart.  Dynamic value RMSE is 1.40 and dynamic
quantity RMSE is 1.49 against the full package benchmark; dynamic price levels
are numerically close but have low curve variance and unstable Pearson
correlation.

These are diagnostic results, not a release of the independent legal policy.
The plots show paper-compatible and legal independent policy lines separately;
they must not be described as package-policy replication curves.

## Scientific decision

The historical package-only estimator gate remains passed.  The pooled
independent policy variable gate now passes under the registered treatment,
timing, and increment criteria, but the pooled policy-substitution regression
gate is not yet passed for value/quantity point estimates.  The next blocker is
to explain the residual event-curve level differences (especially value and
quantity) using the same-sample status/date decomposition and baseline
semantics before beginning the 2025 policy/event study.  Section 301 legal
release remains false.
