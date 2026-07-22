# Pooled policy replication v3: locked clocks and bilateral tariff paths

Date: 2026-07-21

## Purpose

Version 3 separates two objects that were conflated in earlier diagnostics:

1. the event-study clock used to assign relative months; and
2. the partner-specific tariff path used by the dynamic specification.

The package-only benchmark remains the reference for the original paper.  The v3
panel is an independent reconstruction from the legal partner panel and action
ledger; it does not use package tariff variables while building the panel.

## Event clock

For a product-partner pair with a positive reconstructed rate, the paper clock
uses the first positive applicability month.  A date after the 15th is assigned
to the following month, following the arithmetic in the paper's example.  For
products with no partner-specific positive rate, the comparison clock uses the
first positive month observed for the same HS10 at NAICS4, then NAICS3, then
NAICS2; the final fallback is February 2018.  These fallback dates assign a
comparison month only: they do not turn an untreated product into a treated one.

The legal clock is separate: it is the first month in which the bilateral
reconstructed tariff is positive for that partner-product pair.  It is not the
proclamation date.

## Tariff path

The dynamic regressor is the independent 2017 MFN base plus mutually exclusive,
partner-specific, day-weighted increments from Sections 201, 232, and 301.  We
do not expand a China-only action to other partners, and we do not add two
alternative rates within one family.  Temporary exemptions are represented as
zero bilateral increments during the exemption interval; a later positive
month starts the bilateral path.

## Current evidence

The corrected v3 panel contains 43,381,609 ZSTD-Parquet rows.  The v3 common
regression panels each contain 4,197,758 rows.  The policy validation reports no
non-China Section 301 rows, no negative rates, and no component-additivity
mismatches.

The package-only benchmark remains the estimator gate (eight fits, 13 horizons,
maximum PDF distance 1.00962, threshold 1.10).  The v3 same-sample regressions
are diagnostic comparisons.  Event coefficients are sensitive to the treatment
clock and sample; dynamic coefficients are the appropriate place to compare the
independently reconstructed tariff path.  The aggregate diagnostics are stored
under `data/verification/passthru_data/raw_replication_imports/pooled_policy_replication_v3/diagnostics/`.

The current v3 tariff-path correlation with the package tariff on identical
keys is approximately 0.818 (aggregate month-country diagnostic); event-status
agreement is approximately 0.976 for the paper clock and 0.835 for the legal
clock.  These figures do not constitute a policy release gate.  The independent
legal mapping gate remains false and must not be changed by regression fit.

The package-full comparison table is
`pooled_policy_v3_package_comparison.csv`.  Dynamic point estimates are close
to the package benchmark (legal-calendar RMSE is about 0.116, 0.128, 0.062,
and 0.036 for value, quantity, pre-duty price, and duty-inclusive price).  The
event curves have larger level differences because the independently assigned
bilateral event dates and reconstructed raw-outcome sample are not the package
event design; those differences are reported rather than hidden.  The old v2
package-policy anchor is retained as a separate same-sample diagnostic and is
not the package-only replication benchmark.

## Reproducibility commands

```text
.venv\Scripts\python.exe -m scr.passthru_data.pooled_policy_replication_v3 --build-panels --overwrite
.venv\Scripts\python.exe -m scr.passthru_data.pooled_policy_regression_v3 --finalize-only
.venv\Scripts\python.exe -m scr.passthru_data.pooled_policy_diagnostics_v3 --run
```

All detailed diagnostics are Parquet; CSV output is limited to compact
aggregate summaries.  The package benchmark and v3 reconstruction must remain
reported as distinct source modes in any extension analysis.
