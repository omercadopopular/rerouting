# Final historical policy-replication methodology (v4)

This document is the lock-in record for the 2017--2019 historical import
replication.  It separates three objects that must not be conflated.

## Data and policy sources

* **Outcomes.** The common estimation sample is constructed from the local
  Census import archives (`raw_outcomes_package_policy_cif.parquet`).  The
  package-only benchmark additionally uses the authors' analysis file
  `data/fajgelbaum/data/analysis/m_flow_hs10_fm_new.dta` and its corrected
  HS10-normalized cache.  Package values are retained as the benchmark; raw
  value, quantity, CIF/unit-value, and duty-inclusive unit-value fields are
  retained for the outcome bridge.
* **Package policy anchor.** `package_full_policy_anchor.parquet` copies
  `m_status2`, `m_effective_mdate2`, and `m_stattariff2` from the authors'
  package cache onto exactly the raw common sample.  It is a validation anchor,
  not an independent tariff reconstruction.
* **Independent policy.** The paper and legal panels are built from local HTS,
  Chapter 99, and action-ledger sources.  They include Sections 201, 232,
  and 301 plus the independently sourced MFN/base rate.  Missing MFN values
  remain null and are audited; they are never replaced with zero.

## Dates and rates

The paper-compatible clock follows the paper's nearest-month convention:
post-mid-month legal changes are assigned to the following month, while
untreated varieties inherit the earliest targeted NAICS4, then NAICS3, then
NAICS2 date, with February 2018 as the final fallback.  The legal clock keeps
the first partner-specific positive applicability month.  Dynamic regressions
use day-weighted tariff changes; event regressions use the assigned event
month.  Family components are kept separately before the total is formed.

## Estimation and comparisons

Figures 2 and 4a are estimated exactly as in the Stata programs, including
fixed effects, clusters, outcome transformations, singleton behavior, and
dynamic cumulative coefficients.  The registered package/PDF gate is maximum
absolute difference <= 1.10 log points across 13 horizons for all four import
outcomes.  Independent policy substitutions are compared on the same raw
sample in three modes: package anchor, independent paper clock, and
independent legal clock.  The legal-policy comparison is diagnostic; it does
not change the independent legal release gate, which remains false until its
component/rate validation passes.

## Reproducibility artifacts

Versioned panel and regression outputs live under
`data/analysis/passthru_data/policy/pooled_policy_replication_v4/` and
`data/verification/passthru_data/raw_replication_imports/pooled_policy_regressions_v4/`.
Large tables are ZSTD Parquet; CSV files are compact summaries only.  The
three-line plots distinguish package policy (blue), independent paper policy
(green), and independent legal policy (orange), with the package/PDF context
shown separately.

## Scope

This locks the historical methodology only after the package/PDF gate and the
independent paper-policy point-estimate gates are evaluated.  It does not claim
that the independently reconstructed legal rates are exact, and it does not
estimate the 2025 policy/event study.
