# Historical policy replication methodology

## Purpose

This document defines the policy objects used to reproduce the historical
Fajgelbaum import analysis and to prepare a legally dated forward extension.
The objects are intentionally separate. A legal-calendar estimate is not a
failed replication of a paper estimate that uses a different monthly date.

## Like-for-like benchmark rule

A reconstructed policy may be compared with a published estimate only when
the original do-file and the reconstruction use the same policy variable,
dating convention, sample, outcome, estimator, baseline, fixed effects, and
clusters.

For the main import event study (`fig_02_m_event.do`), the paper uses:

- `m_status2`;
- `m_effective_mdate2`;
- `m_stattariff2`;
- nearest-full-month treatment timing.

For the dynamic import specification (`fig_04_dynamic.do` and
`tab_04_sigma_omega.do`), the paper uses the `stat2tf`/`lstattf` path built
from the package's monthly variables. The reconstructed paper-compatible
dynamic object must use the same path.

Figure 1 uses the legal-style package fields (`m_status1`,
`m_effective_mdate1`, and `m_stattariff1`), so that result has a different
like-for-like benchmark. The benchmark contract in
`scr/specifications/historical_policy_benchmark_contract.json` is canonical.

## Dating conventions

The legal object assigns treatment to the month containing the legal effective
date. The paper-compatible object applies the nearest-full-month transformation:
late-month actions move to the following month when that is the month of mostly
full exposure.

Historical examples include:

| Legal effective month | Paper-compatible month |
|---|---|
| 2018-02 | 2018-02 |
| 2018-03 | 2018-04 |
| 2018-06 | 2018-06 |
| 2018-07 | 2018-07 |
| 2018-08 | 2018-09 |
| 2018-09 | 2018-10 |

The legal `m_*1` and paper-compatible `m_*2` objects must both be estimated,
but only the object matching the original program enters that program's
published-paper gate. Legal-calendar event curves are separately labeled
diagnostics and are not scored against the paper's nearest-month event curve.

## Policy objects

### Paper-compatible announced shock

This object is constructed from local official tariff sources, then transformed
to the paper's monthly convention. Where the paper uses an initial announced
shock (`m_increase`), the historical shock is held separate from later legal
rate reductions. Examples confirmed from local HTS notes are 30% for initial
solar treatment, 20% for finished washers, and 50% for washer parts.

### Independent legal statutory schedule

This object retains legal effective dates, contemporaneous rates, exemptions,
replacement-country rates, and source-vintage intervals. Quota/TRQ allocation
is null or bounded when entry-level allocation cannot be recovered from monthly
HS10 data. It is not filled with zero.

### Realized effective duty

Where the raw files support it, collected duty divided by the documented value
denominator is retained as a separately named realized-duty measure. It is not
called a statutory rate and is not substituted for the paper shock.

## Sources and construction

The historical policy layer uses the archived local HTS data, local HTS note
PDFs, source-qualified scope links, and the separately audited Section 301
component. Raw trade outcomes are constructed from local Census archives. The
authors' replication package is read only as a validation anchor; its policy
variables never populate independent policy fields.

The current v2 preflight remains fail-closed for solar, washers, and aluminum
because quota/TRQ entry allocation and some product/country semantics remain
unresolved. Finished-washer scope and bounded 2018/2019 rate schedules are now
represented explicitly in source-qualified code, but the legal quota gate still
blocks pooled policy regression release.

## Required regression comparisons

For each of value, quantity, pre-duty price, and duty-inclusive price, estimate
event and dynamic specifications for:

1. package outcomes with package policy;
2. raw outcomes with package policy;
3. raw outcomes with independent paper-compatible policy;
4. raw outcomes with independent legal-calendar policy.

Only (3), with a do-file-consistent paper calendar, can pass or fail the
historical policy replication gate. (4) is a legal-calendar diagnostic.

## Current status

- Package-only import estimator for Figures 2 and 4a: passed.
- Raw-outcome point-estimate bridge: passed; one duty-inclusive-price CI metric
  remains diagnostic.
- Independent Section 301 variable gate: passed under corrected incremental
  shock semantics.
- Full pooled 201/232/301 policy gate: blocked pending solar/washer/aluminum
  quota and scope resolution.
- 2025 policy/event layer: not estimated; official source ledger incomplete.
