# Pooled policy validation status

The current source-only build is a diagnostic, not a release artifact.  The
validator joins the independently built panel to the corrected package cache
only for measurement.  It reports treatment assignment, legal/paper timing,
additional-rate differences, day-weighted differences, and family-level
coverage.  Total statutory rates are not compared until raw base statutory
scope is resolved; Chapter-99 sentinel values such as `9999.99` are treated as
missing rather than a rate.

Current diagnostic output from the local run (2026-07-20):

- 4,199,002 common package rows;
- treatment assignment match: 0.986741;
- trade-weighted treatment match: 0.975572;
- additional-rate MAE against the package `m_increase` field: 0.029182;
- day-weighted additional-rate diagnostic MAE against package `m_increase`: 0.029232;
- exact legal effective-month match against package `m_effective_mdate1`: 0.432395;
- paper-month within-one match: 0.434619.

These values are not a passed pooled-policy gate.  The comparison now uses
like objects: independently reconstructed additional rates versus package
`m_increase`, and family scopes versus package `m_<family>_hit` flags.  It does
not compare an independent additional rate with the package's total or
day-scaled statutory fields.  Exact legal timing is also not expected to equal
the package's paper-compatible event month; both calendars are retained.  The
historical package-only replication remains a separate passed estimator/PDF
result.  Section 301 v5 and the 2025 policy/event study remain blocked.

## Corrected 2026-07-20 diagnostic

After fixing the exact Section 201 family mapping and recovering principal
Section 232 scopes from local HTS note text, the source-only build contains
2,092,985 action rows. On the 4,199,002-row package comparison universe,
unweighted treatment match is `0.986741`, trade-weighted treatment match is
`0.975572`, and additional-rate MAE against package `m_increase` is
`0.029182`. These remain diagnostics, not a legal gate.

The historical-window source ledger no longer treats conditional or
transitional rules as missing universal scope: `99038061` is explicitly a
conditional contract rule and `99038809` is outside the 2017-01--2019-04 paper
window. The source ledger is therefore complete as a *scope inventory*, but
the rate and calendar comparison still fails. In particular, the current
source-only pooled panel is not yet a validated reproduction of the package
policy variable, so historical pooled-policy regressions are intentionally not
run.

The package policy projection used for this audit is a chunked extraction of
the authors' DTA containing `m_increase`, `m_stattariff1/2`, `m_status2`,
`m_effective_mdate1/2`, and the five family-hit flags. It is a validation anchor
only; no package policy field is used to build the independent panel.

## Pooled-policy v2 fail-closed correction

Version 2 uses a separate legal/paper/package object contract and rejects the
v1 PDF-context link cache as production scope. It reuses the independently
audited Section 301 v2 scope and records the explicit Note 18 solar heading,
but it does not assign quota/TRQ rates without entry-level allocation. The
current preflight is therefore `blocked_missing_or_conditional_scope` for
solar, washer, and aluminum. No pooled policy regression is released until
those source and stacking decisions are resolved.
