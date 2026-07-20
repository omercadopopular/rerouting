# Pooled policy reconstruction v2

## Purpose

This document records the corrective implementation for the independently
constructed 2017-01--2019-04 tariff panel. It does not replace the validated
package-only import benchmark, the raw-trade extension, or the historical
Section 301 v2 evidence.

The central rule is that three policy objects remain separate:

1. `independent_legal`: a legally effective product-partner-date schedule from
   local official sources;
2. `paper_compatible_from_legal`: a deterministic monthly transformation of
   that legal ledger used to reproduce the paper's treatment convention;
3. `package_reference`: the authors' variables, read only for validation.

The package variables are never used to populate an independent policy field.

## Audit result on 2026-07-20

The software suite passes (`151 passed, 7 warnings` after adding the v2 unit
tests), but the pooled policy preflight is correctly blocked. The current v2
source-qualified ledger has complete structural scope for the universal rules
that are locally identifiable, but unresolved quota/TRQ allocation remains for
solar, washers, and aluminum. The preflight therefore reports:

```text
status: blocked_missing_or_conditional_scope
blocked families: solar_201, washer_201, aluminum_232
China Section 301 component: imported from the independently audited v2 scope ledger
package policy used by builder: false
```

This is an honest source blocker, not a failed regression hidden by a zero or a
package-derived assignment.

The rule inventory records the specific unresolved objects. Solar `99034522`,
washer `99034501/02/06`, and aluminum `99038505/06` are quota/TRQ alternatives
whose monthly entry allocation is not observed. The local attributes also show
washer rates of `.18/.45` and aluminum rates carried through 2050; those
intervals require a source-vintage audit before they can be used for the
2017--2019 paper window. No rate is silently converted to zero.

## Why pooled v1 is historical diagnostic evidence

Pooled v1 is retained under its original namespace. It is stale because its
fingerprint did not cover the scientific implementation, source scope, or
stacking semantics. It also:

- admitted PDF-context links as product scope;
- classified quota alternatives as universal positive rules;
- summed mutually exclusive rules;
- compared paper-calendar treatment to legal-calendar dates;
- compared family rates to the pooled package shock even on overlapping-family
  observations.

No v1 checkpoint or panel may be resumed or promoted into v2.

## Source and scope rules

Every scope record must retain the rule code, native HS8, source file/hash,
note/page or structural row, source vintage, effective interval, extraction
method, and confidence class. Only structural same-row links, explicit note
enumerations, and documented heading expansions can enter production scope.
Cross-reference-only and nearby-text links remain diagnostic.

The previous parser incorrectly produced solar links to `85419000` and
`85072080`. Note 18 of the archived HTS revision explicitly identifies
`8541.40.60`; v2 records that structural heading separately and does not infer
batteries or unrelated parts from PDF proximity.

## Rule roles and stacking

Chapter 99 rules are typed as universal, replacement-country, quota/TRQ,
conditional-entry, transitional, exclusion, or administrative. Quota/TRQ
alternatives are not summed. A replacement country rate supersedes the general
rate. Additive pooling occurs only across distinct policy families after
within-family precedence has been resolved.

The legal ledger must preserve unresolved quota allocation as null and report
it. Monthly product-level trade data do not reveal entry-level quota usage.

## Calendar and regression targets

Legal effective dates and paper-compatible nearest-month dates are separate.
Figure 1 uses the legal-style package fields (`m_status1`,
`m_effective_mdate1`, `m_stattariff1`); Figure 2 uses the nearest-month fields
(`m_status2`, `m_effective_mdate2`); table treatment uses `m_hit` and
`m_increase`. A legal rate or date must not be compared to a paper shock merely
because both are called a tariff variable.

## Required completion before historical lock

The missing quota/product semantics for solar, washers, and aluminum must be
resolved from local official source material, or explicitly marked
`blocked_missing_source`. After that, v2 must produce separate legal and
paper-compatible panels and rerun same-sample event and dynamic regressions:

- package outcomes with package policy;
- raw outcomes with package policy;
- raw outcomes with independent paper-compatible policy;
- raw outcomes with independent final-legal policy.

The final-legal series is a legal diagnostic and is not expected to equal the
paper-compatible event curve when legal dates differ from the paper's monthly
encoding.

## Reproduction

Run the fail-closed preflight with:

```text
.venv\Scripts\python.exe -m scr.passthru_data.pooled_policy_replication_v2 --preflight-only
```

Run the tests with:

```text
.venv\Scripts\python.exe -m pytest tests -q -p no:cacheprovider
```

The v2 generated diagnostics are ignored build artifacts under
`data/verification/passthru_data/raw_replication_imports/pooled_policy_replication_v2/`.
They are not evidence that the pooled policy gate has passed.
