# Pooled Section 201/232/301 reconstruction

## Scope and source boundary

The active source-only implementation is
`scr/passthru_data/pooled_policy_replication_v1.py`.  It consumes local annual
HTS files, archived machine-readable release files, and locally extracted PDF
scope links.  It writes an action ledger and family schedule as ZSTD Parquet
and then materializes a month-by-month pooled panel.  The package data are
opened only by the validation function.

The panel window is the original paper window, 2017-01--2019-04 (or the
configured historical window).  The legal day-weighting is inclusive: a rule
active from March 23 through March 31 contributes 9/31 in March.  The
paper-compatible month is recorded separately from the legal date.

## Construction steps

1. Inventory and hash local policy sources using repository-relative paths.
2. Normalize HS8/rule codes to exactly eight digits.
3. Combine independently sourced HS8-to-Chapter-99 scope links with annual
   rule attributes and effective dates.
4. Apply deterministic country exclusions and explicit source-text
   inclusions/exclusions.
5. Expand actions to partner-HS8-month rows and compute legal active shares.
6. Aggregate family components without collapsing family provenance.
7. Join family components to the raw trade universe by partner-HS8-month.
8. Validate treatment, date, and increment fields against the package only as a
   diagnostic; do not use package values to populate the reconstructed panel.

## Current source limitations

The local scope evidence is incomplete for the positive main rules in the
original window.  In particular, the current machine/PDF link cache has no
independent HS8 scope for the main Section 232 steel rules (`99038001`,
`99038002`, `99038061`), the main aluminum rule (`99038501`), or the missing
solar family.  It also lacks some later China and washer rule scopes.  The
annual HTS rows identify the applicable U.S. notes and rates, but they do not
by themselves enumerate the HS8 scope.  These are source blockers, not
permission to infer scope from package treatment or fill rates with zero.

The resulting pooled panel is therefore diagnostic and marked
`built_partial`.  Its treatment and increment metrics are not release gates.
The exact missing families/rules are written to
`pooled_policy_family_source_status.json` and
`pooled_policy_missing_sources.json` under the ignored verification output
directory.

## Reproducibility outputs

The canonical machine-readable outputs are:

- `legal_action_ledger.parquet`;
- `family_policy_schedule.parquet`;
- `independent_final_legal_pooled_policy.parquet`;
- `pooled_policy_validation_comparison.parquet`;
- `pooled_policy_family_validation.parquet`.

Compact grouped summaries may be CSV.  Full keys and row-level mismatches are
Parquet only.  The independent policy and 2025 event gates remain false until
all family scopes, rates, exclusions, and calendars pass their own validation.

## 2026-07-20 source-family correction

The initial pooled build contained a material Chapter-99 classification bug:
it assigned every `990345*` heading to washers and assumed a nonexistent
`990346*` solar family. The local historical attributes show the correct split
is `99034501/02/06` for washers and `99034522/25` for solar. The central family
mapper now uses exact codes and the note parser distinguishes U.S. note 17
from note 18.

The rebuild also recovers principal Section 232 scopes directly from the local
2018 HTS Revision 12 PDF: note 16 heading expansion for steel, note 19 heading
expansion for aluminum, and note 17's explicit `8450.90.20/.60` parts scope
for washer rule `99034506`. These are source-derived links, never package
policy values. The corrected action counts are 13,110 solar, 21,735 washer,
1,794,072 steel, 259,896 aluminum, and 4,172 China.

The independent legal build remains partial. The qualifying-contract steel
rule `99038061` and China rules `99038809/15/16` still lack verified local HS8
scope. They remain unresolved rather than being treated as zero or as package
assignments, so no pooled independent-policy regression is released.
