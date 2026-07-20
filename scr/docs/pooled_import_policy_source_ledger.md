# Pooled policy source ledger

| Family | Official/source inputs used | Current status | Blocker |
|---|---|---|---|
| Section 201 solar | annual HTS and local Chapter-99/archive inventory | `blocked_missing_scope` | no independently linked `990346` scope in the local cache |
| Section 201 washers | annual HTS, archive machine/PDF links | `partial_missing_positive_scope` | at least one positive washer rule lacks an HS8 link |
| Section 232 steel | annual HTS, archive machine/PDF links, U.S. note 16 references | `partial_missing_positive_scope` | main `99038001/02/61` scope is not enumerated by the available link files |
| Section 232 aluminum | annual HTS, archive machine/PDF links, U.S. note 19 references | `partial_missing_positive_scope` | main `99038501` scope is not enumerated by the available link files |
| Section 301 China | annual HTS, archive machine/PDF links, U.S. note 20 references | `partial_missing_positive_scope` | later positive rules have incomplete local scope links |

The ledger records source hashes and relative paths in the generated JSON
manifest.  It does not treat a current 2026 HTS snapshot as a historical
2018 scope source.  A future completion requires the historical note annexes
or another official local scope source for every positive rule and family.

No network download was used in this reconstruction.  No package policy
variable was used in the builder, and no unresolved value was replaced with
zero.

## Corrected source status, 2026-07-20

| Family | Corrected local source evidence | Status | Remaining blocker |
|---|---|---|---|
| Section 201 solar | `99034522/25`; note 18 family split; archived HTS attributes and local links | `complete` | none for the historical source ledger |
| Section 201 washers | `99034501/02/06`; note 17 parts scope `8450.90.20/.60` recovered from local PDF | `complete` | none for the historical source ledger |
| Section 232 steel | note 16 heading expansion from `2018HTSARevision12.pdf`; `99038001/02` recovered | `partial_missing_positive_scope` | `99038061` qualifying-contract HS8 scope |
| Section 232 aluminum | note 19 heading expansion; `99038501/05/06` recovered | `complete` | quota administration remains separately documented |
| Section 301 China | local Section 301 parser and Chapter-99 attributes | `partial_missing_positive_scope` | `99038809/15/16` scope links |

The superseded rows above are retained as historical evidence; the generated
`pooled_policy_family_source_status.json` is the canonical current status.
