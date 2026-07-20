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
