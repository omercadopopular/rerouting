# Fajgelbaum import-policy variable contract

This document fixes the policy objects used in the original-period replication.
It keeps three objects separate:

1. `package_policy_anchor`: the authors' policy variables in
   `m_flow_hs10_fm_new.dta`, used only to reproduce Figures 2 and 4a.
2. `paper_compatible_reconstruction`: an independently sourced schedule whose
   monthly assignment follows the paper's nearest-full-month calendar.  This
   is the only reconstructed policy object that can be compared to the paper's
   event-study treatment variables.
3. `final_legal_reconstruction`: legal effective dates and source scopes from
   local HTS/Chapter-99 records, without paper-specific date or concordance
   adjustments.

The pooled policy builder in `pooled_policy_replication_v1.py` is a diagnostic
source-only layer.  It pools the five families named in the Stata programs:
Section 201 solar and washers, Section 232 steel and aluminum, and Section 301
China.  It never imports package policy fields while building the ledger.

For each family and month, the ledger stores the statutory increment, legal
start/end dates, inclusive active-day share, day-weighted increment, source
rule, and source file.  Overlapping family increments are summed only after
the family components have been retained separately.  Zero-rate Chapter-99
rows are exemption or administrative evidence; they do not create a positive
treatment action.  Unresolved scope or rates remain null and are reported as
missing, never zero-filled.

The package benchmark is accepted on its package/PDF gate independently of the
source-only policy gate.  A policy-source failure therefore does not invalidate
the historical package replication, but it does block a claim that the policy
variables themselves have been reconstructed from raw official sources.
