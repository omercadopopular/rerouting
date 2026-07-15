# Policy Divergence Audit (2017-2019)

## Scope

This audit traces why our reconstructed import policy schedule differs from the replication package variable `m_stattariff1` in `data/fajgelbaum/data/analysis/m_flow_hs10_fm_new.dta`.

## What We Audited

1. **Paper-side construction definitions**
2. **Direct data comparison** on overlapping `hs10-year-month` cells
3. **Difference decomposition** by source coverage, sentinel values, and policy-wave indicators

## Paper Construction (Verified)

From the replication files and paper appendix:

- `m_stattariff1` label: **Statutory Tariff Rate**
- `m_stattariff2` label: **Statutory Tariff Rate (scaled by days of month in effect)**
- `m_hit` label: **Product is subject to Trump Tariffs**

Online Appendix A.2.2 (RTP Appendix PDF) states that import tariff identification uses:

- USITC baseline + revisions (2017:1 to 2019:4)
- **Ad-valorem tariff increases documented in revision files**
- Mid-month implementation timing rule (and day-scaling for elasticity specs)
- Country-specific targeting/exemptions by wave (washer, solar, steel, aluminum, China)
- Exclusions: antidumping/countervailing duties, quota-threshold edge cases, non-trade-war 2018 changes

Replication README also classifies `m_flow_hs10_fm_new.dta` as a **workhorse estimation dataset**, not a raw-source mirror.

## Main Findings

Audit outputs are in `data/verification/passthru_data/`:

- `policy_diff_audit_metrics.csv`
- `policy_source_mix_cells.csv`
- `policy_diff_top_cells.csv`
- `policy_diff_top_cells_no_sentinel.csv`
- `policy_diff_by_hit.csv`
- `policy_diff_audit_report.md`

Key metrics (after adding GSP/CBI/AGOA/DR-CAFTA, `col1_special_text`, and PDF-derived trade-war overlays):

- Matched coverage vs paper `m_stattariff1` rows: `4,199,002 / 4,653,250` (about `90.2%`)
- Mean absolute difference (all matched rows): `45.3193` (inflated by sentinel `9999.99`)
- Excluding sentinel values:
  - mean abs diff: `0.01581`
  - share within `0.01`: `81.11%`
  - share within `0.05`: `90.94%`
  - share within `0.10`: `95.72%`
- Current raw-policy source mix in our panel:
  - `mfn_schedule_only`: `87.61%`
  - `base_preference_raw`: `11.72%`
  - `trade_war_raw_overlay`: `0.66%`

## Root Causes

1. **Special-program preference decoding is still incomplete**
- We now apply raw bilateral preference columns for NAFTA/FTA partners (Canada, Mexico, Korea, Australia, etc.).
- Remaining large mismatches are concentrated in rows where paper has zero/low statutory rates but raw MFN remains positive (e.g., special-program cases not captured by simple indicator-rate columns).
- This points to missing logic on `col1_special_text` program eligibility parsing and broader program-country mapping (GSP/CBI/AGOA/DR-CAFTA group rules).

2. **Trade-war overlay is now built from raw revision PDFs, but still has residual scope noise**
- We now parse Chapter 99 revision pages to map `9903.(45|46|80|85|88).*` rules to `HS8` lists from nearby “provided for in subheading ...” text.
- This closes part of the gap, but extraction still relies on PDF text structure and may include false positives/omissions in dense note blocks.
- The remaining tariff residuals are concentrated where rule-scope interpretation (especially 301 note blocks and exclusion carve-outs) is ambiguous in OCR/text extraction.

3. **Specific-duty / non-ad-valorem handling**
- Sentinel rates (`9999.99`) still inflate headline means and need explicit handling for clean policy comparisons.

4. **`m_stattariff2` day scaling is only partially implemented**
- We now apply month-entry day shares for trade-war increments based on release start dates (`tw_active_share_raw`).
- We still need full day-level treatment for all mid-month policy changes and full replication of paper-side month-weighting conventions.

## Implementation Status

Implemented:

- Selenium-first paginated archive indexing (`page=0..7`)
- Machine-readable download attempts by indexed link
- UI export fallback via release pages
- PDF fallback parse for releases with no export trigger (`basicCorrections2`, `NTE`)
- Chapter 99 PDF parser for trade-war rule-to-HS links (2018-2019 release windows)
- Intermediate persisted artifacts:
  - `data/reference/passthru_data/tradewar_pdf_links.parquet`
  - `data/reference/passthru_data/tradewar_rule_attributes.parquet`
  - `data/analysis/passthru_data/tradewar_overlay_raw.parquet`
- Reproducible audit script:
  - `scr/passthru_data/audit_policy_vs_fajgelbaum.py`

## Next Steps to Align with Paper Variables

1. Tighten Chapter 99 PDF parser block segmentation and exception handling to reduce scope noise in `trade_war_raw_overlay`.
2. Add explicit exclusion logic for quota/exclusion subheadings (`9903.80.60/.61`, similar carve-outs) consistent with US notes.
3. Extend month-day scaling beyond release-start shares to full within-month policy timing for all relevant waves.
4. Implement explicit treatment for specific/compound duties where ad-valorem conversion is required.
5. Re-run `audit_policy_vs_fajgelbaum.py` and decompose residuals by rule code, country group, and product family.
