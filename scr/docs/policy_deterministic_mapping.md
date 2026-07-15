# Deterministic Trade-War Mapping Plan

## Goal

Close remaining gaps for import policy construction by moving from heuristic parsing to deterministic rule application for:

- `301` (`9903.88.*`)
- `232 steel/aluminum` (`9903.80.*`, `9903.85.*`)
- `safeguards` (`9903.45.*`, `9903.46.*`)

## Source Priority

1. Machine-readable USITC archive releases (`data/raw/passthru_data/policy/archive/data/*.csv`)
2. USITC archive PDFs as fallback (`data/raw/passthru_data/policy/archive/pdf/*.pdf`)
3. Manual deterministic overrides (project-controlled)

## Implemented in Code

- Main builder: `scr/passthru_data/build_us_products_partner_panel.py`
- New scope extractor:
  - `_load_tradewar_machine_links(config)` (machine-readable first)
  - `_load_tradewar_pdf_links(config)` (fallback)
- New manual hook:
  - `_load_manual_tradewar_overrides(config)` reads:
    - `data/raw/passthru_data/manual/policy/tradewar_rule_overrides.csv`
- Overlay provenance column:
  - `tw_scope_source_raw` in final panel

## Manual Override File Schema

Path:

- `data/raw/passthru_data/manual/policy/tradewar_rule_overrides.csv`

Required columns:

- `cty_name` (country name)
- `hs8` (8-digit HS code)
- `year`
- `month`
- `tw_increment_rate_raw` (decimal rate, e.g. `0.25`)

Optional columns:

- `tw_rule_code_raw` (8-digit chapter-99 code, e.g. `99038801`)
- `tw_active_share_raw` (default `1.0`)
- `tw_scope_source_raw` (default `manual_override`)

## What To Inspect Next (Deterministic Closure)

1. Gap concentration:
- `data/verification/passthru_data/tradewar_scope_missing_active_rows.csv`
- `data/verification/passthru_data/tradewar_scope_missing_active_hs8.csv`

2. Rule schedule and dates:
- `data/reference/passthru_data/tradewar_rule_attributes.parquet`
- `data/reference/passthru_data/policy_release_catalog.csv`

3. Scope links extracted:
- `data/reference/passthru_data/tradewar_machine_links.parquet`
- `data/reference/passthru_data/tradewar_pdf_links.parquet`

4. Final audit:
- `data/verification/passthru_data/policy_diff_audit_metrics.csv`
- `data/verification/passthru_data/tradewar_scope_active_metrics.csv`

## Current Limitation

The infrastructure is now deterministic-first with provenance, but gap closure still requires adding family-specific explicit scope/exemption rules for high-mass missing buckets.

## Interim Non-232 Closure (Disabled)

Script:

- `scr/passthru_data/generate_non232_overrides.py`

What it did:

- Computes missing policy increments for `2017-2019` on `cty_code x hs10 x year x month`.
- Restricts to non-232 families flagged in reference panel:
  - `m_china_hit`, `m_washer_hit`, `m_solar_hit`.
- Writes explicit override rows to:
  - `data/raw/passthru_data/manual/policy/tradewar_rule_overrides.csv`
- Tags provenance as:
  - `tw_scope_source_raw = baseline_guided_non232`

Status:
- Disabled per user instruction to keep construction raw-source only.
- `tradewar_rule_overrides.csv` has been removed from active inputs.

Important:

- This layer is **reference-guided** and intended to close non-232 gaps while raw-source deterministic tables are completed.
- It should be replaced family-by-family with raw-source deterministic mappings once finalized.
