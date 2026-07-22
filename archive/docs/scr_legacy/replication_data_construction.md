# Historical replication data construction

Date: 2026-07-19
Methodology: `replication_methodology_v2_policy_separated`

## Purpose and scope

This document describes the reproducible construction used for the original-period U.S. import analyses in Figures 2 and 4a of Fajgelbaum et al. It separates four questions that must not be conflated:

1. Can the Python estimator reproduce the authors' results using their package data?
2. Can raw Census trade fields reproduce the package outcomes while package policy is held fixed?
3. Can locally archived policy sources reproduce the historical policy assignment used by the paper?
4. Can an independently verified legal-policy ledger be constructed for a later episode such as 2025?

The first three questions define the historical replication lock. The fourth remains a separate forward-research gate. Figure 4b exports and other paper tables are outside the present import gate.

## Inputs

### Authors' package

The package estimation source is:

- `data/fajgelbaum/data/analysis/m_flow_hs10_fm_new.dta`

The original programs are:

- `data/fajgelbaum/code/main/fig_02_m_event.do`
- `data/fajgelbaum/code/main/fig_04_dynamic.do`
- `data/fajgelbaum/code/main/tab_04_sigma_omega.do`

The reference figures are the replication-package PDFs `fig_02.pdf` and `fig_04a.pdf`. The canonical local reference stores only independently extracted `reference_*` vectors and their PDF hashes. It does not reuse Python estimates to calibrate the PDF geometry.

### Raw trade

Raw trade comes from monthly U.S. Census fixed-width ZIP archives. The local archive inventory contains 156 import and 156 export months from 2013-01 through 2025-12. Each output partition records its source archive, member, archive hash, parser version, period, and native HS10.

The import fields used in the original-period outcome reconstruction are:

| Source field | Meaning | Python field |
|---|---|---|
| `GEN_CIF_MO` | General-import CIF value | `gen_cif_mo` |
| `GEN_QY1_MO` | General-import primary quantity | `gen_qy1_mo` |
| `GEN_VAL_MO` | General-import customs value | `gen_val_mo` |
| `DUT_VAL_MO` | Dutiable value, not duty paid | `dut_val_mo` |
| `CAL_DUT_MO` | Calculated duty | `cal_dut_mo` |

Missing source tokens remain null. Observed zero tokens remain zero. Aggregation uses missing-preserving sums by partner, native HS10, year, and month. Unit values are calculated only after aggregation.

### Historical policy sources

The local policy archive contains:

- 2018 HTS Revision 7 note 20(b), heading 9903.88.01 (List 1);
- Revision 10 note 20(d), heading 9903.88.02 (List 2);
- Revision 12 notes 20(f) and 20(g), headings 9903.88.03 and 9903.88.04 (List 3 and partial statistical exclusions);
- revision 10 and revision 11 machine-readable HTS schedules;
- the 2017 HTS schedule used for a fixed pre-event MFN baseline.

Generated manifests store repository-relative paths and SHA-256 hashes for each source.

## Key normalization

Package and raw keys use the same HS normalizer. A terminal numeric `.0` is stripped before any punctuation removal, leading zeros are preserved, ambiguous scientific notation is rejected, and the result must be exactly ten digits or null. This fixes the earlier corruption in which `801001090.0` became `8010010900` rather than `0801001090`.

Native monthly HS10 is canonical. Any longitudinally concorded code must be a separately labeled derived field with its mapping vintage and audit.

## Outcome construction

The canonical raw import outcomes are:

- value: `m_val = GEN_CIF_MO / 1,000,000`;
- quantity: `m_q1 = GEN_QY1_MO / 1,000,000`;
- pre-duty unit value: `m_p = GEN_CIF_MO / GEN_QY1_MO`;
- duty-inclusive unit value: `m_pduty = (GEN_CIF_MO + CAL_DUT_MO) / GEN_QY1_MO`.

Value and quantity must be positive for logarithms. Unit values require positive CIF and quantity. Duty-inclusive unit value additionally requires observed nonnegative calculated duty. `DUT_VAL_MO` is retained but is never substituted for calculated duty. A statutory-rate multiplier is a counterfactual diagnostic, not the realized-duty outcome.

The earlier use of `GEN_VAL_MO` for the paper-compatible value numerator and the earlier formula `p * (1 + statutory tariff)` for realized duty-inclusive price are historical and invalid.

## Package estimator

### Figure 2 event study

The Python implementation mirrors `fig_02_m_event.do`:

1. retain `year >= 2017` and positive country codes;
2. define time-invariant treatment as the maximum `m_status2` by product-partner ID;
3. use `m_effective_mdate2` for directly dated products;
4. assign missing untreated dates using the minimum treated date in NAICS4, then NAICS3, then NAICS2;
5. use February 2018 only as the final fallback;
6. retain horizons -6 through +6, binning the right tail at +6;
7. omit -6;
8. absorb product-partner, country-month, and HS10-month effects;
9. cluster by HS8 and country;
10. preserve the authors' scaling and singleton behavior.

### Figure 4a dynamic study

The dynamic design mirrors `tab_04_sigma_omega.do` and `fig_04_dynamic.do`. Package modes use the package `lm_*` variables. Python's first differences and leads/lags use exact calendar-month lookup; they never bridge a missing month by shifting to the next stored row. Six leads, the current tariff change, six lags, missing-lead/lag dummies, fixed effects, clusters, and cumulative covariance-based standard errors follow Stata.

## Policy construction

### Three distinct objects

The pipeline deliberately preserves three policy modes:

1. `raw_outcomes_package_section301_policy_anchor`: authors' package policy variables, used only as the validation anchor.
2. `raw_outcomes_paper_compatible_section301_policy`: reconstructed historical assignment designed to reproduce the paper's data construction.
3. `raw_outcomes_independent_section301_legal_calendar`: final official scope and actual legal dates, retained as an independent legal diagnostic.

The paper-compatible object is not labeled independent legal evidence. It uses official sources plus explicit frozen reconciliation records. The final-legal object never receives those paper-specific changes.

### Official scope and rates

The parser extracts 817 List 1 HS8 lines, 279 final List 2 HS8 lines, 5,756 full List 3 HS8 lines, 11 partial HS8 parents, and all 18 final-legal HS10 exclusions. Section 301 increments are 25%, 25%, and 10% for Lists 1, 2, and 3 in the original window.

The fixed pre-event MFN baseline is parsed from exact 2017 HTS text. Simple ad-valorem rates are converted to fractions; compound, specific, missing, or unresolved rates remain null. Missing tariffs are never replaced by zero.

Legal partial-month weights use the exclusive-effective-day convention observed in the package: 25/31 for 6 July, 8/31 for 23 August, and 6/30 for 24 September. The event-study paper calendar remains July, September, and October 2018.

### Paper-compatible reconciliation

The historical package differs from the final legal notices in three reproducible ways:

- five proposal-era List 2 HS8 lines and two proposal-era List 3 HS8 lines are present in the package assignment but absent from final annexes;
- the package behaves as if only the first HS10 in each multi-code List 3 partial-exclusion clause were excluded, giving 11 paper-compatible exclusions rather than all 18 legal exclusions;
- a September/October 2018 HTS revision replaces a set of wood codes. New final-annex codes absent from revision 11 are not back-cast, while 55 old HS10s carry the October event scope/date in the historical panel.

These actions are stored row by row in `paper_compatibility_reconciliation.parquet`. The seven proposal-era additions, seven parser-quirk retentions, and 55 longitudinal HS10 carries are marked `validation_derived`. They are fixed constants rather than runtime copies of package policy columns.

The local archive lacks the official proposed List 2/List 3 annexes and the authors' longitudinal HTS concordance. `paper_compatibility_missing_sources.json` discloses those gaps. This limits independent legal provenance, but not the ability to reproduce the historical package transparently.

### Partner status and propagation

For an affected product, the original panel stores the effective date for all partners and uses:

- `m_status2 = 0` before treatment;
- `m_status2 = 1` after treatment for comparison partners;
- `m_status2 = 2` after treatment for China.

Only a maximum status of 2 defines the treated product-partner ID. Retaining the shared product date is essential: a small product/date mismatch changes NAICS minimum dates and can propagate to many untreated controls. The pipeline therefore validates `m_status2` and `m_effective_mdate2` row by row before estimation and writes a full propagation diagnostic as ZSTD Parquet. `m_ess` is reported diagnostically but is not the Figure 2 treatment source.

## Validation design

### Outcome bridge

Four evidence layers remain separate:

1. package PDF reference;
2. Python estimator on the full package sample;
3. package outcomes on the raw/package common sample;
4. raw Census outcomes on the same common sample with package treatment and policy held fixed.

The bridge identifies estimator, sample-selection, and outcome-construction differences. It does not test independently reconstructed policy.

### Policy substitution

Policy plots hold the raw Census outcomes and product-union sample fixed. Their lines are:

- package-policy anchor;
- paper-compatible reconstructed schedule;
- independent final-legal schedule.

The paper-compatible comparison is the registered historical replication gate. The final-legal line is diagnostic because actual legal dates are not expected to equal the paper's nearest-month event convention.

### Gates

The package/PDF gate requires eight fits, 13 horizons each, valid provenance, correct specification, and maximum absolute distance no greater than 1.10 log points.

The raw-outcome point-estimate gate requires correlation at least 0.95, RMSE no greater than 1.25, maximum pointwise difference no greater than 2.50, and post-treatment sign agreement. CI overlap is reported separately as an inference diagnostic, excluding the normalized zero-width baseline.

The historical policy gate requires:

- exact paper-compatible event status/date encoding on the analysis panel;
- at least 0.999 product assignment, event-month, and increment agreement;
- all eight paper-compatible substitution curves to pass the point-estimate criteria.

Passing that historical gate does not make a 2025 policy ledger ready. Forward policy requires its own official product/date/rate/exclusion/stacking sources.

## Forward trade data and CPI

The independent nominal extension is partitioned monthly through 2025-12, preserves raw duty and quantity fields, and contains no package treatment or tariff variables. Nominal values are canonical.

Local CPI data and the HS6-CPI crosswalk are retained for future work. Real-value fields are not required for Figures 2 or 4a and are not a pending historical replication gate. If used later, CPI-adjusted fields must be separately named and must preserve the nominal source values.

## Reproduction commands

Use the repository environment:

```text
.venv\Scripts\python.exe
```

Run tests:

```text
.venv\Scripts\python.exe -m pytest tests -q -p no:cacheprovider
```

Rebuild and validate historical policy panels:

```text
.venv\Scripts\python.exe -m scr.passthru_data.policy_replication_v2 --overwrite-reconstructed
```

Preflight and resume policy-substitution regressions:

```text
.venv\Scripts\python.exe -m scr.passthru_data.policy_regression_v2 --preflight-only
.venv\Scripts\python.exe -m scr.passthru_data.policy_regression_v2
.venv\Scripts\python.exe -m scr.passthru_data.policy_regression_v2 --finalize-only
```

Generated panels, checkpoints, tables, plots, and manifests are ignored build outputs. Detailed keys and row traces are ZSTD Parquet; CSV is limited to compact summaries.

## Boundary for 2025 work

The locked historical method has two calendar outputs: paper-compatible nearest-month timing for historical comparability and legal-effective timing for substantive legal measurement. A future 2025 application must use the latter logic with a new versioned official ledger. It must not reuse the seven proposal-era corrections, the 2018 parser quirk, or the 55-code historical carry unless a new official concordance independently justifies them.

No February 2025 event regression is authorized by this document. That step begins only after the 2025 ledger's product scope, dates, rates, exclusions, and stacking rules pass their own provenance and validation gates.

## Locked empirical result (2026-07-19)

The complete current-hash policy-substitution grid contains 24 validated fits: three policy modes, two specifications, and four outcomes. Each fit has 13 horizons and its own coefficient Parquet, sample audit, and hash-validating manifest. The aggregate finalizer accepts exactly 24 fits, leaves no `current_fit.json`, and can be rerun with zero new estimations.

The reconstructed paper-compatible event-study curves match the package-policy anchor to numerical precision. The largest absolute differences are below `4e-9` log points for value, quantity, pre-duty price, and duty-inclusive price. In the dynamic specification, correlations range from `0.993819` to `0.999802`; maximum absolute gaps range from `0.061659` to `0.187121`; post-treatment signs agree at every horizon; and diagnostic CI overlap ranges from `0.895775` to `0.910708`. All eight paper-compatible comparisons pass the registered point-estimate gate.

The historical lock therefore rests on three separate pieces of evidence:

1. the package-only estimator reproduces the Figure 2 and Figure 4a PDF vectors, with a maximum absolute difference of `0.868619` against a `1.10` threshold;
2. raw Census outcome point estimates reproduce the package-policy common-sample estimates, with the event duty-inclusive-price CI overlap retained as a failed secondary diagnostic rather than hidden;
3. the independently rebuilt paper-compatible Section 301 status, event date, and increment assignments pass their variable/encoding gates, and their substitution curves pass for all eight outcome/specification pairs.

The final-legal 2018 schedule is intentionally not substituted for the paper-compatible event calendar. Its dynamic curves are close to the package-policy anchor, but its event curves differ because the legal August/September dates and final scope are not the paper's nearest-month historical encoding. This is a calendar-definition result, not a reason to copy package policy into the legal ledger.

Canonical diagnostic figures are generated under:

- `data/verification/passthru_data/raw_replication_imports/policy_replication_v2/regressions/figures/historical_replication_four_line_event.png`;
- `data/verification/passthru_data/raw_replication_imports/policy_replication_v2/regressions/figures/historical_replication_four_line_dynamic.png`;
- `data/verification/passthru_data/raw_replication_imports/policy_replication_v2/regressions/figures/section301_policy_substitution_event.png`;
- `data/verification/passthru_data/raw_replication_imports/policy_replication_v2/regressions/figures/section301_policy_substitution_dynamic.png`.

These are generated evidence and are not committed. The scripts, frozen specifications, tests, and this construction document are committed so the figures can be regenerated.

## 2026-07-20 pooled-policy v2 correction

The pooled 201/232/301 policy layer is not yet locked. Version 2 separates
legal-effective rates from the paper-compatible monthly treatment transformation
and keeps authors' package variables as validation targets only. The current
fail-closed preflight identifies unresolved quota/TRQ semantics for solar,
washers, and aluminum. Existing v1 panels remain historical diagnostics and
must not be resumed. The raw trade extension through 2025 remains usable as a
nominal, policy-free data layer; it does not imply that the independent tariff
ledger or a 2025 event study is ready.
