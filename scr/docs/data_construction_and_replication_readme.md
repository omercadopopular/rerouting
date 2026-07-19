# Data construction and replication README

Date: 2026-07-18
Methodology version: replication_methodology_v2

## Scope

This repository reconstructs the import event-study evidence in Figures 2 and 4a of the Fajgelbaum replication package and builds a raw Census trade-outcome layer for later extension. The package benchmark is not a claim to have replicated every table, export figure, or policy result in the paper. Figure 4b exports, other tables, and independent policy reconstruction remain separate tracks.

## Source data

The replication anchor is the authors’ analysis file:

- data/fajgelbaum/data/analysis/m_flow_hs10_fm_new.dta
- SHA-256: b6bf5890a2acf10aabecc0bbf664440214f8ba6c62b0ee1a4d82b27e7e9b40d9
- internal Stata timestamp: 23 October 2019 22:15

The reference curves are extracted from the bundled replication outputs:

- data/fajgelbaum/results/main/fig_02.pdf
- data/fajgelbaum/results/main/fig_04a.pdf

They are not extracted from bib/w25638.pdf or main.pdf. Their hashes and extraction provenance are stored in the versioned PDF reference manifest.

Raw trade inputs are the local monthly Census fixed-width ZIP archives. The local inventory covers imports and exports from 2013-01 through 2025-12. The archive layout is read from the embedded documentation when present and is fingerprinted by archive vintage.

## Raw import fields

The import detail layout uses one-based positions represented by these zero-based Python slices:

| Field | Positions | Meaning |
|---|---:|---|
| dut_val_mo | 88:103 | Imports-for-consumption dutiable value |
| cal_dut_mo | 103:118 | Calculated duty |
| gen_qy1_mo | 148:163 | General-import primary quantity |
| gen_val_mo | 178:193 | General-import total/customs value |
| gen_cif_mo | 208:223 | General-import CIF value |

GEN_VAL_MO and GEN_CIF_MO are distinct fields. DUT_VAL_MO is dutiable value, not calculated duty. Missing tokens remain null; observed zero tokens remain zero.

## Aggregation and outcomes

Raw detail rows are first aggregated by partner, native HS10, year, and month. Aggregation uses missing-preserving sums. Unit values are calculated only after aggregation.

The paper-compatible import outcomes are:

- m_val = gen_cif_mo / 1,000,000
- m_q1 = gen_qy1_mo / 1,000,000
- m_p = gen_cif_mo / gen_qy1_mo
- m_pduty = (gen_cif_mo + cal_dut_mo) / gen_qy1_mo

Value and quantity require positive source values. Pre-duty price requires positive CIF and quantity. Duty-inclusive price additionally requires observed nonnegative calculated duty. Missing quantities and duties are never replaced with zero.

The legacy trade_value alias means GEN_VAL_MO only. It is not a valid paper-compatible CIF outcome.

## Keys and common samples

Native monthly HS10 is the canonical raw key. HS10 normalization strips only a terminal numeric .0 suffix, preserves leading zeros, rejects ambiguous scientific notation, and returns exactly ten digits or null. Country codes and monthly dates use the same normalization on package and raw sides.

The common sample is constructed from normalized country-HS10-month keys. Outcome masks are applied symmetrically. Each stage records row counts, products, countries, months, deterministic key hashes, and treatment hashes in Parquet audits.

## Figure 2 event study

The Python implementation mirrors fig_02_m_event.do:

1. retain year >= 2017 and positive country codes;
2. use package outcomes;
3. define treatment as the maximum m_status2 by product ID;
4. use m_effective_mdate2 for treated timing;
5. assign untreated timing by NAICS4, then NAICS3, then NAICS2, with February 2018 as the final fallback;
6. bin horizons at +6 and retain horizons from -6;
7. omit -6;
8. estimate with id, country-month, and HS10-month fixed effects;
9. cluster by HS8 and country;
10. preserve the original coefficient scaling and singleton behavior.

## Figure 4a dynamic study

The dynamic preparation mirrors tab_04_sigma_omega.do and fig_04_dynamic.do. It uses the package m_stattariff2 shock and package lm_val, lm_q1, lm_p, and lm_pduty variables in package modes.

Stata D., F#, and L# operators require exact calendar months. A stored next row is not necessarily the next month. The Python implementation therefore looks up exact id-month targets and creates Stata-equivalent missing lead/lag indicators before replacing missing regressors with zero. Fixed effects are HS10-month, country-month, and country-NAICS4; clusters are HS8 and country. Cumulative coefficients and standard errors use the original covariance-based lincom ordering.

The earlier row-based diff/shift implementation is historical and invalid for the canonical dynamic benchmark.

## PDF reference

The canonical PDF reference is extracted independently from replication-package vector geometry. The extraction records PDF hashes, transformed tick coordinates, affine axis fits, rejected numeric candidates, horizons, confidence intervals, and extraction code fingerprints. Estimator coefficients are never used to calibrate PDF values.

The prior Figure 4a extraction accidentally included the x-axis label -6 as a y-axis tick. The corrected extractor rejects labels outside the subplot y-span and validates affine tick residuals.

## Validation layers

Four curves are kept separate:

1. replication-package PDF output;
2. Python estimator on the full package sample;
3. package outcomes on the package/raw common sample;
4. raw Census CIF outcomes with package treatment and package policy held fixed.

These identify estimator replication, sample-selection effects, and raw outcome reconstruction separately. Independent legal policy validation is not part of the raw outcome bridge.

## Gates

The package gate requires all eight fits, 13 horizons, correct fixed effects and clusters, exact dynamic observation counts, valid PDF geometry, and strict point-estimate agreement.

The raw outcome point gate requires correlation at least 0.95, RMSE no greater than 1.25, maximum pointwise difference no greater than 2.50, and post-treatment sign agreement for every outcome/specification pair. Confidence-interval overlap is reported as a separate inference gate.

The independent Section 301 policy gate remains false. No 2025 event study is estimated in this methodology-lock phase.

## Forward raw outcomes

After the original-period gates pass, the corrected import parser is applied month by month through 2025-12. Forward partitions preserve GEN_VAL_MO, GEN_CIF_MO, GEN_QY1_MO, DUT_VAL_MO, CAL_DUT_MO, source provenance, native HS10, quantity flags, and CIF-based outcome fields. Nominal fields remain canonical; CPI-adjusted values, if built, are separate fields.

No package tariff, treatment, or shock variable enters the independent extension panel.

## Reproduction commands

Use the repository virtual environment:

    .venv\Scripts\python.exe

Run tests:

    .venv\Scripts\python.exe -m pytest tests -q -p no:cacheprovider

Run the versioned pipeline in this order:

1. PDF-reference preflight and extraction.
2. Corrected raw import parsing.
3. Package-full event and dynamic fits.
4. Common-sample construction.
5. Sixteen raw-outcome bridge fits with resume/checkpoints.
6. Finalization and diagnostics.
7. Point-estimate and inference plots.
8. Forward outcome construction through the locally available months.
9. Final validation and gate report.

Each expensive step supports preflight, one-fit selection, resume, and finalizer-only execution. Generated Parquet, manifests, plots, and checkpoints are ignored build outputs and are not committed.

## Limitations and next step

This methodology locks the original-period estimator and raw outcome construction. It does not validate the independent Section 301 legal mapping, its statutory rates, or its day-weighted calendar. It does not estimate the February 2025 event. The next scientific phase is to build and verify a versioned 2025 legal-policy ledger, then preregister and estimate the forward event study only after that policy gate passes.
