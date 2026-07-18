# Passthru Pipeline

## Purpose

`scr/pipeline_passthru_data.py` rebuilds the passthrough data layer from raw sources and now includes raw-only tariff reconstruction for import-policy regressions.

The pipeline currently covers:

- Census monthly import and export raw trade archives
- BLS CPI observations for the CPI series referenced by the replication crosswalk
- HS10 and HS6-BEC concordance tables
- HTS policy-source catalog metadata from first-source USITC endpoints
- Minimal monthly trade panels for imports and exports
- Raw-only bilateral policy panel with baseline + revision overlays
- Imports workhorse regression panel constructed from raw bilateral policy outputs
- Validation diagnostics against `data/fajgelbaum/data/analysis`

It still does not fully reconstruct every replication field for both flows. The remaining gaps are mostly on the exports side and on exact replication identifiers/hits. Missing or approximate fields include:

- `x_hit`, `x_stattariff1`, `x_mfn_tariff`, `x_ess`
- exact replication `hit` partitions for imports
- exact replication `id` coding (imports uses deterministic raw-construction ids)

## Step Order

The CLI runs these steps in order unless `--only-step` is used:

1. `download_trade`
2. `download_cpi`
3. `download_concordances`
4. `download_policy_sources`
5. `download_policy_updates`
6. `build_hs10_codes`
7. `build_hs6_bec`
8. `build_cpi_hs6x`
9. `build_trade_panels`
10. `build_imports_with_package_shocks`
11. `build_rerouting_controls`
12. `run_rerouting_regressions`
13. `audit_trade_regression_sources`
14. `build_trade_workhorse_panels`
15. `run_trade_regressions`
16. `plot_trade_regressions`
17. `verify_data`

Archived raw-policy reconstruction steps:
- `download_policy_sources`
- `download_policy_updates`
- `build_hts_monthly_schedule`
- `build_tradewar_overlay_raw`
- `build_us_products_partner_hs10_panel`

These archived steps run only with `--enable-archived-policy-pipeline`.

`build_tradewar_overlay_raw` is a narrow repair/revalidation step. It rebuilds
`data/analysis/passthru_data/tradewar_overlay_raw.parquet` from the hydrated raw
trade-war source caches, reapplies that overlay to
`us_products_partner_hs10_monthly.parquet`, and recomputes the raw statutory tariff
columns without rebuilding the full HS10 product-partner panel.

## Raw-Only Policy Logic (Current Standard)

The intended policy construction flow is:

1. Download source files (machine-readable first, PDF fallback only when machine-readable is unavailable).
2. Process PDF fallbacks into structured rows and extract revision scope links.
3. Build baseline monthly tariffs from annual schedule + revisions.
4. Forward-fill within `hs8` when a month has no new numeric ad-valorem rate.
5. Build full bilateral policy panel.
6. Run diagnostics against the replication package.

This is now implemented as a raw-only policy path. Reference-package fallback is disabled for policy construction and workhorse regression panel construction.

Before any mutating step, the pipeline can also run an inventory-only pass with `--inventory-only`. That writes raw-data diagnostics and exits.

## Directories

- Raw downloads: `data/raw/passthru_data`
- Staging outputs: `data/staging/passthru_data`
- Reference outputs: `data/reference/passthru_data`
- Analysis outputs: `data/analysis/passthru_data`
- Validation artifacts: `data/verification/passthru_data`
- Replication reference: `data/fajgelbaum/data/analysis`
- HTS source documentation: `scr/docs/hts_policy_sources.md`

## Current Data Logic

### Trade

`scr/passthru_data/download_trade.py` discovers official Census monthly ZIP archives from the Census Foreign Trade pages, checks which ZIPs already exist locally, and downloads only missing files. Existing ZIPs are reused without re-download.

Each ZIP is parsed from fixed-width detail records into a monthly panel keyed by:

- `partner_code`
- `hs10`
- `year`
- `month`

The staged panel keeps:

- `trade_value`
- `quantity`
- HS hierarchy columns `hs8`, `hs6`, `hs4`, `hs2`
- `period`, `mdate`, `flow`

The built analysis files are:

- `m_flow_hs10_fm_new.parquet`
- `x_flow_hs10_fm_new.parquet`

The pipeline also writes validation-window subsets clipped to the overlap window ending in `2019-12`.

### CPI

`scr/passthru_data/download_cpi.py` seeds the CPI series universe from the Fajgelbaum `cpi_hs6x.dta` file because the broader BLS metadata path was unreliable in the working environment. It then requests observations from the BLS API over the requested year span and preserves the seeded human-readable descriptions whenever the API does not return usable catalog metadata.

This means the CPI extension is temporal, not a broader expansion of the item universe.

### Concordances

`download_concordances.py` builds:

- HS10 descriptions from raw trade concordance text files extracted from the Census ZIPs
- HS6 to BEC mapping from the WITS concordance ZIP

### HTS Policy Sources

`download_policy_sources.py` now builds a first-source source layer for U.S. import tariff reconstruction. It currently does six things:

- fetches HTS release metadata from `https://hts.usitc.gov/reststop/`
- paginates the USITC archive list at `https://www.usitc.gov/harmonized_tariff_information/hts/archive/list?page=<n>` to index machine-readable revision links
- fetches chapter bounds for the current HTS release from `https://hts.usitc.gov/reststop/ranges?docNumber=<chapter>`
- exports current-release chapter CSVs from `https://hts.usitc.gov/reststop/exportList`
- builds annual ZIP candidates using the `tariff_data_<year>.zip` URL pattern
- downloads archive CSV/XLS/XLSX/JSON revision files from the indexed archive-link catalog
- uses Selenium browser fallback for annual ZIP and archive machine-readable downloads when direct HTTP is blocked
- downloads archive full-edition PDF `finalCopy` files as fallback when archive machine-readable retrieval is unavailable

The step writes:

- `data/reference/passthru_data/policy_release_catalog.parquet`
- `data/reference/passthru_data/policy_current_release_ranges.parquet`
- `data/reference/passthru_data/policy_source_probes.parquet`
- `data/verification/passthru_data/policy_source_inventory.json`
- `data/verification/passthru_data/policy_source_downloads.json`
- `data/verification/passthru_data/policy_current_release_exports.json`

and documents the exact source map in `scr/docs/hts_policy_sources.md`.

In the current environment, the HTS `ranges` and `exportList` endpoints are retrievable for the current release, so the pipeline now downloads a full current-release chapter CSV snapshot under `data/raw/passthru_data/policy/current`. Archive full-edition PDFs are retrievable under `data/raw/passthru_data/policy/archive/pdf`.

Direct GET downloads from `www.usitc.gov/sites/default/files/...` and the annual ZIP pattern can still fail with `403 Access Denied`, but the Selenium fallback now downloads annual ZIP files in-browser where direct requests are blocked. Archive machine-readable updates are attempted with the same fallback; unmatched releases continue to use PDF fallback.

### Monthly HTS Schedule

`build_hts_monthly_schedule.py` constructs a monthly tariff schedule from:

- annual baseline ZIPs under `data/raw/passthru_data/policy/annual`
- archive machine-readable update files under `data/raw/passthru_data/policy/archive/data`
- parsed archive PDF fallbacks (only for releases flagged by `policy_source_downloads.json` as `archive_pdf_fallback`)

The builder normalizes rates, applies source priority (`archive_*` over annual baseline), expands effective-date intervals to monthly rows, and writes:

- `data/reference/passthru_data/hts_monthly_hs8_schedule.parquet`
- `data/reference/passthru_data/hts_monthly_hs10_schedule.parquet`

For 2017 fallback releases with no machine-readable export trigger (`basicCorrections2`, `NTE`), the builder now parses HTS lines from PDF using `pdfplumber` and caches parsed rows under `data/staging/passthru_data/policy/pdf_parsed/`.

### Partner-HS10 Monthly Panel

`build_us_products_partner_panel.py` merges imports and exports with the monthly HTS HS10 schedule to produce:

- `data/analysis/passthru_data/us_products_partner_hs10_monthly.parquet`
- bilateral statutory import tariff fields `m_statutory_tariff1` and `m_statutory_tariff2`
- bilateral statutory rates from raw annual HTS preference columns (FTA/GSP/CBI/AGOA/DR-CAFTA logic)
- trade-war overlay increments from machine-readable archive links first, PDF-derived links second, plus manual deterministic overrides when provided

The key is `cty_code × hs10 × year × month`, with trade values/quantities from both flows and tariff fields from the HTS schedule and raw overlays.

Note on `cty_code=-9999`:
- `WORLD` aggregate rows are retained in intermediate processing outputs for diagnostics/broadcasting logic.
- Regression-ready workhorse outputs explicitly exclude nonpositive country codes (`cty_code <= 0`), so `-9999` is not present in regression estimation panels.

### CPI to HS6 Crosswalk

`build_cpi_hs6x.py` now uses the replication-package `cpi_hs6x.dta` file as the canonical HS6-to-CPI mapping. The passthru pipeline therefore treats that crosswalk as a fixed research input rather than trying to reconstruct it heuristically from BLS metadata.

## Time Windows

Default pipeline window:

- start: `2013-01`
- end: `2019-12`

If `--latest-available` is set, the pipeline extends the build window to the latest likely complete month. Validation still remains clipped to the overlap window ending at `2019-12`, because the replication reference does not extend beyond that.

## Validation

`scr/passthru_data/verify_data.py` now does two distinct checks.

### Structural diagnostics

For each built dataset, it reports:

- row counts
- column overlap
- duplicate-key counts
- key overlap against the Fajgelbaum reference

### Overlap-window validation

For the common `2013-01` to `2019-12` window, it also reports:

- matched-row counts on shared keys
- shared numeric column comparisons
- year-level matched sums
- compact partner / HS aggregate samples
- warnings when replication-critical columns are absent

This validation is strong enough to check whether the passthrough pipeline is internally coherent and whether shared columns broadly line up with the reference files. It is not a substitute for later regression validation.

## Divergence Analysis

The current divergences against `data/fajgelbaum/data/analysis` come from three different sources. They should not all be treated as pipeline bugs.

### 1. Trade units differ by construction

The passthru trade panels keep raw Census-scale trade values and quantities. The replication package stores those same variables in millions.

Evidence:

- `data/fajgelbaum/code/main/fig_02_m_event.do` rescales `m_*` variables by `1000000`
- `data/fajgelbaum/code/main/fig_03_x_event.do` rescales `x_*` variables by `1000000`
- `data/fajgelbaum/simulation/load_raw_data_sectors_and_trade.m` sets `scale = 1e6` with the comment that the raw data are in millions of dollars
- Matched row samples show `m_q1`, `x_q1`, and most `m_val`, `x_val` pairs line up after multiplying the replication values by `1e6`

Implication:

- large numeric gaps in raw verification summaries are mostly a unit mismatch, not a source mismatch
- import values still show small gaps after rescaling because the replication package appears rounded at the million-scale representation

### 2. The replication trade files are estimation datasets, not raw-source mirrors

`m_flow_hs10_fm_new.dta` and `x_flow_hs10_fm_new.dta` already contain tariff and event variables that are absent from the passthru build. The Stata programs also immediately filter these files for estimation use.

Evidence:

- the replication README describes them as "workhorse estimation" datasets
- `data/fajgelbaum/code/main/tab_01_sumstats.do` loads only `m_hit==1`, positive trade, valid country codes, and event-date variables
- many replication scripts merge in `hs6_bec`, `cpi_hs6x`, `hs10_codes`, and other product-level covariates with `keep(master match)`

Implication:

- lower key overlap for trade rows is expected even if the raw Census parsing is correct
- remaining row-count differences can come from package-side filtering, prebuilt tariff/event merges, and decisions about which observations remain in the estimation sample
- current passthru trade panels are source-consistent enough for a raw-data stage, but they are not yet paper-ready substitutes

### 3. `cpi_hs6x` should be treated as a canonical package input

The replication README describes `cpi_hs6x.dta` as a manually constructed crosswalk between HS6 and BLS CPI final goods. The passthru pipeline now uses that file directly as the canonical mapping.

Implication:

- `cpi_hs6x` should no longer be interpreted as a reconstruction target
- CPI discrepancies should now be limited to series metadata and observation downloads, not the HS6-to-CPI mapping itself

### 4. Concordances behave differently from the CPI crosswalk

The two non-CPI concordances behave much better.

- `hs6_bec`: exact key match to the replication package
- `hs10_codes`: full reference coverage, but the passthru build contains additional HS10 rows beyond the packaged file

Implication:

- `hs6_bec` appears to match the original source and the replication package cleanly
- `hs10_codes` likely reflects a broader source universe than the package retained, rather than a parser failure

## Potential Problems To Resolve

Before moving to regressions, the main unresolved risks are:

- trade verification still compares raw passthru units to million-scaled replication units unless explicitly normalized
- staged CPI metadata can still degrade if the BLS API omits catalog metadata and we fail to preserve the seeded descriptions
- trade panels still lack tariff, status, hit, ESS, effective-date, and ID fields that define the estimation sample in the replication package
- `hs10_codes` may need an explicit package-style restriction rule if later steps assume the narrower packaged code universe

## Practical Interpretation

At this stage, the public-source passthru pipeline appears broadly consistent with the replication package for:

- raw trade values and quantities, once units are normalized
- `hs6_bec`
- the overlapping subset of `hs10_codes`

The main remaining replication gap is therefore not the raw-download stage. It is the package-specific enrichment layer:

- tariff and event construction
- estimation-sample restrictions
- any package-side rounding or storage conventions that should be mirrored for validation

## Operational Notes

- The trade downloader is resumable by file existence.
- The CPI downloader is effectively resumable at the staging-output level, but still refreshes the batch JSON payloads when run.
- `--inventory-only` is the safe first command after any long pause in work.
- The current verification report should be read as a gate before adding tariff-policy logic, not as evidence that the paper is already replicated.

## 2026-07-16 corrected package benchmark and bridge gate

The package cache was rebuilt with the shared HS10 normalizer after identifying
the numeric-Stata `.0` shift (`801001090.0` had previously become `8010010900`).
The corrected package/common key overlap is 4,199,002 rows. All eight package-only
import fits (four Figure 2 event outcomes and four Figure 4a dynamic outcomes)
have 13 aligned horizons. Against the frozen local vector-extraction reference,
the maximum absolute difference is 1.00962 log points, so the package/PDF gate
passes. The raw-outcome/package-policy bridge is complete but fails its gate for
CI overlap and/or distance in several outcomes; Section 301 v5 and the 2025
extension remain blocked.

## 2026-07-15 v5 replication milestone

- Detailed validation, key, and timing artifacts now have canonical ZSTD Parquet paths; legacy CSVs remain fallback-only compatibility artifacts.
- Pipeline ordering is represented by an explicit dependency DAG. `--only-step` runs exactly one step and reports missing prerequisites without silently running them.
- The package-only benchmark is written under `data/verification/passthru_data/trade_regressions/package_benchmark_v5/` and is deliberately independent of the raw Census key universe.
- Section 301 v5 owns a 60-fit/72-artifact grid and keeps the legal release gate false until the package/common-sample bridge is materialized and passes its stated thresholds.

## 2026-07-16 bridge diagnosis and extension inventory

Projection-based diagnostics now record package/raw outcome equivalence, staged sample
losses, confidence-interval overlap, and metric sensitivity under
`.../common_sample/bridge/diagnosis/`. Package value and quantity are million-scaled
relative to raw trade levels; prices are approximately unit-compatible. This scale
finding is recorded diagnostically and does not change estimator definitions. Price
and duty-price bridge gates remain failed, so Section 301 v5 estimation is paused.

The local trade inventory confirms 156 import and 156 export archives for every
month from 2013-01 through 2025-12, plus one auxiliary concordance per flow. The
archive-native extension v2 now contains all 312 ZSTD Parquet partitions with
zero monthly value, key, or quantity reconciliation failures and no
package-policy columns. Price and duty-price bridge metrics remain failed, so
Section 301 v5 estimation and the 2025 event regression remain blocked.

The archive-native validator passes a stratified set of 10 flow-months,
including 2013-01, 2018-07, 2019-04, 2020-01, and 2025-12. A complete 312-archive
scan is complete. Archive-native extension v2 covers all 312 flow-months and
preserves import duty fields separately; its staging comparison passes while
native concordance-vintage and CPI real-value gates remain pending.

### 2026-07-17 aligned bridge and extension v2

The canonical package manifest was re-finalized from the complete eight-fit
checkpoint grid and now records the passed PDF gate. A new import-only aligned
bridge has 4,197,758 identical package/raw keys with symmetric outcome masks;
the historical natural-sample bridge remains a separate failed diagnostic.
Archive-native extension v2 is being built separately with raw duty fields and
no package-policy columns. The 2025 policy layer remains preflight-only.

### 2026-07-16 bridge completion

The corrected resumable bridge completed all 16 package-common/raw-outcome
checkpoints under the current estimator fingerprint. Finalization now selects
exact fit IDs and uses the correct event/dynamic horizon columns. The registered
bridge gate remains failed: event/val and event/p fail CI overlap, event/pduty
fails the maximum-distance threshold, dynamic/p fails correlation and CI
overlap, and dynamic/pduty fails CI overlap. Section 301 v5 remains blocked.

### 2026-07-18 bridge v3 and source-level extension audits

The source-separated aligned bridge was rerun with one mode/specification held
in memory at a time. All 16 checkpoints validate and the finalizer reports the
registered metrics without changing thresholds. Five comparisons remain failed:
event/value and event/price fail confidence-interval overlap, event/duty-price
fails maximum distance and overlap, dynamic/price fails correlation and overlap,
and dynamic/duty-price fails overlap. Section 301 v5 remains blocked.

The bridge forensic audit finds package `lm_*` first differences equivalent to
the package-derived log differences within 1e-5 (maximum numerical gap below
2e-6). This rules out a log-variable reconstruction error as the explanation
for the remaining price discrepancies; outcome units, raw source rounding, and
registered CI behavior remain diagnostic questions.

The archive-native extension continues to reconcile all 312 local flow-months
to the raw-only staging projection with no policy columns. Native monthly HTS
description audits pass, while obsolete-code mapping and source-level quantity
token semantics remain separate gates. Nominal extension data may be used for
recent-period construction; the independent policy and 2025 event gates remain
false.

The package-common anchor contains 4,199,002 rows; the source-separated raw
panel contains 4,197,758, leaving exactly 1,244 package-common keys absent from
the raw panel. These keys are stored in a diagnostic ZSTD Parquet anti-join with
month, country, HS2, and HS4 summaries. They are not imputed or treated as
zeros.

### 2026-07-18 duty-inclusive-price correction

The paper package labels `m_pduty` as `(value + duty) / quantity`. The prior
raw bridge instead constructed this outcome as unit value times one plus the
package statutory tariff. The archive layout establishes that `dut_val_mo` is
dutiable value and `cal_dut_mo` is calculated duty; the corrected diagnostic
therefore uses `(trade_value + cal_dut_mo) / quantity` while keeping package
treatment timing and policy regressors fixed.

This correction reduces the event-study maximum duty-price gap from 3.27546 to
2.09774 log points. The dynamic duty-price bridge now passes all registered
metrics (correlation 0.99910, RMSE 0.08948, maximum gap 0.14298, CI overlap
0.81289). The event duty-price curve passes correlation, RMSE, maximum-gap, and
sign criteria but still fails CI overlap (0.71073). An exact coefficient
decomposition shows the remaining post-treatment event gap is predominantly
the raw pre-duty-price gap, not the realized-duty factor. The overall raw
outcome bridge therefore remains failed, and the independent policy gate is
unchanged.

### 2026-07-18 canonical v4 bridge and raw-outcome extension

The corrected v4 bridge is now the canonical diagnostic namespace. It contains
16 valid source-separated checkpoints and uses calculated Census duty in
`(trade_value + cal_dut_mo) / quantity`; v3 remains historical. The v4 gate
passes all point-estimate comparisons, event and dynamic duty-inclusive price,
and all quantity comparisons. It still fails event/value confidence-interval
overlap, event/pre-duty-price confidence-interval overlap, and dynamic/pre-duty
price Pearson correlation and interval overlap. No registered threshold was
changed.

The independent raw-outcome extension v1 contains 156 import partitions from
2013-01 through 2025-12 (39,640,207 rows), with nominal value, quantity
missing/zero flags, `dut_val_mo`, `cal_dut_mo`, and separately derived price
fields. It contains no package-policy variables. This is validated against the
archive-native staging partitions; ZIP-native reparse, cross-vintage
concordance, and CPI-real gates remain pending.
