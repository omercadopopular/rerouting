# Data Sources

## U.S. merchandise trade

Monthly import detail files are the U.S. Census Bureau fixed-width merchandise-trade archives. The import database is documented by the Census Bureau at [Import and Export Data](https://www.census.gov/foreign-trade/data/). The local source archives follow the Census `IMDBYYMM.ZIP` convention and contain `IMP_DETL.TXT` and a contemporaneous country lookup. The locked historical replication uses January 2017 through April 2019. The separately identified long-horizon specification uses archive-native observations through October 2020.

The import fields used are general customs value, general CIF value, first quantity, ordinary duty value, and calculated duty value. We retain nominal values as canonical. CPI files remain in the repository for future real-value applications but are not required for Figures 2 or 4a.

## MFN tariffs and the Harmonized Tariff Schedule

The baseline statutory tariff is constructed from United States International Trade Commission Harmonized Tariff Schedule releases. Current and archived HTS resources are available through the [USITC HTS service](https://hts.usitc.gov/). Structured HTS tables are used where available. A reviewed table extraction is used when a historical annex is available only as PDF. The source ledger records the local source, official URL, format, extraction method, and fingerprint.

## Section 201

The safeguard actions cover large residential washing machines and solar products. Product scope, legal dates, and rates are obtained from official proclamations, Federal Register notices, and USITC safeguard materials. The schedules are expanded to the applicable HS10-month-partner observations.

## Section 232

Steel and aluminum actions are reconstructed from official proclamations, Federal Register annexes, and the corresponding HTS modifications. The construction is partner specific: country exemptions and their effective dates are represented in the bilateral schedule rather than treated as product-only changes.

## Section 301

Lists 1--3 are reconstructed from the Office of the U.S. Trade Representative’s [Section 301 tariff actions](https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions) and their official annexes. HTS codes, rates, and effective dates are parsed from structured tables when present and reviewed PDF-table extractions otherwise.

## Authors' replication package

The Fajgelbaum et al. replication package is stored locally under `data/fajgelbaum`. It has two limited roles. First, its estimation data and Stata programs validate the Python estimator against the paper’s published Figures 2 and 4a. Second, the authors’ policy variables serve as a validation anchor. They are never labeled as independently reconstructed tariff data.

## Exclusions

Following the paper’s data appendix, the locked construction excludes antidumping and countervailing duties, tariff changes unrelated to the trade war, and the very small set of tariff-rate-quota increments that apply only after an entry-level quota threshold is crossed. Monthly partner-HS10 trade does not identify the tier used by each customs entry. The paper estimates that these quota-threshold cases cover roughly $16 million out of approximately $300 billion in targeted annual imports. Base HTS rates remain in the panel; only the unobservable quota-contingent increment is excluded.
