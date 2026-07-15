# HTS Policy Sources

## Machine-Readable-First Source Map

Primary metadata source:
- `https://hts.usitc.gov/reststop/releaseList`
- `https://hts.usitc.gov/reststop/currentRelease`
- `https://hts.usitc.gov/reststop/releaseDetails?release=<release>`

Primary historical revision index source (machine-readable links):
- `https://www.usitc.gov/harmonized_tariff_information/hts/archive/list?page=<n>` (current implementation paginates `n=0..7`)

Primary working machine-readable source for the current HTS release:
- `https://hts.usitc.gov/reststop/ranges?docNumber=<chapter>`
- `https://hts.usitc.gov/reststop/exportList?from=<start>&to=<end>&format=CSV&styles=true`

Annual baseline candidate source:
- `https://www.usitc.gov/tariff_affairs/documents/tariff_data/tariff_data_<year>.zip`

Archive machine-readable patterns observed from the index:
- `https://www.usitc.gov/sites/default/files/tata/hts/hts_<year>_<edition>_<format>.<ext>`
- `https://www.usitc.gov/sites/default/files/tata/hts/hts_<year>_<edition>_data.<ext>`

Archive PDF fallback pattern:
- `https://hts.usitc.gov/reststop/file?release=<release>&filename=finalCopy`

Machine-readable UI export fallback page:
- `https://hts.usitc.gov/view/release?release=<release>`

## Current Implementation

- The pipeline now treats HTS `reststop` endpoints as the canonical release catalog source.
- It uses `ranges` plus `exportList` to download machine-readable CSV chapter exports for the current HTS release.
- It builds annual ZIP candidates for each requested year.
- It paginates `usitc.gov/.../archive/list` to build an explicit machine-readable revision index.
- It maps release metadata onto indexed archive links when possible, and downloads unmatched indexed links by year as well.
- If indexed CSV/JSON/XLSX files are unavailable for a release, it attempts UI-export fallback (`from=0101.00.0000`, `to=9999.99.9999`, format=CSV) before PDF fallback.
- It uses Selenium browser fallback for annual ZIP and archive machine-readable downloads when direct HTTP is blocked.
- It uses archive full-edition PDFs as fallback when archive machine-readable retrieval is unavailable.

## Coverage Notes

- Requested years in this repo run: `2013, 2014, 2015, 2016, 2017, 2018, 2019`
- Current HTS release at fetch time: `2026HTSRev5` / `2026 HTS Revision 5`
- Releases in catalog for requested years: `46`
- Archive machine-readable links indexed for requested years: `85`
- Annual ZIP URLs returning success on HEAD probe: `0`
- Archive machine-readable URLs returning success on HEAD probe: `0`

## Retrieval Findings

- `exportList` is a working machine-readable endpoint on `hts.usitc.gov`.
- `ranges?docNumber=<chapter>` returns the correct start/end bounds for chapter-level exports.
- The current HTS frontend does not pass a release identifier into `exportList`; testing release/session variants produced identical output, so the endpoint should be treated as current-release only.
- Direct downloads from `www.usitc.gov/tariff_affairs/...` and `www.usitc.gov/sites/default/files/...` can be blocked from this environment with `Access Denied` responses.
- The archive-list pages provide a much better link-discovery surface than deterministic filename guessing.
- Annual ZIP downloads are recoverable through Selenium browser sessions in this environment.
- Archive full-edition PDFs remain retrievable via `reststop/file?release=<release>&filename=finalCopy`.

Known HTML-only releases from the current ruleset:
- `2017 HTSA Basic Edition`
- `2017 HTSA Revision 1 Edition`

Likely HTML-only releases requiring manual confirmation:
- `2018 HTSA Revision 1.2 (Effective Date 03/01/2018)`
- `2018 HTSA Revision 4.1`
- `2018 Revision 1.1`

Candidate machine-readable releases in the requested window:
- `37` releases marked as likely CSV/XLS/JSON candidates

## Important Caveat

- In this environment, the current-release machine-readable endpoints on `hts.usitc.gov` are retrievable.
- Direct GET downloads from `www.usitc.gov/sites/default/files/...` and the annual ZIP pattern can still return `403 Access Denied`.
- The downloader retries blocked annual/archive URLs via Selenium and records both direct-request and browser-attempt outcomes in the manifest.
- For releases where archive machine-readable files still cannot be obtained, PDF fallback remains active.

## PDF-to-CSV Fallback Runbook (2017-2019)

Goal:
- Convert archive full-edition PDF releases into row-level CSV files.
- Feed those extracted rows into HS8 x 9903-rule scope reconstruction.

Command (batch extraction):
- `uv run --with pandas --with pymupdf python scr/passthru_data/extract_hts_pdf_to_csv.py --batch --start-year 2017 --end-year 2019 --fallback-only`

Inputs:
- PDF files: `data/raw/passthru_data/policy/archive/pdf/*.pdf`
- Revision catalog and machine-status files:
  - `data/reference/passthru_data/policy_release_catalog.csv`
  - `data/verification/passthru_data/policy_machine_vs_pdf_status_2017_2019.csv`

Outputs:
- Extracted CSV rows:
  - `data/staging/passthru_data/policy/pdf_extract/<release>_extracted_rows.csv`
- Extraction manifest:
  - `data/staging/passthru_data/policy/pdf_extract/pdf_extract_manifest_2017_2019.csv`

Latest batch status:
- attempted releases: `42`
- extracted (`ok`): `35`
- skipped (machine-readable already available): `7`
- failed: `0`
- total extracted rows: `690,670`

Integration into panel build:
- `scr/passthru_data/build_us_products_partner_panel.py` now uses:
  1. machine-readable scope links
  2. PDF-extracted-CSV links (fallback)
  3. direct PDF parser links (fallback)

## Carry-Forward Rule (No Policy Change)

Implemented in:
- `scr/passthru_data/build_hts_monthly_schedule.py`
- function: `_forward_fill_hs8_rates(...)`

Rule:
- Within each `hs8`, after monthly expansion and source-priority resolution, if `mfn_ad_val_rate` is missing in month `t`, carry forward the last observed non-missing ad-valorem rate from prior months.
- This prevents sparse revision rows (especially archive CSV/PDF extracts) from wiping out a valid baseline tariff between policy changes.

Tracking fields added to schedule output:
- `mfn_ad_val_rate_observed` (raw pre-fill value)
- `mfn_ad_val_rate_ffilled` (boolean flag for carry-forward imputation)

Key implementation notes:
- Direct PDF parsing in panel build requires `PyMuPDF` (`fitz`). If unavailable, direct PDF links become empty and overlay coverage drops materially.
- Current source tag in the built panel is `tw_scope_source_raw = machine_or_pdf` (not yet split into `machine` vs `pdf_csv` vs `pdf_parser`).

Measured impact (current raw-only build, 2017-2019 bilateral audit window, `cty_code > 0`):
- baseline rows: `4,208,227`
- rows with our rate: `4,199,002`
- mean absolute diff (non-sentinel): `0.01436`
- share within 1pp (non-sentinel): `85.41%`
- share within 5pp (non-sentinel): `93.97%`
- active baseline rows: `127,090`
- active rows captured by our overlay: `84,744`
- active true positives: `45,000`
- active recall: `35.41%`
- active precision: `53.10%`
- missing active rows: `82,090`

Interpretation:
- PDF processing is operational and integrated into the policy build.
- Remaining gap is now concentrated in trade-war scope and timing logic (especially country applicability/exclusions), not in baseline MFN coverage.
