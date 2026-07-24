# Replication of Fajgelbaum and Khandelwal’s 2025 tariff analysis

## Scope

This pipeline reproduces the import pass-through analysis in Pablo Fajgelbaum and Amit Khandelwal, *Tariffs in 2025: Short-Run Impacts on the U.S. Economy* (March 2026). The target is the import portion of Figure 4, Table 4, and the associated fixed-effect and horizon robustness exercises. Export retaliation, structural welfare calculations, and domestic-sector outcomes are outside this pipeline.

The authoritative paper is [the March 2026 BPEA manuscript](https://akhandelwal8.github.io/files/bpea_TW2025/brookings_57.pdf). No public authors’ replication package was available when this implementation was written. The implementation therefore treats the published equations and sample definitions as the specification authority, compares the quarterly estimates with reported table entries, and compares the event-study figures visually. It does not claim an exact machine-readable comparison with unpublished figure data.

This implementation supersedes the earlier extension that imposed a common February 2025 statutory event and estimated the 2020 paper’s distributed-lag regression. The replacement follows the new paper: realized Census duties determine event treatment, clean staggered local projections describe dynamics, and quarterly applied-tariff changes are instrumented with statutory changes for the preferred pass-through estimates.

## Census import construction

The raw inputs are the [Census Monthly International Trade Database](https://www.census.gov/foreign-trade/data/) archives, `IMDBYYMM.ZIP`. The fixed-width `IMP_DETL.TXT` member contains imports for consumption by origin, ten-digit HTS code, rate provision, district, and month.

The fields used are:

| Raw field | Meaning | Role |
|---|---|---|
| `rate_prov` | Two-digit rate provision | Preserved before aggregation |
| `con_qy1_mo`, `con_qy2_mo` | Consumption quantities | Quantity and unit values |
| `con_val_mo` | Consumption customs value | Import value and tariff denominator |
| `dut_val_mo` | Dutiable consumption value | Audit only |
| `cal_dut_mo` | Recorded calculated duty | Applied tariff and duty-inclusive price |
| `con_cha_mo`, `con_cif_mo` | Consumption charges and CIF | Diagnostics |

General-import fields are retained for reconciliation diagnostics but do not enter the paper outcomes. The superseded extension incorrectly paired general-import value and quantity with consumption duties.

Rows are first collapsed to origin–HS10–rate-provision–month and then to the paper’s origin–HS10–month variety. Let \(V_{igt}\) denote consumption customs value, \(Q_{igt}\) the first consumption quantity, and \(D_{igt}\) recorded calculated duties:

\[
\tau^{applied}_{igt}=\frac{D_{igt}}{V_{igt}},
\qquad
p_{igt}=\frac{V_{igt}}{Q_{igt}},
\qquad
p^{duty}_{igt}=\frac{V_{igt}+D_{igt}}{Q_{igt}}.
\]

Value must be positive for tariff and log-value outcomes. Quantity must be positive for both unit-value outcomes. Missing and observed-zero quantities remain distinct.

Rate provision 79 identifies Chapter 99 entries for which Census does not report calculated duties. The pipeline does not impute an unobserved duty. The canonical all-provision measure retains provision-79 import value in the denominator and sums the duties Census actually records. It also records the provision-79 value share and flags the measure as incomplete. A separately labeled sensitivity excludes provision-79 value and duties from both numerator and denominator. The sensitivity is not silently substituted for the canonical measure.

The builder processes:

- January 2017–December 2019 for the harmonized first-trade-war comparison;
- January 2024–December 2025 for the 2025 episode.

Each source archive and output partition receives a SHA-256 fingerprint. Consumption value, quantity, and recorded duties must reconcile within \(\max(1,10^{-8}|T|)\). Canonical outputs are month-partitioned ZSTD Parquet under `data/processed/trade/fk2025/`.

## Applied and statutory tariffs

Applied tariffs are the primary treatment:

\[
\log(1+\tau^{applied}_{igt})
=\log\left(1+\frac{D_{igt}}{V_{igt}}\right).
\]

This incorporates exemptions, USMCA compliance, shipment timing, exclusions, and other realized entry behavior visible in collected duties. It avoids assigning a single statutory rate to entries governed by different provisions.

The independently constructed statutory schedule is retained as an instrument for the quarterly regressions. It is built from [USITC HTS schedules and revisions](https://hts.usitc.gov/), [USTR Section 301 actions](https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions), official Section 232 and IEEPA actions, reciprocal schedules, and Annex-II exclusions. Local copies and hashes of the operative schedules are recorded in the policy manifest. Mid-month rates are calendar-day weighted:

\[
\bar\tau^{stat}_{igt}
=\frac{1}{D_t}\sum_{d\in t}\tau^{stat}_{igd}.
\]

The effective day is included among days in force. Entry-dependent metal content, USMCA compliance, in-transit status, quota-tier status, and similar ambiguities cannot be recovered from an origin–HS10–month cell. They remain explicit flags; no authors-package tariff is used to fill them.

The quarterly analysis therefore reports two instruments:

- `paper_coverage` is the primary paper-comparable instrument. It retains every trade row with a mapped ordinary rate, applies all observable published partner/product schedules, and omits only the unobservable entry-level component while preserving its ambiguity flags.
- `deterministic` is a strict sensitivity. It retains only simple ad-valorem ordinary rates, observable flat partner schedules, no complex content/certification scope, and no unverified inherited Section 201/232 component.

Applied-duty event studies do not depend on either statutory convention.

## Event-study design

For each variety, let \(\bar\tau_{ig,0}\) be its maximum applied tariff during the pre-war year: 2017 for the first episode and 2024 for the second. Treatment begins in the first month satisfying

\[
\tau^{applied}_{igt}>\bar\tau_{ig,0}+0.02.
\]

Treatment is absorbing even if the observed tariff subsequently declines. For post-treatment horizons, newly treated varieties are compared with clean controls satisfying \(D_{i,t+h}=0\). For pretrend horizons \(h\leq-2\), the reference LP-DiD implementation requires controls to remain untreated at the event base month \(t\); \(h=-1\) is normalized:

\[
y_{ig,t+h}-y_{ig,t-1}
=\alpha_{gt}+\alpha_i+\beta_h\Delta D_{igt}+\varepsilon_{igt}.
\]

The regressions include HS10-by-base-month and origin fixed effects. Standard errors are clustered by origin and HS8. Outcomes are \(100\log(1+\tau^{applied})\), \(100\log V\), \(100\log p\), and \(100\log p^{duty}\).

The paper-window 2018–19 series uses horizons \([-6,+12]\). The paper-window
2025 series uses \([-6,+6]\). Horizon \(-1\) is the normalized zero reference
because \(y_{t-1}-y_{t-1}=0\). Figure 4B estimates the treated cohorts
separately for China and the rest of the world; China means Census origin code
`5700`, without combining Hong Kong. Both subgroup regressions retain the full
clean-control universe. Filtering to China before applying product-time fixed
effects would leave only one origin in a product-time cell and eliminate the
identifying variation.

The appendix also reports a separately fingerprinted horizon extension. For
the 2018–19 episode, treatment cohorts remain restricted to January 2018
through December 2019, while outcomes are extended through December 2021 and
estimated through \(+24\). Later tariff episodes cannot enter the 2018
treatment cohort. For 2025, the requested axis extends through \(+12\), but
local Census archives end in December 2025. The estimator reports every
supported horizon through \(+11\) and records \(+12\) as right-censored:
the \(+12\) risk set contains zero treated observations. It never manufactures
a \(+12\) observation or imports an outcome from 2026.

## Quarterly pass-through

The paper benchmark uses 2024Q1–2025Q4, with the final reported quarter containing January–November source data. Values, quantities, and duties are summed within origin–HS10–quarter. Unit values and applied tariffs are reconstructed from the quarterly sums. The statutory rate is the consumption-value-weighted mean of the day-weighted monthly schedule.

On a common complete sample, the preferred regression is

\[
\Delta\log y_{igt}
=\alpha_{gt}+\alpha_i+
\beta\Delta\log(1+\tau^{applied}_{igt})+\varepsilon_{igt},
\]

where \(\Delta\log(1+\tau^{applied})\) is instrumented by \(\Delta\log(1+\tau^{stat})\). Fixed effects are HS10-by-quarter and origin; standard errors are clustered by origin and HS8.

The five reported columns are the first stage, import value, quantity, pre-duty unit value, and duty-inclusive unit value. With a common sample,

\[
\widehat\beta_{p^{duty}}=1+\widehat\beta_p
\]

up to numerical precision. A second, separately labeled robustness result uses the complete December 2025 data. It is not substituted for the paper-cutoff benchmark.

The alternative-horizon exercise uses period-over-period changes for monthly, quarterly, and semiannual data. Its annual result follows Table A.3 by taking year-over-year changes in quarterly observations (for example, 2025Q1 minus 2024Q1); it does not collapse the data to two annual observations.

## Validation gates

The pipeline reports separate gates:

1. Source archives and consumption totals reconcile.
2. Rate-provision and variety keys are unique.
3. Applied-tariff descriptive aggregates agree with the paper.
4. Every event curve contains its registered horizons and clean comparison groups.
5. Event curves are checked for the paper’s horizons, normalization, sign, scale, and published qualitative landmarks; exact vector comparison remains unavailable because the authors have not published the underlying figure data.
6. Table 4 estimates are compared with the reported coefficients and standard errors.
7. The quarterly first stage must be relevant; failures of the statutory instrument do not invalidate the applied-tariff event study.

No sample, treatment threshold, or statutory component may be changed merely to improve agreement.

## Empirical results

The completed event grid contains 16 curves and 232 horizon-specific regressions. Every curve passes its checkpoint, fingerprint, horizon, and finite-standard-error validation. At horizon \(h=6\), the principal 2025 estimates (percentage log points) are:

| Treated cohort | Applied tariff | Import value | Pre-duty price | Duty-inclusive price |
|---|---:|---:|---:|---:|
| All partners | 11.390 | -22.444 | -1.239 | 10.157 |
| China | 19.321 | -54.529 | -0.176 | 19.148 |
| Rest of world | 9.833 | -16.368 | -1.670 | 8.166 |

These reproduce the paper’s published qualitative landmarks: a much larger import contraction from China than from the rest of the world, little short-run foreign-price adjustment, and near-complete pass-through into duty-inclusive prices. Exact numerical event-curve validation is unavailable because the authors have not published machine-readable Figure 4 vectors.

### Applied-tariff descriptive check

For December 2025, the canonical all-provision applied rates are 9.416% for all origins, 31.065% for China, and 3.836% for Canada and Mexico, compared with the paper’s reported 9.6%, 31.7%, and 3.9%. Provision 79 accounts for 1.740%, 3.298%, and 0.516% of import value in those groups. Excluding provision 79 produces 9.583%, 32.125%, and 3.856%; these are reported as sensitivity values, not the baseline.

### Table 4 comparison

The primary paper-cutoff sample uses January 2024 through November 2025:

| Outcome | Paper estimate (SE) | Replication estimate (SE) |
|---|---:|---:|
| First stage | 0.42 (0.06) | 0.376 (0.052) |
| Import value | -1.81 (0.50) | -1.922 (0.552) |
| Quantity | -1.71 (0.54) | -1.888 (0.622) |
| Pre-duty unit value | -0.10 (0.06) | -0.033 (0.080) |
| Duty-inclusive unit value | 0.90 (0.06) | 0.967 (0.080) |

The replication has 1,224,214 observations versus 1,192,687 in the paper and a first-stage \(F\)-statistic of 51.51 versus 52.4. The first stage, value, and quantity estimates lie within one reported paper standard error. The two price coefficients are 0.0668 from their published values, narrowly exceeding the registered 0.0600 tolerance. Their accounting identity holds exactly:

\[
\widehat\beta_{p^{duty}}-\widehat\beta_p=1.
\]

The registered paper-cutoff Table 4 gate is therefore recorded as **failed, narrowly**, not overwritten by a qualitative judgment. With complete December 2025 data, the corresponding estimates are \(-0.044\) for pre-duty price and \(0.956\) for duty-inclusive price; that separately labeled robustness passes all registered coefficient and sample checks.

The strict deterministic sensitivity has only 369,936 observations. It is useful for bounding the role of entry-dependent statutory components but is too selective to stand in for the paper-comparable sample.

### Alternative change horizons

For the pre-duty-price coefficient in Table A.3, the paper-coverage November estimates are:

| Frequency | Paper | Replication | Replication N | First-stage F |
|---|---:|---:|---:|---:|
| Monthly | -0.05 | 0.085 | 2,671,567 | 101.71 |
| Quarterly | -0.10 | -0.033 | 1,224,214 | 51.51 |
| Semiannual | -0.15 | -0.155 | 644,108 | 22.30 |
| Annual | -0.08 | -0.014 | 669,849 | 18.42 |

The semiannual result is extremely close. The annual first stage and observation count remain materially different from the paper’s reported \(F=1421.4\) and \(N=532{,}390\). The annual implementation follows the paper’s stated year-over-year quarterly difference, but this discrepancy remains unresolved and is not presented as a replicated robustness result.

## Cumulative monthly pass-through

The binary-treatment local projections above trace cumulative outcome
responses; their coefficients are not pass-through elasticities. We therefore
add a continuous cumulative local-projection IV. For episode base month \(t\)
and horizon \(h\geq0\), the directly estimated duty-inclusive equation is

\[
100\left[\log p^{duty}_{ig,t+h}-\log p^{duty}_{ig,t-1}\right]
=\alpha_{gt,h}+\alpha_{i,h}
+\beta_h\,100\left[
\log(1+\tau^{applied}_{ig,t+h})
-\log(1+\tau^{applied}_{ig,t-1})\right]
+\varepsilon_{ig,t,h}.
\]

The cumulative applied-tariff change is instrumented by the same endpoint
change in the independently constructed statutory tariff. Fixed effects are
HS10-by-base-month and origin, and standard errors are clustered by origin
and HS8. This differs from an IV for the initial tariff shock: if statutory
rates change again within the horizon, those changes enter both cumulative
endpoints.

For computational reproducibility, the two high-dimensional fixed effects
are absorbed by alternating projections after recursive singleton removal.
The resulting scalar, exactly identified 2SLS coefficient is computed from
the residualized outcome, applied tariff, and statutory instrument.
Inference uses the two-way CRV1 score covariance (origin plus HS8 minus their
intersection) and a \(t\) critical value based on the smaller cluster count.
A synthetic equivalence test compares this implementation with the
formula-based reference estimator; the point estimates agree to numerical
precision and clustered standard errors must agree within two percent.

Every horizon uses the identical complete sample for pre-duty and
duty-inclusive prices. Because the raw Census fields imply

\[
\log p^{duty}_{igt}
=\log p_{igt}+\log(1+\tau^{applied}_{igt}),
\]

the pre-duty coefficient is exactly
\(\widehat\beta^p_h=\widehat\beta^{duty}_h-1\), with an identical standard
error. The pipeline estimates the duty-inclusive equation directly, derives
the pre-duty coefficient from the identity, and rejects any sample for which
the row-level identity fails at \(10^{-8}\).

For 2018, base months run from January 2018 through December 2019 and outcomes
are followed through \(h=24\). The independent paper-compatible statutory
ledger ends in December 2019. For 2020--21 endpoints, each
origin--HS10's last nonmissing December-2019 statutory rate is held fixed;
unmapped rates remain missing. For 2025, base months run from January through
December 2025 and the independent day-weighted
`statutory_paper_coverage_rate` is used. Local outcomes support horizons
\(h=0,\ldots,11\); no \(h=12\) coefficient is imputed.

All 37 horizon checkpoints validate and no first-stage \(F\)-statistic is
below 10. In the 2018 episode, duty-inclusive cumulative pass-through equals
0.940 (SE 0.096) at \(h=0\), 1.090 (0.057) at \(h=6\), 1.201 (0.057) at
\(h=12\), and 1.088 (0.052) at \(h=24\); the minimum first-stage \(F\) is
125.0. For 2025, the corresponding coefficients are 1.072 (0.063) at
\(h=0\), 1.037 (0.085) at \(h=3\), 1.018 (0.104) at \(h=6\), and 1.113
(0.182) at \(h=8\). They fall to 0.637 at \(h=9\) and 0.365 at \(h=11\),
but the effective sample contracts from 1,465,779 to 101,343 observations;
the \(h=11\) interval is \([-0.340,1.069]\). The supported 2025 evidence
through month eight is therefore consistent with near-complete pass-through.
The later decline is a right-tail estimate with a changing risk set and wide
uncertainty, not a precise estimate of tariff reversal.

## Commands

```powershell
.venv\Scripts\python.exe -m scr.data_construction.extension_2025 --inventory-only
.venv\Scripts\python.exe -m scr.data_construction.extension_2025
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --preflight-only
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --run-event
.venv\Scripts\python.exe -m scr.data_construction.extension_2025 --build-event-horizon-extension
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --run-event-extension
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --finalize-event-extension
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --run-iv --iv-instrument-scope paper_coverage
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --run-iv --iv-instrument-scope deterministic --iv-cutoff paper
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --finalize-only
.venv\Scripts\python.exe -m scr.pass_through.cumulative_lp_iv --build-panels
.venv\Scripts\python.exe -m scr.pass_through.cumulative_lp_iv --run
.venv\Scripts\python.exe -m scr.pass_through.cumulative_lp_iv --finalize-only
```

The same stages are available through:

```powershell
.venv\Scripts\python.exe master_pipeline.py --extension-2025 --estimate-extension-2025
.venv\Scripts\python.exe master_pipeline.py --cumulative-pass-through
```
