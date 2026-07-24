# Fajgelbaum–Khandelwal 2025 import pipeline

`scr.data_construction.extension_2025` parses the Census imports-for-consumption fields and preserves the two-digit rate provision before aggregating to origin–HS10–month. It writes rate-provision and variety partitions under `data/processed/trade/fk2025/`.

`scr.data_construction.policy_extension_2025` constructs independently sourced statutory instruments under `data/processed/tariffs/fk2025/`. Applied tariffs from recorded Census duties remain the event-study treatment. Statutory rates are used only for the quarterly IV and explicitly retain entry-dependent ambiguity flags.

The workhorse exposes two statutory variables. `statutory_paper_coverage_rate`
is the primary paper-comparable instrument: it preserves the broad trade sample,
applies observable published schedules, and omits unobservable entry-level
components while retaining flags. `statutory_deterministic_rate` is a strict
sensitivity that retains only simple and fully deterministic rates. The alias
`statutory_total_rate` resolves to the paper-coverage instrument for backward
compatibility; new code should use the explicit name.

`scr.pass_through.extension_2025` provides:

- clean staggered-treatment local projections for 2018–19 and 2025;
- a separately checkpointed event-horizon extension to requested horizons
  \(+24\) for 2018--19 and \(+12\) for 2025;
- China/rest-of-world 2025 event curves;
- the quarterly Table 4 import IV;
- fixed-effect and alternative-horizon robustness results;
- paper-cutoff and complete-December outputs.

The public stages are `--preflight-only`, `--run-event`, `--run-iv`, and
`--finalize-only`. Every curve is checkpointed with source, estimator, and
specification fingerprints. Machine-readable artifacts are ZSTD Parquet; CSV
is used only for compact paper comparisons.

The extra 2020--21 consumption partitions are built with
`scr.data_construction.extension_2025 --build-event-horizon-extension` under
`data/processed/trade/fk2025_event_horizon_extension/`. They never alter the
paper-window partition fingerprint. Extended fits use
`--run-event-extension` and `--finalize-event-extension`. The 2018 treatment
window is frozen to 2018--19. The 2025 request is right-censored because local
outcomes end in December 2025: \(+11\) is the last supported coefficient and
\(+12\) has zero treated observations. Unsupported horizons are recorded in
the manifest rather than imputed.

The event checkpoint fingerprint is deliberately separate from the quarterly-IV fingerprint: report or robustness-code edits cannot invalidate scientifically unchanged event fits. LP-DiD post-treatment controls satisfy \(D_{i,t+h}=0\); pretrend controls remain untreated at event base \(t\), and \(h=-1\) is normalized. China and rest-of-world curves select different newly treated cohorts while retaining the full control pool required by product-time fixed effects. The annual Table A.3 horizon is a year-over-year change in quarterly observations.

The superseded common-February event and statutory distributed-lag estimators are not part of this pipeline.

## Cumulative local-projection IV pass-through

`scr.pass_through.cumulative_lp_iv` is a separately checkpointed complement
to the event-study figures. The event-study coefficient is a cumulative
outcome response to a binary newly-treated indicator; it is not itself a
pass-through elasticity. The cumulative LP-IV instead estimates, at each
nonnegative horizon \(h\),

\[
100\{\log p^{duty}_{ig,t+h}-\log p^{duty}_{ig,t-1}\}
=\alpha_{gt,h}+\alpha_{i,h}
+\beta_h\,100\{\log(1+\tau^{applied}_{ig,t+h})
-\log(1+\tau^{applied}_{ig,t-1})\}+\varepsilon_{ig,t,h}.
\]

The cumulative realized-tariff change is instrumented with the corresponding
cumulative independently constructed statutory change. Fixed effects are
HS10-by-base-month and origin; standard errors are clustered by origin and
HS8. Every horizon uses a common sample with positive pre-duty and
duty-inclusive unit values at both endpoints. Consequently
\(\log p^{duty}=\log p+\log(1+\tau^{applied})\) holds row by row, and the
pre-duty coefficient is exactly \(\widehat\beta^p_h
=\widehat\beta^{duty}_h-1\), with the same standard error. The code estimates
the duty-inclusive equation directly and validates this identity before
writing a checkpoint.

The historical specification uses the independent paper-compatible
statutory clock. The ledger is observed through December 2019; for outcomes
in 2020--21, each partner--HS10's last nonmissing December-2019 rate is held
fixed. Unmapped rates remain missing. This isolates persistence of the
historical episode rather than inferring later statutory actions. The 2025
specification uses the `statutory_paper_coverage_rate` day-weighted legal
schedule. It is estimated through \(h=11\), the last horizon supported by
local outcomes ending in December 2025.

Run the standalone stages with:

```powershell
.venv\Scripts\python.exe -m scr.pass_through.cumulative_lp_iv --build-panels
.venv\Scripts\python.exe -m scr.pass_through.cumulative_lp_iv --run
.venv\Scripts\python.exe -m scr.pass_through.cumulative_lp_iv --finalize-only
```

The same operations are available through
`scr.pass_through.extension_2025` and `master_pipeline.py
--cumulative-pass-through`.

## Completed empirical state

- 60 Census months were parsed: 2017--2019 and 2024--2025.
- The trade panel contains 15,727,822 origin--HS10--month rows and
  21,423,674 origin--HS10--rate-provision--month rows.
- The event grid contains 16 curves and 232 independently resumable horizon
  fits. All validate.
- At \(h=6\), the all-origin 2025 estimates are 11.390 tariff points,
  -22.444 value log points, -1.239 pre-duty-price points, and 10.157
  duty-inclusive-price points.
- The paper-cutoff quarterly IV contains 1,224,214 observations and has a
  first-stage \(F=51.51\). It reproduces the value and quantity elasticities
  closely. The pre-duty and duty-inclusive coefficients are each 0.0668 from
  their paper values, narrowly outside the registered 0.0600 tolerance.
- The complete-December robustness passes all registered Table 4 checks.
- The strict deterministic sensitivity is disclosed separately because its
  369,936-observation sample is not paper-comparable.
- The annual Table A.3 robustness remains unresolved; it must not be described
  as replicated.
- The cumulative LP-IV grid contains 37 validated horizons: \(0\)--\(24\)
  for 2018 and \(0\)--\(11\) for 2025. No first-stage \(F\) is below 10.
- Historical duty-inclusive pass-through is 0.940 on impact, 1.090 at six
  months, 1.201 at twelve months, and 1.088 at twenty-four months.
- The 2025 coefficient remains close to one through month eight. Estimates at
  months 9--11 fall, but the effective sample contracts to 101,343 by the
  endpoint and its confidence interval includes complete pass-through.

The canonical result manifest is
`data/processed/trade/regressions/fk2025/manifest.json`. A status of
`complete_with_failed_quarterly_iv_gate` means that the event grid is complete
but the paper-cutoff quarterly coefficient gate failed; it does not mean that
estimation stopped.
