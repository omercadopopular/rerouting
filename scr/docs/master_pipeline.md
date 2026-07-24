# Master pipeline

The default `master_pipeline.py` command preserves the locked historical replication. `--extension-2025` adds construction of the Fajgelbaum–Khandelwal consumption/applied-tariff panel and statutory IV. `--estimate-extension-2025` runs the clean local-projection event studies and quarterly IV only after their distinct prerequisites pass.

The event gate depends on reconciled Census consumption data and recorded
duties. The quarterly IV additionally requires a constructed statutory
instrument. A statutory-instrument or Table 4 comparison failure cannot
overwrite or relabel the applied-tariff event result. The primary
`paper_coverage` instrument and strict `deterministic` sensitivity are distinct
empirical objects.

Extension outputs are versioned under `data/processed/trade/fk2025/`, `data/processed/tariffs/fk2025/`, `data/processed/trade/regressions/fk2025/`, and `figs/extension_2025/`.

The completed event grid has 16 curves and 232 horizon fits. The primary
January 2024--November 2025 quarterly IV is numerically close to Table 4 but
narrowly fails the registered one-paper-standard-error price-coefficient gate;
the separately labeled complete-December robustness passes. The annual
Table A.3 robustness remains unresolved. These statuses are preserved in the
canonical result manifest and must not be collapsed to one generic
“extension-ready” flag.

`--extend-event-horizons` adds the separately fingerprinted horizon exercise:
the 2018--19 treatment cohorts are followed through requested horizon \(+24\),
the 2025 series is requested through \(+12\) but right-censored at the local
December 2025 endpoint (the last supported coefficient is \(+11\)), and
Appendix Figure 2 uses the validated historical dynamic coefficients through
\(+12\). This flag does not replace or mutate the paper-window results.

`--cumulative-pass-through` runs a distinct continuous-treatment exercise for
both episodes. It builds harmonized monthly panels, estimates cumulative
local-projection IV pass-through at horizons \(0,\ldots,24\) for 2018 and
\(0,\ldots,11\) for 2025, validates the price accounting identity, and writes
the new appendix figures. This is not another rendering of
`--extend-event-horizons`: that stage reports cumulative outcome responses to
a binary newly-treated indicator, while this stage measures the outcome
change per cumulative change in the tariff actually paid.
