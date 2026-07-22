# Pass-through pipeline

Entry point: `python -m scr.pass_through.pipeline`.

The default run materializes the package benchmark, constructs three explicitly sourced historical panels, validates 24 locked checkpoints, and produces the event and dynamic figures. `--reestimate-locked` reruns rather than migrates the already validated checkpoints. `--extended` adds the separately labeled \([-6,24]\) regressions with the April 2019 tariff level frozen afterward.

Outputs are under `data/processed/trade/regressions`; publication PDFs are under `figs/replication`.
