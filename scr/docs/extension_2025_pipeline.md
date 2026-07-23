# February 2025 extension pipeline

The extension is isolated from the locked historical replication.

1. `scr.data_construction.extension_2025` inventories official Census archives, derives the supported horizon, parses monthly imports, and writes reconciled ZSTD Parquet partitions.
2. The same module inventories official policy actions but authorizes estimation only after the product/date/rate/exclusion/stacking ledger is complete.
3. `scr.pass_through.extension_2025` performs rank preflight, builds separate short and long fit grids, checkpoints individual regressions, and plots only a complete validated grid.
4. `master_pipeline.py --extension-2025` runs construction and preflight. Add `--estimate-extension-2025` only after the policy gate passes.

For the currently available December 2025 cutoff, the short window is `[-6,+6]` and the long window is `[-6,+10]`. The code targets `+24` and expands without changing the registered specification when official monthly data become available. The current trade build has 17 ZSTD partitions and 4,949,734 rows; all monthly source reconciliations pass, and a second invocation reuses all 17 validated partitions.

Only the adapted bilateral fixed-effect design enters the estimable grid. A common China-by-month treatment is absorbed by the historical country--month fixed effects. The historical dynamic country--NAICS4 effect also remains a diagnostic until a current, reviewed HS10-to-NAICS concordance is available. Dynamic specifications use one legal tariff path; an event-clock label is not duplicated into scientifically identical dynamic fits.

Data are written under `data/processed/trade/extension_2025/` and `data/processed/tariffs/extension_2025/`. Regression artifacts are written under `data/processed/trade/regressions/extension_2025/`. Only finalized PDF figures belong under `figs/extension_2025/`. At present the trade gate passes and the policy gate fails, so the figure directory is intentionally not populated with empirical 2025 estimates.
