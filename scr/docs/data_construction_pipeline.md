# Data-construction pipeline

Entry point: `python -m scr.data_construction.pipeline`.

The stage validates or reconstructs the MFN+201+232+301 tariff panel, writes the event-clock artifact, strips package-policy fields from the raw Census outcome sample, and optionally reparses monthly Census archives. Canonical large artifacts are ZSTD Parquet under `data/processed/tariffs` and `data/processed/trade`. See [`docs/trade_data_construction.md`](../../docs/trade_data_construction.md) and [`docs/tariff_data_construction.md`](../../docs/tariff_data_construction.md) for the scientific definitions.
