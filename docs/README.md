# Historical Trade-War Replication

This directory documents the locked methodology used to reconstruct U.S. import outcomes and the 2018 trade-war tariff variables, reproduce the import event studies in Fajgelbaum et al., and define a separate longer-horizon extension. The implementation is organized into two auditable stages:

1. [`scr/data_construction`](../scr/data_construction/) reads raw Census and official tariff sources and creates processed trade and tariff artifacts.
2. [`scr/pass_through`](../scr/pass_through/) estimates the event-study and dynamic pass-through specifications and produces the replication figures.

The root [`master_pipeline.py`](../master_pipeline.py) calls these stages in order. Large Parquet artifacts are local under `data/processed`; code, source descriptions, compact manifests, and publication PDFs are versioned.

The accepted historical methodology is deliberately narrower than a claim that every diagnostic gate passes. The package-only estimator reproduces the paper figures within the registered 1.10-log-point tolerance. With raw Census outcomes held fixed, the independent paper-clock policy construction closely reproduces seven of eight policy-substitution curves. Event-study quantity is the disclosed exception. The legal-clock series is an alternative timing specification, not the series compared to the paper.

## Reading guide

- [Data sources](data_sources.md)
- [Trade-data construction](trade_data_construction.md)
- [Tariff-data construction](tariff_data_construction.md)
- [Regression methodology](replication_methodology.md)
- [Replication results and gates](replication_results.md)
- [Reproducibility instructions](reproducibility.md)
- [Academic appendix](replication/appendix.pdf)
- [February 2025 extension design and current gates](extension.md)

The archive-native 2025 trade-data gate passes through December 2025. The independent 2025 statutory-policy gate remains blocked, so the appendix documents the registered `[-6,+6]` and support-limited `[-6,+10]` specifications but does not present 2025 estimates as completed empirical results.
