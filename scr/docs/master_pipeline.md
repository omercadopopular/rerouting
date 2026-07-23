# Master pipeline

`master_pipeline.py` calls data construction before estimation. The historical switches are `--rebuild-tariffs`, `--build-archives`, `--extended`, and `--overwrite`. The isolated forward application is selected with `--extension-2025`; `--estimate-extension-2025` requests estimation but remains subject to the independent 2025 policy gate. No switch silently infers missing tariff values. The default command reproduces only the locked historical methodology.

The forward horizon contract contains two independent estimates: `[-6,+6]` and `[-6,+24]` or the latest supported right tail. With Census data through December 2025, the latter is `[-6,+10]`. Unsupported horizons are not written.
