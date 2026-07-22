# Reproducibility

## Environment

Commands are run from the repository root with the project virtual environment:

```powershell
.venv\Scripts\python.exe -m pytest tests -q -p no:cacheprovider
```

Raw Census archives, official policy-source files, and the authors’ replication package must be present in their documented local directories. The pipeline never downloads sources implicitly.

## Full historical pipeline

To materialize the validated local source artifacts, recreate the fixed replication panels, validate the 24 checkpoints, and plot the locked \([-6,6]\) results:

```powershell
.venv\Scripts\python.exe master_pipeline.py
```

To force reconstruction of the tariff sources already stored locally:

```powershell
.venv\Scripts\python.exe master_pipeline.py --rebuild-tariffs
```

To parse the Census archives rather than use the already validated archive-native partitions:

```powershell
.venv\Scripts\python.exe master_pipeline.py --build-archives
```

To request the separately labeled \([-6,24]\) regressions:

```powershell
.venv\Scripts\python.exe master_pipeline.py --extended
```

The extension holds the April 2019 tariff level fixed afterward and therefore does not represent later tariff actions.

## Storage contract

Large tables and regression keys are ZSTD Parquet under `data/processed/trade` and `data/processed/tariffs`. They are ignored by Git. CSV is limited to compact summaries. Canonical manifests store repository-relative paths and SHA-256 fingerprints. Publication figures are written to `figs/replication`.
