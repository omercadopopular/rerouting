# Master pipeline

`master_pipeline.py` calls data construction before estimation. It has four explicit switches: `--rebuild-tariffs`, `--build-archives`, `--extended`, and `--overwrite`. No switch downloads data or silently infers missing tariff values. The default command reproduces only the locked historical methodology.
