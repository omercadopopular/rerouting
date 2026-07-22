# Replication development archive

This directory preserves superseded replication implementations and their historical documentation. Nothing here is called by `master_pipeline.py`.

- `scr/passthru_data/`: exploratory and versioned development modules replaced by `scr/data_construction/` and `scr/pass_through/`.
- `tests/legacy/`: tests for the archived implementations.
- `docs/scr_legacy/`: historical logs, diagnostics, and superseded specifications.
- `replication/`: local generated outputs from the development pipeline. Large files are intentionally ignored by Git; only its inventory is versioned.

The archive exists for provenance. It is not an alternative active pipeline and should not be imported by new code.
