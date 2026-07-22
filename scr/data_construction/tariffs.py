"""Canonical construction of the locked historical tariff data.

The public pipeline can rebuild the source-auditable Section 201, 232, and
301 schedules through :mod:`tariff_construction` and
:mod:`partner_tariffs`.  ``materialize_validated`` is a one-time migration
path for the already validated artifacts created before the repository was
reorganized.  It verifies and records their hashes; it does not silently
reinterpret them.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json

VERSION = "historical_tariffs_locked_v1"
HISTORICAL_ACTION_CUTOFF = "2018-09-30"
TARIFF_TERMINAL_MONTH = "2019-04"


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _validate(path: Path) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    compressions = {
        parquet.metadata.row_group(group).column(column).compression
        for group in range(parquet.metadata.num_row_groups)
        for column in range(parquet.metadata.row_group(group).num_columns)
    }
    if compressions != {"ZSTD"}:
        raise RuntimeError(f"Canonical artifact is not ZSTD Parquet: {path}: {compressions}")
    result = {
        "rows": int(parquet.metadata.num_rows),
        "columns": list(parquet.schema_arrow.names),
        "compression": "ZSTD",
        "sha256": sha256_file(path),
    }
    del parquet
    return result


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        shutil.copyfile(source, temporary)
        _validate(temporary)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def _legacy_sources(config: PipelineConfig) -> dict[str, Path]:
    data = config.repo_root / "data"
    v4 = data / "analysis" / "passthru_data" / "policy" / "pooled_policy_replication_v4"
    return {
        "historical_tariffs": data / "analysis" / "passthru_data" / "policy" / "pooled_policy_replication_v3" / "pooled_policy_replication_v3_panel.parquet",
        "historical_event_clock": v4 / "historical_event_clock_v4.parquet",
        "package_policy_anchor": v4 / "package_full_policy_anchor.parquet",
    }


def canonical_paths(config: PipelineConfig) -> dict[str, Path]:
    final = config.processed_tariff_dir / "final"
    return {
        "historical_tariffs": final / "historical_tariffs.parquet",
        "historical_event_clock": final / "historical_event_clock.parquet",
        # The anchor is retained strictly as a validation input.  It is never
        # labeled as independently reconstructed policy.
        "package_policy_anchor": final / "package_policy_validation_anchor.parquet",
    }


def materialize_validated(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Migrate the validated historical artifacts into canonical paths."""
    sources = _legacy_sources(config)
    destinations = canonical_paths(config)
    records: dict[str, Any] = {}
    for role, source in sources.items():
        destination = destinations[role]
        if destination.exists() and not overwrite:
            destination_validation = _validate(destination)
            records[role] = {
                "source": _relative(config, source) if source.exists() else "archived_legacy_artifact; rebuildable_from_raw_sources",
                "canonical_path": _relative(config, destination),
                **destination_validation,
            }
            continue
        if not source.exists():
            raise FileNotFoundError(f"Canonical {role} is absent and migration source is unavailable: {source}; use --rebuild-tariffs")
        source_validation = _validate(source)
        if overwrite or not destination.exists() or sha256_file(destination) != source_validation["sha256"]:
            _atomic_copy(source, destination)
        destination_validation = _validate(destination)
        if destination_validation["sha256"] != source_validation["sha256"]:
            raise RuntimeError(f"Migration hash mismatch for {role}")
        records[role] = {
            "source": _relative(config, source),
            "canonical_path": _relative(config, destination),
            **destination_validation,
        }

    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "construction_mode": "validated_artifact_migration",
        "historical_action_cutoff": HISTORICAL_ACTION_CUTOFF,
        "terminal_tariff_month_for_forward_extension": TARIFF_TERMINAL_MONTH,
        "families": ["MFN baseline", "Section 201", "Section 232", "Section 301"],
        "quota_threshold_increments": "excluded_consistent_with_paper_appendix",
        "antidumping_countervailing_duties": "excluded_consistent_with_paper_appendix",
        "unrelated_treaty_changes": "excluded_consistent_with_paper_appendix",
        "artifacts": records,
    }
    write_metadata_json(config.processed_tariff_dir / "historical_tariff_manifest.json", manifest)
    return manifest


def rebuild_from_sources(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Rebuild the pooled tariff path from the locally stored raw sources."""
    from .tariff_construction import build_pooled_v2_panels
    from .partner_tariffs import build_policy_v3

    v2 = build_pooled_v2_panels(config, overwrite=overwrite)
    v3 = build_policy_v3(config, overwrite=overwrite)
    return {
        "version": VERSION,
        "status": "constructed",
        "pooled_source_schedule": v2,
        "partner_specific_tariffs": v3,
        "note": "Run the historical event-clock step after the raw replication sample has been constructed.",
    }


def write_source_ledger(config: PipelineConfig) -> Path:
    """Write the compact, human-reviewable source ledger for the locked build."""
    ledger = {
        "version": VERSION,
        "sources": [
            {
                "role": "MFN baseline and HTS schedules",
                "authority": "United States International Trade Commission",
                "url": "https://hts.usitc.gov/reststop/file?release=currentRelease&filename=htsdata",
                "format": "USITC HTS tables and archived schedules",
                "method": "tabular extraction; HTS chapter tables are projected to HS10",
            },
            {
                "role": "Section 201 solar and washer actions",
                "authority": "USITC / Federal Register / Presidential proclamations",
                "url": "https://www.usitc.gov/trade_remedy/731_ad_701_cvd/investigations/safeguard.htm",
                "format": "official HTS tables and proclamation schedules",
                "method": "structured tables where available; reviewed PDF tables otherwise",
            },
            {
                "role": "Section 232 steel and aluminum actions",
                "authority": "USITC / Federal Register / Presidential proclamations",
                "url": "https://www.federalregister.gov/presidential-documents/proclamations",
                "format": "official annex tables, including PDF annexes",
                "method": "HTS code extraction followed by partner/date/rate schedule construction",
            },
            {
                "role": "Section 301 Lists 1--3",
                "authority": "Office of the United States Trade Representative",
                "url": "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions",
                "format": "official list notices and annex tables",
                "method": "tabular extraction and reviewed PDF-table extraction",
            },
        ],
    }
    destination = config.processed_tariff_dir / "source_ledger.json"
    return write_metadata_json(destination, ledger)
