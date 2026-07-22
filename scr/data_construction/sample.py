"""Materialize the raw-outcome sample used by the locked replication."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json

VERSION = "historical_replication_sample_v1"


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def build_replication_outcomes(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Copy only raw outcomes and design keys from the verified common sample.

    Package policy variables are explicitly excluded.  They are joined later
    only for the validation-anchor regression.
    """
    source = config.repo_root / "data" / "verification" / "passthru_data" / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif" / "raw_outcomes_package_policy_cif.parquet"
    destination = config.processed_trade_dir / "final" / "historical_replication_outcomes.parquet"
    if not source.exists() and not destination.exists():
        raise FileNotFoundError(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{VERSION}.tmp")
    con = duckdb.connect()
    try:
        excluded = {"m_effective_mdate2", "m_stattariff2", "m_status2", "m_ess", "policy_mode", "policy_source", "policy_semantics"}
        if overwrite or not destination.exists():
            if not source.exists():
                raise FileNotFoundError(source)
            columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{_sql(source)}')").fetchall()]
            keep = [column for column in columns if column not in excluded]
            projection = ",".join(f'"{column}"' for column in keep)
            temporary.unlink(missing_ok=True)
            con.execute(f"COPY (SELECT {projection} FROM read_parquet('{_sql(source)}')) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            temporary.replace(destination)
        rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{_sql(destination)}')").fetchone()[0])
        leaked = sorted(excluded.intersection(row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{_sql(destination)}')").fetchall()))
        if leaked:
            raise RuntimeError(f"Package-policy columns leaked into raw outcome artifact: {leaked}")
    finally:
        con.close()
        temporary.unlink(missing_ok=True)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "source": _relative(config, source) if source.exists() else "archived_legacy_common_sample",
        "source_sha256": sha256_file(source) if source.exists() else None,
        "artifact": _relative(config, destination),
        "artifact_sha256": sha256_file(destination),
        "rows": rows,
        "policy_columns_present": False,
        "sample_role": "fixed historical raw-outcome replication sample",
    }
    write_metadata_json(config.processed_trade_dir / "historical_replication_outcomes_manifest.json", manifest)
    return manifest
