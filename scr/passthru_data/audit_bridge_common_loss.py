"""Decompose package-common keys absent from the source-separated raw panel."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse

import duckdb
from .config import PipelineConfig
from .io_utils import write_metadata_json


VERSION = "bridge_common_key_loss_audit_v1"


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def audit_common_key_loss(config: PipelineConfig) -> dict[str, Any]:
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    package = root / "common_sample" / "package_common_sample_hs10fixed.parquet"
    raw = root / "common_sample_v3" / "raw_outcomes_package_policy.parquet"
    out = root / "common_sample_v3" / "bridge_diagnosis"
    out.mkdir(parents=True, exist_ok=True)
    if not package.exists() or not raw.exists():
        raise FileNotFoundError(f"Missing package-common or source-separated raw panel: {package}, {raw}")
    con = duckdb.connect(database=":memory:")
    try:
        p = str(package).replace("'", "''")
        r = str(raw).replace("'", "''")
        package_rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0])
        raw_rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{r}')").fetchone()[0])
        destination = out / "package_common_lost_raw_keys.parquet"
        d = str(destination).replace("'", "''")
        query = f"""
            WITH p AS (
                SELECT cty_code, hs10, year, month, id, naics_str
                FROM read_parquet('{p}')
            ), r AS (
                SELECT cty_code, hs10, year, month
                FROM read_parquet('{r}')
            )
            SELECT p.*
            FROM p ANTI JOIN r USING (cty_code, hs10, year, month)
        """
        con.execute(f"COPY ({query}) TO '{d}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        lost = int(con.execute(f"SELECT count(*) FROM read_parquet('{d}')").fetchone()[0])
        summaries: dict[str, str] = {}
        for dimension, expression in {
            "month": "concat(cast(year as varchar), '-', lpad(cast(month as varchar), 2, '0'))",
            "country": "cast(cty_code as varchar)",
            "hs2": "left(hs10, 2)",
            "hs4": "left(hs10, 4)",
        }.items():
            target = out / f"package_common_loss_by_{dimension}.parquet"
            t = str(target).replace("'", "''")
            con.execute(f"COPY (SELECT {expression} AS group_value, count(*) AS rows FROM read_parquet('{d}') GROUP BY 1 ORDER BY rows DESC) TO '{t}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            summaries[dimension] = _relative(config, target)
    finally:
        con.close()
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "package_source": _relative(config, package),
        "raw_source": _relative(config, raw),
        "package_rows": package_rows,
        "raw_rows": raw_rows,
        "lost_rows": lost,
        "lost_key_path": _relative(config, destination),
        "summary_paths": summaries,
        "status": "diagnostic_complete",
        "interpretation": "Keys are present in the package-common anchor but absent from the source-separated raw panel; no missing values are filled.",
    }
    write_metadata_json(out / "package_common_key_loss_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(audit_common_key_loss(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
