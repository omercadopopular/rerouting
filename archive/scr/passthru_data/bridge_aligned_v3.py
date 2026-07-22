"""Build source-separated aligned bridge panels for the next bridge audit."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import duckdb

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json


VERSION = "bridge_v3_source_separation"


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v3"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def build_source_separated_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    package = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"
    raw = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
    if not package.exists() or not raw.exists():
        raise FileNotFoundError(f"Missing package or raw import source: {package}, {raw}")
    out = root(config)
    package_out = out / "package_common_sample_anchor.parquet"
    raw_out = out / "raw_outcomes_package_policy.parquet"
    if package_out.exists() and raw_out.exists() and not overwrite:
        return json.loads((out / "aligned_bridge_manifest.json").read_text(encoding="utf-8"))
    p, r = str(package).replace("'", "''"), str(raw).replace("'", "''")
    design = """
        p.id, p.cty_code, p.cty_name, p.hs10, p.hs8, p.hs6, p.hs4, p.hs2,
        p.year, p.month, p.mdate, p.m_effective_mdate2, p.m_stattariff2,
        p.m_status2, p.m_ess, p.naics_str
    """
    package_design = design + ", p.lm_p, p.lm_pduty, p.lm_q1, p.lm_val"
    package_values = """
        CASE WHEN p.m_val > 0 AND r.m_val > 0 THEN p.m_val END AS m_val,
        CASE WHEN p.m_q1 > 0 AND r.m_q1 > 0 THEN p.m_q1 END AS m_q1,
        CASE WHEN p.m_p > 0 AND r.m_val > 0 AND r.m_q1 > 0 THEN p.m_p END AS m_p,
        CASE WHEN p.m_pduty > 0 AND r.m_val > 0 AND r.m_q1 > 0 AND p.m_stattariff2 IS NOT NULL THEN p.m_pduty END AS m_pduty
    """
    raw_values = """
        CASE WHEN p.m_val > 0 AND r.m_val > 0 THEN r.m_val / 1000000.0 END AS m_val,
        CASE WHEN p.m_q1 > 0 AND r.m_q1 > 0 THEN r.m_q1 / 1000000.0 END AS m_q1,
        CASE WHEN p.m_p > 0 AND r.m_val > 0 AND r.m_q1 > 0 THEN r.m_val / NULLIF(r.m_q1, 0) END AS m_p,
        CASE WHEN p.m_pduty > 0 AND r.m_val > 0 AND r.m_q1 > 0 AND p.m_stattariff2 IS NOT NULL THEN (r.m_val / NULLIF(r.m_q1, 0)) * (1 + p.m_stattariff2) END AS m_pduty
    """
    join = f"FROM read_parquet('{p}') p INNER JOIN read_parquet('{r}') r USING (cty_code, hs10, year, month) WHERE p.cty_code > 0"
    con = duckdb.connect(database=":memory:")
    try:
        for destination, select_design, values in ((package_out, package_design, package_values), (raw_out, design, raw_values)):
            if destination.exists() and not overwrite:
                continue
            temporary = destination.with_name(f".{destination.name}.tmp")
            temporary.unlink(missing_ok=True)
            query = f"SELECT {select_design}, {values} {join}"
            con.execute(f"COPY ({query}) TO '{str(temporary).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{str(temporary).replace(chr(39), chr(39)*2)}')").fetchone()[0])
            if rows <= 0:
                temporary.unlink(missing_ok=True)
                raise RuntimeError(f"Empty source-separated panel: {destination}")
            temporary.replace(destination)
        package_rows, raw_rows, package_keys, raw_keys = con.execute(f"""
            SELECT (SELECT count(*) FROM read_parquet('{str(package_out).replace(chr(39), chr(39)*2)}')),
                   (SELECT count(*) FROM read_parquet('{str(raw_out).replace(chr(39), chr(39)*2)}')),
                   (SELECT count(DISTINCT (cty_code,hs10,year,month)) FROM read_parquet('{str(package_out).replace(chr(39), chr(39)*2)}')),
                   (SELECT count(DISTINCT (cty_code,hs10,year,month)) FROM read_parquet('{str(raw_out).replace(chr(39), chr(39)*2)}'))
        """).fetchone()
    finally:
        con.close()
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "package_path": _relative(config, package_out),
        "raw_path": _relative(config, raw_out),
        "package_source": _relative(config, package),
        "raw_source": _relative(config, raw),
        "package_source_sha256": sha256_file(package),
        "raw_source_sha256": sha256_file(raw),
        "package_rows": int(package_rows),
        "raw_rows": int(raw_rows),
        "package_distinct_keys": int(package_keys),
        "raw_distinct_keys": int(raw_keys),
        "raw_contains_package_lm_columns": False,
        "policy_semantics_changed": False,
        "status": "complete" if int(package_rows) == int(raw_rows) and int(package_keys) == int(raw_keys) else "failed",
    }
    write_metadata_json(out / "aligned_bridge_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    print(build_source_separated_panels(PipelineConfig.default(), overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
