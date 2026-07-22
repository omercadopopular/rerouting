"""Versioned full-policy panels for the final historical replication.

This module keeps three policy objects on one frozen raw-outcome sample:
the authors' package variables (validation anchor), an independently built
paper-compatible clock, and the independent legal clock.  The package object
is copied only from the authors' cache; it is never used to construct the
independent policy panels.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json
from .pooled_policy_replication_v2 import analysis_root as v2_analysis_root

VERSION = "pooled_policy_replication_v4"
FAMILIES = ("solar_201", "washer_201", "steel_232", "aluminum_232", "china_301")


def analysis_root(config: PipelineConfig) -> Path:
    p = config.analysis_dir / "policy" / VERSION
    p.mkdir(parents=True, exist_ok=True)
    return p


def verification_root(config: PipelineConfig) -> Path:
    p = config.verification_dir / "raw_replication_imports" / VERSION
    p.mkdir(parents=True, exist_ok=True)
    return p


def _q(path: Path) -> str:
    return str(path).replace("'", "''")


def _artifact(path: Path) -> dict[str, Any]:
    pf = pq.ParquetFile(path)
    compression = sorted({pf.metadata.row_group(r).column(c).compression
                           for r in range(pf.metadata.num_row_groups)
                           for c in range(pf.metadata.row_group(r).num_columns)})
    return {"rows": int(pf.metadata.num_rows), "columns": list(pf.schema_arrow.names),
            "compression": compression, "sha256": sha256_file(path)}


def _write_copy(con: duckdb.DuckDBPyConnection, query: str, dest: Path) -> None:
    tmp = dest.with_name(f".{dest.name}.{VERSION}.tmp")
    tmp.unlink(missing_ok=True)
    try:
        con.execute(f"COPY ({query}) TO '{_q(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        a = _artifact(tmp)
        if a["rows"] <= 0 or a["compression"] != ["ZSTD"]:
            raise RuntimeError(f"invalid temporary policy artifact: {tmp}")
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)


def build_package_anchor(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Materialise package policy fields on the exact common raw sample."""
    out = analysis_root(config) / "package_full_policy_anchor.parquet"
    raw = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif" / "raw_outcomes_package_policy_cif.parquet"
    cache = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"
    if not raw.exists() or not cache.exists():
        raise FileNotFoundError("corrected raw common sample and package cache are required")
    con = duckdb.connect()
    try:
        query = f"""
        SELECT r.* EXCLUDE(m_effective_mdate2, m_stattariff2, m_status2, m_ess),
               p.m_effective_mdate2, p.m_stattariff2, p.m_status2, p.m_ess,
               'package_full_policy_anchor' AS policy_mode,
               '{str(cache).replace("'", "''")}' AS policy_source
        FROM read_parquet('{_q(raw)}') r
        JOIN read_parquet('{_q(cache)}') p USING (cty_code, hs10, year, month)
        """
        if overwrite or not out.exists():
            _write_copy(con, query, out)
    finally:
        con.close()
    result = {"version": VERSION, "mode": "package_full_policy_anchor",
              "path": str(out), "source_raw": str(raw), "source_package": str(cache),
              "artifact": _artifact(out), "package_policy_used_as_builder_input": True,
              "supersedes": "pooled_policy_replication_v3 Section-301-only anchor; v3 anchor is historical diagnostic",
              "created_at_utc": datetime.now(timezone.utc).isoformat()}
    write_metadata_json(out.with_suffix(".json"), result)
    return result


def build_independent_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Copy independent v2 panels into a frozen v4 namespace, with audits."""
    srcroot = v2_analysis_root(config)
    outroot = analysis_root(config)
    result: dict[str, Any] = {"version": VERSION, "panels": {}}
    con = duckdb.connect()
    try:
        for mode, filename in (("independent_paper", "paper_compatible_full_trade_policy_panel.parquet"),
                               ("independent_legal", "independent_legal_full_trade_policy_panel.parquet")):
            src = srcroot / filename
            dest = outroot / f"{mode}_full_policy_panel.parquet"
            if overwrite or not dest.exists():
                _write_copy(con, f"SELECT *, '{mode}' AS policy_mode, '{str(src).replace(chr(39), chr(39)*2)}' AS policy_source FROM read_parquet('{_q(src)}')", dest)
            missing = con.execute(f"SELECT count(*) FROM read_parquet('{_q(dest)}') WHERE independent_base_mfn_rate IS NULL").fetchone()[0]
            total = con.execute(f"SELECT count(*) FROM read_parquet('{_q(dest)}')").fetchone()[0]
            result["panels"][mode] = {"path": str(dest), "artifact": _artifact(dest), "missing_base_mfn": int(missing), "rows": int(total), "source": str(src)}
    finally:
        con.close()
    write_metadata_json(outroot / "pooled_policy_replication_v4_manifest.json", result)
    return result


def audit_mfn(config: PipelineConfig) -> dict[str, Any]:
    out = verification_root(config)
    panel = analysis_root(config) / "independent_paper_full_policy_panel.parquet"
    raw = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif" / "raw_outcomes_package_policy_cif.parquet"
    con = duckdb.connect()
    try:
        q = f"""SELECT p.year,p.month,p.cty_code,p.hs10,p.independent_base_mfn_rate
                FROM read_parquet('{_q(panel)}') p JOIN read_parquet('{_q(raw)}') r USING(cty_code,hs10,year,month)
                WHERE p.independent_base_mfn_rate IS NULL"""
        tmp = out / "missing_mfn_keys.parquet"
        _write_copy(con, q, tmp)
        summary = con.execute(f"SELECT year,month,count(*) AS rows FROM read_parquet('{_q(tmp)}') GROUP BY ALL ORDER BY 1,2").fetchdf()
    finally:
        con.close()
    summary.to_csv(out / "missing_mfn_summary.csv", index=False)
    result = {"version": VERSION, "missing_keys": _artifact(tmp), "summary": str(out / "missing_mfn_summary.csv"), "status": "unresolved_not_zero_filled"}
    write_metadata_json(out / "missing_mfn_audit.json", result)
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()
    cfg = PipelineConfig.default()
    if args.build:
        print(json.dumps({"package": build_package_anchor(cfg, overwrite=args.overwrite),
                          "independent": build_independent_panels(cfg, overwrite=args.overwrite),
                          "mfn": audit_mfn(cfg)}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
