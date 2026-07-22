"""Build an import-only, outcome-aligned package/raw bridge.

The historical bridge used a bilateral partner panel to define its common
sample.  That admits export-only keys and lets package and raw regressions use
different outcome restrictions.  This module defines the bridge universe once
from the raw import panel, joins package treatment/design fields, and masks
each outcome symmetrically so paired regressions have identical eligible rows.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import duckdb
import pandas as pd

from .config import PipelineConfig
from .io_utils import _json_default, sha256_file, write_metadata_json


VERSION = "bridge_v2_aligned_import"
OUTCOMES = ("val", "q1", "p", "pduty")


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v2"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _paths(config: PipelineConfig) -> tuple[Path, Path]:
    package = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"
    raw = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
    if not package.exists() or not raw.exists():
        raise FileNotFoundError(f"Missing package cache or raw import base: {package}, {raw}")
    return package, raw


def build_aligned_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Build two identically keyed panels with symmetric outcome masks."""
    package, raw = _paths(config)
    out = root(config)
    package_out = out / "package_common_sample_aligned.parquet"
    raw_out = out / "raw_outcomes_package_policy_aligned.parquet"
    if package_out.exists() and raw_out.exists() and not overwrite:
        return json.loads((out / "aligned_bridge_manifest.json").read_text(encoding="utf-8"))

    # The package cache is already restricted to the paper window.  The raw
    # side is the import-only panel; no bilateral partner panel is consulted.
    p = str(package).replace("'", "''")
    r = str(raw).replace("'", "''")
    select_design = """
        p.id, p.cty_code, p.cty_name, p.hs10, p.hs8, p.hs6, p.hs4, p.hs2,
        p.year, p.month, p.mdate, p.m_effective_mdate2, p.m_stattariff2,
        p.m_status2, p.m_ess, p.naics_str, p.lm_p, p.lm_pduty,
        p.lm_q1, p.lm_val
    """
    # Symmetric masks are essential: a package estimate and its raw counterpart
    # must enter on exactly the same positive/finite outcome rows.
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
        CASE WHEN p.m_pduty > 0 AND r.m_val > 0 AND r.m_q1 > 0 AND p.m_stattariff2 IS NOT NULL
             THEN (r.m_val / NULLIF(r.m_q1, 0)) * (1 + p.m_stattariff2) END AS m_pduty
    """
    join = f"""
      FROM read_parquet('{p}') p
      INNER JOIN read_parquet('{r}') r USING (cty_code, hs10, year, month)
      WHERE p.cty_code > 0
    """
    queries = {
        package_out: f"SELECT {select_design}, {package_values} {join}",
        raw_out: f"SELECT {select_design}, {raw_values} {join}",
    }
    con = duckdb.connect(database=":memory:")
    try:
        for destination, query in queries.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_name(f".{destination.name}.tmp")
            temporary.unlink(missing_ok=True)
            con.execute(f"COPY ({query}) TO '{str(temporary).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            check = con.execute(f"SELECT count(*) FROM read_parquet('{str(temporary).replace(chr(39), chr(39)*2)}')").fetchone()[0]
            if int(check) <= 0:
                temporary.unlink(missing_ok=True)
                raise RuntimeError(f"Aligned panel is empty: {destination}")
            temporary.replace(destination)
    finally:
        con.close()

    con = duckdb.connect(database=":memory:")
    try:
        package_rows, raw_rows, package_keys, raw_keys = con.execute(
            f"""
            SELECT (SELECT count(*) FROM read_parquet('{str(package_out).replace(chr(39), chr(39)*2)}')),
                   (SELECT count(*) FROM read_parquet('{str(raw_out).replace(chr(39), chr(39)*2)}')),
                   (SELECT count(DISTINCT (cty_code,hs10,year,month)) FROM read_parquet('{str(package_out).replace(chr(39), chr(39)*2)}')),
                   (SELECT count(DISTINCT (cty_code,hs10,year,month)) FROM read_parquet('{str(raw_out).replace(chr(39), chr(39)*2)}'))
            """
        ).fetchone()
        outcome_rows = {}
        for outcome in OUTCOMES:
            outcome_rows[outcome] = int(con.execute(f"SELECT count(*) FROM read_parquet('{str(package_out).replace(chr(39), chr(39)*2)}') WHERE m_{outcome} IS NOT NULL").fetchone()[0])
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
        "outcome_nonmissing_rows": outcome_rows,
        "import_only": True,
        "symmetric_outcome_masks": True,
        "policy_semantics_changed": False,
        "status": "complete" if int(package_rows) == int(raw_rows) and int(package_keys) == int(raw_keys) else "failed",
    }
    write_metadata_json(out / "aligned_bridge_manifest.json", manifest)
    _write_loss_audit(config, package, raw, package_out, out)
    return manifest


def _write_loss_audit(config: PipelineConfig, package: Path, raw: Path, aligned: Path, out: Path) -> None:
    """Record the exact package-to-import loss without serializing large keys."""
    con = duckdb.connect(database=":memory:")
    p, r, a = [str(path).replace("'", "''") for path in (package, raw, aligned)]
    try:
        lost = con.execute(
            f"""
            SELECT p.cty_code, p.hs10, p.year, p.month,
                   p.m_val, p.m_q1,
                   CASE WHEN r.cty_code IS NULL THEN 'missing_raw_import_key' ELSE 'not_aligned' END AS loss_reason
            FROM read_parquet('{p}') p
            LEFT JOIN read_parquet('{r}') r USING (cty_code, hs10, year, month)
            WHERE p.cty_code > 0 AND r.cty_code IS NULL
            """
        ).fetchdf()
        if not lost.empty:
            lost.to_parquet(out / "lost_import_keys.parquet", index=False, compression="zstd")
        stages = []
        base_count = int(con.execute(f"SELECT count(*) FROM read_parquet('{p}') WHERE cty_code > 0").fetchone()[0])
        joined_count = int(con.execute(f"SELECT count(*) FROM read_parquet('{a}')").fetchone()[0])
        stages.extend([
            {"stage": "package_eligible_import", "rows": base_count},
            {"stage": "raw_import_key_join", "rows": joined_count},
            {"stage": "lost_raw_import_key", "rows": base_count - joined_count},
        ])
        for outcome in OUTCOMES:
            rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{a}') WHERE m_{outcome} IS NOT NULL").fetchone()[0])
            stages.append({"stage": f"aligned_nonmissing_{outcome}", "rows": rows})
    finally:
        con.close()
    frame = pd.DataFrame(stages)
    frame.to_parquet(out / "aligned_sample_loss_audit.parquet", index=False, compression="zstd")
    frame.to_csv(out / "aligned_sample_loss_summary.csv", index=False)
    write_metadata_json(out / "aligned_sample_loss_manifest.json", {
        "version": VERSION,
        "package_source": _relative(config, package),
        "raw_source": _relative(config, raw),
        "aligned_source": _relative(config, aligned),
        "lost_key_path": _relative(config, out / "lost_import_keys.parquet"),
        "stages_path": _relative(config, out / "aligned_sample_loss_audit.parquet"),
        "status": "diagnostic",
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    print(build_aligned_panels(PipelineConfig.default(), overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
