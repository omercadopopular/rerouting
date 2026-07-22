"""Build the canonical source-separated bridge with realized Census duty.

Version 3 and version 4 remain historical.  Version 5 is a new CIF-based namespace so
that the corrected duty-inclusive outcome cannot be confused with the old
statutory-rate multiplier.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import duckdb

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json


VERSION = "bridge_v5_cif_calculated_duty"


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def build_source_separated_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    package = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"
    extension_root = config.analysis_dir / "extension_v4_cif"
    extension_glob = extension_root / "flow=imports" / "year=*" / "month=*" / "part.parquet"
    if not package.exists():
        raise FileNotFoundError(f"Missing corrected package cache: {package}")
    if not list(extension_root.glob("flow=imports/year=*/month=*/part.parquet")):
        raise FileNotFoundError(f"Missing archive-native import partitions under {extension_root}")
    out = root(config)
    package_out = out / "package_common_sample_anchor.parquet"
    raw_out = out / "raw_outcomes_package_policy_cif.parquet"
    p = _sql_path(package)
    r = _sql_path(extension_glob)
    design = """
        p.id, p.cty_code, p.cty_name, p.hs10, p.hs8, p.hs6, p.hs4, p.hs2,
        p.year, p.month, p.mdate, p.m_effective_mdate2, p.m_stattariff2,
        p.m_status2, p.m_ess, p.naics_str
    """
    package_values = """
        CASE WHEN p.m_val > 0 AND r.gen_cif_mo > 0 THEN p.m_val END AS m_val,
        CASE WHEN p.m_q1 > 0 AND r.gen_qy1_mo > 0 THEN p.m_q1 END AS m_q1,
        CASE WHEN p.m_p > 0 AND r.gen_cif_mo > 0 AND r.gen_qy1_mo > 0 THEN p.m_p END AS m_p,
        CASE WHEN p.m_pduty > 0 AND r.gen_cif_mo > 0 AND r.gen_qy1_mo > 0
                  AND r.cal_dut_mo IS NOT NULL AND r.cal_dut_mo >= 0 THEN p.m_pduty END AS m_pduty,
        p.lm_p, p.lm_pduty, p.lm_q1, p.lm_val
    """
    raw_values = """
        CASE WHEN r.gen_cif_mo > 0 THEN r.gen_cif_mo / 1000000.0 END AS m_val,
        CASE WHEN r.gen_qy1_mo > 0 THEN r.gen_qy1_mo / 1000000.0 END AS m_q1,
        CASE WHEN r.gen_cif_mo > 0 AND r.gen_qy1_mo > 0 THEN r.gen_cif_mo::DOUBLE / r.gen_qy1_mo END AS m_p,
        CASE WHEN r.gen_cif_mo > 0 AND r.gen_qy1_mo > 0 AND r.cal_dut_mo IS NOT NULL
                  AND r.cal_dut_mo >= 0 THEN (r.gen_cif_mo::DOUBLE + r.cal_dut_mo) / r.gen_qy1_mo END AS m_pduty,
        r.gen_val_mo, r.gen_cif_mo, r.gen_qy1_mo, r.dut_val_mo, r.cal_dut_mo, r.quantity_missing, r.quantity_zero,
        r.source_archive, r.source_member,
        r.source_sha256, r.parser_version
    """
    join = f"""
        FROM read_parquet('{p}') p
        INNER JOIN read_parquet('{r}', hive_partitioning=false) r
          ON p.cty_code = try_cast(r.partner_code AS BIGINT)
         AND p.hs10 = r.hs10 AND p.year = r.year AND p.month = try_cast(r.month AS BIGINT)
        WHERE p.cty_code > 0
    """
    con = duckdb.connect(database=":memory:")
    try:
        for destination, values in ((package_out, package_values), (raw_out, raw_values)):
            if destination.exists() and not overwrite:
                continue
            temporary = destination.with_name(f".{destination.name}.v4.tmp")
            temporary.unlink(missing_ok=True)
            query = f"SELECT {design}, {values} {join}"
            con.execute(f"COPY ({query}) TO '{_sql_path(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            check = con.execute(f"SELECT count(*) FROM read_parquet('{_sql_path(temporary)}')").fetchone()[0]
            if int(check) <= 0:
                temporary.unlink(missing_ok=True)
                raise RuntimeError(f"Empty v4 bridge panel: {destination}")
            temporary.replace(destination)
        stats = con.execute(f"""
            SELECT
              (SELECT count(*) FROM read_parquet('{_sql_path(package_out)}')) AS package_rows,
              (SELECT count(*) FROM read_parquet('{_sql_path(raw_out)}')) AS raw_rows,
              (SELECT count(DISTINCT (cty_code,hs10,year,month)) FROM read_parquet('{_sql_path(package_out)}')) AS package_keys,
              (SELECT count(DISTINCT (cty_code,hs10,year,month)) FROM read_parquet('{_sql_path(raw_out)}')) AS raw_keys,
              (SELECT count(*) FROM read_parquet('{_sql_path(raw_out)}') WHERE m_pduty IS NOT NULL) AS realized_pduty_rows
        """).fetchone()
    finally:
        con.close()
    package_rows, raw_rows, package_keys, raw_keys, realized = map(int, stats)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "complete" if package_rows == raw_rows and package_keys == raw_keys else "failed",
        "package_path": _relative(config, package_out),
        "raw_path": _relative(config, raw_out),
        "package_source": _relative(config, package),
        "extension_source_glob": _relative(config, extension_glob),
        "package_source_sha256": sha256_file(package),
        "package_rows": package_rows,
        "raw_rows": raw_rows,
        "package_distinct_keys": package_keys,
        "raw_distinct_keys": raw_keys,
        "realized_pduty_rows": realized,
        "raw_outcome_formula": "(gen_cif_mo + cal_dut_mo) / gen_qy1_mo",
        "dut_val_mo_role": "retained dutiable-value source field; not used as calculated duty",
        "raw_contains_package_policy": False,
        "policy_semantics_changed": False,
    }
    write_metadata_json(out / "aligned_bridge_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build_source_separated_panels(PipelineConfig.default(), overwrite=args.overwrite), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
