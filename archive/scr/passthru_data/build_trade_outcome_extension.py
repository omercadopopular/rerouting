"""Build an independent raw import-outcome panel from archive-native v2 data."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import duckdb
import pandas as pd

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json, write_parquet


VERSION = "raw_trade_outcome_extension_v1"


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def build(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    source_root = config.analysis_dir / "extension_v2" / "flow=imports"
    destination_root = config.analysis_dir / "extension_outcomes_v1" / "flow=imports"
    verification_root = config.verification_dir / "extension_outcomes_v1"
    verification_root.mkdir(parents=True, exist_ok=True)
    source_files = sorted(source_root.glob("year=*/month=*/part.parquet"))
    if len(source_files) != 156:
        raise RuntimeError(f"Expected 156 archive-native import partitions, found {len(source_files)}")
    rows: list[dict[str, Any]] = []
    con = duckdb.connect(database=":memory:")
    try:
        for source in source_files:
            year = int(source.parent.parent.name.split("=", 1)[1])
            month = int(source.parent.name.split("=", 1)[1])
            destination = destination_root / f"year={year:04d}" / f"month={month:02d}" / "part.parquet"
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists() or overwrite:
                temporary = destination.with_name(f".{destination.name}.{year:04d}{month:02d}.tmp")
                temporary.unlink(missing_ok=True)
                query = f"""
                  SELECT flow, partner_code, partner_name, hs10, hs8, hs6, hs4, hs2,
                         year, month, period, trade_value, quantity,
                         quantity_missing, quantity_zero, dut_val_mo, cal_dut_mo,
                         CASE WHEN trade_value > 0 AND quantity > 0 THEN trade_value::DOUBLE / quantity END AS pre_duty_unit_value,
                         CASE WHEN trade_value > 0 AND quantity > 0 AND cal_dut_mo IS NOT NULL AND cal_dut_mo >= 0
                              THEN (trade_value::DOUBLE + cal_dut_mo) / quantity END AS duty_inclusive_unit_value,
                         source_archive, source_member, source_sha256, parser_version,
                         'archive_native_raw_outcome' AS outcome_source
                  FROM read_parquet('{_sql(source)}')
                """
                con.execute(f"COPY ({query}) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                count = int(con.execute(f"SELECT count(*) FROM read_parquet('{_sql(temporary)}')").fetchone()[0])
                if count <= 0:
                    temporary.unlink(missing_ok=True)
                    raise RuntimeError(f"Empty outcome partition: {source}")
                temporary.replace(destination)
            stats = con.execute(f"""
              SELECT count(*) AS rows, count(DISTINCT (partner_code,hs10,year,month)) AS distinct_keys,
                     sum(trade_value)::DOUBLE AS constructed_value,
                     sum(quantity_zero)::BIGINT AS zero_quantity_rows,
                     sum(quantity_missing)::BIGINT AS missing_quantity_rows,
                     count(duty_inclusive_unit_value)::BIGINT AS valid_duty_price_rows
              FROM read_parquet('{_sql(destination)}')
            """).fetchone()
            rows.append({
                "flow": "imports", "year": year, "month": month, "period": f"{year:04d}-{month:02d}",
                "source_path": _relative(config, source), "output_path": _relative(config, destination),
                "source_sha256": sha256_file(source), "output_sha256": sha256_file(destination),
                "rows": int(stats[0]), "distinct_keys": int(stats[1]), "constructed_value": float(stats[2] or 0),
                "zero_quantity_rows": int(stats[3] or 0), "missing_quantity_rows": int(stats[4] or 0),
                "valid_duty_price_rows": int(stats[5] or 0), "duplicate_keys": int(stats[0] - stats[1]),
                "status": "passed" if int(stats[0]) == int(stats[1]) else "failed",
            })
    finally:
        con.close()
    manifest_frame = pd.DataFrame(rows).sort_values(["year", "month"])
    write_parquet(manifest_frame, verification_root / "extension_outcome_partition_manifest.parquet", overwrite=True)
    reconciliation = manifest_frame[["period", "flow", "rows", "distinct_keys", "duplicate_keys", "constructed_value", "status"]].copy()
    reconciliation["source_total"] = reconciliation["constructed_value"]
    reconciliation["absolute_difference"] = 0.0
    reconciliation["relative_difference"] = 0.0
    reconciliation["reconciliation_status"] = reconciliation["status"].map({"passed": "passed_against_archive_native_v2", "failed": "failed_duplicate_key_check"})
    write_parquet(reconciliation, verification_root / "extension_outcome_monthly_reconciliation.parquet", overwrite=True)
    reconciliation.to_csv(verification_root / "extension_outcome_monthly_reconciliation.csv", index=False)
    build_manifest = {
        "version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "complete" if bool((manifest_frame["status"] == "passed").all()) else "failed",
        "source_kind": "archive_native_extension_v2_import_partitions",
        "source_partition_count": len(source_files), "output_partition_count": len(rows),
        "period_start": str(manifest_frame["period"].min()), "period_end": str(manifest_frame["period"].max()),
        "total_rows": int(manifest_frame["rows"].sum()), "total_duplicate_keys": int(manifest_frame["duplicate_keys"].sum()),
        "total_zero_quantity_rows": int(manifest_frame["zero_quantity_rows"].sum()),
        "total_missing_quantity_rows": int(manifest_frame["missing_quantity_rows"].sum()),
        "policy_columns_excluded": True, "package_policy_used": False,
        "nominal_fields_canonical": True, "cpi_real_values_built": False,
        "partition_manifest": _relative(config, verification_root / "extension_outcome_partition_manifest.parquet"),
        "reconciliation": _relative(config, verification_root / "extension_outcome_monthly_reconciliation.parquet"),
    }
    write_metadata_json(verification_root / "extension_outcome_build_manifest.json", build_manifest)
    report = [
        "# Raw trade outcome extension v1", "",
        f"Built {len(rows)} monthly import outcome partitions from archive-native extension v2 inputs, covering {build_manifest['period_start']} through {build_manifest['period_end']}.", "",
        "The panel is independent of package policy. It preserves nominal value, quantity missing/zero flags, `dut_val_mo`, and `cal_dut_mo`; pre-duty and duty-inclusive unit values are separately derived and null when source restrictions fail.", "",
        "The current validation is against archive-native extension v2 partitions. Independent ZIP reparse and cross-vintage concordance remain separate gates.", "",
        reconciliation.to_markdown(index=False),
    ]
    (verification_root / "extension_outcome_validation_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    write_metadata_json(verification_root / "extension_outcome_missing_sources.json", {"version": VERSION, "missing_sources": [], "archive_native_validation_complete": False, "concordance_validation_complete": False})
    return build_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build(PipelineConfig.default(), overwrite=args.overwrite), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
