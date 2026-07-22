"""Last-mile pooled policy reconstruction.

Version 3 separates the paper event clock from the bilateral tariff path.  The
v2 paper panel expanded product actions to every partner; that is appropriate
for some event-clock comparisons but is not a valid bilateral statutory-rate
series for the dynamic regression.  v3 derives both objects from the
partner-specific schedule and records the two clocks explicitly.
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
from .tariff_construction import analysis_root as v2_analysis_root
from .tariff_construction import relative, root as v2_root

VERSION = "pooled_policy_replication_v3"


def analysis_root(config: PipelineConfig) -> Path:
    path = config.analysis_dir / "policy" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def verification_root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def _validate_parquet(path: Path) -> dict[str, Any]:
    pf = pq.ParquetFile(path)
    compression = sorted({pf.metadata.row_group(r).column(c).compression
                          for r in range(pf.metadata.num_row_groups)
                          for c in range(pf.metadata.row_group(r).num_columns)})
    if compression != ["ZSTD"]:
        raise RuntimeError(f"{path} is not ZSTD-compressed: {compression}")
    return {
        "rows": int(pf.metadata.num_rows),
        "columns": list(pf.schema_arrow.names),
        "compression": compression,
        "sha256": sha256_file(path),
    }


def _source_paths(config: PipelineConfig) -> tuple[Path, Path]:
    v2 = v2_analysis_root(config)
    legal = v2 / "independent_legal_full_trade_policy_panel.parquet"
    actions = v2 / "legal_action_ledger_v2.parquet"
    if not legal.exists() or not actions.exists():
        raise FileNotFoundError("v2 partner-specific legal panel and action ledger are required")
    return legal, actions


def build_policy_v3(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Build corrected event clocks and bilateral tariff paths."""
    legal, actions = _source_paths(config)
    out = analysis_root(config)
    destination = out / "pooled_policy_replication_v3_panel.parquet"
    if destination.exists() and not overwrite:
        return {"version": VERSION, "panel": relative(config, destination), **_validate_parquet(destination)}

    family_dates = "least(" + ",".join(
        f"CASE WHEN coalesce({family}_additional_rate, 0) > 0 THEN {family}_legal_effective_date END"
        for family in ("solar_201", "washer_201", "steel_232", "aluminum_232", "china_301")
    ) + ")"
    family_rates = "coalesce(" + ", 0) + coalesce(".join(
        f"{family}_day_weighted_additional_rate" for family in
        ("solar_201", "washer_201", "steel_232", "aluminum_232", "china_301")
    ) + ", 0)"
    # The SQL expression above is deliberately generated from typed family
    # columns; no package policy column enters the independent rate path.
    family_rates = " + ".join(
        f"coalesce({family}_day_weighted_additional_rate, 0)" for family in
        ("solar_201", "washer_201", "steel_232", "aluminum_232", "china_301")
    )
    temporary = destination.with_name(f".{destination.name}.{VERSION}.tmp")
    temporary.unlink(missing_ok=True)
    con = duckdb.connect()
    try:
        query = f"""
        WITH source AS (
          SELECT *, year * 12 + month - 1 AS month_index,
                 {family_dates} AS source_effective_date,
                 {family_rates} AS bilateral_dayweighted_additional_rate
          FROM read_parquet('{_sql(legal)}')
        ), positive AS (
          SELECT *,
                 min(month_index) FILTER (WHERE bilateral_dayweighted_additional_rate > 0)
                   OVER (PARTITION BY cty_code, hs10) AS partner_first_positive_index,
                 min(month_index) FILTER (WHERE bilateral_dayweighted_additional_rate > 0)
                   OVER (PARTITION BY hs10) AS product_first_positive_index
          FROM source
        ), first_dates AS (
          SELECT *,
             min(source_effective_date) FILTER (
               WHERE bilateral_dayweighted_additional_rate > 0
                 AND month_index = partner_first_positive_index
             ) OVER (PARTITION BY cty_code, hs10) AS partner_source_date
          FROM positive
        ), clocks AS (
          SELECT *,
             CASE
               WHEN partner_first_positive_index IS NOT NULL
                 THEN make_date((partner_first_positive_index / 12)::INTEGER,
                                 (partner_first_positive_index % 12)::INTEGER + 1, 1)
               WHEN product_first_positive_index IS NOT NULL
                 THEN make_date((product_first_positive_index / 12)::INTEGER,
                                 (product_first_positive_index % 12)::INTEGER + 1, 1)
             END AS legal_first_applicable_month,
             CASE
               WHEN partner_first_positive_index IS NOT NULL THEN
                 CASE
                   WHEN partner_source_date IS NOT NULL
                    AND strftime(partner_source_date, '%Y-%m') = strftime(
                      make_date((partner_first_positive_index / 12)::INTEGER,
                                 (partner_first_positive_index % 12)::INTEGER + 1, 1), '%Y-%m')
                    AND extract(day FROM partner_source_date) > 15
                     THEN make_date((partner_first_positive_index / 12)::INTEGER,
                                     (partner_first_positive_index % 12)::INTEGER + 2, 1)
                   ELSE make_date((partner_first_positive_index / 12)::INTEGER,
                                   (partner_first_positive_index % 12)::INTEGER + 1, 1)
                 END
               WHEN product_first_positive_index IS NOT NULL THEN
                 make_date((product_first_positive_index / 12)::INTEGER,
                            (product_first_positive_index % 12)::INTEGER + 1, 1)
             END AS paper_event_month,
             CASE WHEN partner_first_positive_index IS NOT NULL
                    AND month_index >= partner_first_positive_index THEN 2
                  WHEN partner_first_positive_index IS NULL
                    AND product_first_positive_index IS NOT NULL
                    AND month_index >= product_first_positive_index THEN 1
                  ELSE 0 END::TINYINT AS paper_event_status,
             CASE WHEN bilateral_dayweighted_additional_rate > 0
                    AND month_index >= partner_first_positive_index THEN 2
                  ELSE 0 END::TINYINT AS legal_event_status
          FROM first_dates
        )
        SELECT *,
               independent_base_mfn_rate + bilateral_dayweighted_additional_rate
                 AS paper_dynamic_total_tariff,
               independent_base_mfn_rate + bilateral_dayweighted_additional_rate
                 AS legal_dynamic_total_tariff,
               CAST(legal_first_applicable_month AS DATE) AS legal_event_month,
               CAST(paper_event_month AS DATE) AS paper_event_mdate,
               'bilateral_partner_specific_v3' AS tariff_scope_convention,
               'paper_nearest_month_event_clock_v3' AS event_clock_convention,
               'first_positive_partner_applicability_v3' AS legal_date_convention,
               '{VERSION}' AS policy_replication_version
        FROM clocks
        """
        con.execute(f"COPY ({query}) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        _validate_parquet(temporary)
        temporary.replace(destination)
    finally:
        con.close()
        temporary.unlink(missing_ok=True)

    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "panel": relative(config, destination),
        "source_legal_panel": relative(config, legal),
        "source_action_ledger": relative(config, actions),
        "source_hashes": {relative(config, p): sha256_file(p) for p in (legal, actions)},
        "artifact": _validate_parquet(destination),
        "paper_event_clock": "partner applicability for treated varieties; product clock for permanent-exempt comparisons",
        "legal_event_clock": "first positive bilateral tariff applicability, not proclamation date",
        "dynamic_tariff": "fixed 2017 MFN plus partner-specific day-weighted family increments",
        "section301_scope": "China only",
        "within_family_stacking": "mutually exclusive source-qualified rates are not added",
        "package_policy_used_as_builder_input": False,
    }
    write_metadata_json(out / "pooled_policy_replication_v3_manifest.json", manifest)
    return manifest


def validate_policy_v3(config: PipelineConfig) -> dict[str, Any]:
    panel = analysis_root(config) / "pooled_policy_replication_v3_panel.parquet"
    if not panel.exists():
        raise FileNotFoundError(panel)
    con = duckdb.connect()
    try:
        p = _sql(panel)
        checks = con.execute(f"""
          SELECT
            count(*) AS rows,
            count(*) FILTER (WHERE coalesce(china_301_additional_rate,0)>0 AND cty_code<>5700) AS nonchina_301_rows,
            count(*) FILTER (WHERE paper_event_status=2 AND paper_event_status<>legal_event_status) AS event_status_disagreement,
            count(*) FILTER (WHERE bilateral_dayweighted_additional_rate<0) AS negative_rates,
            count(*) FILTER (WHERE independent_additional_rate IS NOT NULL
                              AND abs(independent_additional_rate -
                                  (coalesce(solar_201_additional_rate,0)+coalesce(washer_201_additional_rate,0)+
                                   coalesce(steel_232_additional_rate,0)+coalesce(aluminum_232_additional_rate,0)+
                                   coalesce(china_301_additional_rate,0))) > 1e-10) AS component_mismatch,
            count(*) FILTER (WHERE paper_event_status=1 AND bilateral_dayweighted_additional_rate>0) AS exempt_positive_rate_rows
          FROM read_parquet('{p}')
        """).fetchdf().iloc[0].to_dict()
    finally:
        con.close()
    result = {
        "version": VERSION,
        "panel": relative(config, panel),
        "artifact": _validate_parquet(panel),
        "checks": checks,
        "status": "passed" if checks["nonchina_301_rows"] == 0 and checks["negative_rates"] == 0 else "failed",
        "legal_policy_release_gate": False,
    }
    write_metadata_json(verification_root(config) / "pooled_policy_v3_variable_gate.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.default()
    if args.build:
        print(json.dumps(build_policy_v3(config, overwrite=args.overwrite), indent=2, default=str))
    if args.validate:
        print(json.dumps(validate_policy_v3(config), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
