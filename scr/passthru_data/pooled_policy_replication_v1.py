"""Independent pooled import-policy reconstruction for the 2017--2019 paper window.

This module deliberately keeps the authors' package policy on the validation
side of the boundary.  The builder consumes local HTS/Chapter-99 source
artifacts and produces family components before pooling them.  Package columns
are read only by the validator and never by :func:`build_pooled_policy`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import hashlib
import json
import os
import re
import tempfile

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import (
    normalize_hs_code,
    read_table,
    sha256_file,
    write_metadata_json,
    write_parquet,
)
from .build_us_products_partner_panel import (
    _canonical_country,
    _eligible_countries_by_deterministic_grouping,
    _extract_countries_from_rule,
    _load_reference_tradewar_links,
    _load_tradewar_machine_links,
    _load_tradewar_pdf_csv_links,
    _load_tradewar_pdf_links,
    _load_tradewar_rule_attributes,
    _rule_family,
)


VERSION = "pooled_policy_replication_v1"
FAMILIES = (
    "solar_201",
    "washer_201",
    "steel_232",
    "aluminum_232",
    "china_301",
)
FAMILY_LABELS = {
    "solar_201": "section201_solar",
    "washer_201": "section201_washer",
    "steel_232": "section232_steel",
    "aluminum_232": "section232_aluminum",
    "china_301": "section301_china",
}
EXPECTED_POSITIVE_RULE_PREFIXES = {
    "solar_201": ("990346",),
    "washer_201": ("990345",),
    "steel_232": ("99038001", "99038002", "99038061"),
    "aluminum_232": ("99038501",),
    "china_301": ("990388",),
}
PAPER_THRESHOLDS = {
    "package_key_coverage": 0.99,
    "treatment_match": 0.95,
    "trade_weighted_treatment_match": 0.98,
    "effective_month_exact_match": 0.95,
    "effective_month_within_one_match": 0.99,
    "increment_within_10bp": 0.90,
    "increment_within_50bp": 0.95,
    "trade_weighted_increment_mae": 0.005,
    "unclassified_mismatch_trade_share": 0.01,
}


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def analysis_root(config: PipelineConfig) -> Path:
    path = config.analysis_dir / "policy" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _local_source_inventory(config: PipelineConfig) -> dict[str, Any]:
    roots = [config.raw_dir / "policy", config.reference_dir]
    rows: list[dict[str, Any]] = []
    for source_root in roots:
        if not source_root.exists():
            continue
        for path in sorted(source_root.rglob("*")):
            if not path.is_file() or "_tmp_selenium" in path.parts:
                continue
            if path.suffix.lower() not in {".csv", ".parquet", ".json", ".pdf", ".xls", ".xlsx", ".txt"}:
                continue
            rows.append({
                "path": relative(config, path),
                "bytes": int(path.stat().st_size),
                "sha256": sha256_file(path),
                "source_root": relative(config, source_root),
            })
    required_patterns = {
        "hts_2017": any("hts_2017" in row["path"].lower() for row in rows),
        "hts_2018": any("hts_2018" in row["path"].lower() for row in rows),
        "chapter99": any("chapter99" in row["path"].lower() or "chapter_99" in row["path"].lower() for row in rows),
        "policy_revisions": any("revision" in row["path"].lower() for row in rows),
        "chapter99_scope_source": any(
            ("chapter_99" in row["path"].lower() or "chapter99" in row["path"].lower())
            and (row["path"].lower().endswith(".csv") or row["path"].lower().endswith(".parquet"))
            for row in rows
        ),
    }
    missing = [name for name, present in required_patterns.items() if not present]
    return {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": rows,
        "required_pattern_checks": required_patterns,
        "missing_source_categories": missing,
        "status": "complete" if not missing else "blocked_missing_sources",
    }


def _all_links(config: PipelineConfig) -> pd.DataFrame:
    prefixes = ("990345", "990346", "990380", "990385", "990388")
    frames: list[pd.DataFrame] = []
    for loader, args in (
        (_load_reference_tradewar_links, (config, "tradewar_all_policy_links.parquet", prefixes)),
        (_load_tradewar_machine_links, (config,)),
        (_load_tradewar_pdf_links, (config,)),
        (_load_tradewar_pdf_csv_links, (config, False)),
    ):
        try:
            frame = loader(*args)
        except Exception:
            frame = pd.DataFrame()
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    out = pd.concat(frames, ignore_index=True)
    for column in ("hs8", "rule_code"):
        out[column] = out[column].map(lambda value: normalize_hs_code(value, 8))
    out["release_start_date"] = pd.to_datetime(out["release_start_date"], errors="coerce")
    out["release_end_date"] = pd.to_datetime(out["release_end_date"], errors="coerce")
    out["family"] = out["rule_code"].map(_rule_family)
    start = pd.Timestamp(f"{config.start_period}-01")
    end = pd.Timestamp(f"{config.end_period}-01") + pd.offsets.MonthEnd(1)
    out = out[
        out["family"].isin(FAMILIES)
        & out["release_end_date"].fillna(end).ge(start)
        & out["release_start_date"].fillna(start).le(end)
    ]
    out = out.drop_duplicates(["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    # The same HS8/rule appears in many archive revisions.  Preserve the
    # provenance list but collapse duplicate scope rows before joining to the
    # monthly rate attributes; otherwise the join multiplies every revision by
    # every active month and can exceed memory without adding information.
    out = (
        out.groupby(["hs8", "rule_code", "family"], as_index=False)
        .agg(
            release_name=("release_name", lambda values: "|".join(sorted(set(map(str, values))))),
            release_start_date=("release_start_date", "min"),
            release_end_date=("release_end_date", "max"),
        )
    )
    return out.reset_index(drop=True)


def _expand_actions(config: PipelineConfig, links: pd.DataFrame, attrs: pd.DataFrame) -> pd.DataFrame:
    if links.empty or attrs.empty:
        return pd.DataFrame()
    attrs = attrs.copy()
    attrs["rule_code"] = attrs["rule_code"].map(lambda value: normalize_hs_code(value, 8))
    attrs["year"] = pd.to_numeric(attrs["year"], errors="coerce").astype("Int64")
    attrs["month"] = pd.to_numeric(attrs["month"], errors="coerce").astype("Int64")
    attrs["increment_rate"] = pd.to_numeric(attrs["increment_rate"], errors="coerce")
    attrs["effective_start"] = pd.to_datetime(attrs.get("effective_start"), errors="coerce")
    attrs["effective_end"] = pd.to_datetime(attrs.get("effective_end"), errors="coerce")
    attrs = attrs[attrs["year"].between(int(config.start_period[:4]), int(config.end_period[:4]))].copy()
    merged = links.merge(attrs, on="rule_code", how="inner", suffixes=("", "_attribute"))
    start_year = int(config.start_period[:4])
    end_year = int(config.end_period[:4])
    # A zero-rate Chapter-99 row is an exemption/administrative row, not a
    # positive treatment component.  Preserve it in the source audit, but do
    # not let it create a treated action or a duplicate stack contribution.
    merged = merged[
        merged["year"].between(start_year, end_year)
        & merged["increment_rate"].notna()
        & (merged["increment_rate"] > 0)
    ].copy()
    if merged.empty:
        return pd.DataFrame()
    countries = _country_universe(config)
    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        family = str(row["family"])
        year, month = int(row["year"]), int(row["month"])
        eligible = _eligible_countries_by_deterministic_grouping(str(row["rule_code"]), year, month, countries)
        # Known-family partner rules are encoded by the official Chapter-99
        # rule family.  Text inclusion/exclusion is applied where present,
        # without consulting the package.
        include, exclude = _extract_countries_from_rule(row.get("description"), str(row["rule_code"]))
        if include:
            eligible = [country for country in eligible if country in {_canonical_country(value) for value in include}]
        if exclude:
            excluded = {_canonical_country(value) for value in exclude}
            eligible = [country for country in eligible if country not in excluded]
        if not eligible:
            continue
        legal_start = row.get("effective_start")
        if pd.isna(legal_start):
            legal_start = row.get("release_start_date")
        legal_end = row.get("effective_end")
        if pd.isna(legal_end):
            legal_end = row.get("release_end_date")
        if pd.isna(legal_start):
            continue
        legal_start = pd.Timestamp(legal_start)
        legal_end = pd.Timestamp(legal_end) if pd.notna(legal_end) else legal_start
        month_start = pd.Timestamp(year=year, month=month, day=1)
        month_end = month_start + pd.offsets.MonthEnd(1)
        active_start = max(legal_start.normalize(), month_start)
        active_end = min(legal_end.normalize(), month_end)
        active_days = max(0, int((active_end - active_start).days + 1))
        days_in_month = int(month_end.day)
        if active_days <= 0:
            continue
        rate = float(row["increment_rate"])
        rows.extend({
            "action_id": f"{row['rule_code']}|{row['hs8']}|{row['release_name']}|{country}|{year:04d}-{month:02d}",
            "policy_family": FAMILY_LABELS[family],
            "family": family,
            "rule_code": str(row["rule_code"]),
            "hs8": str(row["hs8"]),
            "partner_name": country,
            "year": year,
            "month": month,
            "period": f"{year:04d}-{month:02d}",
            "legal_effective_date": legal_start,
            "legal_end_date": legal_end,
            "active_days": active_days,
            "days_in_month": days_in_month,
            "active_share": active_days / days_in_month,
            "additional_rate": rate,
            "day_weighted_additional_rate": rate * active_days / days_in_month,
            "release_name": str(row.get("release_name") or ""),
            "source_scope": "local_hts_chapter99_machine_or_pdf",
        } for country in eligible)
    if not rows:
        return pd.DataFrame()
    actions = pd.DataFrame(rows).drop_duplicates("action_id").reset_index(drop=True)
    return actions


def _country_universe(config: PipelineConfig) -> list[str]:
    panel = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel.exists():
        return []
    try:
        import duckdb
        escaped = str(panel).replace("'", "''")
        frame = duckdb.connect().execute(
            f"SELECT DISTINCT upper(cty_name) AS cty_name FROM read_parquet('{escaped}') WHERE cty_name IS NOT NULL"
        ).fetchdf()
    except Exception:
        frame = read_table(panel, columns=["cty_name"])
    return sorted({_canonical_country(value) for value in frame["cty_name"].dropna().astype(str)})


def _active_share(legal_start: Any, legal_end: Any, year: int, month: int) -> float:
    """Return the inclusive legal-day share for one calendar month."""
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.offsets.MonthEnd(1)
    left = max(pd.Timestamp(legal_start).normalize(), start)
    right = min(pd.Timestamp(legal_end).normalize(), end)
    if right < left:
        return 0.0
    return float((right - left).days + 1) / float(end.day)


def _family_source_status(links: pd.DataFrame, attrs: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Classify independently sourced family/rule scope without package data."""
    linked_rules = set(links.get("rule_code", pd.Series(dtype="string")).dropna().astype(str))
    attributed_rules = set(attrs.get("rule_code", pd.Series(dtype="string")).dropna().astype(str))
    statuses: dict[str, dict[str, Any]] = {}
    family_prefix = {
        "solar_201": "990346",
        "washer_201": "990345",
        "steel_232": "990380",
        "aluminum_232": "990385",
        "china_301": "990388",
    }
    for family in FAMILIES:
        prefix = family_prefix[family]
        family_links = {code for code in linked_rules if code.startswith(prefix)}
        family_attrs = {code for code in attributed_rules if code.startswith(prefix)}
        positive_rules = sorted(
            code for code in family_attrs
            if (float(attrs.loc[attrs["rule_code"].astype(str).eq(code), "increment_rate"].max())
                if not attrs.empty and "increment_rate" in attrs else 0.0) > 0
        )
        positive_missing = [code for code in positive_rules if code not in linked_rules]
        statuses[family] = {
            "linked_rule_count": int(len(family_links)),
            "attributed_rule_count": int(len(family_attrs)),
            "attribute_rules_without_scope_links": sorted(family_attrs - linked_rules),
            "expected_positive_rule_prefixes": list(EXPECTED_POSITIVE_RULE_PREFIXES[family]),
            "positive_attribute_rules": positive_rules,
            "expected_positive_rules_without_scope_links": positive_missing,
            "scope_status": "complete" if family_links and family_attrs and not positive_missing else "partial_missing_positive_scope",
        }
    return statuses


def _family_components(actions: pd.DataFrame) -> pd.DataFrame:
    if actions.empty:
        return pd.DataFrame()
    keys = ["partner_name", "hs8", "year", "month", "family"]
    grouped = actions.groupby(keys, as_index=False).agg(
        action_count=("action_id", "nunique"),
        additional_rate=("additional_rate", "sum"),
        day_weighted_additional_rate=("day_weighted_additional_rate", "sum"),
        legal_effective_date=("legal_effective_date", "min"),
        source_action_ids=("action_id", lambda values: "|".join(sorted(set(map(str, values))))),
    )
    grouped["paper_effective_month"] = (
        grouped["legal_effective_date"].dt.to_period("M").astype(str)
    )
    grouped["policy_family"] = grouped["family"].map(FAMILY_LABELS)
    return grouped


def _materialize_pooled_panel(config: PipelineConfig, components: pd.DataFrame) -> Path:
    out = analysis_root(config) / "independent_final_legal_pooled_policy.parquet"
    component_path = analysis_root(config) / "family_policy_schedule.parquet"
    write_parquet(components, component_path, overwrite=True)
    if components.empty:
        write_parquet(pd.DataFrame(), out, overwrite=True)
        return out
    import duckdb
    raw_panel = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    component_sql = str(component_path).replace("'", "''")
    raw_sql = str(raw_panel).replace("'", "''")
    # Join at HS8 first so the source schedule remains compact; family columns
    # are retained separately before pooling and never silently overwritten.
    con = duckdb.connect()
    try:
        family_sql = ", ".join(
            f"max(CASE WHEN family = '{family}' THEN additional_rate END) AS {family}_additional_rate, "
            f"max(CASE WHEN family = '{family}' THEN day_weighted_additional_rate END) AS {family}_day_weighted_additional_rate, "
            f"min(CASE WHEN family = '{family}' THEN legal_effective_date END) AS {family}_legal_effective_date, "
            f"min(CASE WHEN family = '{family}' THEN paper_effective_month END) AS {family}_paper_effective_month, "
            f"max(CASE WHEN family = '{family}' THEN source_action_ids END) AS {family}_source_action_ids"
            for family in FAMILIES
        )
        family_select = ", ".join(
            f"a.{family}_additional_rate, a.{family}_day_weighted_additional_rate, "
            f"a.{family}_legal_effective_date, a.{family}_paper_effective_month, "
            f"CASE WHEN a.{family}_additional_rate IS NULL THEN 0 ELSE 1 END AS {family}_hit"
            for family in FAMILIES
        )
        add_sum = " + ".join(f"coalesce(a.{family}_additional_rate, 0)" for family in FAMILIES)
        weighted_sum = " + ".join(f"coalesce(a.{family}_day_weighted_additional_rate, 0)" for family in FAMILIES)
        legal_dates = ", ".join(f"a.{family}_legal_effective_date" for family in FAMILIES)
        paper_dates = ", ".join(f"a.{family}_paper_effective_month" for family in FAMILIES)
        query = f"""
        WITH c AS (
          SELECT partner_name, hs8, year, month, family,
                 sum(additional_rate) AS additional_rate,
                 sum(day_weighted_additional_rate) AS day_weighted_additional_rate,
                 min(legal_effective_date) AS legal_effective_date,
                 min(paper_effective_month) AS paper_effective_month,
                 max(source_action_ids) AS source_action_ids
          FROM read_parquet('{component_sql}')
          GROUP BY ALL
        ), p AS (
          SELECT cty_code, upper(cty_name) AS cty_name, hs10, hs8, year, month,
                 base_statutory_rate_raw, m_val, m_q1
          FROM read_parquet('{raw_sql}')
          WHERE year BETWEEN {int(config.start_period[:4])} AND {int(config.end_period[:4])}
        )
        , a AS (
          SELECT partner_name, hs8, year, month, {family_sql}
          FROM c
          GROUP BY partner_name, hs8, year, month
        )
        SELECT p.cty_code, p.cty_name, p.hs10, p.hs8, p.year, p.month,
               CASE WHEN p.base_statutory_rate_raw >= 9000 THEN NULL ELSE p.base_statutory_rate_raw END AS base_statutory_rate_raw,
               {family_select},
               ({add_sum}) AS independent_additional_rate,
               ({weighted_sum}) AS independent_day_weighted_additional_rate,
               CASE WHEN ({add_sum}) > 0 THEN TRUE ELSE FALSE END AS independent_treated,
               CASE WHEN p.base_statutory_rate_raw IS NULL OR p.base_statutory_rate_raw >= 9000 THEN NULL
                    ELSE p.base_statutory_rate_raw + ({add_sum}) END AS independent_total_statutory_rate,
               CASE WHEN p.base_statutory_rate_raw IS NULL OR p.base_statutory_rate_raw >= 9000 THEN NULL
                    ELSE p.base_statutory_rate_raw + ({weighted_sum}) END AS independent_total_day_weighted_rate,
               strftime(least({legal_dates}), '%Y-%m') AS independent_legal_effective_month,
               coalesce({paper_dates}) AS independent_paper_effective_month
        FROM p LEFT JOIN a
          ON upper(p.cty_name)=a.partner_name AND p.hs8=a.hs8
           AND p.year=a.year AND p.month=a.month
        """
        fd, temporary_name = tempfile.mkstemp(prefix=f".{out.name}.", suffix=".tmp", dir=out.parent)
        os.close(fd)
        Path(temporary_name).unlink(missing_ok=True)
        temporary = Path(temporary_name)
        try:
            con.execute(f"COPY ({query}) TO '{str(temporary).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(temporary)
            if parquet_file.metadata.num_rows <= 0 or parquet_file.metadata.num_row_groups <= 0:
                raise ValueError("Pooled policy Parquet validation failed")
            del parquet_file
            temporary.replace(out)
        finally:
            if temporary.exists():
                temporary.unlink()
        return out
    finally:
        con.close()


def build_pooled_policy(config: PipelineConfig) -> dict[str, Any]:
    inventory = _local_source_inventory(config)
    write_metadata_json(root(config) / "source_inventory.json", inventory)
    write_metadata_json(root(config) / "pooled_policy_missing_sources.json", {"missing": inventory["missing_source_categories"], "status": inventory["status"]})
    if inventory["status"] != "complete":
        return {"status": "blocked_missing_sources", "inventory": inventory}
    links = _all_links(config)
    attrs = _load_tradewar_rule_attributes(config)
    family_source_status = _family_source_status(links, attrs)
    missing_families = [family for family, status in family_source_status.items() if status["scope_status"] != "complete"]
    write_metadata_json(
        root(config) / "pooled_policy_family_source_status.json",
        {
            "families": family_source_status,
            "missing_families": missing_families,
            "release_blocker": "one or more positive-rate family rules lack an independently sourced HS8 scope",
        },
    )
    write_metadata_json(
        root(config) / "pooled_policy_missing_sources.json",
        {
            "missing": inventory["missing_source_categories"],
            "status": "blocked_partial_family_scope" if missing_families else inventory["status"],
            "missing_families": missing_families,
            "family_source_status": family_source_status,
        },
    )
    actions = _expand_actions(config, links, attrs)
    action_path = analysis_root(config) / "legal_action_ledger.parquet"
    write_parquet(actions, action_path, overwrite=True)
    components = _family_components(actions)
    panel_path = _materialize_pooled_panel(config, components)
    manifest = {
        "version": VERSION,
        "status": "built_partial" if missing_families else ("built" if not actions.empty else "blocked_no_actions"),
        "source_mode": "independent_local_official_sources",
        "source_inventory": relative(config, root(config) / "source_inventory.json"),
        "action_ledger": relative(config, action_path),
        "family_schedule": relative(config, analysis_root(config) / "family_policy_schedule.parquet"),
        "pooled_panel": relative(config, panel_path),
        "families": {family: int((actions["family"] == family).sum()) if not actions.empty else 0 for family in FAMILIES},
        "rows": int(len(actions)),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "builder_fingerprint": _hash_payload({"version": VERSION, "families": FAMILIES, "stacking": "sum_family_components"}),
        "package_policy_used_by_builder": False,
        "missing_families": missing_families,
        "family_source_status": family_source_status,
    }
    write_metadata_json(root(config) / "pooled_policy_build_manifest.json", manifest)
    return manifest


def _package_reference(config: PipelineConfig) -> Path:
    return config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"


def validate_pooled_policy(config: PipelineConfig) -> dict[str, Any]:
    panel_path = analysis_root(config) / "independent_final_legal_pooled_policy.parquet"
    reference = _package_reference(config)
    if not panel_path.exists() or not reference.exists():
        result = {"status": "blocked_missing_artifact", "panel": relative(config, panel_path), "reference": relative(config, reference)}
        write_metadata_json(root(config) / "pooled_policy_replication_gate.json", result)
        return result
    import duckdb
    legal_sql = str(panel_path).replace("'", "''")
    pkg_sql = str(reference).replace("'", "''")
    comparison_path = root(config) / "pooled_policy_validation_comparison.parquet"
    con = duckdb.connect()
    try:
        query = f"""
        SELECT p.cty_code, p.hs10, p.year, p.month,
               p.independent_treated, p.independent_additional_rate,
               p.independent_day_weighted_additional_rate,
               p.solar_201_additional_rate, p.solar_201_day_weighted_additional_rate,
               p.washer_201_additional_rate, p.washer_201_day_weighted_additional_rate,
               p.steel_232_additional_rate, p.steel_232_day_weighted_additional_rate,
               p.aluminum_232_additional_rate, p.aluminum_232_day_weighted_additional_rate,
               p.china_301_additional_rate, p.china_301_day_weighted_additional_rate,
               p.independent_total_day_weighted_rate,
               r.m_status2, r.m_effective_mdate2, r.m_stattariff2,
               r.m_val,
               CASE WHEN p.independent_legal_effective_month IS NULL OR r.m_effective_mdate2 IS NULL THEN NULL
                    WHEN strftime(r.m_effective_mdate2, '%Y-%m') = p.independent_legal_effective_month THEN 1 ELSE 0 END AS effective_month_exact,
               CASE WHEN p.independent_paper_effective_month IS NULL OR r.m_effective_mdate2 IS NULL THEN NULL
                    WHEN abs(date_diff('month', cast(r.m_effective_mdate2 AS DATE), cast(p.independent_paper_effective_month || '-01' AS DATE))) <= 1 THEN 1 ELSE 0 END AS paper_month_within_one,
               CASE WHEN r.m_stattariff2 IS NULL OR p.independent_additional_rate IS NULL THEN NULL
                    ELSE abs(cast(p.independent_additional_rate AS DOUBLE) - cast(r.m_stattariff2 AS DOUBLE)) END AS additional_abs_diff,
               CASE WHEN r.m_stattariff2 IS NULL OR p.independent_day_weighted_additional_rate IS NULL THEN NULL
                    ELSE abs(cast(p.independent_day_weighted_additional_rate AS DOUBLE) - cast(r.m_stattariff2 AS DOUBLE)) END AS dayweighted_abs_diff
        FROM read_parquet('{legal_sql}') p
        JOIN read_parquet('{pkg_sql}') r USING (cty_code, hs10, year, month)
        WHERE r.year BETWEEN 2017 AND 2019
        """
        temporary_name = str(comparison_path).replace("'", "''")
        con.execute(f"COPY ({query}) TO '{temporary_name}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        aggregate = con.execute(
            f"""
            SELECT count(*) AS rows,
                   avg(CASE WHEN ((cast(m_status2 AS DOUBLE)=2) = independent_treated) THEN 1.0 ELSE 0.0 END) AS treatment_match,
                   sum(CASE WHEN cast(m_val AS DOUBLE)>0 AND ((cast(m_status2 AS DOUBLE)=2) = independent_treated) THEN cast(m_val AS DOUBLE) ELSE 0 END)
                     / nullif(sum(CASE WHEN cast(m_val AS DOUBLE)>0 THEN cast(m_val AS DOUBLE) ELSE 0 END), 0) AS trade_weighted_treatment_match,
                   avg(additional_abs_diff) AS additional_mae,
                   avg(dayweighted_abs_diff) AS dayweighted_additional_mae,
                   avg(CASE WHEN additional_abs_diff <= 0.001 THEN 1.0 ELSE 0.0 END) AS increment_within_10bp,
                   avg(CASE WHEN additional_abs_diff <= 0.005 THEN 1.0 ELSE 0.0 END) AS increment_within_50bp,
                   avg(effective_month_exact) AS effective_month_exact_match,
                   avg(paper_month_within_one) AS paper_month_within_one_match,
                   sum(CASE WHEN m_val > 0 AND additional_abs_diff IS NOT NULL THEN m_val ELSE 0 END) /
                     nullif(sum(CASE WHEN m_val > 0 THEN m_val ELSE 0 END), 0) AS trade_weighted_comparable_share,
                   sum(CASE WHEN m_val > 0 THEN m_val * coalesce(dayweighted_abs_diff, 0) ELSE 0 END) /
                     nullif(sum(CASE WHEN m_val > 0 AND dayweighted_abs_diff IS NOT NULL THEN m_val ELSE 0 END), 0) AS trade_weighted_dayweighted_mae
            FROM read_parquet('{temporary_name}')
            """
        ).fetchdf().iloc[0].to_dict()
        family_rows = []
        for family in FAMILIES:
            additional = f"{family}_additional_rate"
            weighted = f"{family}_day_weighted_additional_rate"
            family_rows.append(
                con.execute(
                    f"""
                    SELECT '{family}' AS family,
                           count(*) FILTER (WHERE {additional} IS NOT NULL) AS independent_scope_rows,
                           avg(CASE WHEN {additional} IS NOT NULL AND cast(m_status2 AS DOUBLE)=2 THEN 1.0 ELSE 0.0 END)
                             FILTER (WHERE {additional} IS NOT NULL) AS package_treatment_match,
                           avg(abs(cast({additional} AS DOUBLE)-cast(m_stattariff2 AS DOUBLE)))
                             FILTER (WHERE {additional} IS NOT NULL AND m_stattariff2 IS NOT NULL) AS additional_mae,
                           avg(abs(cast({weighted} AS DOUBLE)-cast(m_stattariff2 AS DOUBLE)))
                             FILTER (WHERE {weighted} IS NOT NULL AND m_stattariff2 IS NOT NULL) AS dayweighted_mae,
                           sum(CASE WHEN m_val > 0 AND {additional} IS NOT NULL THEN m_val ELSE 0 END) /
                             nullif(sum(CASE WHEN m_val > 0 THEN m_val ELSE 0 END), 0) AS trade_weighted_scope_share
                    FROM read_parquet('{temporary_name}')
                    """
                ).fetchdf().iloc[0].to_dict()
            )
        family_summary = pd.DataFrame(family_rows)
        family_path = root(config) / "pooled_policy_family_validation.parquet"
        write_parquet(family_summary, family_path, overwrite=True)
        family_summary.to_csv(root(config) / "pooled_policy_family_validation_summary.csv", index=False)
    finally:
        con.close()
    if not comparison_path.exists():
        result = {"status": "blocked_empty_comparison"}
        write_metadata_json(root(config) / "pooled_policy_replication_gate.json", result)
        return result
    family_status = json.loads((root(config) / "pooled_policy_family_source_status.json").read_text(encoding="utf-8"))
    missing_families = family_status.get("missing_families", [])
    result = {
        "version": VERSION,
        "status": "diagnostic_complete",
        "rows": int(aggregate.get("rows", 0) or 0),
        "treatment_match": float(aggregate.get("treatment_match", 0.0) or 0.0),
        "trade_weighted_treatment_match": float(aggregate.get("trade_weighted_treatment_match", 0.0) or 0.0),
        "additional_mae_vs_package_policy_field": float(aggregate.get("additional_mae", 0.0) or 0.0),
        "dayweighted_additional_mae_vs_package_policy_field": float(aggregate.get("dayweighted_additional_mae", 0.0) or 0.0),
        "increment_within_10bp": float(aggregate.get("increment_within_10bp", 0.0) or 0.0),
        "increment_within_50bp": float(aggregate.get("increment_within_50bp", 0.0) or 0.0),
        "effective_month_exact_match": float(aggregate.get("effective_month_exact_match", 0.0) or 0.0),
        "paper_month_within_one_match": float(aggregate.get("paper_month_within_one_match", 0.0) or 0.0),
        "trade_weighted_comparable_share": float(aggregate.get("trade_weighted_comparable_share", 0.0) or 0.0),
        "trade_weighted_dayweighted_mae": float(aggregate.get("trade_weighted_dayweighted_mae", 0.0) or 0.0),
        "total_rate_comparison": "not_evaluated_until_base_statutory_scope_is_resolved",
        "missing_families": missing_families,
        "comparison_path": relative(config, comparison_path),
        "family_validation_path": relative(config, root(config) / "pooled_policy_family_validation.parquet"),
        "package_policy_used_by_builder": False,
        "independent_legal_gate": False,
        "paper_compatible_gate": False,
        "ready_for_full_historical_replication_rerun": False,
        "ready_for_2025_policy_extension": False,
        "event_2025_ready": False,
        "note": "Initial pooled comparison is diagnostic; family-specific rate and timing validation must be completed before release.",
    }
    write_metadata_json(root(config) / "pooled_policy_replication_gate.json", result)
    return result


def run(config: PipelineConfig) -> dict[str, Any]:
    built = build_pooled_policy(config)
    if built.get("status") == "blocked_missing_sources":
        return built
    validation = validate_pooled_policy(config)
    return {"build": built, "validation": validation}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build and validate independent pooled 201/232/301 import policy")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.default()
    result = validate_pooled_policy(config) if args.validate_only else run(config)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
