"""Section 301 regression sensitivity v3 with synchronized raw/package inputs."""

from __future__ import annotations

import gc
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
import hashlib
import json

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyfixest as pf

from .config import PipelineConfig
from .io_utils import normalize_hs_code, sha256_file, write_data_dictionary, write_metadata_json, write_parquet
from .trade_regression_common import write_markdown_report


KEY_COLUMNS = ["cty_code", "hs10", "year", "month"]
OUTCOMES = ("val", "q1", "p", "pduty")
WINDOWS = {
    "paper_6m": {"event_min": -6, "event_max": 6, "baseline": -6, "label": "Paper-faithful 6-month window"},
    "common_12m": {"event_min": -12, "event_max": 12, "baseline": -12, "label": "Common 12-month window"},
}
SENSITIVITY_VERSION = "v3"
LEGACY_INVALID_VERSION = "v2"


@dataclass(frozen=True, slots=True)
class VariantSpec:
    code: str
    label: str
    bridges: tuple[str, ...]
    outcome_source: str
    target_source: str
    calendar_source: str
    tariff_source: str


VARIANTS: tuple[VariantSpec, ...] = (
    VariantSpec("A", "Package benchmark", ("pooled_outcome_bridge", "china301_policy_bridge"), "pkg", "pkg", "pkg_legal", "pkg"),
    VariantSpec("B", "Raw outcomes / package treatment", ("pooled_outcome_bridge", "china301_policy_bridge"), "raw", "pkg", "pkg_legal", "pkg"),
    VariantSpec("C_map_only", "Raw outcomes / raw treatment / frozen package calendar", ("china301_policy_bridge",), "raw", "raw", "pkg_frozen", "pkg"),
    VariantSpec("C_legal", "Raw outcomes / raw treatment / raw legal calendar", ("china301_policy_bridge",), "raw", "raw", "raw_legal", "pkg"),
    VariantSpec("C_paper", "Raw outcomes / raw treatment / raw paper calendar", ("china301_policy_bridge",), "raw", "raw", "raw_paper", "pkg"),
    VariantSpec("D_legal", "Raw outcomes / raw treatment / raw legal tariff", ("china301_policy_bridge",), "raw", "raw", "raw_legal", "raw"),
    VariantSpec("D_paper", "Raw outcomes / raw treatment / raw paper tariff", ("china301_policy_bridge",), "raw", "raw", "raw_paper", "raw"),
)

BRIDGES = ("pooled_outcome_bridge", "china301_policy_bridge")


def _duplicate_source_variant(variant: VariantSpec, outcome: str) -> str | None:
    if outcome == "pduty":
        return None
    if variant.code == "D_legal":
        return "C_legal"
    if variant.code == "D_paper":
        return "C_paper"
    return None


def _clone_fit_result(frame: pd.DataFrame, source_variant: VariantSpec, target_variant: VariantSpec) -> pd.DataFrame:
    clone = frame.copy()
    clone["variant"] = target_variant.code
    clone["variant_label"] = target_variant.label
    clone["calendar"] = target_variant.calendar_source
    clone["reused_from"] = source_variant.code
    return clone


def _expected_fit_count() -> int:
    total = 0
    for bridge in BRIDGES:
        bridge_variants = _bridge_variants(bridge)
        for window in WINDOWS:
            for outcome in OUTCOMES:
                total += sum(1 for variant in bridge_variants if _duplicate_source_variant(variant, outcome) is None)
    return total


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _month_index(year: pd.Series, month: pd.Series) -> pd.Series:
    return (pd.to_numeric(year, errors="coerce") * 12 + pd.to_numeric(month, errors="coerce") - 1).astype("Int64")


def _key_series(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["cty_code"].astype("Int64").astype(str)
        + "|"
        + frame["hs10"].astype("string")
        + "|"
        + frame["year"].astype("Int64").astype(str)
        + "|"
        + frame["month"].astype("Int64").astype(str)
    )


def _hash_keys(frame: pd.DataFrame) -> str:
    payload = "|".join(sorted(_key_series(frame).tolist()))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _pair_id(frame: pd.DataFrame) -> pd.Series:
    if "id" in frame.columns:
        return frame["id"]
    return pd.factorize(frame["cty_code"].astype("Int64").astype(str) + "|" + frame["hs10"].astype("string"), sort=False)[0]


def _artifact_dir(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / SENSITIVITY_VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def _cache_dir(config: PipelineConfig) -> Path:
    path = _artifact_dir(config) / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _checkpoint_dir(config: PipelineConfig) -> Path:
    path = _artifact_dir(config) / "checkpoints"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _diagnostic_dir(config: PipelineConfig) -> Path:
    path = _artifact_dir(config) / "diagnostics"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _output_path(config: PipelineConfig, name: str) -> Path:
    return _artifact_dir(config) / name


def _cache_paths(config: PipelineConfig) -> dict[str, Path]:
    cache = _cache_dir(config)
    return {
        "package": cache / "package_paper_window.parquet",
        "package_meta": cache / "package_paper_window.metadata.json",
        "raw": cache / "raw_paper_window.parquet",
        "raw_meta": cache / "raw_paper_window.metadata.json",
    }


def _legacy_status_paths(config: PipelineConfig) -> tuple[Path, Path]:
    v2_dir = config.verification_dir / "raw_replication_imports" / LEGACY_INVALID_VERSION
    v2_dir.mkdir(parents=True, exist_ok=True)
    return v2_dir / "status.json", config.verification_dir / "raw_replication_imports" / "section301_regression_sensitivity_v1_status.json"


def _source_metadata(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "exists": path.exists(),
        "modified_time": pd.Timestamp(stat.st_mtime, unit="s", tz="UTC").isoformat(),
        "size": int(stat.st_size),
        "sha256": sha256_file(path),
    }


def _write_invalid_legacy_status(config: PipelineConfig) -> None:
    v2_status_path, v1_status_path = _legacy_status_paths(config)
    payload = {
        "version": LEGACY_INVALID_VERSION,
        "valid": False,
        "reason": "package DTA unused; stale workhorse used; preflight path differs from actual loader path; package/raw inputs identical; no v2 coefficients; run timed out",
    }
    write_metadata_json(v2_status_path, payload)
    write_metadata_json(v1_status_path, {**payload, "version": "v1"})


def _package_source_columns() -> list[str]:
    return [
        "id",
        "cty_code",
        "cty_name",
        "hs10",
        "hs8",
        "hs6",
        "hs4",
        "hs2",
        "year",
        "month",
        "mdate",
        "m_val",
        "m_q1",
        "m_p",
        "m_pduty",
        "m_duty",
        "m_dut_val",
        "m_alum_hit",
        "m_china_hit",
        "m_hit",
        "m_solar_hit",
        "m_steel_hit",
        "m_washer_hit",
        "m_effective_date",
        "m_effective_mdate1",
        "m_effective_mdate2",
        "m_stattariff1",
        "m_stattariff2",
        "m_status1",
        "m_status2",
        "eu",
        "m_T",
        "m_applied_tariff",
        "m_apptariff",
        "m_ess",
        "m_increase",
        "m_valduty",
        "naics",
        "naics_str",
        "lm_p",
        "lm_pduty",
        "lm_q1",
        "lm_val",
        "lm_valduty",
    ]


def _raw_source_columns() -> list[str]:
    return [
        "cty_code",
        "cty_name",
        "hs10",
        "hs8",
        "hs6",
        "hs4",
        "hs2",
        "year",
        "month",
        "mdate",
        "period",
        "m_val",
        "m_q1",
        "m_p",
        "m_pduty",
        "base_statutory_rate_raw",
        "m_statutory_tariff1",
        "m_statutory_tariff2",
        "m_policy_source",
        "source_type",
        "release_name",
        "tw_increment_rate_raw",
        "tw_active_share_raw",
        "tw_rule_code_raw",
        "tw_scope_source_raw",
    ]


def _filter_period(frame: pd.DataFrame) -> pd.DataFrame:
    year = pd.to_numeric(frame["year"], errors="coerce")
    month = pd.to_numeric(frame["month"], errors="coerce")
    return frame.loc[(year >= 2017) & ((year < 2019) | ((year == 2019) & (month <= 4)))].copy()


def _package_cache_fresh(config: PipelineConfig, cache_path: Path, meta_path: Path, source: Path) -> bool:
    if not cache_path.exists() or not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    src = _source_metadata(source)
    return (
        meta.get("source_path") == src["path"]
        and meta.get("source_sha256") == src["sha256"]
        and meta.get("source_size") == src["size"]
        and meta.get("source_modified_time") == src["modified_time"]
        and pd.Timestamp(cache_path.stat().st_mtime, unit="s", tz="UTC") >= pd.Timestamp(source.stat().st_mtime, unit="s", tz="UTC")
    )


def _raw_cache_fresh(cache_path: Path, meta_path: Path, source: Path) -> bool:
    if not cache_path.exists() or not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    src = _source_metadata(source)
    return (
        meta.get("source_path") == src["path"]
        and meta.get("source_sha256") == src["sha256"]
        and meta.get("source_size") == src["size"]
        and meta.get("source_modified_time") == src["modified_time"]
    )


def _build_package_paper_window_cache(config: PipelineConfig, overwrite: bool = False) -> tuple[Path, dict[str, Any]]:
    source = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    paths = _cache_paths(config)
    cache_path = paths["package"]
    meta_path = paths["package_meta"]
    if cache_path.exists() and not overwrite and _package_cache_fresh(config, cache_path, meta_path, source):
        return cache_path, json.loads(meta_path.read_text(encoding="utf-8"))
    if not source.exists():
        raise FileNotFoundError(f"Missing package benchmark DTA: {source}")
    columns = _package_source_columns()
    available = set(pd.read_stata(source, convert_categoricals=False, iterator=True, chunksize=1).__next__().columns)
    use_columns = [column for column in columns if column in available]
    row_count = 0
    treated_rows = 0
    treated_products: set[str] = set()
    period_min: str | None = None
    period_max: str | None = None
    writer: pq.ParquetWriter | None = None
    chunk_reader = pd.read_stata(source, columns=use_columns, convert_categoricals=False, iterator=True, chunksize=250_000)
    try:
        for chunk in chunk_reader:
            chunk = _filter_period(chunk)
            if chunk.empty:
                continue
            chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
            chunk["hs8"] = chunk["hs10"].str.slice(0, 8)
            chunk["hs6"] = chunk["hs10"].str.slice(0, 6)
            chunk["hs4"] = chunk["hs10"].str.slice(0, 4)
            chunk["hs2"] = chunk["hs10"].str.slice(0, 2)
            chunk["cty_code"] = pd.to_numeric(chunk["cty_code"], errors="coerce").astype("Int64")
            chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce").astype("Int64")
            chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce").astype("Int64")
            if "naics_str" in chunk.columns:
                chunk["naics_str"] = chunk["naics_str"].astype("string")
            if "m_status2" in chunk.columns:
                chunk["m_status2"] = pd.to_numeric(chunk["m_status2"], errors="coerce").fillna(0).astype("Int64")
            if "m_ess" in chunk.columns:
                chunk["m_ess"] = pd.to_numeric(chunk["m_ess"], errors="coerce").fillna(0).astype("Int64")
            if "m_china_hit" in chunk.columns:
                chunk["m_china_hit"] = pd.to_numeric(chunk["m_china_hit"], errors="coerce").fillna(0).astype("Int64")
            for column in ("m_val", "m_q1", "m_p", "m_pduty", "m_stattariff1", "m_stattariff2"):
                if column in chunk.columns:
                    chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
            if "m_effective_mdate2" in chunk.columns:
                chunk["m_effective_mdate2"] = pd.to_numeric(chunk["m_effective_mdate2"], errors="coerce").astype("Int64")
            if "m_effective_mdate1" in chunk.columns:
                chunk["m_effective_mdate1"] = pd.to_numeric(chunk["m_effective_mdate1"], errors="coerce").astype("Int64")
            if "mdate" in chunk.columns:
                chunk["mdate"] = pd.to_numeric(chunk["mdate"], errors="coerce")
            if {"year", "month"}.issubset(chunk.columns):
                period = chunk[["year", "month"]].dropna().copy()
                if not period.empty:
                    current_min = f"{int(period['year'].min()):04d}-{int(period['month'].min()):02d}"
                    current_max = f"{int(period['year'].max()):04d}-{int(period['month'].max()):02d}"
                    period_min = current_min if period_min is None else min(period_min, current_min)
                    period_max = current_max if period_max is None else max(period_max, current_max)
            treated_mask = chunk.get("m_status2", pd.Series(0, index=chunk.index)).eq(2)
            treated_rows += int(treated_mask.sum())
            if treated_mask.any() and "hs10" in chunk.columns:
                treated_products.update(chunk.loc[treated_mask, "hs10"].astype("string").dropna().tolist())
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(cache_path, table.schema, compression="zstd")
            writer.write_table(table)
            row_count += int(len(chunk))
    finally:
        if writer is not None:
            writer.close()
    if row_count == 0:
        raise RuntimeError("Package benchmark cache is empty after filtering to the paper window.")
    metadata = {
        **_source_metadata(source),
        "source_path": str(source),
        "source_sha256": sha256_file(source),
        "source_size": int(source.stat().st_size),
        "source_modified_time": pd.Timestamp(source.stat().st_mtime, unit="s", tz="UTC").isoformat(),
        "cache_path": str(cache_path),
        "rows": int(row_count),
        "treated_rows": int(treated_rows),
        "treated_products": int(len(treated_products)),
        "period_min": period_min,
        "period_max": period_max,
        "columns": use_columns,
        "opened_path": str(source),
    }
    write_metadata_json(meta_path, metadata)
    return cache_path, metadata


def _build_raw_paper_window_cache(config: PipelineConfig, overwrite: bool = False) -> tuple[Path, dict[str, Any]]:
    source = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    paths = _cache_paths(config)
    cache_path = paths["raw"]
    meta_path = paths["raw_meta"]
    if cache_path.exists() and not overwrite and _raw_cache_fresh(cache_path, meta_path, source):
        return cache_path, json.loads(meta_path.read_text(encoding="utf-8"))
    if not source.exists():
        raise FileNotFoundError(f"Missing current raw panel: {source}")
    wanted = _raw_source_columns()
    schema = pq.read_schema(source).names
    columns = [column for column in wanted if column in schema]
    query = f"""
        SELECT {", ".join(columns)}
        FROM read_parquet(?)
        WHERE cty_code > 0
          AND year >= 2017
          AND (year < 2019 OR month <= 4)
    """
    frame = duckdb.connect(database=":memory:").execute(query, [str(source)]).fetch_df()
    if frame.empty:
        raise RuntimeError("Current raw panel subset is empty after filtering to the paper window.")
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["hs8"] = frame["hs10"].str.slice(0, 8)
    frame["hs6"] = frame["hs10"].str.slice(0, 6)
    frame["hs4"] = frame["hs10"].str.slice(0, 4)
    frame["hs2"] = frame["hs10"].str.slice(0, 2)
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    for column in ("m_val", "m_q1", "m_p", "m_pduty", "m_statutory_tariff1", "m_statutory_tariff2", "base_statutory_rate_raw", "tw_increment_rate_raw", "tw_active_share_raw"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("m_policy_source", "source_type", "release_name", "tw_rule_code_raw", "tw_scope_source_raw", "cty_name", "period"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    frame["raw_policy_family"] = np.where(
        frame.get("tw_rule_code_raw", pd.Series(pd.NA, index=frame.index, dtype="string")).astype("string").str.replace(r"\D", "", regex=True).str.startswith("990388", na=False),
        "china301",
        "other",
    )
    frame["raw_policy_source"] = frame.get("m_policy_source", pd.Series(pd.NA, index=frame.index, dtype="string")).astype("string")
    frame["raw_hs10"] = frame["hs10"]
    frame["raw_policy_source_path"] = str(source)
    write_parquet(frame, cache_path, overwrite=True)
    metadata = {
        **_source_metadata(source),
        "source_path": str(source),
        "source_sha256": sha256_file(source),
        "source_size": int(source.stat().st_size),
        "source_modified_time": pd.Timestamp(source.stat().st_mtime, unit="s", tz="UTC").isoformat(),
        "cache_path": str(cache_path),
        "rows": int(len(frame)),
        "treated_rows": int(pd.to_numeric(frame.get("tw_increment_rate_raw"), errors="coerce").gt(0).sum()),
        "treated_products": int(frame.loc[pd.to_numeric(frame.get("tw_increment_rate_raw"), errors="coerce").gt(0), "hs10"].astype("string").nunique()),
        "period_min": None if frame.empty else f"{int(frame['year'].min()):04d}-{int(frame['month'].min()):02d}",
        "period_max": None if frame.empty else f"{int(frame['year'].max()):04d}-{int(frame['month'].max()):02d}",
        "columns": frame.columns.tolist(),
        "opened_path": str(source),
    }
    write_metadata_json(meta_path, metadata)
    return cache_path, metadata


def _write_preflight(config: PipelineConfig, package_meta: dict[str, Any], raw_meta: dict[str, Any], package_path: Path, raw_path: Path) -> tuple[Path, Path, list[dict[str, Any]]]:
    out_dir = _artifact_dir(config)
    overlay_path = config.analysis_dir / "tradewar_overlay_raw.parquet"
    final_panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    overlay_stat = overlay_path.stat() if overlay_path.exists() else None
    final_stat = final_panel_path.stat() if final_panel_path.exists() else None
    package_stat = package_path.stat()
    raw_stat = raw_path.stat()
    v2_status_path, _ = _legacy_status_paths(config)
    records: list[dict[str, Any]] = []
    for role, path, meta, stat in (
        ("package_dta_source", config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta", package_meta, package_path.stat()),
        ("current_panel_raw_source", config.analysis_dir / "us_products_partner_hs10_monthly.parquet", raw_meta, raw_path.stat()),
        ("package_cache", package_path, package_meta, package_stat),
        ("raw_cache", raw_path, raw_meta, raw_stat),
        ("legacy_v2_status", v2_status_path, {}, v2_status_path.stat() if v2_status_path.exists() else None),
        ("legacy_workhorse", config.analysis_dir / "trade_regressions" / "workhorse" / "m_flow_hs10_fm_new_regression.parquet", {}, None),
        ("legacy_section301_panel", config.analysis_dir / "section301_imports_hs10.parquet", {}, None),
    ):
        exists = path.exists()
        readable = exists
        row_count = None
        period_min = meta.get("period_min")
        period_max = meta.get("period_max")
        treated_rows = meta.get("treated_rows")
        treated_products = meta.get("treated_products")
        failure = None
        source_path = meta.get("source_path", str(path))
        local_source_path = str(path)
        modified_time = None if not exists else pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").isoformat()
        size = None if not exists else int(path.stat().st_size)
        if exists and path.suffix.lower() == ".parquet":
            try:
                row_count = int(len(pd.read_parquet(path, columns=["cty_code"]) if "cty_code" in pq.read_schema(path).names else pd.read_parquet(path)))
            except Exception as exc:
                readable = False
                failure = str(exc)
        elif exists and path.suffix.lower() == ".dta":
            try:
                row_count = int(len(pd.read_stata(path, convert_categoricals=False, iterator=True, chunksize=1).__next__()))
            except Exception as exc:
                readable = False
                failure = str(exc)
        elif exists and path.suffix.lower() == ".json":
            row_count = 1
        if role == "package_cache":
            valid = exists and readable and meta.get("source_path") == str(config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta") and package_stat.st_mtime >= config.fajgelbaum_analysis_dir.joinpath("m_flow_hs10_fm_new.dta").stat().st_mtime
        elif role == "raw_cache":
            valid = exists and readable and meta.get("source_path") == str(config.analysis_dir / "us_products_partner_hs10_monthly.parquet") and raw_stat.st_mtime >= config.analysis_dir.joinpath("us_products_partner_hs10_monthly.parquet").stat().st_mtime
        elif role == "package_dta_source":
            valid = exists and readable and (treated_rows or 0) > 0 and (period_max or "") >= "2019-04"
        elif role == "current_panel_raw_source":
            valid = exists and readable and (treated_rows or 0) > 0 and (period_max or "") >= "2019-04"
        else:
            valid = False
        if overlay_stat is not None and exists:
            newer_than_overlay = path.stat().st_mtime >= overlay_stat.st_mtime
        else:
            newer_than_overlay = None
        if final_stat is not None and exists:
            newer_than_final_panel = path.stat().st_mtime >= final_stat.st_mtime
        else:
            newer_than_final_panel = None
        if role in {"legacy_workhorse", "legacy_section301_panel", "legacy_v2_status"}:
            valid = False
            failure = "legacy input or status marker is intentionally invalid for v3"
        if role in {"package_cache", "raw_cache"} and not valid:
            failure = "cache missing, stale, or unreadable"
        records.append(
            {
                "role": role,
                "path": str(path),
                "exists": exists,
                "readable": readable,
                "row_count": row_count,
                "modified_time": modified_time,
                "size": size,
                "period_min": period_min,
                "period_max": period_max,
                "treated_rows": treated_rows,
                "treated_products": treated_products,
                "source_path_from_metadata": source_path,
                "local_source_path": local_source_path,
                "newer_than_overlay": newer_than_overlay,
                "newer_than_final_panel": newer_than_final_panel,
                "valid_for_run": bool(valid),
                "failure_reason": failure,
            }
        )
    preflight_df = pd.DataFrame(records)
    preflight_csv = out_dir / "preflight.csv"
    preflight_json = out_dir / "preflight.json"
    preflight_df.to_csv(preflight_csv, index=False)
    write_metadata_json(preflight_json, {"version": SENSITIVITY_VERSION, "records": records})
    return preflight_csv, preflight_json, records


def _build_master_panel(config: PipelineConfig, package_path: Path, raw_path: Path) -> pd.DataFrame:
    query = f"""
        SELECT
            p.cty_code,
            p.hs10,
            p.year,
            p.month,
            p.cty_name AS pkg_cty_name,
            r.cty_name AS raw_cty_name,
            p.m_val AS pkg_m_val,
            p.m_q1 AS pkg_m_q1,
            p.m_p AS pkg_m_p,
            p.m_pduty AS pkg_m_pduty,
            p.m_stattariff1 AS pkg_m_stattariff1,
            p.m_stattariff2 AS pkg_m_stattariff2,
            p.m_status2 AS pkg_m_status2,
            p.m_ess AS pkg_m_ess,
            p.m_effective_mdate2 AS pkg_m_effective_mdate2,
            p.m_china_hit AS pkg_m_china_hit,
            p.m_hit AS pkg_m_hit,
            p.m_alum_hit AS pkg_m_alum_hit,
            p.m_steel_hit AS pkg_m_steel_hit,
            p.m_washer_hit AS pkg_m_washer_hit,
            p.m_solar_hit AS pkg_m_solar_hit,
            p.naics_str AS pkg_naics_str,
            p.m_increase AS pkg_m_increase,
            p.m_applied_tariff AS pkg_m_applied_tariff,
            p.m_apptariff AS pkg_m_apptariff,
            CAST(NULL AS VARCHAR) AS pkg_m_policy_source,
            p.m_T AS pkg_m_T,
            p.m_valduty AS pkg_m_valduty,
            p.m_duty AS pkg_m_duty,
            p.m_dut_val AS pkg_m_dut_val,
            r.m_val AS raw_m_val,
            r.m_q1 AS raw_m_q1,
            CAST(NULL AS DOUBLE) AS raw_m_statutory_tariff1,
            r.m_statutory_tariff2 AS raw_m_statutory_tariff2,
            r.base_statutory_rate_raw AS raw_base_statutory_rate_raw,
            r.tw_increment_rate_raw AS raw_tw_increment_rate_raw,
            r.tw_active_share_raw AS raw_tw_active_share_raw,
            r.tw_rule_code_raw AS raw_tw_rule_code_raw,
            r.tw_scope_source_raw AS raw_tw_scope_source_raw,
            r.m_policy_source AS raw_m_policy_source,
            r.source_type AS raw_source_type,
            r.release_name AS raw_release_name
        FROM read_parquet(?) AS p
        INNER JOIN read_parquet(?) AS r
            USING (cty_code, hs10, year, month)
    """
    master = duckdb.connect(database=":memory:").execute(query, [str(package_path), str(raw_path)]).fetch_df()
    if master.empty:
        raise RuntimeError("Section 301 sensitivity run requires a non-empty raw/package intersection.")
    master["hs10"] = master["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    master["hs8"] = master["hs10"].str.slice(0, 8)
    master["hs6"] = master["hs10"].str.slice(0, 6)
    master["hs4"] = master["hs10"].str.slice(0, 4)
    master["hs2"] = master["hs10"].str.slice(0, 2)
    master["cty_code"] = pd.to_numeric(master["cty_code"], errors="coerce").astype("Int64")
    master["year"] = pd.to_numeric(master["year"], errors="coerce").astype("Int64")
    master["month"] = pd.to_numeric(master["month"], errors="coerce").astype("Int64")
    master["mdate_index"] = _month_index(master["year"], master["month"])
    master["id"] = pd.factorize(master["cty_code"].astype("Int64").astype(str) + "|" + master["hs10"].astype("string"), sort=False)[0]
    master["ct"] = pd.factorize(master["cty_code"].astype("Int64").astype(str) + "|" + master["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    master["ht"] = pd.factorize(master["hs10"].astype("string") + "|" + master["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    master["naics_str"] = master["pkg_naics_str"].astype("string")
    master["naics4"] = master["naics_str"].str.slice(0, 4)
    master["naics3"] = master["naics_str"].str.slice(0, 3)
    master["naics2"] = master["naics_str"].str.slice(0, 2)
    master["raw_m_p"] = pd.to_numeric(master["raw_m_val"], errors="coerce") / pd.to_numeric(master["raw_m_q1"], errors="coerce")
    master["raw_m_pduty"] = master["raw_m_p"] * (1.0 + pd.to_numeric(master["raw_m_statutory_tariff2"], errors="coerce").fillna(0.0))
    for column in [c for c in master.columns if c.endswith("_m_status2") or c.endswith("_m_ess") or c.endswith("_m_china_hit") or c.endswith("_m_hit") or c.endswith("_m_alum_hit") or c.endswith("_m_steel_hit") or c.endswith("_m_washer_hit") or c.endswith("_m_solar_hit")]:
        master[column] = pd.to_numeric(master[column], errors="coerce").fillna(0).astype("Int64")
    for column in [c for c in master.columns if c.endswith("_m_val") or c.endswith("_m_q1") or c.endswith("_m_p") or c.endswith("_m_pduty") or c.endswith("_m_stattariff1") or c.endswith("_m_stattariff2") or c.endswith("_m_increase") or c.endswith("_m_applied_tariff") or c.endswith("_m_apptariff") or c.endswith("_tw_increment_rate_raw") or c.endswith("_tw_active_share_raw") or c.endswith("_raw") and c.startswith("raw_")]:
        if column in master.columns:
            master[column] = pd.to_numeric(master[column], errors="coerce")
    for column in ("pkg_m_policy_source", "raw_m_policy_source", "raw_source_type", "raw_release_name", "raw_tw_rule_code_raw", "raw_tw_scope_source_raw", "raw_base_statutory_rate_raw", "pkg_cty_name", "raw_cty_name"):
        if column in master.columns:
            master[column] = master[column].astype("string")
    master["pkg_pair_target"] = (
        master.groupby("id", sort=False)["pkg_m_status2"].transform(lambda s: s.eq(2).any()).astype(bool)
        & master["pkg_m_china_hit"].eq(1)
    ).astype("int8")
    master["raw_pair_target"] = (
        master["cty_code"].eq(5700)
        & master.groupby("id", sort=False)["raw_tw_increment_rate_raw"].transform(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).gt(0).any())
    ).astype("int8")
    first_pkg = master.loc[master["pkg_m_status2"].eq(2)].groupby("id")["mdate_index"].min()
    master["pkg_first_active_mdate2"] = master["id"].map(first_pkg).astype("Int64")
    master["pkg_paper_first_active_mdate2"] = master["pkg_first_active_mdate2"]
    active_raw = pd.to_numeric(master["raw_tw_increment_rate_raw"], errors="coerce").fillna(0).gt(0)
    first_raw = master.loc[active_raw].groupby("id")["mdate_index"].min()
    first_raw_share = master.loc[active_raw].sort_values(KEY_COLUMNS).groupby("id")["raw_tw_active_share_raw"].first()
    raw_first_map = first_raw.astype("Int64")
    raw_paper_map = raw_first_map.copy()
    if not first_raw_share.empty:
        partial = first_raw_share.gt(0) & first_raw_share.lt(1)
        raw_paper_map.loc[partial.index[partial]] = raw_paper_map.loc[partial.index[partial]] + 1
    master["raw_first_active_mdate2"] = master["id"].map(raw_first_map).astype("Int64")
    master["raw_paper_first_active_mdate2"] = master["id"].map(raw_paper_map).astype("Int64")
    master["raw_policy_family"] = np.where(
        master["raw_tw_rule_code_raw"].astype("string").str.replace(r"\D", "", regex=True).str.startswith("990388", na=False),
        "china301",
        "other",
    )
    return master.sort_values(KEY_COLUMNS).reset_index(drop=True)


def _calendar_index(master: pd.DataFrame, variant: VariantSpec) -> pd.Series:
    if variant.calendar_source == "pkg_legal":
        fill_source = "pkg_first_active_mdate2"
    elif variant.calendar_source == "raw_legal":
        fill_source = "raw_first_active_mdate2"
    elif variant.calendar_source == "raw_paper":
        fill_source = "raw_paper_first_active_mdate2"
    elif variant.calendar_source == "pkg_frozen":
        fill_source = "pkg_first_active_mdate2"
    else:
        raise ValueError(f"Unknown calendar source: {variant.calendar_source}")

    base = master[fill_source].astype("Float64")
    for sector_col in ("naics4", "naics3", "naics2"):
        fill = master.groupby(sector_col, dropna=False)[fill_source].transform("min").astype("Float64")
        base = base.fillna(fill)
    base = base.fillna(pd.Period("2018-02", freq="M").year * 12 + pd.Period("2018-02", freq="M").month - 1)
    return base


def _target_column(master: pd.DataFrame, variant: VariantSpec) -> pd.Series:
    if variant.target_source == "pkg":
        if "pkg_pair_target" in master.columns:
            return master["pkg_pair_target"].astype("int8")
        pair = _pair_id(master)
        return (
            master.groupby(pair, sort=False)["pkg_m_status2"].transform(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).eq(2).any()).astype(bool)
            & pd.to_numeric(master.get("pkg_m_china_hit", pd.Series(0, index=master.index)), errors="coerce").fillna(0).eq(1)
        ).astype("int8")
    if variant.target_source == "raw":
        if "raw_pair_target" in master.columns:
            return master["raw_pair_target"].astype("int8")
        pair = _pair_id(master)
        return (
            master.get("cty_code", pd.Series(0, index=master.index)).astype("Int64").eq(5700)
            & master.groupby(pair, sort=False)["raw_tw_increment_rate_raw"].transform(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).gt(0).any())
        ).astype("int8")
    raise ValueError(f"Unknown target source: {variant.target_source}")


def _outcome_column(master: pd.DataFrame, variant: VariantSpec, outcome: str) -> pd.Series:
    source = "pkg" if variant.outcome_source == "pkg" else "raw"
    column = f"{source}_m_{outcome}"
    return pd.to_numeric(master[column], errors="coerce")


def _tariff_column(master: pd.DataFrame, variant: VariantSpec) -> pd.Series:
    source = "pkg" if variant.tariff_source == "pkg" else "raw"
    if source == "pkg":
        column = "pkg_m_stattariff2" if "pkg_m_stattariff2" in master.columns else "pkg_m_statutory_tariff2"
    else:
        column = "raw_m_statutory_tariff2" if "raw_m_statutory_tariff2" in master.columns else "raw_m_stattariff2"
    return pd.to_numeric(master[column], errors="coerce")


def _raw_tariff_series(master: pd.DataFrame) -> pd.Series:
    column = "raw_m_statutory_tariff2" if "raw_m_statutory_tariff2" in master.columns else "raw_m_stattariff2"
    return pd.to_numeric(master[column], errors="coerce")


def _package_tariff_series(master: pd.DataFrame) -> pd.Series:
    column = "pkg_m_stattariff2" if "pkg_m_stattariff2" in master.columns else "pkg_m_statutory_tariff2"
    return pd.to_numeric(master[column], errors="coerce")


def _preflight_records(config: PipelineConfig, package_meta: dict[str, Any], raw_meta: dict[str, Any], package_path: Path, raw_path: Path) -> list[dict[str, Any]]:
    overlay_path = config.analysis_dir / "tradewar_overlay_raw.parquet"
    final_panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    overlay_stat = overlay_path.stat() if overlay_path.exists() else None
    final_stat = final_panel_path.stat() if final_panel_path.exists() else None
    records: list[dict[str, Any]] = []
    source_rows = [
        ("package_dta_source", config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta", package_meta, True),
        ("current_panel_raw_source", config.analysis_dir / "us_products_partner_hs10_monthly.parquet", raw_meta, True),
        ("package_cache", package_path, package_meta, False),
        ("raw_cache", raw_path, raw_meta, False),
        ("legacy_v2_status", _legacy_status_paths(config)[0], {}, False),
        ("legacy_workhorse", config.analysis_dir / "trade_regressions" / "workhorse" / "m_flow_hs10_fm_new_regression.parquet", {}, False),
        ("legacy_section301_panel", config.analysis_dir / "section301_imports_hs10.parquet", {}, False),
    ]
    for role, path, meta, is_source in source_rows:
        exists = path.exists()
        readable = exists
        row_count = meta.get("rows")
        if row_count is None and exists and path.suffix.lower() == ".parquet":
            try:
                row_count = int(pq.read_metadata(path).num_rows)
            except Exception:
                row_count = None
        if row_count is None and exists and path.suffix.lower() == ".dta":
            try:
                row_count = int(len(pd.read_stata(path, convert_categoricals=False, iterator=True, chunksize=1).__next__()))
            except Exception:
                row_count = None
        if row_count is None and exists and path.suffix.lower() == ".json":
            row_count = 1
        modified_time = None if not exists else pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").isoformat()
        size = None if not exists else int(path.stat().st_size)
        newer_than_overlay = None if overlay_stat is None or not exists else bool(path.stat().st_mtime >= overlay_stat.st_mtime)
        newer_than_final_panel = None if final_stat is None or not exists else bool(path.stat().st_mtime >= final_stat.st_mtime)
        valid = False
        failure_reason = None
        if role in {"package_dta_source", "current_panel_raw_source"}:
            valid = bool(exists and readable and (meta.get("treated_rows") or 0) > 0 and (meta.get("period_max") or "") >= "2019-04")
            if not valid:
                failure_reason = "source is missing, unreadable, untargeted, or does not reach 2019-04"
        elif role in {"package_cache", "raw_cache"}:
            valid = bool(exists and readable and meta.get("source_path") and row_count and row_count > 0)
            if not valid:
                failure_reason = "cache missing, stale, or unreadable"
        else:
            failure_reason = "legacy input or invalid marker"
        records.append(
            {
                "role": role,
                "path": str(path),
                "exists": exists,
                "readable": readable,
                "row_count": row_count,
                "modified_time": modified_time,
                "size": size,
                "period_min": meta.get("period_min"),
                "period_max": meta.get("period_max"),
                "treated_rows": meta.get("treated_rows"),
                "treated_products": meta.get("treated_products"),
                "source_path_from_metadata": meta.get("source_path", str(path)),
                "local_source_path": str(path),
                "newer_than_overlay": newer_than_overlay,
                "newer_than_final_panel": newer_than_final_panel,
                "valid_for_run": valid,
                "failure_reason": failure_reason,
            }
        )
    return records


def _build_common_sample_keys(master: pd.DataFrame, analysis: str, window: str, outcome: str, variant_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    key_sets = []
    for variant_code, frame in variant_frames.items():
        work = frame.loc[frame["l_outcome"].notna()].copy()
        key_sets.append(set(_key_series(work).tolist()))
    common = set.intersection(*key_sets) if key_sets else set()
    hs8 = master["hs8"] if "hs8" in master.columns else master["hs10"].astype("string").str.slice(0, 8)
    out = master.loc[_key_series(master).isin(common), KEY_COLUMNS].drop_duplicates().copy()
    out["hs8"] = hs8.loc[out.index].astype("string")
    out["naics_str"] = master["naics_str"].loc[out.index].astype("string") if "naics_str" in master.columns else pd.NA
    out["analysis"] = analysis
    out["window"] = window
    out["outcome"] = outcome
    out["sample_group"] = "common"
    return out


def _prepare_event_frame(master: pd.DataFrame, variant: VariantSpec, window: str, outcome: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    spec = WINDOWS[window]
    work = master.copy()
    work["T"] = _target_column(work, variant)
    work["d_index"] = _calendar_index(work, variant)
    for sector_col in ("naics4", "naics3", "naics2"):
        fill = work.groupby(sector_col, dropna=False)["d_index"].transform("min")
        work.loc[work["d_index"].isna() & (work["T"] == 0), "d_index"] = fill
    default_index = pd.Period("2018-02", freq="M").year * 12 + pd.Period("2018-02", freq="M").month - 1
    work.loc[work["d_index"].isna() & (work["T"] == 0), "d_index"] = default_index
    work["event_time"] = work["mdate_index"] - work["d_index"]
    work = work.loc[work["event_time"].notna()].copy()
    work["event_time"] = work["event_time"].astype(int)
    work.loc[work["event_time"] >= spec["event_max"], "event_time"] = spec["event_max"]
    work = work.loc[work["event_time"] >= spec["event_min"]].copy()

    value = _outcome_column(work, variant, "val")
    quantity = _outcome_column(work, variant, "q1")
    base_price = pd.Series(np.where(quantity.gt(0), value / quantity, np.nan), index=work.index)
    tariff_rate = _tariff_column(work, variant)
    if outcome == "val":
        outcome_values = value
    elif outcome == "q1":
        outcome_values = quantity
    elif outcome == "p":
        outcome_values = base_price
    elif outcome == "pduty":
        outcome_values = base_price * (1.0 + tariff_rate.fillna(0.0))
    else:
        raise ValueError(f"Unknown outcome: {outcome}")
    work["l_outcome"] = np.where(outcome_values.gt(0), 100.0 * np.log(outcome_values * 1_000_000.0), np.nan)
    baseline = spec["baseline"]
    event_values = [value for value in range(spec["event_min"], spec["event_max"] + 1) if value != baseline]
    for value_ in event_values:
        sign = "m" if value_ < 0 else "p"
        et = f"et_{sign}{abs(value_)}"
        yt = f"yt_{sign}{abs(value_)}"
        work[et] = ((work["T"] == 1) & (work["event_time"] == value_)).astype(int)
        work[yt] = (work["event_time"] == value_).astype(int)
    event_terms = []
    for value_ in event_values:
        sign = "m" if value_ < 0 else "p"
        et = f"et_{sign}{abs(value_)}"
        yt = f"yt_{sign}{abs(value_)}"
        if work[et].nunique(dropna=False) > 1:
            event_terms.append(et)
        if work[yt].nunique(dropna=False) > 1:
            event_terms.append(yt)
    meta = {
        "window": window,
        "window_label": spec["label"],
        "baseline": baseline,
        "event_values": event_values,
        "event_terms": event_terms,
        "target_col": "T",
        "calendar_col": "d_index",
        "tariff_col": variant.tariff_source,
    }
    return work, meta


def _run_event_study(frame: pd.DataFrame, variant: VariantSpec, window: str, outcome: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    work, meta = _prepare_event_frame(frame, variant, window, outcome)
    work = work.loc[work["l_outcome"].notna()].copy()
    rhs_terms = meta.get("event_terms") or [f"et_{'m' if value < 0 else 'p'}{abs(value)}" for value in meta["event_values"]]
    rhs = " + ".join(rhs_terms)
    if not rhs_terms:
        rows = [{"horizon": meta["baseline"], "term": "baseline", "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0}]
        result = pd.DataFrame(rows)
        result["analysis"] = variant.bridges[0] if len(variant.bridges) == 1 else None
        result["variant"] = variant.code
        result["variant_label"] = variant.label
        result["window"] = window
        result["window_label"] = meta["window_label"]
        result["outcome"] = outcome
        result["nobs"] = int(len(work))
        result["r2"] = np.nan
        result["sample_rows"] = int(len(work))
        result["sample_keys"] = int(work[KEY_COLUMNS].drop_duplicates().shape[0])
        result["treated_products"] = int(work.loc[work["T"] == 1, "hs10"].astype("string").nunique())
        result["positive_outcome_rows"] = int(work["l_outcome"].notna().sum())
        return result, {"work": work}
    fit = pf.feols(
        f"l_outcome ~ {rhs} | id + ct + ht",
        work,
        vcov={"CRV1": "hs8 + cty_code"},
        fixef_rm="none",
        copy_data=False,
        store_data=False,
        lean=True,
    )
    tidy = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
    rows: list[dict[str, Any]] = [{"horizon": meta["baseline"], "term": "baseline", "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0}]
    for value in meta["event_values"]:
        term = f"et_{'m' if value < 0 else 'p'}{abs(value)}"
        match = tidy.loc[tidy["term"] == term]
        if match.empty:
            rows.append({"horizon": value, "term": term, "estimate": np.nan, "std_error": np.nan, "conf_low": np.nan, "conf_high": np.nan})
        else:
            record = match.iloc[0].to_dict()
            record["horizon"] = value
            rows.append(record)
    result = pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
    result["analysis"] = variant.bridges[0] if len(variant.bridges) == 1 else None
    result["variant"] = variant.code
    result["variant_label"] = variant.label
    result["window"] = window
    result["window_label"] = meta["window_label"]
    result["outcome"] = outcome
    result["nobs"] = int(getattr(fit, "_N"))
    result["r2"] = float(getattr(fit, "_r2"))
    result["sample_rows"] = int(len(work))
    result["sample_keys"] = int(work[KEY_COLUMNS].drop_duplicates().shape[0])
    result["treated_products"] = int(work.loc[work["T"] == 1, "hs10"].astype("string").nunique())
    result["positive_outcome_rows"] = int(work["l_outcome"].notna().sum())
    return result, {"work": work}


def _comparison_metrics(merged: pd.DataFrame) -> dict[str, Any]:
    finite = merged.loc[merged[["estimate_benchmark", "estimate_candidate"]].notna().all(axis=1)].copy()
    if finite.empty:
        return {
            "coefficient_curve_correlation": np.nan,
            "coefficient_rmse": np.nan,
            "max_abs_coefficient_difference": np.nan,
            "average_post_treatment_difference": np.nan,
            "sign_agreement_rate": np.nan,
            "ci_overlap_rate": np.nan,
        }
    diff = finite["estimate_candidate"] - finite["estimate_benchmark"]
    post = finite.loc[finite["horizon"] >= 0]
    corr = float(finite[["estimate_benchmark", "estimate_candidate"]].corr().iloc[0, 1]) if len(finite) > 1 else np.nan
    ci_overlap = ((finite["conf_low_benchmark"] <= finite["conf_high_candidate"]) & (finite["conf_low_candidate"] <= finite["conf_high_benchmark"])).mean()
    sign_agree = (np.sign(finite["estimate_benchmark"].fillna(0.0)) == np.sign(finite["estimate_candidate"].fillna(0.0))).mean()
    return {
        "coefficient_curve_correlation": corr,
        "coefficient_rmse": float(np.sqrt(np.nanmean(diff**2))),
        "max_abs_coefficient_difference": float(np.nanmax(np.abs(diff))),
        "average_post_treatment_difference": float(post["estimate_candidate"].mean() - post["estimate_benchmark"].mean()) if not post.empty else np.nan,
        "sign_agreement_rate": float(sign_agree),
        "ci_overlap_rate": float(ci_overlap),
    }


def _variance_summary(reference: pd.DataFrame, comparison: pd.DataFrame) -> pd.DataFrame:
    merged = reference.merge(comparison, on=["analysis", "window", "outcome", "horizon"], how="inner", suffixes=("_benchmark", "_candidate"))
    if merged.empty:
        return merged
    merged["difference"] = merged["estimate_candidate"] - merged["estimate_benchmark"]
    merged["pooled_std_error"] = np.sqrt(merged["std_error_benchmark"].fillna(0.0) ** 2 + merged["std_error_candidate"].fillna(0.0) ** 2)
    merged["difference_over_pooled_se"] = np.where(merged["pooled_std_error"] > 0, merged["difference"] / merged["pooled_std_error"], np.nan)
    merged["sign_agreement"] = np.sign(merged["estimate_benchmark"].fillna(0.0)) == np.sign(merged["estimate_candidate"].fillna(0.0))
    merged["ci_overlap"] = (merged["conf_low_benchmark"] <= merged["conf_high_candidate"]) & (merged["conf_low_candidate"] <= merged["conf_high_benchmark"])
    return merged


def _write_report(config: PipelineConfig, summary: dict[str, Any]) -> Path:
    report_path = _artifact_dir(config) / "section301_regression_sensitivity_report.md"
    lines = [
        "# Section 301 Regression Sensitivity v3",
        "",
        f"- Ready for extension: `{summary['ready_for_extension']}`",
        f"- Common sample keys: `{summary['common_sample_keys']:,}`",
        f"- Package source: `{summary['package_source_path']}`",
        f"- Raw source: `{summary['raw_source_path']}`",
        "",
        "## Variants",
        "",
    ]
    for variant in VARIANTS:
        lines.append(f"- `{variant.code}`: {variant.label}")
    lines.extend(["", "## Windows", ""])
    for window, spec in WINDOWS.items():
        lines.append(f"- `{window}`: {spec['label']} (baseline {spec['baseline']})")
    lines.extend(["", "## Notes", "", "- v2 artifacts remain preserved but invalid.", "- v3 caches use the current raw panel and the actual benchmark DTA."])
    return write_markdown_report(report_path, lines)


def _sample_audit_row(analysis: str, window: str, outcome: str, variant: VariantSpec, prepared: pd.DataFrame, work: pd.DataFrame, baseline_hash: str | None) -> dict[str, Any]:
    keys = _key_series(work)
    key_hash = hashlib.sha1("|".join(sorted(keys.tolist())).encode("utf-8")).hexdigest()
    return {
        "analysis": analysis,
        "window": window,
        "outcome": outcome,
        "variant": variant.code,
        "variant_label": variant.label,
        "calendar": variant.calendar_source,
        "pre_estimation_rows": int(len(prepared)),
        "positive_outcome_rows": int(prepared["l_outcome"].notna().sum()),
        "event_eligible_rows": int(len(work)),
        "estimation_rows": int(len(work)),
        "pyfixest_nobs": int(len(work)),
        "key_hash": key_hash,
        "identical_to_A": variant.code == "A" or (baseline_hash is not None and key_hash == baseline_hash),
    }


def _bridge_variants(bridge: str) -> tuple[VariantSpec, ...]:
    return tuple(variant for variant in VARIANTS if bridge in variant.bridges)


def _write_partitioned_common_keys(config: PipelineConfig, frames: list[pd.DataFrame]) -> tuple[Path, int]:
    out_dir = _artifact_dir(config) / "ck"
    out_dir.mkdir(parents=True, exist_ok=True)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=KEY_COLUMNS + ["hs8", "naics_str", "analysis", "window", "outcome", "sample_group"])
    combined = combined.drop_duplicates().sort_values(["analysis", "window", "outcome"] + KEY_COLUMNS)
    con = duckdb.connect(database=":memory:")
    con.register("combined", combined)
    out_dir_sql = str(out_dir).replace("'", "''")
    con.execute(
        f'COPY (SELECT * FROM combined) TO \'{out_dir_sql}\' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (analysis, "window", sample_group))'
    )
    con.close()
    partitions = combined[["analysis", "window", "sample_group"]].drop_duplicates().astype("string")
    write_metadata_json(out_dir / "manifest.json", {"rows": int(len(combined)), "partitions": partitions.to_dict(orient="records"), "columns": combined.columns.tolist()})
    return out_dir, int(len(combined))


def _fit_manifest_payload(config: PipelineConfig, analysis: str, window: str, outcome: str, variant: VariantSpec, package_meta: dict[str, Any], raw_meta: dict[str, Any], sample_hash: str, treatment_hash: str) -> dict[str, Any]:
    return {
        "analysis": analysis,
        "window": window,
        "outcome": outcome,
        "variant": asdict(variant),
        "source_fingerprints": {
            "package": package_meta.get("sha256"),
            "raw": raw_meta.get("sha256"),
        },
        "spec_version": SENSITIVITY_VERSION,
        "spec_hash": hashlib.sha256(json.dumps({"analysis": analysis, "window": window, "outcome": outcome, "variant": asdict(variant)}, sort_keys=True).encode("utf-8")).hexdigest(),
        "sample_hash": sample_hash,
        "treatment_hash": treatment_hash,
        "code_hash": _code_hash(),
        "ready_for_extension": False,
    }


def _checkpoint_paths(config: PipelineConfig, analysis: str, window: str, outcome: str, variant: VariantSpec) -> tuple[Path, Path]:
    base = _checkpoint_dir(config) / analysis / window / outcome / variant.code
    base.mkdir(parents=True, exist_ok=True)
    return base / "coefficients.parquet", base / "manifest.json"


def _fit_one(
    config: PipelineConfig,
    analysis: str,
    window: str,
    outcome: str,
    variant: VariantSpec,
    frame: pd.DataFrame,
    package_meta: dict[str, Any],
    raw_meta: dict[str, Any],
    overwrite: bool,
) -> pd.DataFrame:
    coeff_path, manifest_path = _checkpoint_paths(config, analysis, window, outcome, variant)
    sample_hash = _hash_keys(frame)
    treatment_hash = hashlib.sha1(
        "|".join(sorted(frame.loc[frame["T"] == 1, "hs10"].astype("string").dropna().unique().tolist())).encode("utf-8")
    ).hexdigest()
    manifest = _fit_manifest_payload(config, analysis, window, outcome, variant, package_meta, raw_meta, sample_hash, treatment_hash)
    if coeff_path.exists() and manifest_path.exists() and not overwrite:
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            if existing == manifest:
                return pd.read_parquet(coeff_path)
        except Exception:
            pass
    result, _ = _run_event_study(frame, variant, window, outcome)
    result["analysis"] = analysis
    result["calendar"] = variant.calendar_source
    write_parquet(result, coeff_path, overwrite=True)
    write_metadata_json(manifest_path, manifest)
    return result


def _write_current_fit_marker(path: Path, payload: dict[str, Any]) -> None:
    write_metadata_json(path, payload)


def _build_diagnostics(master: pd.DataFrame, artifact_dir: Path) -> dict[str, Any]:
    china = master.loc[master["cty_code"].eq(5700)].copy()
    pkg_active = _target_column(china, next(variant for variant in VARIANTS if variant.code == "A")).eq(1)
    raw_active = _target_column(china, next(variant for variant in VARIANTS if variant.code == "C_legal")).eq(1)
    confusion = pd.crosstab(pkg_active, raw_active, dropna=False).rename(index={False: "pkg0", True: "pkg1"}, columns={False: "raw0", True: "raw1"})
    confusion_path = artifact_dir / "section301_treatment_map_comparison.parquet"
    write_parquet(confusion.reset_index(), confusion_path, overwrite=True)
    confusion.reset_index().to_csv(confusion_path.with_suffix(".csv"), index=False)

    timing = china.loc[pkg_active | raw_active, ["cty_code", "hs10", "year", "month", "pkg_first_active_mdate2", "raw_first_active_mdate2", "raw_paper_first_active_mdate2"]].copy()
    timing["legal_month_gap"] = pd.to_numeric(timing["raw_first_active_mdate2"], errors="coerce") - pd.to_numeric(timing["pkg_first_active_mdate2"], errors="coerce")
    timing["paper_month_gap"] = pd.to_numeric(timing["raw_paper_first_active_mdate2"], errors="coerce") - pd.to_numeric(timing["pkg_first_active_mdate2"], errors="coerce")
    write_parquet(timing, artifact_dir / "section301_treatment_timing_comparison.parquet", overwrite=True)
    timing.groupby(["legal_month_gap", "paper_month_gap"], dropna=False).size().reset_index(name="rows").to_csv(artifact_dir / "section301_treatment_timing_comparison.csv", index=False)

    outcome_rows = []
    for outcome in OUTCOMES:
        pkg = pd.to_numeric(master[f"pkg_m_{outcome}"], errors="coerce")
        raw = pd.to_numeric(master[f"raw_m_{outcome}"], errors="coerce")
        mask = pkg.gt(0) & raw.gt(0)
        if outcome in {"val", "q1"}:
            centered = np.log(raw[mask] / pkg[mask]).replace([np.inf, -np.inf], np.nan)
        elif outcome == "p":
            centered = np.log((raw[mask] / master.loc[mask, "raw_m_q1"]) / (pkg[mask] / master.loc[mask, "pkg_m_q1"]))
        else:
            centered = np.log((raw[mask] / master.loc[mask, "raw_m_q1"]) * (1 + _raw_tariff_series(master).loc[mask].fillna(0.0)) / ((pkg[mask] / master.loc[mask, "pkg_m_q1"]) * (1 + _package_tariff_series(master).loc[mask].fillna(0.0))))
        outcome_rows.append({"outcome": outcome, "rows": int(mask.sum()), "corr": float(pkg[mask].corr(raw[mask])) if mask.sum() > 1 else np.nan, "gap_p50": float(centered.abs().quantile(0.5)) if len(centered.dropna()) else np.nan, "gap_p90": float(centered.abs().quantile(0.9)) if len(centered.dropna()) else np.nan, "gap_p95": float(centered.abs().quantile(0.95)) if len(centered.dropna()) else np.nan, "gap_p99": float(centered.abs().quantile(0.99)) if len(centered.dropna()) else np.nan})
    pd.DataFrame(outcome_rows).to_csv(artifact_dir / "section301_outcome_source_comparison.csv", index=False)

    raw_mask = _target_column(master, next(variant for variant in VARIANTS if variant.code == "C_legal")).eq(1)
    pkg_mask = _target_column(master, next(variant for variant in VARIANTS if variant.code == "A")).eq(1)
    raw_only = master.loc[raw_mask & ~pkg_mask].copy()
    if not raw_only.empty:
        value_2017 = raw_only.loc[raw_only["year"].eq(2017)].groupby("hs10")["raw_m_val"].sum().rename("pre_2017_value")
        contrib = raw_only.assign(value_gap=(raw_only["raw_m_val"].fillna(0.0) * raw_only["raw_tw_increment_rate_raw"].fillna(0.0))).groupby("hs10")["value_gap"].sum().rename("value_x_increment")
        counts = raw_only.groupby("hs10").size().rename("affected_months")
        raw_only_rank = pd.concat([value_2017, contrib, counts], axis=1).fillna(0).reset_index().sort_values(["value_x_increment", "pre_2017_value"], ascending=False)
    else:
        raw_only_rank = pd.DataFrame(columns=["hs10", "pre_2017_value", "value_x_increment", "affected_months"])
    write_parquet(raw_only_rank, artifact_dir / "section301_raw_only_assignment_contribution.parquet", overwrite=True)
    raw_only_rank.head(200).to_csv(artifact_dir / "section301_raw_only_assignment_contribution.csv", index=False)
    return {
        "treatment_confusion_path": str(confusion_path),
        "treatment_timing_path": str(artifact_dir / "section301_treatment_timing_comparison.csv"),
        "outcome_source_path": str(artifact_dir / "section301_outcome_source_comparison.csv"),
        "raw_only_assignment_path": str(artifact_dir / "section301_raw_only_assignment_contribution.csv"),
    }


def run_section301_regression_sensitivity(config: PipelineConfig) -> dict[str, Any]:
    artifact_dir = _artifact_dir(config)
    _write_invalid_legacy_status(config)
    package_path, package_meta = _build_package_paper_window_cache(config, overwrite=config.overwrite)
    raw_path, raw_meta = _build_raw_paper_window_cache(config, overwrite=config.overwrite)
    preflight_csv, preflight_json, preflight_records = _write_preflight(config, package_meta, raw_meta, package_path, raw_path)
    required = [record for record in preflight_records if record["role"] in {"package_dta_source", "current_panel_raw_source", "package_cache", "raw_cache"}]
    if not all(record["valid_for_run"] for record in required):
        reasons = [record["failure_reason"] for record in required if not record["valid_for_run"]]
        raise RuntimeError(f"Section 301 v3 sensitivity run refused by preflight: {reasons}")
    master = _build_master_panel(config, package_path, raw_path)
    if master.empty:
        raise RuntimeError("Section 301 sensitivity run requires a non-empty raw/package intersection.")
    china = master.loc[master["cty_code"].eq(5700), ["pkg_m_val", "raw_m_val", "pkg_m_q1", "raw_m_q1", "pkg_m_stattariff2"]].copy()
    china["raw_tariff"] = _raw_tariff_series(master).loc[china.index]
    china_numeric = china.apply(pd.to_numeric, errors="coerce")
    identical_pairs = (
        china_numeric["pkg_m_val"].fillna(-999999.0).equals(china_numeric["raw_m_val"].fillna(-999999.0))
        and china_numeric["pkg_m_q1"].fillna(-999999.0).equals(china_numeric["raw_m_q1"].fillna(-999999.0))
        and china_numeric["pkg_m_stattariff2"].fillna(-999999.0).equals(china_numeric["raw_tariff"].fillna(-999999.0))
    )
    if identical_pairs:
        raise RuntimeError("Section 301 sensitivity run refused: package and raw China outcomes/rates are identical on the common key universe.")
    diagnostics = _build_diagnostics(master, _diagnostic_dir(config))
    common_frames: list[pd.DataFrame] = []
    sample_audit_rows: list[dict[str, Any]] = []
    coeff_frames: list[pd.DataFrame] = []
    progress_path = artifact_dir / "progress.json"
    current_fit_path = artifact_dir / "current_fit.json"
    completed: list[dict[str, Any]] = []

    for bridge in BRIDGES:
        bridge_variants = _bridge_variants(bridge)
        for window in WINDOWS:
            for outcome in OUTCOMES:
                frames_by_variant: dict[str, pd.DataFrame] = {}
                prepared_by_variant: dict[str, pd.DataFrame] = {}
                work_by_variant: dict[str, pd.DataFrame] = {}
                fit_by_variant: dict[str, pd.DataFrame] = {}
                for variant in bridge_variants:
                    source_code = _duplicate_source_variant(variant, outcome)
                    if source_code is not None and source_code in prepared_by_variant:
                        prepared_by_variant[variant.code] = prepared_by_variant[source_code]
                        frames_by_variant[variant.code] = prepared_by_variant[source_code]
                        continue
                    prepared, _ = _prepare_event_frame(master, variant, window, outcome)
                    prepared_by_variant[variant.code] = prepared
                    frames_by_variant[variant.code] = prepared
                common = _build_common_sample_keys(master, bridge, window, outcome, frames_by_variant)
                common_frames.append(common)
                baseline_hash = None
                for variant in bridge_variants:
                    source_code = _duplicate_source_variant(variant, outcome)
                    if source_code is not None and source_code in fit_by_variant:
                        prepared = prepared_by_variant[source_code]
                        work = work_by_variant[source_code]
                        source_result = fit_by_variant[source_code]
                        result = _clone_fit_result(source_result, next(spec for spec in bridge_variants if spec.code == source_code), variant)
                        coeff_frames.append(result)
                        sample_audit_rows.append(_sample_audit_row(bridge, window, outcome, variant, prepared, work, baseline_hash))
                        fit_by_variant[variant.code] = result
                        completed.append({"analysis": bridge, "window": window, "outcome": outcome, "variant": variant.code, "rows": int(len(result)), "reused_from": source_code})
                        write_parquet(result, _checkpoint_paths(config, bridge, window, outcome, variant)[0], overwrite=True)
                        write_metadata_json(
                            _checkpoint_paths(config, bridge, window, outcome, variant)[1],
                            {
                                **_fit_manifest_payload(config, bridge, window, outcome, variant, package_meta, raw_meta, _hash_keys(work), hashlib.sha1("|".join(sorted(work.loc[work["T"] == 1, "hs10"].astype("string").dropna().unique().tolist())).encode("utf-8")).hexdigest()),
                                "reused_from": source_code,
                            },
                        )
                        continue
                    prepared = prepared_by_variant[variant.code]
                    work = prepared.loc[prepared["l_outcome"].notna()].copy()
                    key_mask = _key_series(work).isin(set(_key_series(common).tolist()))
                    work = work.loc[key_mask].copy()
                    work_by_variant[variant.code] = work
                    _write_current_fit_marker(current_fit_path, {"analysis": bridge, "window": window, "outcome": outcome, "variant": variant.code, "rows": int(len(work)), "duplicate": False})
                    sample_audit_rows.append(_sample_audit_row(bridge, window, outcome, variant, prepared, work, baseline_hash))
                    result = _fit_one(config, bridge, window, outcome, variant, work, package_meta, raw_meta, overwrite=config.overwrite)
                    result["analysis"] = bridge
                    coeff_frames.append(result)
                    fit_by_variant[variant.code] = result
                    if variant.code == "A":
                        baseline_hash = _hash_keys(work)
                    completed.append({"analysis": bridge, "window": window, "outcome": outcome, "variant": variant.code, "rows": int(len(result))})
                    write_metadata_json(progress_path, {"completed_fits": completed, "remaining_fits": max(0, _expected_fit_count() - len(completed)), "last_completed": completed[-1], "version": SENSITIVITY_VERSION})
                    gc.collect()
                gc.collect()
    common_key_dir, common_key_rows = _write_partitioned_common_keys(config, common_frames)
    coeffs = pd.concat(coeff_frames, ignore_index=True)
    coeffs_path = _output_path(config, "section301_regression_sensitivity_coefficients.csv")
    comparison_rows: list[pd.DataFrame] = []
    for bridge in BRIDGES:
        bridge_coeffs = coeffs.loc[coeffs["analysis"].eq(bridge)].copy()
        benchmark = bridge_coeffs.loc[bridge_coeffs["variant"].eq("A")].copy()
        for variant in _bridge_variants(bridge):
            cand = bridge_coeffs.loc[bridge_coeffs["variant"].eq(variant.code)].copy()
            merged = _variance_summary(benchmark, cand)
            if merged.empty:
                continue
            merged["analysis"] = bridge
            merged["benchmark_variant"] = "A"
            merged["variant"] = variant.code
            merged["variant_label"] = variant.label
            merged["calendar"] = variant.calendar_source
            comparison_rows.append(merged)
    comparison = pd.concat(comparison_rows, ignore_index=True) if comparison_rows else pd.DataFrame()
    comparison_path = _output_path(config, "section301_regression_sensitivity_comparison.csv")
    sample_audit = pd.DataFrame(sample_audit_rows)
    sample_audit_path = _output_path(config, "section301_sample_audit.csv")
    coeffs.to_csv(coeffs_path, index=False)
    comparison.to_csv(comparison_path, index=False)
    sample_audit.to_csv(sample_audit_path, index=False)
    comparison_metrics: dict[str, Any] = {}
    if not comparison.empty:
        for (analysis, variant, window, outcome), subset in comparison.groupby(["analysis", "variant", "window", "outcome"], dropna=False):
            comparison_metrics[f"{analysis}:{variant}:{window}:{outcome}"] = _comparison_metrics(subset)
    summary = {
        "ready_for_extension": False,
        "benchmark_variant": "A",
        "analysis_bridges": list(BRIDGES),
        "common_sample_keys": int(common_key_rows),
        "package_source_path": str(config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"),
        "raw_source_path": str(config.analysis_dir / "us_products_partner_hs10_monthly.parquet"),
        "coefficients_path": str(coeffs_path),
        "comparison_path": str(comparison_path),
        "sample_audit_path": str(sample_audit_path),
        "common_keys_path": str(common_key_dir),
        "preflight_csv": str(preflight_csv),
        "preflight_json": str(preflight_json),
        "diagnostics": diagnostics,
        "comparison_metrics": comparison_metrics,
        "variants": {variant.code: variant.label for variant in VARIANTS},
        "windows": {window: {"label": spec["label"], "baseline": spec["baseline"]} for window, spec in WINDOWS.items()},
    }
    summary_path = _output_path(config, "section301_regression_sensitivity_summary.json")
    write_metadata_json(summary_path, summary)
    report_path = _write_report(config, summary | {"summary_path": str(summary_path)})
    summary["report_path"] = str(report_path)
    write_metadata_json(summary_path, summary)
    return {
        "coefficients_path": str(coeffs_path),
        "comparison_path": str(comparison_path),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "rows": int(len(coeffs)),
        "comparison_rows": int(len(comparison)),
        "common_sample_keys": int(summary["common_sample_keys"]),
    }
