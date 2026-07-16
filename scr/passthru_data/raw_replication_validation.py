"""Validate the raw-source 2017-2019 import reconstruction against the RTP package."""

from __future__ import annotations

from calendar import monthrange
from datetime import datetime, timezone
from typing import Any
import importlib.util
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .build_us_products_partner_panel import (
    _load_tradewar_machine_links,
    _load_tradewar_pdf_csv_link_provenance,
    _load_tradewar_pdf_links,
)
from .io_utils import normalize_hs_code, read_table, write_metadata_json, write_parquet


KEYS = ["cty_code", "hs10", "year", "month"]
PACKAGE_COLUMNS = KEYS + [
    "m_val", "m_q1", "m_stattariff1", "m_stattariff2", "m_hit", "m_ess",
    "m_status2", "m_effective_mdate2", "m_china_hit", "m_steel_hit", "m_alum_hit",
    "m_washer_hit", "m_solar_hit",
]
RAW_COLUMNS = KEYS + [
    "m_val", "m_q1", "m_statutory_tariff1", "m_statutory_tariff2", "m_policy_source",
    "mfn_text_rate", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw",
]
FAMILY_COLUMNS = ["m_china_hit", "m_steel_hit", "m_alum_hit", "m_washer_hit", "m_solar_hit"]
RATE_TOL = 1e-6
PP_TOL = 0.005
PP_GAP_TOL = 0.5
TRADE_VALUE_TOL = 1.0

SOURCE_HEALTH_COLUMNS = [
    "artifact",
    "candidate",
    "expected_path",
    "exists",
    "readable",
    "row_count",
    "key_columns_expected",
    "key_columns_present",
    "missing_key_columns",
    "exception_message",
    "appears_placeholder",
    "zero_byte",
    "invalid_parquet",
    "file_size_bytes",
    "file_attributes",
    "observed_columns_json",
]

SOURCE_ARTIFACT_SPECS = (
    {
        "artifact": "policy_release_catalog",
        "base_dir": "reference_dir",
        "candidates": (
            {
                "candidate": "csv",
                "filename": "policy_release_catalog.csv",
                "key_columns": ["release_name", "year", "release_start_date", "release_end_date"],
            },
            {
                "candidate": "parquet",
                "filename": "policy_release_catalog.parquet",
                "key_columns": ["release_name", "year", "release_start_date", "release_end_date"],
            },
        ),
    },
    {
        "artifact": "policy_archive_revision_index",
        "base_dir": "reference_dir",
        "candidates": (
            {
                "candidate": "csv",
                "filename": "policy_archive_revision_index.csv",
                "key_columns": ["year", "file_name", "archive_release_name", "file_ext"],
            },
            {
                "candidate": "parquet",
                "filename": "policy_archive_revision_index.parquet",
                "key_columns": ["year", "file_name", "archive_release_name", "file_ext"],
            },
        ),
    },
    {
        "artifact": "tradewar_machine_links",
        "base_dir": "reference_dir",
        "candidates": (
            {
                "candidate": "parquet",
                "filename": "tradewar_machine_links.parquet",
                "key_columns": ["hs8", "rule_code", "release_name"],
            },
        ),
    },
    {
        "artifact": "tradewar_pdf_links",
        "base_dir": "reference_dir",
        "candidates": (
            {
                "candidate": "parquet",
                "filename": "tradewar_pdf_links.parquet",
                "key_columns": ["hs8", "rule_code", "release_name"],
            },
        ),
    },
    {
        "artifact": "tradewar_rule_attributes",
        "base_dir": "reference_dir",
        "candidates": (
            {
                "candidate": "parquet",
                "filename": "tradewar_rule_attributes.parquet",
                "key_columns": ["rule_code", "year", "month", "increment_rate"],
            },
        ),
    },
    {
        "artifact": "tradewar_overlay_raw",
        "base_dir": "analysis_dir",
        "candidates": (
            {
                "candidate": "parquet",
                "filename": "tradewar_overlay_raw.parquet",
                "key_columns": ["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw"],
            },
        ),
    },
)


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["cty_code"] = pd.to_numeric(out["cty_code"], errors="coerce").astype("Int64")
    out["hs10"] = out["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["month"] = pd.to_numeric(out["month"], errors="coerce").astype("Int64")
    return out.dropna(subset=KEYS).copy()


def _to_numeric(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")


def _add_reference_status(merged: pd.DataFrame) -> None:
    """Add paper-side tariff status fields using tariff timing, not event-study status."""
    _to_numeric(
        merged,
        [
            "ref_m_hit",
            "ref_m_ess",
            "ref_m_status2",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            *[f"ref_{column}" for column in FAMILY_COLUMNS],
        ],
    )
    merged["ref_subject"] = merged["ref_m_hit"].eq(1).fillna(False)
    merged["ref_active"] = merged["ref_m_status2"].gt(0).fillna(False)
    merged["ref_event_status"] = merged["ref_m_ess"].fillna(0)


def _add_raw_status(merged: pd.DataFrame) -> None:
    _to_numeric(
        merged,
        [
            "raw_m_val",
            "raw_m_q1",
            "raw_m_statutory_tariff1",
            "raw_m_statutory_tariff2",
            "raw_tw_increment_rate_raw",
        ],
    )
    merged["raw_active"] = merged["raw_tw_increment_rate_raw"].notna()


def _rate_match_mask(series: pd.Series) -> pd.Series:
    return series.le(RATE_TOL).fillna(False)


def _share(mask: pd.Series) -> float:
    return float(mask.mean()) if len(mask) else 0.0


def _metric(name: str, value: int | float | bool | str) -> dict[str, int | float | bool | str]:
    return {"metric": name, "value": value}


def _artifact_name(stem: str, artifact_suffix: str = "", extension: str = ".csv") -> str:
    return f"{stem}{artifact_suffix}{extension}"


def _effective_period(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.to_period("M").astype("string")


def _normalize_hs8(series: pd.Series) -> pd.Series:
    def _one(value: object) -> object:
        if pd.isna(value):
            return pd.NA
        text = re.sub(r"\D", "", str(value))
        if not text:
            return pd.NA
        return text.zfill(10)[:8] if len(text) > 8 else text.zfill(8)

    return series.map(_one).astype("string")


def _normalize_rule_code(series: pd.Series) -> pd.Series:
    return series.map(lambda value: normalize_hs_code(value, 8)).astype("string")


def _month_active_share_from_effective_date(effective_date: Any, year: Any, month: Any) -> float | pd.NA:
    effective = pd.to_datetime(effective_date, errors="coerce")
    year_value = pd.to_numeric(pd.Series([year]), errors="coerce").iloc[0]
    month_value = pd.to_numeric(pd.Series([month]), errors="coerce").iloc[0]
    if pd.isna(effective) or pd.isna(year_value) or pd.isna(month_value):
        return pd.NA
    year_int = int(year_value)
    month_int = int(month_value)
    period = pd.Period(year=year_int, month=month_int, freq="M")
    start = effective.to_period("M")
    if period < start:
        return 0.0
    if period > start:
        return 1.0
    days = monthrange(year_int, month_int)[1]
    return float((days - effective.day + 1) / days)


def _safe_load_table(path: str | None, columns: list[str] | None = None) -> tuple[pd.DataFrame | None, str | None]:
    if path is None:
        return None, "missing_path"
    try:
        return read_table(Path(path), columns=columns), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _load_table_with_required_columns(path: Path, required_columns: list[str]) -> pd.DataFrame:
    """Load a table, falling back to a full read when a fixture omits requested columns."""
    try:
        frame = read_table(path, columns=required_columns)
    except Exception:
        frame = read_table(path)
    for column in required_columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, required_columns].copy()


def _looks_like_placeholder(path: Path, error_message: str | None, file_size: int | None) -> bool:
    if file_size == 0:
        return False
    if error_message is None:
        return False
    text = error_message.lower()
    return any(
        marker in text
        for marker in (
            "cloud file provider",
            "provider is not running",
            "invalid argument",
            "errno 22",
            "cannot access a closed file",
        )
    )


def _inspect_source_candidate(path: Path, artifact: str, candidate: str, key_columns: list[str]) -> dict[str, Any]:
    exists = path.exists()
    file_size = None
    file_attributes = None
    if exists:
        try:
            stat_result = path.stat()
            file_size = int(stat_result.st_size)
            file_attributes = int(getattr(stat_result, "st_file_attributes", 0))
        except Exception:
            file_size = None
            file_attributes = None

    row: dict[str, Any] = {
        "artifact": artifact,
        "candidate": candidate,
        "expected_path": str(path),
        "exists": bool(exists),
        "readable": False,
        "row_count": None,
        "key_columns_expected": json.dumps(key_columns),
        "key_columns_present": False,
        "missing_key_columns": json.dumps(key_columns),
        "exception_message": None,
        "appears_placeholder": False,
        "zero_byte": bool(file_size == 0),
        "invalid_parquet": False,
        "file_size_bytes": file_size,
        "file_attributes": file_attributes,
        "observed_columns_json": None,
    }
    if not exists:
        row["exception_message"] = "missing"
        return row
    try:
        frame = read_table(path)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        row["exception_message"] = message
        row["appears_placeholder"] = _looks_like_placeholder(path, message, file_size)
        row["invalid_parquet"] = path.suffix.lower() == ".parquet" and not row["appears_placeholder"] and not row["zero_byte"]
        return row

    columns = [str(column) for column in frame.columns]
    missing = [column for column in key_columns if column not in columns]
    row["readable"] = True
    row["row_count"] = int(len(frame))
    row["key_columns_present"] = not missing
    row["missing_key_columns"] = json.dumps(missing)
    row["observed_columns_json"] = json.dumps(columns)
    row["appears_placeholder"] = False
    row["invalid_parquet"] = False
    return row


def build_raw_source_health_report(config: PipelineConfig) -> tuple[pd.DataFrame, dict[str, Any], dict[str, pd.DataFrame | None], dict[str, str | None]]:
    """Inspect expected source artifacts and identify source-layer blockers."""
    report_rows: list[dict[str, Any]] = []
    source_tables: dict[str, pd.DataFrame | None] = {
        "machine_links": None,
        "pdf_links": None,
        "rule_attrs": None,
        "overlay": None,
    }
    source_errors: dict[str, str | None] = {}

    artifact_name_map = {
        "tradewar_machine_links": "machine_links",
        "tradewar_pdf_links": "pdf_links",
        "tradewar_rule_attributes": "rule_attrs",
        "tradewar_overlay_raw": "overlay",
    }

    for spec in SOURCE_ARTIFACT_SPECS:
        artifact = spec["artifact"]
        base_dir = config.analysis_dir if spec["base_dir"] == "analysis_dir" else config.reference_dir
        selected_frame: pd.DataFrame | None = None
        selected_error: str | None = None
        for candidate_spec in spec["candidates"]:
            path = base_dir / candidate_spec["filename"]
            row = _inspect_source_candidate(path, artifact, candidate_spec["candidate"], candidate_spec["key_columns"])
            report_rows.append(row)
            healthy = row["readable"] and bool(row["row_count"]) and row["key_columns_present"]
            if healthy and selected_frame is None:
                try:
                    selected_frame = read_table(path)
                    selected_error = None
                except Exception as exc:
                    selected_frame = None
                    selected_error = f"{type(exc).__name__}: {exc}"
                else:
                    break
            if selected_error is None and row["exception_message"]:
                selected_error = row["exception_message"]
        source_errors[artifact] = selected_error or ("unreadable_or_empty" if selected_frame is None else None)
        table_key = artifact_name_map.get(artifact)
        if table_key is not None:
            source_tables[table_key] = selected_frame

    report = pd.DataFrame(report_rows, columns=SOURCE_HEALTH_COLUMNS)
    artifact_status: dict[str, dict[str, Any]] = {}
    for spec in SOURCE_ARTIFACT_SPECS:
        artifact = spec["artifact"]
        artifact_rows = [row for row in report_rows if row["artifact"] == artifact]
        healthy_rows = [row for row in artifact_rows if row["readable"] and row["row_count"] and row["key_columns_present"]]
        artifact_status[artifact] = {
            "blocked": not bool(healthy_rows),
            "selected_candidate": healthy_rows[0]["candidate"] if healthy_rows else None,
            "selected_path": healthy_rows[0]["expected_path"] if healthy_rows else None,
            "row_count": int(healthy_rows[0]["row_count"]) if healthy_rows else None,
        }
    summary: dict[str, Any] = {
        "blocked_by_source_availability": bool(any(not status["row_count"] or status["selected_path"] is None for status in artifact_status.values())),
        "blocking_artifacts": sorted([artifact for artifact, status in artifact_status.items() if status["selected_path"] is None]),
        "artifacts": artifact_status,
    }
    return report, summary, source_tables, source_errors


def compare_raw_reconstruction(reference: pd.DataFrame, raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return cell-level discrepancies and a machine-readable metric table.

    The package stores trade values and quantities at million scale. Raw Census values
    remain in source units, so package trade fields are rescaled only for comparison.
    """
    ref = _normalize(reference).rename(columns={column: f"ref_{column}" for column in reference.columns if column not in KEYS})
    built = _normalize(raw).rename(columns={column: f"raw_{column}" for column in raw.columns if column not in KEYS})
    merged = ref.merge(built, on=KEYS, how="outer", indicator=True)
    merged["_merge"] = merged["_merge"].astype("string")
    _to_numeric(merged, ["ref_m_val", "ref_m_q1"])
    _add_reference_status(merged)
    _add_raw_status(merged)
    merged["trade_value_abs_diff"] = (merged["raw_m_val"] - merged["ref_m_val"] * 1_000_000.0).abs()
    merged["quantity_abs_diff"] = (merged["raw_m_q1"] - merged["ref_m_q1"] * 1_000_000.0).abs()
    merged["rate1_abs_diff"] = (merged["raw_m_statutory_tariff1"] - merged["ref_m_stattariff1"]).abs()
    merged["rate2_abs_diff"] = (merged["raw_m_statutory_tariff2"] - merged["ref_m_stattariff2"]).abs()
    merged["is_non_ad_valorem_or_sentinel"] = merged["raw_m_statutory_tariff1"].ge(9999).fillna(False)
    merged["ref_treated"] = merged["ref_active"].astype("int8")
    merged["raw_treated"] = merged["raw_active"].astype("int8")

    conditions = [
        merged["_merge"].eq("left_only"),
        merged["_merge"].eq("right_only"),
        merged["ref_active"] & ~merged["raw_active"],
        ~merged["ref_active"] & merged["raw_active"],
        merged["is_non_ad_valorem_or_sentinel"],
        merged["rate1_abs_diff"].gt(RATE_TOL),
        merged["rate2_abs_diff"].gt(RATE_TOL),
        merged["trade_value_abs_diff"].gt(TRADE_VALUE_TOL),
    ]
    labels = [
        "missing_raw_key",
        "missing_reference_key",
        "missing_raw_policy_scope",
        "extra_raw_policy_scope",
        "non_ad_valorem_or_sentinel",
        "statutory_rate_mismatch",
        "day_weighted_rate_mismatch",
        "trade_value_mismatch",
    ]
    merged["discrepancy_type"] = np.select(conditions, labels, default="match")

    matched = merged.loc[merged["_merge"].eq("both")].copy()
    matched_non_sentinel = matched.loc[~matched["is_non_ad_valorem_or_sentinel"]].copy()
    active_reference = merged.loc[merged["ref_active"]].copy()
    active_matched = matched.loc[matched["ref_active"] | matched["raw_active"]].copy()
    active_non_sentinel = active_matched.loc[~active_matched["is_non_ad_valorem_or_sentinel"]].copy()
    trade_value_match = matched["trade_value_abs_diff"].le(TRADE_VALUE_TOL).fillna(False)
    rate1_match = _rate_match_mask(matched_non_sentinel["rate1_abs_diff"])
    rate2_match = _rate_match_mask(matched_non_sentinel["rate2_abs_diff"])
    active_rate1_match = _rate_match_mask(active_non_sentinel["rate1_abs_diff"])
    active_rate2_match = _rate_match_mask(active_non_sentinel["rate2_abs_diff"])

    metrics = [
        _metric("reference_rows", int(len(ref))),
        _metric("raw_rows", int(len(built))),
        _metric("matched_rows", int(len(matched))),
        _metric("paper_key_coverage_rate", float(len(matched) / max(len(ref), 1))),
        _metric("reference_subject_rows", int(merged["ref_subject"].sum())),
        _metric("reference_active_rows", int(merged["ref_active"].sum())),
        _metric("active_matched_rows", int(len(active_matched))),
        _metric("active_non_sentinel_rows", int(len(active_non_sentinel))),
        _metric("exact_treatment_status_rows", int((matched["ref_treated"] == matched["raw_treated"]).sum())),
        _metric("treatment_match_rate", _share(matched["ref_treated"] == matched["raw_treated"])),
        _metric("statutory_rate_match_rows", int(rate1_match.sum())),
        _metric("statutory_rate_match_rate_non_sentinel", _share(rate1_match)),
        _metric("day_weighted_rate_match_rows", int(rate2_match.sum())),
        _metric("day_weighted_rate_match_rate_non_sentinel", _share(rate2_match)),
        _metric("tariff_active_key_coverage_rate", float(active_reference["_merge"].eq("both").mean()) if len(active_reference) else 1.0),
        _metric("tariff_active_treatment_match_rate", _share(active_matched["ref_treated"] == active_matched["raw_treated"])),
        _metric("tariff_active_statutory_rate_match_rate", _share(active_rate1_match)),
        _metric("tariff_active_day_weighted_rate_match_rate", _share(active_rate2_match)),
        _metric("raw_trade_value_match_rate", _share(trade_value_match)),
    ]
    by_type = merged["discrepancy_type"].value_counts(dropna=False)
    metrics.extend({"metric": f"discrepancy_{key}", "value": int(value)} for key, value in by_type.items())
    return merged, pd.DataFrame(metrics)


def summarize_discrepancies(cells: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build compact validation summaries for policy-gap triage."""
    discrepancy = cells.loc[cells["discrepancy_type"].ne("match")].copy()
    if discrepancy.empty:
        empty = pd.DataFrame(columns=["rows"])
        return {
            "by_type": empty,
            "by_year_month": empty,
            "by_family": empty,
            "by_country": empty,
            "by_hs2": empty,
            "by_rate_bucket": empty,
        }

    def _count(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        return frame.groupby(columns, dropna=False, observed=True).size().reset_index(name="rows").sort_values("rows", ascending=False)

    out: dict[str, pd.DataFrame] = {
        "by_type": _count(discrepancy, ["discrepancy_type"]),
        "by_year_month": _count(discrepancy, ["year", "month", "discrepancy_type"]),
    }
    active_policy = discrepancy.loc[discrepancy["ref_active"] | discrepancy["raw_active"]].copy()
    if active_policy.empty:
        out["by_family"] = pd.DataFrame(columns=["family", "discrepancy_type", "rows"])
    else:
        family_rows: list[pd.DataFrame] = []
        for family in FAMILY_COLUMNS:
            ref_family = f"ref_{family}"
            if ref_family not in active_policy:
                continue
            mask = pd.to_numeric(active_policy[ref_family], errors="coerce").fillna(0).ne(0)
            grouped = _count(active_policy.loc[mask], ["discrepancy_type"])
            grouped.insert(0, "family", family)
            family_rows.append(grouped)
        out["by_family"] = pd.concat(family_rows, ignore_index=True) if family_rows else pd.DataFrame(columns=["family", "discrepancy_type", "rows"])

    for source_column, output_name in [("cty_code", "by_country"), ("hs10", "by_hs2")]:
        temp = active_policy.copy()
        if source_column == "hs10":
            temp["hs2"] = temp["hs10"].astype("string").str.slice(0, 2)
            out[output_name] = _count(temp, ["hs2", "discrepancy_type"]).head(200)
        else:
            out[output_name] = _count(temp, [source_column, "discrepancy_type"]).head(200)

    active_policy["rate_bucket"] = pd.cut(
        active_policy["rate1_abs_diff"],
        bins=[-np.inf, RATE_TOL, 0.01, 0.05, 0.10, 0.25, np.inf],
        labels=["exact", "lte_1pp", "lte_5pp", "lte_10pp", "lte_25pp", "gt_25pp"],
    ).astype("string").fillna("missing_rate_diff")
    out["by_rate_bucket"] = _count(active_policy, ["rate_bucket", "discrepancy_type"])

    china_missing = discrepancy.loc[
        discrepancy["discrepancy_type"].eq("missing_raw_policy_scope") & discrepancy["ref_m_china_hit"].eq(1)
    ].copy()
    if china_missing.empty:
        out["china_301_top_hs8"] = pd.DataFrame(columns=["hs8", "rows"])
        out["china_301_top_month"] = pd.DataFrame(columns=["year", "month", "rows"])
        out["china_301_top_hs8_month_wave"] = pd.DataFrame(columns=["hs8", "year", "month", "ref_effective_period", "rows"])
    else:
        china_missing["hs8"] = china_missing["hs10"].astype("string").str.slice(0, 8)
        china_missing["ref_effective_period"] = _effective_period(china_missing["ref_m_effective_mdate2"])
        out["china_301_top_hs8"] = _count(china_missing, ["hs8"]).head(200)
        out["china_301_top_month"] = _count(china_missing, ["year", "month"]).head(200)
        out["china_301_top_hs8_month_wave"] = _count(china_missing, ["hs8", "year", "month", "ref_effective_period"]).head(400)
    return out


def _prepare_china_trace_frame(frame: pd.DataFrame | None) -> pd.DataFrame | None:
    if frame is None or frame.empty:
        return None
    out = frame.copy()
    for column in ("hs8", "rule_code", "tw_rule_code_raw", "cty_name"):
        if column in out.columns:
            out[column] = out[column].astype("string")
    if "hs8" in out.columns:
        out["hs8"] = _normalize_hs8(out["hs8"])
    return out


def _trace_stage(
    raw_link_rows: int | pd.NA,
    rule_attr_rows: int | pd.NA,
    overlay_increment_rows: int | pd.NA,
    panel_increment_rows: int | pd.NA,
    source_unavailable: bool,
) -> str:
    if source_unavailable:
        return "source_unavailable"
    if pd.isna(raw_link_rows) or raw_link_rows == 0:
        return "absent_from_raw_links"
    if pd.isna(rule_attr_rows) or rule_attr_rows == 0:
        return "raw_links_missing_rule_attrs"
    if pd.isna(overlay_increment_rows) or overlay_increment_rows == 0:
        return "lost_before_overlay"
    if pd.isna(panel_increment_rows) or panel_increment_rows == 0:
        return "lost_after_overlay"
    return "present_with_increment"


def _load_china_301_discrepancies(output_dir: Path) -> pd.DataFrame:
    path = output_dir / "raw_replication_discrepancies.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing raw replication discrepancies artifact: {path}")

    columns = ["hs10", "year", "month", "ref_m_effective_mdate2", "ref_m_china_hit", "discrepancy_type"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(path).replace("'", "''")
            query = f"""
                SELECT hs10, year, month, ref_m_effective_mdate2, ref_m_china_hit, discrepancy_type
                FROM read_parquet('{escaped_path}')
                WHERE discrepancy_type = 'missing_raw_policy_scope'
                  AND ref_m_china_hit = 1
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
    else:
        frame = read_table(path, columns=columns)
        frame = frame.loc[
            frame["discrepancy_type"].eq("missing_raw_policy_scope") & pd.to_numeric(frame["ref_m_china_hit"], errors="coerce").eq(1)
        ].copy()
    if frame.empty:
        return frame
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["ref_effective_period"] = _effective_period(frame["ref_m_effective_mdate2"])
    frame["hs8"] = _normalize_hs8(frame["hs10"])
    return frame.dropna(subset=["hs8", "year", "month"]).copy()


def _load_china_301_panel_slice(config: PipelineConfig, buckets: pd.DataFrame) -> pd.DataFrame | None:
    panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel_path.exists() or buckets.empty:
        return None

    columns = ["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(panel_path).replace("'", "''")
            bucket_frame = buckets.loc[:, ["hs8", "year", "month"]].drop_duplicates().copy()
            bucket_frame["hs8"] = bucket_frame["hs8"].astype("string")
            bucket_frame["year"] = pd.to_numeric(bucket_frame["year"], errors="coerce").astype("Int64")
            bucket_frame["month"] = pd.to_numeric(bucket_frame["month"], errors="coerce").astype("Int64")
            con.register("china_buckets", bucket_frame)
            query = f"""
                SELECT
                    CAST(p.cty_code AS BIGINT) AS cty_code,
                    CAST(p.hs10 AS VARCHAR) AS hs10,
                    CAST(p.year AS BIGINT) AS year,
                    CAST(p.month AS BIGINT) AS month,
                    p.tw_increment_rate_raw
                FROM read_parquet('{escaped_path}') AS p
                INNER JOIN china_buckets AS b
                    ON CAST(p.year AS BIGINT) = b.year
                    AND CAST(p.month AS BIGINT) = b.month
                    AND substr(CAST(p.hs10 AS VARCHAR), 1, 8) = b.hs8
                WHERE CAST(p.cty_code AS BIGINT) = 5700
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
        if frame.empty:
            return frame
    else:
        frame = read_table(panel_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame = frame.loc[frame["cty_code"].eq(5700)].copy()
        frame = frame.merge(buckets.loc[:, ["hs8", "year", "month"]].drop_duplicates(), left_on=["year", "month"], right_on=["year", "month"], how="inner")
        frame = frame.loc[frame["hs10"].astype("string").str.slice(0, 8).eq(frame["hs8"].astype("string"))].copy()

    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["hs8"] = frame["hs10"].str.slice(0, 8)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
    return frame.dropna(subset=["cty_code", "hs10", "year", "month"]).copy()


def _load_china_301_panel_trace_slice(config: PipelineConfig, hs10_values: pd.Series, year: int, month: int) -> pd.DataFrame | None:
    panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel_path.exists() or hs10_values.empty:
        return None

    values = sorted({normalize_hs_code(value, 10) for value in hs10_values.dropna().astype(str)})
    values = [value for value in values if value]
    if not values:
        return None

    columns = ["cty_code", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(panel_path).replace("'", "''")
            hs10_list = ", ".join(f"'{value}'" for value in values)
            query = f"""
                SELECT
                    CAST(cty_code AS BIGINT) AS cty_code,
                    CAST(cty_name AS VARCHAR) AS cty_name,
                    CAST(hs10 AS VARCHAR) AS hs10,
                    CAST(year AS BIGINT) AS year,
                    CAST(month AS BIGINT) AS month,
                    tw_increment_rate_raw,
                    tw_rule_code_raw,
                    tw_scope_source_raw
                FROM read_parquet('{escaped_path}')
                WHERE CAST(cty_code AS BIGINT) = 5700
                  AND CAST(year AS BIGINT) = {int(year)}
                  AND CAST(month AS BIGINT) = {int(month)}
                  AND CAST(hs10 AS VARCHAR) IN ({hs10_list})
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
        if frame.empty:
            return frame
    else:
        frame = read_table(panel_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame = frame.loc[
            frame["cty_code"].eq(5700)
            & frame["year"].eq(int(year))
            & frame["month"].eq(int(month))
            & frame["hs10"].isin(values)
        ].copy()

    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    return frame.dropna(subset=["cty_code", "hs10", "year", "month"]).copy()


def _load_china_301_key_panel_slice(config: PipelineConfig, keys: pd.DataFrame) -> pd.DataFrame | None:
    """Load the exact China panel rows needed for the key-level diagnostic."""
    panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel_path.exists() or keys.empty:
        return None

    key_frame = keys.loc[:, ["cty_code", "hs10", "year", "month"]].drop_duplicates().copy()
    key_frame["cty_code"] = pd.to_numeric(key_frame["cty_code"], errors="coerce").astype("Int64")
    key_frame["hs10"] = key_frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    key_frame["year"] = pd.to_numeric(key_frame["year"], errors="coerce").astype("Int64")
    key_frame["month"] = pd.to_numeric(key_frame["month"], errors="coerce").astype("Int64")
    key_frame = key_frame.dropna(subset=["cty_code", "hs10", "year", "month"]).copy()
    if key_frame.empty:
        return None

    columns = ["cty_code", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(panel_path).replace("'", "''")
            con.register("china_keys", key_frame)
            query = f"""
                SELECT
                    CAST(p.cty_code AS BIGINT) AS cty_code,
                    CAST(p.hs10 AS VARCHAR) AS hs10,
                    CAST(p.year AS BIGINT) AS year,
                    CAST(p.month AS BIGINT) AS month,
                    p.tw_increment_rate_raw,
                    p.tw_rule_code_raw,
                    p.tw_scope_source_raw
                FROM read_parquet('{escaped_path}') AS p
                INNER JOIN china_keys AS k
                    ON CAST(p.cty_code AS BIGINT) = k.cty_code
                    AND CAST(p.hs10 AS VARCHAR) = k.hs10
                    AND CAST(p.year AS BIGINT) = k.year
                    AND CAST(p.month AS BIGINT) = k.month
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
        if frame.empty:
            return frame
    else:
        frame = read_table(panel_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame = frame.merge(key_frame, on=["cty_code", "hs10", "year", "month"], how="inner")

    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
    return frame.dropna(subset=["cty_code", "hs10", "year", "month"]).copy()


def _load_china_301_rate_panel_slice(config: PipelineConfig, keys: pd.DataFrame) -> pd.DataFrame | None:
    """Load the exact China panel rows needed for the rate-level diagnostic."""
    panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel_path.exists() or keys.empty:
        return None

    key_frame = keys.loc[:, ["cty_code", "hs10", "year", "month"]].drop_duplicates().copy()
    key_frame["cty_code"] = pd.to_numeric(key_frame["cty_code"], errors="coerce").astype("Int64")
    key_frame["hs10"] = key_frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    key_frame["year"] = pd.to_numeric(key_frame["year"], errors="coerce").astype("Int64")
    key_frame["month"] = pd.to_numeric(key_frame["month"], errors="coerce").astype("Int64")
    key_frame = key_frame.dropna(subset=["cty_code", "hs10", "year", "month"]).copy()
    if key_frame.empty:
        return None

    columns = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "mfn_ad_val_rate",
        "base_pref_rate_raw",
        "base_statutory_rate_raw",
        "tw_increment_rate_raw",
        "tw_active_share_raw",
        "m_statutory_tariff1",
        "m_statutory_tariff2",
        "m_policy_source",
        "tw_rule_code_raw",
        "tw_scope_source_raw",
    ]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(panel_path).replace("'", "''")
            con.register("china_rate_keys", key_frame)
            query = f"""
                SELECT
                    CAST(p.cty_code AS BIGINT) AS cty_code,
                    CAST(p.hs10 AS VARCHAR) AS hs10,
                    CAST(p.year AS BIGINT) AS year,
                    CAST(p.month AS BIGINT) AS month,
                    p.mfn_ad_val_rate,
                    p.base_pref_rate_raw,
                    p.base_statutory_rate_raw,
                    p.tw_increment_rate_raw,
                    p.tw_active_share_raw,
                    p.m_statutory_tariff1,
                    p.m_statutory_tariff2,
                    p.m_policy_source,
                    p.tw_rule_code_raw,
                    p.tw_scope_source_raw
                FROM read_parquet('{escaped_path}') AS p
                INNER JOIN china_rate_keys AS k
                    ON CAST(p.cty_code AS BIGINT) = k.cty_code
                    AND CAST(p.hs10 AS VARCHAR) = k.hs10
                    AND CAST(p.year AS BIGINT) = k.year
                    AND CAST(p.month AS BIGINT) = k.month
            """
            try:
                frame = con.execute(query).fetchdf()
            except Exception:
                frame = pd.DataFrame()
        finally:
            con.close()
        if frame.empty:
            columns = ["cty_code", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"]
            frame = read_table(panel_path, columns=columns)
            frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
            frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
            frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
            frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
            frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
            frame = frame.merge(key_frame, on=["cty_code", "hs10", "year", "month"], how="inner")
    else:
        frame = read_table(panel_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["mfn_ad_val_rate"] = pd.to_numeric(frame["mfn_ad_val_rate"], errors="coerce")
        frame["base_pref_rate_raw"] = pd.to_numeric(frame["base_pref_rate_raw"], errors="coerce")
        frame["base_statutory_rate_raw"] = pd.to_numeric(frame["base_statutory_rate_raw"], errors="coerce")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame["tw_active_share_raw"] = pd.to_numeric(frame["tw_active_share_raw"], errors="coerce")
        frame["m_statutory_tariff1"] = pd.to_numeric(frame["m_statutory_tariff1"], errors="coerce")
        frame["m_statutory_tariff2"] = pd.to_numeric(frame["m_statutory_tariff2"], errors="coerce")
        frame = frame.merge(key_frame, on=["cty_code", "hs10", "year", "month"], how="inner")

    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    for column in [
        "mfn_ad_val_rate",
        "base_pref_rate_raw",
        "base_statutory_rate_raw",
        "tw_active_share_raw",
        "m_statutory_tariff1",
        "m_statutory_tariff2",
        "m_policy_source",
        "tw_rule_code_raw",
        "tw_scope_source_raw",
    ]:
        if column not in frame.columns:
            frame[column] = pd.NA
    for column in [
        "mfn_ad_val_rate",
        "base_pref_rate_raw",
        "base_statutory_rate_raw",
        "tw_increment_rate_raw",
        "tw_active_share_raw",
        "m_statutory_tariff1",
        "m_statutory_tariff2",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["cty_code", "hs10", "year", "month"]).copy()


def _classify_china_301_rate_stage(row: pd.Series) -> str:
    if int(row.get("duplicate_reference_key_rows") or 0) > 1:
        return "duplicate_reference_key"
    if int(row.get("duplicate_raw_key_rows") or 0) > 1:
        return "duplicate_raw_key"
    if not bool(row.get("ref_active")) or int(row.get("ref_m_china_hit") or 0) != 1 or int(row.get("cty_code") or 0) != 5700:
        return "non_china_or_inactive_reference_leak"
    if not bool(row.get("raw_key_present")):
        if bool(row.get("overlay_hs8_month_present")):
            return "hs8_overlay_present_exact_hs10_absent"
        return "raw_key_absent"
    if pd.isna(row.get("raw_panel_increment")):
        return "raw_key_present_no_increment"

    discrepancy_type = str(row.get("discrepancy_type") or "")
    if discrepancy_type == "statutory_rate_mismatch":
        return "statutory_rate_mismatch"
    if discrepancy_type == "day_weighted_rate_mismatch":
        return "day_weighted_rate_mismatch"
    if discrepancy_type in {"missing_raw_policy_scope", "missing_raw_key"}:
        return "panel_increment_present_but_validation_mismatch"
    return "requires_full_model_review"


def _load_china_301_key_hs10_slice(config: PipelineConfig, keys: pd.DataFrame) -> pd.DataFrame | None:
    """Load any-country panel rows for the key-level hs10 presence check."""
    panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel_path.exists() or keys.empty:
        return None

    key_frame = keys.loc[:, ["hs10", "year", "month"]].drop_duplicates().copy()
    key_frame["hs10"] = key_frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    key_frame["year"] = pd.to_numeric(key_frame["year"], errors="coerce").astype("Int64")
    key_frame["month"] = pd.to_numeric(key_frame["month"], errors="coerce").astype("Int64")
    key_frame = key_frame.dropna(subset=["hs10", "year", "month"]).copy()
    if key_frame.empty:
        return None

    columns = ["cty_code", "cty_name", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(panel_path).replace("'", "''")
            con.register("china_hs10_keys", key_frame)
            query = f"""
                SELECT
                    CAST(p.cty_code AS BIGINT) AS cty_code,
                    CAST(p.hs10 AS VARCHAR) AS hs10,
                    CAST(p.year AS BIGINT) AS year,
                    CAST(p.month AS BIGINT) AS month,
                    p.tw_increment_rate_raw,
                    p.tw_rule_code_raw,
                    p.tw_scope_source_raw
                FROM read_parquet('{escaped_path}') AS p
                INNER JOIN china_hs10_keys AS k
                    ON CAST(p.hs10 AS VARCHAR) = k.hs10
                    AND CAST(p.year AS BIGINT) = k.year
                    AND CAST(p.month AS BIGINT) = k.month
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
        if frame.empty:
            return frame
    else:
        frame = read_table(panel_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame = frame.merge(key_frame, on=["hs10", "year", "month"], how="inner")

    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
    return frame.dropna(subset=["hs10", "year", "month"]).copy()


def _load_china_301_key_overlay_slice(config: PipelineConfig, buckets: pd.DataFrame) -> pd.DataFrame | None:
    """Load China overlay rows for the key-level hs8/month presence check."""
    overlay_path = config.analysis_dir / "tradewar_overlay_raw.parquet"
    if not overlay_path.exists() or buckets.empty:
        return None

    bucket_frame = buckets.loc[:, ["hs8", "year", "month"]].drop_duplicates().copy()
    bucket_frame["hs8"] = bucket_frame["hs8"].astype("string")
    bucket_frame["year"] = pd.to_numeric(bucket_frame["year"], errors="coerce").astype("Int64")
    bucket_frame["month"] = pd.to_numeric(bucket_frame["month"], errors="coerce").astype("Int64")
    bucket_frame = bucket_frame.dropna(subset=["hs8", "year", "month"]).copy()
    if bucket_frame.empty:
        return None

    columns = ["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(overlay_path).replace("'", "''")
            con.register("china_buckets", bucket_frame)
            query = f"""
                SELECT
                    CAST(cty_name AS VARCHAR) AS cty_name,
                    CAST(hs8 AS VARCHAR) AS hs8,
                    CAST(year AS BIGINT) AS year,
                    CAST(month AS BIGINT) AS month,
                    tw_increment_rate_raw,
                    tw_rule_code_raw,
                    tw_scope_source_raw
                FROM read_parquet('{escaped_path}')
                WHERE UPPER(CAST(cty_name AS VARCHAR)) = 'CHINA'
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
        if frame.empty:
            return frame
        frame["hs8"] = frame["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame = frame.merge(bucket_frame, on=["hs8", "year", "month"], how="inner")
    else:
        frame = read_table(overlay_path, columns=columns)
        frame["cty_name"] = frame["cty_name"].astype("string").str.upper()
        frame["hs8"] = frame["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
        frame = frame.loc[frame["cty_name"].eq("CHINA")].copy()
        frame = frame.merge(bucket_frame, on=["hs8", "year", "month"], how="inner")

    frame["tw_increment_rate_raw"] = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce")
    return frame.dropna(subset=["hs8", "year", "month"]).copy()


def _build_china_301_validation_universe(reference: pd.DataFrame) -> pd.DataFrame:
    """Summarize the benchmark universe choices for the China-only validation path."""
    frame = reference.copy()
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce")
    frame["m_status2"] = pd.to_numeric(frame["m_status2"], errors="coerce")
    frame["m_china_hit"] = pd.to_numeric(frame["m_china_hit"], errors="coerce")
    frame["is_china_partner"] = frame["cty_code"].eq(5700)
    frame["is_active"] = frame["m_status2"].gt(0)
    frame["is_china_hit"] = frame["m_china_hit"].eq(1)
    frame["included_in_china_current"] = frame["is_china_partner"] & frame["is_china_hit"]
    buckets = [
        ("all_reference_rows", frame.index == frame.index),
        ("active_reference_rows", frame["is_active"]),
        ("china_hit_reference_rows", frame["is_china_hit"]),
        ("china_partner_reference_rows", frame["is_china_partner"]),
        ("active_china_hit_reference_rows", frame["is_active"] & frame["is_china_hit"]),
        ("active_china_partner_reference_rows", frame["is_active"] & frame["is_china_partner"]),
        (
            "china_hit_partner_current_validation_rows",
            frame["included_in_china_current"],
        ),
        (
            "active_china_hit_nonchina_partner_reference_rows",
            frame["is_active"] & frame["is_china_hit"] & ~frame["is_china_partner"],
        ),
    ]
    rows: list[dict[str, Any]] = []
    total = max(len(frame), 1)
    for label, mask in buckets:
        selected = frame.loc[mask].copy()
        rows.append(
            {
                "universe": label,
                "rows": int(len(selected)),
                "share": float(len(selected) / total),
                "selected_for_current_validation": bool(label == "china_hit_partner_current_validation_rows"),
            }
        )
    return pd.DataFrame(rows)


def _build_china_301_validation_decomposition(reference: pd.DataFrame) -> pd.DataFrame:
    """Break the benchmark into China-partner versus non-China-partner China-policy cells."""
    frame = reference.copy()
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce")
    frame["m_status2"] = pd.to_numeric(frame["m_status2"], errors="coerce")
    frame["m_china_hit"] = pd.to_numeric(frame["m_china_hit"], errors="coerce")
    frame["is_china_partner"] = frame["cty_code"].eq(5700)
    frame["is_active"] = frame["m_status2"].gt(0)
    frame["is_china_hit"] = frame["m_china_hit"].eq(1)
    frame["is_current_validation"] = frame["is_china_partner"] & frame["is_china_hit"]

    rows: list[dict[str, Any]] = []
    total = max(len(frame), 1)
    active_total = max(int(frame["is_active"].sum()), 1)
    china_hit_total = max(int(frame["is_china_hit"].sum()), 1)
    active_china_hit_total = max(int((frame["is_active"] & frame["is_china_hit"]).sum()), 1)
    for bucket_name, mask, base_total in [
        ("all_reference_rows", frame.index == frame.index, total),
        ("active_reference_rows", frame["is_active"], active_total),
        ("china_hit_reference_rows", frame["is_china_hit"], china_hit_total),
        ("active_china_hit_reference_rows", frame["is_active"] & frame["is_china_hit"], active_china_hit_total),
    ]:
        selected = frame.loc[mask].copy()
        rows.append(
            {
                "bucket": bucket_name,
                "rows": int(len(selected)),
                "share_of_reference": float(len(selected) / total),
                "share_of_bucket_base": float(len(selected) / base_total),
                "included_in_current_validation": bool(bucket_name == "china_hit_partner_current_validation_rows" and selected["is_china_partner"].all()),
                "china_partner_rows": int(selected["is_china_partner"].sum()),
                "nonchina_partner_rows": int((~selected["is_china_partner"]).sum()),
            }
        )

    current = frame.loc[frame["is_current_validation"]].copy()
    rows.append(
        {
            "bucket": "china_hit_partner_current_validation_rows",
            "rows": int(len(current)),
            "share_of_reference": float(len(current) / total),
            "share_of_bucket_base": float(len(current) / china_hit_total),
            "included_in_current_validation": True,
            "china_partner_rows": int(len(current)),
            "nonchina_partner_rows": 0,
        }
    )
    return pd.DataFrame(rows)


def _find_first_evidence_line(path: Path, patterns: list[str]) -> tuple[int | None, str | None]:
    """Return the first matching line number and text for a set of substrings."""
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return None, None
    for pattern in patterns:
        for line_no, line in enumerate(lines, start=1):
            if pattern.lower() in line.lower():
                return line_no, line.strip()
    return None, None


def _build_china_301_variable_semantics_table(config: PipelineConfig) -> pd.DataFrame:
    """Summarize the paper-side tariff variable semantics used by the validation layer."""
    root = config.repo_root
    sources = [
        root / "data" / "fajgelbaum" / "code" / "main" / "tab_01_sumstats.do",
        root / "data" / "fajgelbaum" / "code" / "main" / "fig_01_rates.do",
        root / "data" / "fajgelbaum" / "code" / "main" / "fig_02_m_event.do",
        root / "scr" / "passthru_data" / "raw_replication_validation.py",
    ]
    rows: list[dict[str, Any]] = []
    specs = [
        {
            "variable": "m_target",
            "label": "Targeted product indicator",
            "inferred_level": "cty_code x hs10 product",
            "benchmark_condition": "derived as max(m_status2) by product and used with m_china_hit==1 for China summaries",
            "interpretation": "Product-level tariff targeting derived from the tariff-status field; not a separate raw source field.",
            "patterns": ["gegen m_target = max(m_status2), by(id)", "replace m_target = m_target==2"],
        },
        {
            "variable": "m_status2",
            "label": "Tariff status",
            "inferred_level": "row-month",
            "benchmark_condition": "m_status2 > 0 defines active tariff status in the validator",
            "interpretation": "Paper-side time-varying tariff status; active tariffs are distinguished from event-study status.",
            "patterns": ["replace m_target = m_target==2", "g m_status = m_status2"],
        },
        {
            "variable": "m_china_hit",
            "label": "China product-scope indicator",
            "inferred_level": "row-month",
            "benchmark_condition": "m_china_hit==1 & m_target==1 & cty_name==\"CHINA\" in the paper tables and figures",
            "interpretation": "Product scope membership for the China trade-war rows; paired with target and partner filters.",
            "patterns": ["m_china_hit==1 & m_target==1", "china_stat = m_stattariff1"],
        },
        {
            "variable": "m_ess",
            "label": "Event-study status",
            "inferred_level": "row-month",
            "benchmark_condition": "event-study treatment status used in the dynamic design, not the tariff-active validation status",
            "interpretation": "Event-study treatment flag; useful for dynamic regressions but not the tariff-activeness definition in validation.",
            "patterns": ["g m_status = m_status2", "label define statusLBL 0 \"All other\" 1 \"Exempt\" 2 \"Targeted\""],
        },
        {
            "variable": "m_stattariff1",
            "label": "Statutory Tariff Rate",
            "inferred_level": "row-month",
            "benchmark_condition": "used as the China statutory rate in rate tables when m_target==1 and m_china_hit==1",
            "interpretation": "Paper-side statutory rate variable for figure/table comparisons.",
            "patterns": ["china_stat = m_stattariff1", "sum m_stattariff1 if m_china_hit==1 & m_target==1"],
        },
        {
            "variable": "m_stattariff2",
            "label": "Day-scaled statutory Tariff Rate",
            "inferred_level": "row-month",
            "benchmark_condition": "used in the event-study outcome construction as the duty-inclusive scaled statutory rate",
            "interpretation": "Day-weighted statutory rate used in the event-study setup and day-scaled rate comparisons.",
            "patterns": ["g m_stattariff = m_stattariff2", "Log Duty-Inclusive Unit Value"],
        },
        {
            "variable": "cty_code",
            "label": "Partner country code",
            "inferred_level": "country",
            "benchmark_condition": "cty_code==5700 is the China partner filter used in the corrected validation universe",
            "interpretation": "Partner-country key used to isolate China import rows in the corrected validator.",
            "patterns": ["cty_code==5700", "cty_code > 0"],
        },
        {
            "variable": "cty_name",
            "label": "Partner country name",
            "inferred_level": "country",
            "benchmark_condition": "cty_name==\"CHINA\" is the paper's China-specific partner filter",
            "interpretation": "Country label used in the published figures and tables to isolate China rows.",
            "patterns": ["cty_name==\"CHINA\"", "cty_name==strupper(\"china\")"],
        },
    ]
    for spec in specs:
        source_line = None
        source_file = None
        evidence_text = None
        for source in sources:
            line_no, line_text = _find_first_evidence_line(source, spec["patterns"])
            if line_no is not None:
                source_file = str(source.relative_to(root))
                source_line = int(line_no)
                evidence_text = line_text
                break
        rows.append(
            {
                "variable": spec["variable"],
                "label": spec["label"],
                "inferred_level": spec["inferred_level"],
                "benchmark_condition": spec["benchmark_condition"],
                "source_file": source_file,
                "source_line": source_line,
                "evidence_text": evidence_text,
                "interpretation": spec["interpretation"],
            }
        )
    return pd.DataFrame(rows)


def build_china_301_variable_semantics_from_artifacts(config: PipelineConfig) -> dict[str, Any]:
    """Write the machine-readable semantics table used by the corrected China validator."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    table = _build_china_301_variable_semantics_table(config)
    path = output_dir / "raw_replication_china_301_variable_semantics.csv"
    table.to_csv(path, index=False)
    return {
        "trace_path": str(path),
        "rows": int(len(table)),
        "variables": table["variable"].astype("string").tolist() if not table.empty else [],
    }


def _build_china_301_reference_semantics_frame(reference: pd.DataFrame) -> pd.DataFrame:
    """Derive China validation semantics from the benchmark panel."""
    frame = reference.copy()
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["m_status2"] = pd.to_numeric(frame["m_status2"], errors="coerce")
    frame["m_china_hit"] = pd.to_numeric(frame["m_china_hit"], errors="coerce")
    frame["m_ess"] = pd.to_numeric(frame["m_ess"], errors="coerce")
    frame["ref_active"] = frame["m_status2"].gt(0).fillna(False)
    frame["ref_target_product"] = frame.groupby(["cty_code", "hs10"], dropna=False)["m_status2"].transform(lambda s: s.gt(0).any())
    frame["ref_target_product"] = frame["ref_target_product"].fillna(False)
    frame["ref_partner_china"] = frame["cty_code"].eq(5700)
    frame["ref_country_scope"] = np.where(frame["ref_partner_china"], "china", "other")
    frame["ref_current_validation"] = frame["ref_partner_china"] & frame["m_china_hit"].eq(1)
    frame["ref_corrected_validation"] = frame["ref_partner_china"] & frame["m_china_hit"].eq(1) & frame["ref_target_product"]
    return frame


def _classify_china_301_universe_stage(row: pd.Series) -> str:
    if int(row.get("cty_code") or 0) != 5700 and bool(row.get("ref_m_china_hit")):
        return "non_china_product_scope_only"
    if int(row.get("cty_code") or 0) != 5700 and bool(row.get("ref_active")):
        return "non_china_other_target"
    if not bool(row.get("ref_m_china_hit")):
        return "benchmark_target_inconsistent"
    if not bool(row.get("ref_active")):
        return "china_scope_inactive"
    if not bool(row.get("ref_target_product")):
        return "benchmark_target_inconsistent"
    if not bool(row.get("raw_key_present")):
        return "requires_full_model_review"
    if pd.notna(row.get("raw_panel_increment")):
        return "china_active_applied"
    return "raw_application_inconsistent"


def build_china_301_universe_trace_from_artifacts(config: PipelineConfig) -> dict[str, Any]:
    """Write the China 301 universe trace that separates product scope from target status."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    disc_path = output_dir / "raw_replication_discrepancies.parquet"
    if not disc_path.exists():
        raise FileNotFoundError(f"Missing raw replication discrepancies artifact: {disc_path}")

    columns = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_m_effective_mdate2",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_tw_increment_rate_raw",
        "raw_tw_rule_code_raw",
        "raw_tw_scope_source_raw",
        "raw_active",
        "discrepancy_type",
        "_merge",
    ]
    frame = read_table(disc_path, columns=columns)
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["ref_m_status2"] = pd.to_numeric(frame["ref_m_status2"], errors="coerce")
    frame["ref_m_china_hit"] = pd.to_numeric(frame["ref_m_china_hit"], errors="coerce")
    frame["ref_m_stattariff1"] = pd.to_numeric(frame["ref_m_stattariff1"], errors="coerce")
    frame["ref_m_stattariff2"] = pd.to_numeric(frame["ref_m_stattariff2"], errors="coerce")
    frame["raw_active"] = pd.to_numeric(frame.get("raw_tw_increment_rate_raw"), errors="coerce").notna()
    frame["ref_active"] = frame["ref_m_status2"].gt(0).fillna(False)
    frame["ref_effective_period"] = _effective_period(frame["ref_m_effective_mdate2"])
    frame["ref_target_product"] = frame.groupby(["cty_code", "hs10"], dropna=False)["ref_m_status2"].transform(lambda s: s.gt(0).any())
    frame["current_missing_raw_scope"] = frame["discrepancy_type"].eq("missing_raw_policy_scope")
    frame["corrected_missing_raw_scope"] = frame["current_missing_raw_scope"] & frame["cty_code"].eq(5700) & frame["ref_target_product"] & frame["ref_active"]

    overlay_lookup = _load_china_301_key_overlay_slice(config, frame.loc[:, ["hs8", "year", "month"]].drop_duplicates()) if "hs8" in frame.columns else None
    if overlay_lookup is None:
        overlay_lookup = pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"])
    else:
        overlay_lookup = overlay_lookup.drop_duplicates(["hs8", "year", "month"], keep="first").copy()

    trace = frame.copy()
    trace["raw_key_present"] = trace["_merge"].eq("both")
    trace["duplicate_reference_key_rows"] = trace.groupby(["cty_code", "hs10", "year", "month"], dropna=False)["hs10"].transform("size").astype("Int64")
    trace["duplicate_raw_key_rows"] = trace["duplicate_reference_key_rows"]
    trace["raw_panel_increment"] = pd.to_numeric(trace["raw_tw_increment_rate_raw"], errors="coerce")
    trace["raw_panel_rule_code"] = trace["raw_tw_rule_code_raw"].astype("string")
    trace["raw_panel_policy_source"] = trace["raw_tw_scope_source_raw"].astype("string")
    trace["raw_panel_hs10_present"] = trace["raw_key_present"]
    trace["raw_panel_hs8_month_present"] = trace["raw_key_present"]
    trace = trace.merge(
        overlay_lookup.loc[:, ["hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw"]].rename(
            columns={"tw_increment_rate_raw": "overlay_increment", "tw_rule_code_raw": "overlay_rule_code"}
        ),
        on=["hs8", "year", "month"],
        how="left",
    )
    trace["overlay_hs8_month_present"] = trace["overlay_increment"].notna()
    trace["diagnosed_stage"] = trace.apply(_classify_china_301_universe_stage, axis=1)

    trace["current_missing_raw_scope"] = trace["current_missing_raw_scope"].fillna(False)
    trace["corrected_missing_raw_scope"] = trace["corrected_missing_raw_scope"].fillna(False)
    trace["ref_m_china_hit"] = pd.to_numeric(trace["ref_m_china_hit"], errors="coerce")
    trace["ref_m_status2"] = pd.to_numeric(trace["ref_m_status2"], errors="coerce")
    trace["ref_m_stattariff1"] = pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce")
    trace["ref_m_stattariff2"] = pd.to_numeric(trace["ref_m_stattariff2"], errors="coerce")

    trace = trace.loc[
        :,
        [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_target_product",
            "ref_active",
            "ref_m_status2",
            "ref_m_china_hit",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "ref_effective_period",
            "current_missing_raw_scope",
            "corrected_missing_raw_scope",
            "raw_key_present",
            "raw_panel_hs10_present",
            "raw_panel_hs8_month_present",
            "raw_panel_increment",
            "raw_panel_rule_code",
            "raw_panel_policy_source",
            "overlay_hs8_month_present",
            "overlay_increment",
            "overlay_rule_code",
            "discrepancy_type",
            "duplicate_reference_key_rows",
            "duplicate_raw_key_rows",
            "diagnosed_stage",
        ],
    ].copy()
    trace = trace.sort_values(["corrected_missing_raw_scope", "current_missing_raw_scope", "cty_code", "hs10", "year", "month"], ascending=[False, False, True, True, True, True]).reset_index(drop=True)

    trace_path = output_dir / "raw_replication_china_301_universe_trace.csv"
    # Canonical row-level artifact; retain the legacy CSV for backward-
    # compatible diagnostics and existing downstream readers.
    write_parquet(trace, trace_path.with_suffix(".parquet"), overwrite=True)
    trace.to_csv(trace_path, index=False)

    by_country = (
        trace.groupby(["cty_code", "diagnosed_stage"], dropna=False, observed=True)
        .agg(rows=("hs10", "size"), current_missing_raw_scope_rows=("current_missing_raw_scope", "sum"), corrected_missing_raw_scope_rows=("corrected_missing_raw_scope", "sum"))
        .reset_index()
    )
    by_month = (
        trace.groupby(["year", "month", "diagnosed_stage"], dropna=False, observed=True)
        .agg(rows=("hs10", "size"), current_missing_raw_scope_rows=("current_missing_raw_scope", "sum"), corrected_missing_raw_scope_rows=("corrected_missing_raw_scope", "sum"))
        .reset_index()
    )
    by_status = (
        trace.groupby(["ref_target_product", "ref_active", "diagnosed_stage"], dropna=False, observed=True)
        .agg(rows=("hs10", "size"), current_missing_raw_scope_rows=("current_missing_raw_scope", "sum"), corrected_missing_raw_scope_rows=("corrected_missing_raw_scope", "sum"))
        .reset_index()
    )
    by_semantics = (
        trace.groupby(["diagnosed_stage"], dropna=False, observed=True)
        .agg(rows=("hs10", "size"), current_missing_raw_scope_rows=("current_missing_raw_scope", "sum"), corrected_missing_raw_scope_rows=("corrected_missing_raw_scope", "sum"))
        .reset_index()
        .sort_values(["rows", "diagnosed_stage"], ascending=[False, True])
    )
    by_country.to_csv(output_dir / "raw_replication_china_301_universe_by_country.csv", index=False)
    by_month.to_csv(output_dir / "raw_replication_china_301_universe_by_month.csv", index=False)
    by_status.to_csv(output_dir / "raw_replication_china_301_universe_by_status.csv", index=False)
    by_semantics.to_csv(output_dir / "raw_replication_china_301_universe_by_semantics.csv", index=False)

    return {
        "trace_path": str(trace_path),
        "by_country_path": str(output_dir / "raw_replication_china_301_universe_by_country.csv"),
        "by_month_path": str(output_dir / "raw_replication_china_301_universe_by_month.csv"),
        "by_status_path": str(output_dir / "raw_replication_china_301_universe_by_status.csv"),
        "by_semantics_path": str(output_dir / "raw_replication_china_301_universe_by_semantics.csv"),
        "rows": int(len(trace)),
        "stage_counts": trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict(),
    }


def _build_china_301_metric_denominators(cells: pd.DataFrame) -> pd.DataFrame:
    """Summarize the denominator choices used by the China-current validator."""
    if cells.empty:
        return pd.DataFrame(columns=["metric", "kind", "value", "notes"])

    matched = cells.loc[cells["_merge"].eq("both")].copy()
    active_reference = cells.loc[cells["ref_active"]].copy()
    active_matched = matched.loc[matched["ref_active"] | matched["raw_active"]].copy()
    active_non_sentinel = active_matched.loc[~active_matched["is_non_ad_valorem_or_sentinel"]].copy()
    selected_reference_rows = int((cells["_merge"].ne("right_only")).sum())
    selected_raw_rows = int((cells["_merge"].ne("left_only")).sum())

    rows = [
        {"metric": "reference_rows", "kind": "count", "value": selected_reference_rows, "notes": "Selected China-partner benchmark rows"},
        {"metric": "raw_rows", "kind": "count", "value": selected_raw_rows, "notes": "Selected China raw rows"},
        {"metric": "exact_key_matched_rows", "kind": "count", "value": int(len(matched)), "notes": "Outer-merge matched keys"},
        {"metric": "benchmark_active_rows", "kind": "count", "value": int(len(active_reference)), "notes": "Benchmark rows with m_status2 > 0"},
        {"metric": "benchmark_active_matched_rows", "kind": "count", "value": int(active_reference["_merge"].eq("both").sum()), "notes": "Matched active benchmark rows"},
        {"metric": "either_active_matched_rows", "kind": "count", "value": int(len(active_matched)), "notes": "Matched rows active on either side"},
        {"metric": "raw_active_rows", "kind": "count", "value": int(pd.to_numeric(cells["raw_active"], errors="coerce").fillna(False).sum()), "notes": "Raw rows with a tariff increment"},
        {"metric": "raw_active_matched_rows", "kind": "count", "value": int(pd.to_numeric(active_matched["raw_active"], errors="coerce").fillna(False).sum()), "notes": "Matched rows with a tariff increment"},
        {"metric": "paper_key_coverage_rate", "kind": "rate", "value": float(len(matched) / max(selected_reference_rows, 1)), "notes": "Matched keys / selected benchmark keys"},
        {"metric": "tariff_active_key_coverage_rate", "kind": "rate", "value": float(active_reference["_merge"].eq("both").mean()) if len(active_reference) else 1.0, "notes": "Matched active benchmark keys / active benchmark keys"},
        {"metric": "tariff_active_treatment_match_rate", "kind": "rate", "value": float((active_matched["ref_treated"] == active_matched["raw_treated"]).mean()) if len(active_matched) else 1.0, "notes": "Treatment match on matched active rows"},
        {"metric": "tariff_active_statutory_rate_match_rate", "kind": "rate", "value": float(active_non_sentinel["rate1_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0, "notes": "Statutory rate match on matched active non-sentinel rows"},
        {"metric": "tariff_active_day_weighted_rate_match_rate", "kind": "rate", "value": float(active_non_sentinel["rate2_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0, "notes": "Day-weighted rate match on matched active non-sentinel rows"},
        {"metric": "missing_raw_key_rows", "kind": "count", "value": int((cells["discrepancy_type"].eq("missing_raw_key")).sum()), "notes": "Reference rows with no raw key"},
        {"metric": "missing_reference_key_rows", "kind": "count", "value": int((cells["discrepancy_type"].eq("missing_reference_key")).sum()), "notes": "Raw-only rows"},
        {"metric": "missing_raw_policy_scope_rows", "kind": "count", "value": int((cells["discrepancy_type"].eq("missing_raw_policy_scope")).sum()), "notes": "Reference active rows without raw policy scope"},
        {"metric": "statutory_rate_mismatch_rows", "kind": "count", "value": int((cells["discrepancy_type"].eq("statutory_rate_mismatch")).sum()), "notes": "Statutory-rate mismatches"},
        {"metric": "day_weighted_rate_mismatch_rows", "kind": "count", "value": int((cells["discrepancy_type"].eq("day_weighted_rate_mismatch")).sum()), "notes": "Day-weighted-rate mismatches"},
        {"metric": "trade_value_mismatch_rows", "kind": "count", "value": int((cells["discrepancy_type"].eq("trade_value_mismatch")).sum()), "notes": "Trade-value mismatches"},
    ]
    return pd.DataFrame(rows)


def _build_china_301_rate_difference_quantiles(cells: pd.DataFrame) -> pd.DataFrame:
    """Quantify the size of statutory/day-weighted gaps in percentage points."""
    if cells.empty:
        return pd.DataFrame(columns=["metric", "quantile", "value_pp", "rows"])

    matched = cells.loc[cells["_merge"].eq("both")].copy()
    active_matched = matched.loc[matched["ref_active"] | matched["raw_active"]].copy()
    active_non_sentinel = active_matched.loc[~active_matched["is_non_ad_valorem_or_sentinel"]].copy()
    if active_non_sentinel.empty:
        return pd.DataFrame(columns=["metric", "quantile", "value_pp", "rows"])

    quantiles = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]
    rows: list[dict[str, Any]] = []
    for metric, source in [
        ("statutory_rate_abs_diff_pp", "rate1_abs_diff"),
        ("day_weighted_rate_abs_diff_pp", "rate2_abs_diff"),
    ]:
        series = pd.to_numeric(active_non_sentinel[source], errors="coerce").dropna() * 100.0
        if series.empty:
            continue
        for quantile in quantiles:
            rows.append({"metric": metric, "quantile": quantile, "value_pp": float(series.quantile(quantile)), "rows": int(len(series))})
    return pd.DataFrame(rows)


def _classify_china_301_rate_provenance_stage(row: pd.Series) -> str:
    def _numeric(value: object) -> float:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return float(numeric) if pd.notna(numeric) else 0.0

    def _text(value: object) -> str:
        return "" if pd.isna(value) else str(value)

    raw_formula_stat_gap = abs(_numeric(row.get("raw_formula_statutory_gap")))
    raw_formula_day_gap = abs(_numeric(row.get("raw_formula_day_weighted_gap")))
    ref_stat_gap = abs(_numeric(row.get("ref_rate_gap_statutory")))
    ref_day_gap = abs(_numeric(row.get("ref_rate_gap_day_weighted")))
    timing_stage = _text(row.get("timing_diagnosed_stage"))
    candidate_timing = _text(row.get("closest_candidate_timing"))

    if raw_formula_stat_gap > RATE_TOL or raw_formula_day_gap > RATE_TOL:
        return "raw_formula_bug"
    if _text(row.get("discrepancy_type")) == "day_weighted_rate_mismatch":
        if timing_stage.startswith("benchmark_uses_") or candidate_timing not in {"", "unmapped"}:
            return "benchmark_timing_mismatch"
        if ref_day_gap > RATE_TOL:
            return "benchmark_timing_mismatch"
        return "requires_full_model_review"
    if _text(row.get("discrepancy_type")) == "statutory_rate_mismatch":
        if ref_stat_gap <= 0.01:
            return "benchmark_source_precision_diff"
        return "benchmark_statutory_definition_mismatch"
    return "requires_full_model_review"


def _build_china_301_rate_provenance(rate_trace: pd.DataFrame, timing_trace: pd.DataFrame | None = None) -> pd.DataFrame:
    """Attach a narrow diagnosis to the remaining China 301 rate mismatches."""
    if rate_trace.empty:
        return pd.DataFrame(
            columns=[
                "cty_code",
                "hs10",
                "hs8",
                "year",
                "month",
                "discrepancy_type",
                "ref_effective_period",
                "raw_base_statutory_rate_raw",
                "raw_panel_increment",
                "raw_tw_active_share_raw",
                "raw_m_statutory_tariff1",
                "raw_m_statutory_tariff2",
                "ref_m_stattariff1",
                "ref_m_stattariff2",
                "raw_formula_statutory_gap",
                "raw_formula_day_weighted_gap",
                "ref_rate_gap_statutory",
                "ref_rate_gap_day_weighted",
                "benchmark_implied_increment",
                "benchmark_implied_active_share",
                "timing_diagnosed_stage",
                "closest_candidate_timing",
                "closest_candidate_abs_gap",
                "rate_provenance_stage",
            ]
        )

    frame = rate_trace.loc[
        rate_trace["discrepancy_type"].isin(["statutory_rate_mismatch", "day_weighted_rate_mismatch"])
    ].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "cty_code",
                "hs10",
                "hs8",
                "year",
                "month",
                "discrepancy_type",
                "ref_effective_period",
                "raw_base_statutory_rate_raw",
                "raw_panel_increment",
                "raw_tw_active_share_raw",
                "raw_m_statutory_tariff1",
                "raw_m_statutory_tariff2",
                "ref_m_stattariff1",
                "ref_m_stattariff2",
                "raw_formula_statutory_gap",
                "raw_formula_day_weighted_gap",
                "ref_rate_gap_statutory",
                "ref_rate_gap_day_weighted",
                "benchmark_implied_increment",
                "benchmark_implied_active_share",
                "timing_diagnosed_stage",
                "closest_candidate_timing",
                "closest_candidate_abs_gap",
                "rate_provenance_stage",
            ]
        )

    if timing_trace is not None and not timing_trace.empty:
        timing_cols = [
            "cty_code",
            "hs10",
            "year",
            "month",
            "discrepancy_type",
            "diagnosed_stage",
            "closest_candidate_timing",
            "closest_candidate_abs_gap",
        ]
        timing_cols = [column for column in timing_cols if column in timing_trace.columns]
        timing_frame = timing_trace.loc[:, timing_cols].copy()
        rename_map = {}
        if "diagnosed_stage" in timing_frame.columns:
            rename_map["diagnosed_stage"] = "timing_diagnosed_stage"
        timing_frame = timing_frame.rename(columns=rename_map)
        frame = frame.merge(timing_frame, on=[column for column in ["cty_code", "hs10", "year", "month", "discrepancy_type"] if column in timing_frame.columns and column in frame.columns], how="left")
    else:
        frame["timing_diagnosed_stage"] = pd.NA
        frame["closest_candidate_timing"] = pd.NA
        frame["closest_candidate_abs_gap"] = pd.NA

    frame["rate_provenance_stage"] = frame.apply(_classify_china_301_rate_provenance_stage, axis=1)
    frame["raw_formula_statutory_gap_pp"] = pd.to_numeric(frame["raw_formula_statutory_gap"], errors="coerce").abs() * 100.0
    frame["raw_formula_day_weighted_gap_pp"] = pd.to_numeric(frame["raw_formula_day_weighted_gap"], errors="coerce").abs() * 100.0
    frame["ref_rate_gap_statutory_pp"] = pd.to_numeric(frame["ref_rate_gap_statutory"], errors="coerce").abs() * 100.0
    frame["ref_rate_gap_day_weighted_pp"] = pd.to_numeric(frame["ref_rate_gap_day_weighted"], errors="coerce").abs() * 100.0

    columns = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "discrepancy_type",
        "ref_effective_period",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "raw_formula_statutory_gap_pp",
        "raw_formula_day_weighted_gap_pp",
        "ref_rate_gap_statutory_pp",
        "ref_rate_gap_day_weighted_pp",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "timing_diagnosed_stage",
        "closest_candidate_timing",
        "closest_candidate_abs_gap",
        "rate_provenance_stage",
    ]
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame = frame.loc[:, columns].sort_values(
        ["rate_provenance_stage", "discrepancy_type", "year", "month", "hs10"],
        ascending=[True, True, True, True, True],
    ).reset_index(drop=True)
    return frame


def _build_china_301_rate_mismatch_decomposition(provenance: pd.DataFrame) -> pd.DataFrame:
    """Summarize the remaining rate mismatches by provenance stage and common rate tuple."""
    if provenance.empty:
        return pd.DataFrame(
            columns=[
                "rate_provenance_stage",
                "discrepancy_type",
                "timing_diagnosed_stage",
                "raw_base_statutory_rate_raw",
                "raw_panel_increment",
                "raw_tw_active_share_raw",
                "raw_m_statutory_tariff1",
                "raw_m_statutory_tariff2",
                "ref_m_stattariff1",
                "ref_m_stattariff2",
                "rows",
                "median_raw_formula_statutory_gap_pp",
                "median_raw_formula_day_weighted_gap_pp",
                "median_ref_rate_gap_statutory_pp",
                "median_ref_rate_gap_day_weighted_pp",
            ]
        )

    group_cols = [
        "rate_provenance_stage",
        "discrepancy_type",
        "timing_diagnosed_stage",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
    ]
    for column in group_cols:
        if column not in provenance.columns:
            provenance[column] = pd.NA

    grouped = (
        provenance.groupby(group_cols, dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            median_raw_formula_statutory_gap_pp=("raw_formula_statutory_gap_pp", "median"),
            median_raw_formula_day_weighted_gap_pp=("raw_formula_day_weighted_gap_pp", "median"),
            median_ref_rate_gap_statutory_pp=("ref_rate_gap_statutory_pp", "median"),
            median_ref_rate_gap_day_weighted_pp=("ref_rate_gap_day_weighted_pp", "median"),
        )
        .reset_index()
        .sort_values(
            ["rows", "rate_provenance_stage", "discrepancy_type", "raw_base_statutory_rate_raw"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )
    return grouped


def _load_china_301_rule_attributes(config: PipelineConfig) -> pd.DataFrame | None:
    path = config.reference_dir / "tradewar_rule_attributes.parquet"
    if not path.exists():
        return None

    columns = ["rule_code", "year", "month", "increment_rate"]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(path).replace("'", "''")
            query = f"""
                SELECT
                    CAST(rule_code AS VARCHAR) AS rule_code,
                    CAST(year AS BIGINT) AS year,
                    CAST(month AS BIGINT) AS month,
                    increment_rate
                FROM read_parquet('{escaped_path}')
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
    else:
        frame = read_table(path, columns=columns)

    if frame.empty:
        return frame

    frame["rule_code"] = _normalize_rule_code(frame["rule_code"])
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["increment_rate"] = pd.to_numeric(frame["increment_rate"], errors="coerce")
    return frame.dropna(subset=["rule_code", "year", "month"]).copy()


def _classify_china_301_statutory_component_stage(row: pd.Series) -> str:
    def _numeric(value: object) -> float:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return float(numeric) if pd.notna(numeric) else float("nan")

    def _text(value: object) -> str:
        return "" if pd.isna(value) else str(value).strip()

    def _abs_gap_pp(value: object) -> float:
        numeric = _numeric(value)
        return abs(numeric) if pd.notna(numeric) else float("nan")

    raw_total = _numeric(row.get("raw_total_statutory_rate"))
    raw_base = _numeric(row.get("raw_base_statutory_rate_raw"))
    raw_increment = _numeric(row.get("raw_panel_increment"))
    overlay_increment = _numeric(row.get("overlay_increment"))
    rule_attribute_increment = _numeric(row.get("rule_attribute_increment"))
    ref_statutory_rate = _numeric(row.get("ref_statutory_rate"))
    benchmark_implied_increment = _numeric(row.get("benchmark_implied_increment"))
    raw_vs_reference_gap_pp = _abs_gap_pp(row.get("raw_vs_reference_gap_pp"))
    raw_vs_overlay_gap_pp = _abs_gap_pp(row.get("raw_vs_overlay_increment_gap_pp"))
    raw_vs_rule_gap_pp = _abs_gap_pp(row.get("raw_vs_rule_attribute_increment_gap_pp"))
    benchmark_vs_rule_gap_pp = _abs_gap_pp(row.get("benchmark_implied_vs_rule_attribute_gap_pp"))
    duplicate_rule_attribute_rows = int(pd.to_numeric(pd.Series([row.get("duplicate_rule_attribute_rows")]), errors="coerce").fillna(0).iloc[0])

    raw_rule_code = _text(row.get("raw_rule_code"))
    overlay_rule_code = _text(row.get("overlay_rule_code"))
    normalized_raw_rule_code = _text(normalize_hs_code(raw_rule_code, 8)) if raw_rule_code else ""
    normalized_overlay_rule_code = _text(normalize_hs_code(overlay_rule_code, 8)) if overlay_rule_code else ""

    if pd.notna(raw_total) and raw_total >= 9999:
        return "non_ad_valorem_or_sentinel"
    if pd.notna(raw_base) and raw_base >= 9999:
        return "non_ad_valorem_or_sentinel"
    if not raw_rule_code or not overlay_rule_code or normalized_raw_rule_code != normalized_overlay_rule_code:
        return "rule_code_missing_or_ambiguous"
    if duplicate_rule_attribute_rows > 1:
        return "duplicate_rule_attribute"
    if pd.isna(rule_attribute_increment):
        return "missing_rule_attribute"
    if pd.isna(raw_increment) or pd.isna(overlay_increment):
        return "panel_overlay_increment_mismatch"

    raw_overlay_gap_pp = abs(raw_increment - overlay_increment) * 100.0
    overlay_rule_gap_pp = abs(overlay_increment - rule_attribute_increment) * 100.0
    if raw_overlay_gap_pp > 0.01 and overlay_rule_gap_pp <= 0.01:
        return "panel_overlay_increment_mismatch"
    if overlay_rule_gap_pp > 0.01:
        return "overlay_rule_attribute_mismatch"
    if raw_vs_rule_gap_pp > 0.01:
        return "benchmark_increment_definition_difference"
    if raw_vs_reference_gap_pp <= 1.0:
        return "base_rate_precision_difference"
    if benchmark_vs_rule_gap_pp <= 1.0:
        return "exact_raw_components_match"
    if pd.notna(ref_statutory_rate) and pd.notna(raw_total):
        return "benchmark_increment_definition_difference"
    return "requires_full_model_review"


def _build_china_301_statutory_component_summary(trace: pd.DataFrame, top_n: int = 200) -> pd.DataFrame:
    if trace.empty:
        return pd.DataFrame(
            columns=[
                "diagnosed_component",
                "discrepancy_type",
                "raw_rule_code",
                "overlay_rule_code",
                "raw_policy_source",
                "rule_attribute_increment",
                "rows",
                "median_raw_vs_reference_gap_pp",
                "median_raw_vs_overlay_increment_gap_pp",
                "median_raw_vs_rule_attribute_increment_gap_pp",
                "median_benchmark_implied_vs_rule_attribute_gap_pp",
            ]
        )

    group_cols = [
        "diagnosed_component",
        "discrepancy_type",
        "raw_rule_code",
        "overlay_rule_code",
        "raw_policy_source",
        "rule_attribute_increment",
    ]
    grouped = (
        trace.groupby(group_cols, dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            median_raw_vs_reference_gap_pp=("raw_vs_reference_gap_pp", "median"),
            median_raw_vs_overlay_increment_gap_pp=("raw_vs_overlay_increment_gap_pp", "median"),
            median_raw_vs_rule_attribute_increment_gap_pp=("raw_vs_rule_attribute_increment_gap_pp", "median"),
            median_benchmark_implied_vs_rule_attribute_gap_pp=("benchmark_implied_vs_rule_attribute_gap_pp", "median"),
        )
        .reset_index()
        .sort_values(
            ["rows", "diagnosed_component", "discrepancy_type", "raw_rule_code", "raw_policy_source"],
            ascending=[False, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    return grouped.head(top_n) if top_n is not None else grouped


def _build_china_301_statutory_component_clusters(trace: pd.DataFrame, top_n: int = 100) -> pd.DataFrame:
    if trace.empty:
        return pd.DataFrame(
            columns=[
                "diagnosed_component",
                "discrepancy_type",
                "raw_rule_code",
                "overlay_rule_code",
                "raw_policy_source",
                "year",
                "month",
                "rows",
                "median_raw_vs_reference_gap_pp",
                "median_raw_vs_overlay_increment_gap_pp",
                "median_raw_vs_rule_attribute_increment_gap_pp",
                "median_benchmark_implied_vs_rule_attribute_gap_pp",
            ]
        )

    grouped = (
        trace.groupby(
            [
                "diagnosed_component",
                "discrepancy_type",
                "raw_rule_code",
                "overlay_rule_code",
                "raw_policy_source",
                "year",
                "month",
            ],
            dropna=False,
            observed=True,
        )
        .agg(
            rows=("hs10", "size"),
            median_raw_vs_reference_gap_pp=("raw_vs_reference_gap_pp", "median"),
            median_raw_vs_overlay_increment_gap_pp=("raw_vs_overlay_increment_gap_pp", "median"),
            median_raw_vs_rule_attribute_increment_gap_pp=("raw_vs_rule_attribute_increment_gap_pp", "median"),
            median_benchmark_implied_vs_rule_attribute_gap_pp=("benchmark_implied_vs_rule_attribute_gap_pp", "median"),
        )
        .reset_index()
        .sort_values(
            ["rows", "diagnosed_component", "year", "month", "raw_rule_code"],
            ascending=[False, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    return grouped.head(top_n) if top_n is not None else grouped


def build_china_301_statutory_component_trace_from_artifacts(
    config: PipelineConfig,
    top_n: int = 200,
    artifact_suffix: str = "",
) -> dict[str, Any]:
    """Decompose the remaining China 301 statutory mismatches into component causes."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    def _read_csv_columns(path: Path, columns: list[str]) -> pd.DataFrame:
        frame = pd.read_csv(path)
        selected = [column for column in columns if column in frame.columns]
        return frame.loc[:, selected].copy()

    provenance_path = output_dir / _artifact_name("raw_replication_china_301_rate_provenance", artifact_suffix)
    rate_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_trace", artifact_suffix)
    if not provenance_path.exists() or not rate_trace_path.exists():
        trace_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_trace", artifact_suffix)
        empty = pd.DataFrame(
            columns=[
                "cty_code",
                "hs10",
                "hs8",
                "year",
                "month",
                "ref_effective_period",
                "discrepancy_type",
                "raw_rule_code",
                "overlay_rule_code",
                "raw_policy_source",
                "raw_base_statutory_rate_raw",
                "raw_panel_increment",
                "overlay_increment",
                "rule_attribute_increment",
                "raw_total_statutory_rate",
                "ref_statutory_rate",
                "benchmark_implied_increment",
                "raw_vs_reference_gap_pp",
                "raw_vs_overlay_increment_gap_pp",
                "raw_vs_rule_attribute_increment_gap_pp",
                "benchmark_implied_vs_rule_attribute_gap_pp",
                "duplicate_rule_attribute_rows",
                "diagnosed_component",
            ]
        )
        empty.to_csv(trace_path, index=False)
        summary = _build_china_301_statutory_component_summary(empty, top_n=top_n)
        summary_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_summary", artifact_suffix)
        summary.to_csv(summary_path, index=False)
        clusters = _build_china_301_statutory_component_clusters(empty, top_n=top_n)
        clusters_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_top_clusters", artifact_suffix)
        clusters.to_csv(clusters_path, index=False)
        return {
            "trace_path": str(trace_path),
            "summary_path": str(summary_path),
            "clusters_path": str(clusters_path),
            "rows": 0,
            "stage_counts": {},
        }

    provenance_columns = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "discrepancy_type",
        "ref_effective_period",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "raw_formula_statutory_gap_pp",
        "raw_formula_day_weighted_gap_pp",
        "ref_rate_gap_statutory_pp",
        "ref_rate_gap_day_weighted_pp",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "timing_diagnosed_stage",
        "closest_candidate_timing",
        "closest_candidate_abs_gap",
        "rate_provenance_stage",
    ]
    trace_columns = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_active",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_effective_period",
        "raw_key_present",
        "raw_panel_hs10_present",
        "raw_panel_hs8_month_present",
        "raw_panel_increment",
        "raw_panel_rule_code",
        "raw_panel_policy_source",
        "raw_rule_code",
        "raw_policy_source",
        "raw_mfn_ad_val_rate",
        "raw_base_pref_rate_raw",
        "raw_base_statutory_rate_raw",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_formula_statutory_rate",
        "raw_formula_day_weighted_rate",
        "raw_formula_statutory_gap",
        "raw_formula_day_weighted_gap",
        "ref_rate_gap_statutory",
        "ref_rate_gap_day_weighted",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "overlay_hs8_month_present",
        "overlay_increment",
        "overlay_rule_code",
        "discrepancy_type",
        "duplicate_reference_key_rows",
        "duplicate_raw_key_rows",
        "diagnosed_stage",
    ]
    provenance = _read_csv_columns(provenance_path, provenance_columns)
    provenance = provenance.loc[provenance["rate_provenance_stage"].eq("benchmark_statutory_definition_mismatch")].copy()
    if provenance.empty:
        trace_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_trace", artifact_suffix)
        empty = pd.DataFrame(columns=[
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_effective_period",
            "discrepancy_type",
            "raw_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "raw_base_statutory_rate_raw",
            "raw_panel_increment",
            "overlay_increment",
            "rule_attribute_increment",
            "raw_total_statutory_rate",
            "ref_statutory_rate",
            "benchmark_implied_increment",
            "raw_vs_reference_gap_pp",
            "raw_vs_overlay_increment_gap_pp",
            "raw_vs_rule_attribute_increment_gap_pp",
            "benchmark_implied_vs_rule_attribute_gap_pp",
            "duplicate_rule_attribute_rows",
            "diagnosed_component",
        ])
        empty.to_csv(trace_path, index=False)
        summary = _build_china_301_statutory_component_summary(empty, top_n=top_n)
        summary_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_summary", artifact_suffix)
        summary.to_csv(summary_path, index=False)
        clusters = _build_china_301_statutory_component_clusters(empty, top_n=top_n)
        clusters_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_top_clusters", artifact_suffix)
        clusters.to_csv(clusters_path, index=False)
        return {
            "trace_path": str(trace_path),
            "summary_path": str(summary_path),
            "clusters_path": str(clusters_path),
            "rows": 0,
            "stage_counts": {},
        }

    rate_trace = _read_csv_columns(rate_trace_path, trace_columns)
    rate_trace = rate_trace.loc[rate_trace["discrepancy_type"].eq("statutory_rate_mismatch")].copy()
    if rate_trace.empty:
        trace_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_trace", artifact_suffix)
        empty = pd.DataFrame(columns=[
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_effective_period",
            "discrepancy_type",
            "raw_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "raw_base_statutory_rate_raw",
            "raw_panel_increment",
            "overlay_increment",
            "rule_attribute_increment",
            "raw_total_statutory_rate",
            "ref_statutory_rate",
            "benchmark_implied_increment",
            "raw_vs_reference_gap_pp",
            "raw_vs_overlay_increment_gap_pp",
            "raw_vs_rule_attribute_increment_gap_pp",
            "benchmark_implied_vs_rule_attribute_gap_pp",
            "duplicate_rule_attribute_rows",
            "diagnosed_component",
        ])
        empty.to_csv(trace_path, index=False)
        summary = _build_china_301_statutory_component_summary(empty, top_n=top_n)
        summary_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_summary", artifact_suffix)
        summary.to_csv(summary_path, index=False)
        clusters = _build_china_301_statutory_component_clusters(empty, top_n=top_n)
        clusters_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_top_clusters", artifact_suffix)
        clusters.to_csv(clusters_path, index=False)
        return {
            "trace_path": str(trace_path),
            "summary_path": str(summary_path),
            "clusters_path": str(clusters_path),
            "rows": 0,
            "stage_counts": {},
        }

    trace = provenance.merge(
        rate_trace.loc[
            :,
            [
                "cty_code",
                "hs10",
                "year",
                "month",
                "discrepancy_type",
                "overlay_rule_code",
                "raw_panel_rule_code",
                "raw_panel_policy_source",
                "overlay_increment",
            ],
        ].drop_duplicates(subset=["cty_code", "hs10", "year", "month", "discrepancy_type"]),
        on=["cty_code", "hs10", "year", "month", "discrepancy_type"],
        how="left",
    )

    rule_attrs = _load_china_301_rule_attributes(config)
    if rule_attrs is None or rule_attrs.empty:
        rule_lookup = pd.DataFrame(columns=["rule_code", "year", "month", "rule_attribute_increment", "duplicate_rule_attribute_rows"])
    else:
        relevant_rules = rule_attrs.loc[rule_attrs["rule_code"].astype("string").str.startswith("990388", na=False)].copy()
        rule_lookup = (
            relevant_rules.groupby(["rule_code", "year", "month"], dropna=False, observed=True)
            .agg(
                rule_attribute_increment=("increment_rate", lambda series: pd.to_numeric(series, errors="coerce").dropna().iloc[0] if pd.to_numeric(series, errors="coerce").dropna().size else pd.NA),
                duplicate_rule_attribute_rows=("increment_rate", "size"),
            )
            .reset_index()
        )

    if "raw_rule_code" not in trace.columns:
        trace["raw_rule_code"] = pd.NA
    if "raw_policy_source" not in trace.columns:
        trace["raw_policy_source"] = pd.NA
    if "raw_panel_rule_code" in trace.columns:
        trace["raw_rule_code"] = trace["raw_rule_code"].where(trace["raw_rule_code"].notna(), trace["raw_panel_rule_code"])
    if "raw_panel_policy_source" in trace.columns:
        trace["raw_policy_source"] = trace["raw_policy_source"].where(trace["raw_policy_source"].notna(), trace["raw_panel_policy_source"])
    trace["raw_rule_code"] = _normalize_rule_code(trace["raw_rule_code"])
    trace["overlay_rule_code"] = _normalize_rule_code(trace["overlay_rule_code"])
    trace["raw_policy_source"] = trace["raw_policy_source"].astype("string")
    trace["raw_total_statutory_rate"] = pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce")
    trace["ref_statutory_rate"] = pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce")
    trace["raw_vs_reference_gap_pp"] = (trace["ref_statutory_rate"] - trace["raw_total_statutory_rate"]).abs() * 100.0
    trace["raw_vs_overlay_increment_gap_pp"] = (pd.to_numeric(trace["raw_panel_increment"], errors="coerce") - pd.to_numeric(trace["overlay_increment"], errors="coerce")).abs() * 100.0

    if not rule_lookup.empty:
        trace = trace.merge(rule_lookup, left_on=["raw_rule_code", "year", "month"], right_on=["rule_code", "year", "month"], how="left")
        trace["duplicate_rule_attribute_rows"] = pd.to_numeric(trace["duplicate_rule_attribute_rows"], errors="coerce").fillna(0).astype("Int64")
        trace["rule_attribute_increment"] = pd.to_numeric(trace["rule_attribute_increment"], errors="coerce")
    else:
        trace["rule_attribute_increment"] = pd.NA
        trace["duplicate_rule_attribute_rows"] = pd.Series([0] * len(trace), index=trace.index, dtype="Int64")

    trace["raw_vs_rule_attribute_increment_gap_pp"] = (
        pd.to_numeric(trace["raw_panel_increment"], errors="coerce") - pd.to_numeric(trace["rule_attribute_increment"], errors="coerce")
    ).abs() * 100.0
    trace["benchmark_implied_vs_rule_attribute_gap_pp"] = (
        pd.to_numeric(trace["benchmark_implied_increment"], errors="coerce") - pd.to_numeric(trace["rule_attribute_increment"], errors="coerce")
    ).abs() * 100.0
    trace["diagnosed_component"] = trace.apply(_classify_china_301_statutory_component_stage, axis=1)

    trace = trace.loc[
        :,
        [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_effective_period",
            "discrepancy_type",
            "raw_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "raw_base_statutory_rate_raw",
            "raw_panel_increment",
            "overlay_increment",
            "rule_attribute_increment",
            "raw_total_statutory_rate",
            "ref_statutory_rate",
            "benchmark_implied_increment",
            "raw_vs_reference_gap_pp",
            "raw_vs_overlay_increment_gap_pp",
            "raw_vs_rule_attribute_increment_gap_pp",
            "benchmark_implied_vs_rule_attribute_gap_pp",
            "duplicate_rule_attribute_rows",
            "diagnosed_component",
        ],
    ].copy()
    trace["duplicate_rule_attribute_rows"] = pd.to_numeric(trace["duplicate_rule_attribute_rows"], errors="coerce").fillna(0).astype("Int64")
    trace = trace.sort_values(
        ["diagnosed_component", "discrepancy_type", "year", "month", "hs10"],
        ascending=[True, True, True, True, True],
    ).reset_index(drop=True)

    trace_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_trace", artifact_suffix)
    write_parquet(trace, trace_path.with_suffix(".parquet"), overwrite=True)
    trace.to_csv(trace_path, index=False)

    summary = _build_china_301_statutory_component_summary(trace, top_n=None)
    summary_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_summary", artifact_suffix)
    summary.to_csv(summary_path, index=False)

    clusters = _build_china_301_statutory_component_clusters(trace, top_n=100)
    clusters_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_top_clusters", artifact_suffix)
    clusters.to_csv(clusters_path, index=False)

    stage_counts = trace["diagnosed_component"].value_counts(dropna=False).sort_index().to_dict()
    return {
        "trace_path": str(trace_path),
        "summary_path": str(summary_path),
        "clusters_path": str(clusters_path),
        "rows": int(len(trace)),
        "stage_counts": stage_counts,
    }


def _build_china_301_raw_only_keys(cells: pd.DataFrame) -> pd.DataFrame:
    """Summarize raw-only China benchmark keys that never matched the reference."""
    if cells.empty:
        return pd.DataFrame(columns=["cty_code", "hs10", "hs8", "year", "month", "rows", "raw_active_rows", "raw_treated_rows", "discrepancy_type"])

    raw_only = cells.loc[cells["_merge"].eq("right_only")].copy()
    if raw_only.empty:
        return pd.DataFrame(columns=["cty_code", "hs10", "hs8", "year", "month", "rows", "raw_active_rows", "raw_treated_rows", "discrepancy_type"])

    raw_only["hs8"] = _normalize_hs8(raw_only["hs10"])
    grouped = (
        raw_only.groupby(["cty_code", "hs10", "hs8", "year", "month"], dropna=False, observed=True)
        .agg(rows=("hs10", "size"), raw_active_rows=("raw_active", "sum"), raw_treated_rows=("raw_treated", "sum"))
        .reset_index()
        .sort_values(["rows", "year", "month", "hs10"], ascending=[False, True, True, True])
    )
    grouped["discrepancy_type"] = "missing_reference_key"
    return grouped


def _build_china_301_residual_current(cells: pd.DataFrame) -> pd.DataFrame:
    """Persist the exact-key residual rows after the universe fix."""
    columns = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_active",
        "raw_active",
        "discrepancy_type",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_panel_increment",
        "trade_value_abs_diff",
        "rate1_abs_diff",
        "rate2_abs_diff",
    ]
    if cells.empty:
        return pd.DataFrame(columns=columns)

    residual = cells.loc[cells["_merge"].eq("both") & cells["discrepancy_type"].ne("match")].copy()
    if residual.empty:
        return pd.DataFrame(columns=columns)

    residual["hs8"] = _normalize_hs8(residual["hs10"])
    for column in columns:
        if column not in residual.columns:
            residual[column] = pd.NA
    return residual.loc[:, columns].sort_values(["discrepancy_type", "year", "month", "hs10"]).reset_index(drop=True)


def build_china_301_rate_trace_from_artifacts(config: PipelineConfig, artifact_suffix: str = "") -> dict[str, Any]:
    """Build an exact-key China 301 rate trace from the current validation artifacts."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    disc_path = output_dir / _artifact_name("raw_replication_discrepancies", artifact_suffix, ".parquet")
    if not disc_path.exists():
        raise FileNotFoundError(f"Missing raw replication discrepancies artifact: {disc_path}")

    columns = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_m_effective_mdate2",
        "discrepancy_type",
    ]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(disc_path).replace("'", "''")
            query = f"""
                SELECT
                    CAST(cty_code AS BIGINT) AS cty_code,
                    CAST(hs10 AS VARCHAR) AS hs10,
                    CAST(year AS BIGINT) AS year,
                    CAST(month AS BIGINT) AS month,
                    CAST(ref_m_status2 AS DOUBLE) AS ref_m_status2,
                    CAST(ref_m_china_hit AS DOUBLE) AS ref_m_china_hit,
                    CAST(ref_m_stattariff1 AS DOUBLE) AS ref_m_stattariff1,
                    CAST(ref_m_stattariff2 AS DOUBLE) AS ref_m_stattariff2,
                    CAST(ref_m_effective_mdate2 AS VARCHAR) AS ref_m_effective_mdate2,
                    CAST(discrepancy_type AS VARCHAR) AS discrepancy_type
                FROM read_parquet('{escaped_path}')
                WHERE CAST(cty_code AS BIGINT) = 5700
                  AND CAST(ref_m_china_hit AS DOUBLE) = 1
                  AND CAST(ref_m_status2 AS DOUBLE) > 0
                  AND CAST(discrepancy_type AS VARCHAR) IN (
                      'missing_raw_policy_scope',
                      'missing_raw_key',
                      'statutory_rate_mismatch',
                      'day_weighted_rate_mismatch'
                  )
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
    else:
        frame = read_table(disc_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["ref_m_status2"] = pd.to_numeric(frame["ref_m_status2"], errors="coerce")
        frame["ref_m_china_hit"] = pd.to_numeric(frame["ref_m_china_hit"], errors="coerce")
        frame = frame.loc[
            frame["cty_code"].eq(5700)
            & frame["ref_m_china_hit"].eq(1)
            & frame["ref_m_status2"].gt(0)
            & frame["discrepancy_type"].isin(
                [
                    "missing_raw_policy_scope",
                    "missing_raw_key",
                    "statutory_rate_mismatch",
                    "day_weighted_rate_mismatch",
                ]
            )
        ].copy()

    trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_trace", artifact_suffix)
    empty_cols = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_active",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_effective_period",
        "ref_month_active_share",
        "raw_key_present",
        "raw_panel_hs10_present",
        "raw_panel_hs8_month_present",
        "raw_panel_increment",
        "raw_panel_rule_code",
        "raw_panel_policy_source",
        "raw_mfn_ad_val_rate",
        "raw_base_pref_rate_raw",
        "raw_base_statutory_rate_raw",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_formula_statutory_rate",
        "raw_formula_day_weighted_rate",
        "raw_formula_statutory_gap",
        "raw_formula_day_weighted_gap",
        "ref_rate_gap_statutory",
        "ref_rate_gap_day_weighted",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "overlay_hs8_month_present",
        "overlay_increment",
        "overlay_rule_code",
        "discrepancy_type",
        "duplicate_reference_key_rows",
        "duplicate_raw_key_rows",
        "diagnosed_stage",
    ]
    if frame.empty:
        empty = pd.DataFrame(columns=empty_cols)
        empty.to_csv(trace_path, index=False)
        candidate_path = output_dir / "raw_replication_china_301_rule_assignment_candidates.csv"
        pd.DataFrame(columns=[
            "hs8",
            "candidate_rule_code",
            "policy_family",
            "release_name",
            "release_start_date",
            "source_file",
            "source_page",
            "source_row",
            "structural_note_identifier",
            "extraction_method",
            "matched_rule_text",
            "rule_found_in_same_row",
            "rule_found_only_in_context",
            "same_structural_note_block",
            "cross_family_candidate",
            "source_priority",
        ]).to_csv(candidate_path, index=False)
        return {"trace_path": str(trace_path), "candidate_path": str(candidate_path), "rows": 0, "stage_counts": {}, "top_buckets": []}

    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    frame["ref_active"] = frame["ref_m_status2"].gt(0).fillna(False)
    frame["ref_m_status2"] = pd.to_numeric(frame["ref_m_status2"], errors="coerce")
    frame["ref_m_china_hit"] = pd.to_numeric(frame["ref_m_china_hit"], errors="coerce")
    frame["ref_m_stattariff1"] = pd.to_numeric(frame["ref_m_stattariff1"], errors="coerce")
    frame["ref_m_stattariff2"] = pd.to_numeric(frame["ref_m_stattariff2"], errors="coerce")
    frame["ref_active"] = frame["ref_active"].fillna(frame["ref_m_status2"].gt(0)).astype(bool)
    frame["ref_effective_period"] = _effective_period(frame["ref_m_effective_mdate2"])
    frame["ref_month_active_share"] = pd.Series(
        [
            _month_active_share_from_effective_date(effective_date, year, month)
            for effective_date, year, month in zip(frame["ref_m_effective_mdate2"], frame["year"], frame["month"], strict=False)
        ],
        index=frame.index,
        dtype="object",
    )

    priority = {
        "missing_raw_policy_scope": 0,
        "missing_raw_key": 1,
        "statutory_rate_mismatch": 2,
        "day_weighted_rate_mismatch": 3,
    }
    grouped_rows: list[dict[str, Any]] = []
    for _, bucket in frame.groupby(["cty_code", "hs10", "year", "month"], dropna=False, observed=True):
        bucket = bucket.copy()
        discrepancy_counts = bucket["discrepancy_type"].value_counts(dropna=False)
        discrepancy_type = sorted(discrepancy_counts.index.tolist(), key=lambda value: priority.get(str(value), 99))[0]
        grouped_rows.append(
            {
                "cty_code": int(bucket["cty_code"].iloc[0]),
                "hs10": str(bucket["hs10"].iloc[0]),
                "hs8": str(bucket["hs8"].iloc[0]),
                "year": int(bucket["year"].iloc[0]),
                "month": int(bucket["month"].iloc[0]),
                "ref_active": bool(bucket["ref_active"].iloc[0]),
                "ref_m_status2": float(bucket["ref_m_status2"].iloc[0]) if pd.notna(bucket["ref_m_status2"].iloc[0]) else pd.NA,
                "ref_m_china_hit": float(bucket["ref_m_china_hit"].iloc[0]) if pd.notna(bucket["ref_m_china_hit"].iloc[0]) else pd.NA,
                "ref_m_stattariff1": float(bucket["ref_m_stattariff1"].iloc[0]) if pd.notna(bucket["ref_m_stattariff1"].iloc[0]) else pd.NA,
                "ref_m_stattariff2": float(bucket["ref_m_stattariff2"].iloc[0]) if pd.notna(bucket["ref_m_stattariff2"].iloc[0]) else pd.NA,
                "ref_effective_period": str(bucket["ref_effective_period"].iloc[0]),
                "ref_month_active_share": float(bucket["ref_month_active_share"].iloc[0]) if pd.notna(bucket["ref_month_active_share"].iloc[0]) else pd.NA,
                "discrepancy_type": str(discrepancy_type),
                "duplicate_reference_key_rows": int(len(bucket)),
                "duplicate_raw_key_rows": 0,
            }
        )

    trace = pd.DataFrame(grouped_rows)
    key_frame = trace.loc[:, ["cty_code", "hs10", "year", "month"]].drop_duplicates().copy()
    overlay_buckets = trace.loc[:, ["hs8", "year", "month"]].drop_duplicates().copy()

    rate_panel = _load_china_301_rate_panel_slice(config, key_frame)
    if rate_panel is None:
        rate_panel = pd.DataFrame(columns=["cty_code", "cty_name", "hs10", "year", "month", "mfn_ad_val_rate", "base_pref_rate_raw", "base_statutory_rate_raw", "tw_increment_rate_raw", "tw_active_share_raw", "m_statutory_tariff1", "m_statutory_tariff2", "m_policy_source", "tw_rule_code_raw", "tw_scope_source_raw", "hs8"])
    hs10_panel = _load_china_301_key_hs10_slice(config, key_frame)
    if hs10_panel is None:
        hs10_panel = pd.DataFrame(columns=["cty_code", "cty_name", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw", "hs8"])
    overlay = _load_china_301_key_overlay_slice(config, overlay_buckets)
    if overlay is None:
        overlay = pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"])
    hs8_panel = _load_china_301_panel_slice(config, trace.loc[:, ["hs8", "year", "month"]].drop_duplicates())
    if hs8_panel is None:
        hs8_panel = pd.DataFrame(columns=["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"])

    exact_counts = rate_panel.groupby(["cty_code", "hs10", "year", "month"], dropna=False, observed=True).size() if not rate_panel.empty else pd.Series(dtype="int64")
    hs10_counts = hs10_panel.groupby(["hs10", "year", "month"], dropna=False, observed=True).size() if not hs10_panel.empty else pd.Series(dtype="int64")
    hs8_counts = hs8_panel.groupby(["hs8", "year", "month"], dropna=False, observed=True).size() if not hs8_panel.empty else pd.Series(dtype="int64")
    overlay_counts = overlay.groupby(["hs8", "year", "month"], dropna=False, observed=True).size() if not overlay.empty else pd.Series(dtype="int64")

    exact_index = pd.MultiIndex.from_frame(trace.loc[:, ["cty_code", "hs10", "year", "month"]])
    hs10_index = pd.MultiIndex.from_frame(trace.loc[:, ["hs10", "year", "month"]])
    hs8_index = pd.MultiIndex.from_frame(trace.loc[:, ["hs8", "year", "month"]])
    trace["raw_key_present"] = exact_index.isin(exact_counts.index)
    trace["duplicate_raw_key_rows"] = exact_index.map(exact_counts).fillna(0).astype("Int64")
    trace["raw_panel_hs10_present"] = hs10_index.isin(hs10_counts.index)
    trace["raw_panel_hs8_month_present"] = hs8_index.isin(hs8_counts.index)
    trace["overlay_hs8_month_present"] = hs8_index.isin(overlay_counts.index)

    def _lookup(frame: pd.DataFrame, key_cols: list[str], value_col: str) -> pd.Series:
        if frame.empty or value_col not in frame.columns:
            return pd.Series([pd.NA] * len(trace), index=trace.index, dtype="object")
        mapping = frame.set_index(key_cols)[value_col].to_dict()
        return pd.Series(
            [mapping.get(tuple(values), pd.NA) for values in trace.loc[:, key_cols].itertuples(index=False, name=None)],
            index=trace.index,
        )

    trace["raw_mfn_ad_val_rate"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "mfn_ad_val_rate")
    trace["raw_base_pref_rate_raw"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "base_pref_rate_raw")
    trace["raw_base_statutory_rate_raw"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "base_statutory_rate_raw")
    trace["raw_panel_increment"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "tw_increment_rate_raw")
    trace["raw_tw_active_share_raw"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "tw_active_share_raw")
    trace["raw_m_statutory_tariff1"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "m_statutory_tariff1")
    trace["raw_m_statutory_tariff2"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "m_statutory_tariff2")
    trace["raw_panel_rule_code"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "tw_rule_code_raw")
    trace["raw_panel_policy_source"] = _lookup(rate_panel, ["cty_code", "hs10", "year", "month"], "tw_scope_source_raw")
    trace["overlay_increment"] = _lookup(overlay, ["hs8", "year", "month"], "tw_increment_rate_raw")
    trace["overlay_rule_code"] = _lookup(overlay, ["hs8", "year", "month"], "tw_rule_code_raw")
    trace["raw_formula_statutory_rate"] = pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce") + pd.to_numeric(trace["raw_panel_increment"], errors="coerce")
    trace["raw_formula_day_weighted_rate"] = pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce") + pd.to_numeric(trace["raw_panel_increment"], errors="coerce") * pd.to_numeric(trace["raw_tw_active_share_raw"], errors="coerce")
    trace["raw_formula_statutory_gap"] = pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce") - pd.to_numeric(trace["raw_formula_statutory_rate"], errors="coerce")
    trace["raw_formula_day_weighted_gap"] = pd.to_numeric(trace["raw_m_statutory_tariff2"], errors="coerce") - pd.to_numeric(trace["raw_formula_day_weighted_rate"], errors="coerce")
    trace["ref_rate_gap_statutory"] = pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce") - pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce")
    trace["ref_rate_gap_day_weighted"] = pd.to_numeric(trace["ref_m_stattariff2"], errors="coerce") - pd.to_numeric(trace["raw_m_statutory_tariff2"], errors="coerce")
    trace["benchmark_implied_increment"] = pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce") - pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce")
    denominator = pd.to_numeric(trace["raw_panel_increment"], errors="coerce")
    trace["benchmark_implied_active_share"] = np.where(
        denominator.abs().gt(RATE_TOL),
        (pd.to_numeric(trace["ref_m_stattariff2"], errors="coerce") - pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce")) / denominator,
        pd.NA,
    )

    trace["raw_key_present"] = trace["raw_key_present"].fillna(False)
    trace["raw_panel_hs10_present"] = trace["raw_panel_hs10_present"].fillna(False)
    trace["raw_panel_hs8_month_present"] = trace["raw_panel_hs8_month_present"].fillna(False)
    trace["overlay_hs8_month_present"] = trace["overlay_hs8_month_present"].fillna(False)
    trace["duplicate_raw_key_rows"] = pd.to_numeric(trace["duplicate_raw_key_rows"], errors="coerce").fillna(0).astype("Int64")
    trace["raw_panel_increment"] = pd.to_numeric(trace["raw_panel_increment"], errors="coerce")
    trace["raw_tw_active_share_raw"] = pd.to_numeric(trace["raw_tw_active_share_raw"], errors="coerce")
    trace["raw_mfn_ad_val_rate"] = pd.to_numeric(trace["raw_mfn_ad_val_rate"], errors="coerce")
    trace["raw_base_pref_rate_raw"] = pd.to_numeric(trace["raw_base_pref_rate_raw"], errors="coerce")
    trace["raw_base_statutory_rate_raw"] = pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce")
    trace["raw_m_statutory_tariff1"] = pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce")
    trace["raw_m_statutory_tariff2"] = pd.to_numeric(trace["raw_m_statutory_tariff2"], errors="coerce")
    trace["raw_formula_statutory_rate"] = pd.to_numeric(trace["raw_formula_statutory_rate"], errors="coerce")
    trace["raw_formula_day_weighted_rate"] = pd.to_numeric(trace["raw_formula_day_weighted_rate"], errors="coerce")
    trace["raw_formula_statutory_gap"] = pd.to_numeric(trace["raw_formula_statutory_gap"], errors="coerce")
    trace["raw_formula_day_weighted_gap"] = pd.to_numeric(trace["raw_formula_day_weighted_gap"], errors="coerce")
    trace["ref_rate_gap_statutory"] = pd.to_numeric(trace["ref_rate_gap_statutory"], errors="coerce")
    trace["ref_rate_gap_day_weighted"] = pd.to_numeric(trace["ref_rate_gap_day_weighted"], errors="coerce")
    trace["benchmark_implied_increment"] = pd.to_numeric(trace["benchmark_implied_increment"], errors="coerce")
    trace["benchmark_implied_active_share"] = pd.to_numeric(trace["benchmark_implied_active_share"], errors="coerce")
    trace["diagnosed_stage"] = trace.apply(_classify_china_301_rate_stage, axis=1)
    trace = trace.loc[
        :,
        [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_active",
            "ref_m_status2",
            "ref_m_china_hit",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "ref_effective_period",
            "ref_month_active_share",
            "raw_key_present",
            "raw_panel_hs10_present",
            "raw_panel_hs8_month_present",
            "raw_panel_increment",
            "raw_panel_rule_code",
            "raw_panel_policy_source",
            "raw_mfn_ad_val_rate",
            "raw_base_pref_rate_raw",
            "raw_base_statutory_rate_raw",
            "raw_tw_active_share_raw",
            "raw_m_statutory_tariff1",
            "raw_m_statutory_tariff2",
            "raw_formula_statutory_rate",
            "raw_formula_day_weighted_rate",
            "raw_formula_statutory_gap",
            "raw_formula_day_weighted_gap",
            "ref_rate_gap_statutory",
            "ref_rate_gap_day_weighted",
            "benchmark_implied_increment",
            "benchmark_implied_active_share",
            "overlay_hs8_month_present",
            "overlay_increment",
            "overlay_rule_code",
            "discrepancy_type",
            "duplicate_reference_key_rows",
            "duplicate_raw_key_rows",
            "diagnosed_stage",
        ],
    ].copy()
    trace = trace.fillna({"duplicate_reference_key_rows": 0, "duplicate_raw_key_rows": 0}).sort_values(
        ["discrepancy_type", "duplicate_reference_key_rows", "year", "month", "hs10"],
        ascending=[True, False, True, True, True],
    ).reset_index(drop=True)
    trace.to_csv(trace_path, index=False)
    stage_counts = trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict()
    top_buckets = trace.head(20).to_dict(orient="records")
    summary = {
        "trace_path": str(trace_path),
        "rows": int(len(trace)),
        "stage_counts": stage_counts,
        "top_buckets": top_buckets,
        "problem_shares": {
            "exact_raw_key_absent": float((trace["diagnosed_stage"].eq("raw_key_absent")).mean()) if len(trace) else 0.0,
            "exact_raw_key_present_no_increment": float((trace["diagnosed_stage"].eq("raw_key_present_no_increment")).mean()) if len(trace) else 0.0,
            "exact_validation_mismatch": float((trace["diagnosed_stage"].eq("panel_increment_present_but_validation_mismatch")).mean()) if len(trace) else 0.0,
            "duplicate_reference_key": float((trace["diagnosed_stage"].eq("duplicate_reference_key")).mean()) if len(trace) else 0.0,
            "duplicate_raw_key": float((trace["diagnosed_stage"].eq("duplicate_raw_key")).mean()) if len(trace) else 0.0,
        },
    }
    return summary


def _china_301_rule_code_action_date(rule_code: Any) -> pd.Timestamp | None:
    code = normalize_hs_code(rule_code, 8)
    action_dates = {
        "99038801": pd.Timestamp("2018-07-06"),
        "99038802": pd.Timestamp("2018-08-23"),
        "99038803": pd.Timestamp("2018-09-24"),
        "99038804": pd.Timestamp("2019-05-10"),
    }
    return action_dates.get(code)


def _classify_china_301_rate_timing_stage(row: pd.Series) -> str:
    if abs(pd.to_numeric(row.get("raw_formula_statutory_gap"), errors="coerce") or 0.0) > 1e-6:
        return "raw_formula_bug"
    if abs(pd.to_numeric(row.get("raw_formula_day_weighted_gap"), errors="coerce") or 0.0) > 1e-6:
        return "raw_formula_bug"

    increment_gap = abs(pd.to_numeric(row.get("raw_vs_ref_increment_gap"), errors="coerce") or 0.0)
    share_gap = abs(pd.to_numeric(row.get("raw_vs_ref_active_share_gap"), errors="coerce") or 0.0)
    stat_gap = abs(pd.to_numeric(row.get("ref_rate_gap_statutory"), errors="coerce") or 0.0)
    day_gap = abs(pd.to_numeric(row.get("ref_rate_gap_day_weighted"), errors="coerce") or 0.0)

    if increment_gap > 0.005 and share_gap <= 0.02:
        return "increment_rate_mismatch"
    if increment_gap <= 0.005 and abs(stat_gap - day_gap) <= 0.005 and stat_gap > 0.005:
        return "base_rate_mismatch"

    closest_candidate = str(row.get("closest_candidate_timing") or "")
    closest_gap = pd.to_numeric(row.get("closest_candidate_abs_gap"), errors="coerce")
    if pd.notna(closest_gap) and closest_gap <= 0.02:
        if closest_candidate == "full_month":
            return "benchmark_uses_full_month"
        if closest_candidate == "legal_effective_date":
            return "benchmark_uses_legal_effective_day"
        if closest_candidate == "previous_day":
            return "benchmark_uses_previous_day"
        if closest_candidate == "next_day":
            return "benchmark_uses_next_day"
        if closest_candidate == "action_month_start":
            return "benchmark_uses_action_month_start"

    ref_share = pd.to_numeric(row.get("ref_implied_active_share"), errors="coerce")
    if pd.notna(ref_share) and 0.0 <= float(ref_share) <= 1.0:
        if closest_candidate == "unmapped":
            return "requires_full_model_review"
        return "benchmark_uses_unknown_partial_month"

    return "requires_full_model_review"


def build_china_301_rate_timing_trace_from_artifacts(config: PipelineConfig, artifact_suffix: str = "") -> dict[str, Any]:
    """Build a China 301 rate/timing diagnostic from the current rate trace."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    rate_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_trace", artifact_suffix)
    if not rate_trace_path.exists():
        raise FileNotFoundError(f"Missing China 301 rate trace artifact: {rate_trace_path}")

    columns = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_tw_active_share_raw",
        "ref_effective_period",
        "raw_panel_rule_code",
        "overlay_rule_code",
        "raw_panel_policy_source",
        "discrepancy_type",
        "raw_formula_statutory_gap",
        "raw_formula_day_weighted_gap",
        "ref_rate_gap_statutory",
        "ref_rate_gap_day_weighted",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "raw_key_present",
        "raw_panel_hs10_present",
        "raw_panel_hs8_month_present",
        "overlay_hs8_month_present",
        "diagnosed_stage",
    ]
    if rate_trace_path.suffix.lower() == ".csv" and rate_trace_path.with_suffix(".parquet").exists():
        trace = read_table(rate_trace_path.with_suffix(".parquet"), columns=columns)
    elif rate_trace_path.suffix.lower() == ".csv":
        trace = pd.read_csv(rate_trace_path, usecols=lambda col: col in set(columns))
    else:
        trace = read_table(rate_trace_path, columns=columns)
    if trace.empty:
        empty_cols = [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "discrepancy_type",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "raw_m_statutory_tariff1",
            "raw_m_statutory_tariff2",
            "raw_base_statutory_rate_raw",
            "raw_panel_increment",
            "raw_tw_active_share_raw",
            "ref_effective_period",
            "raw_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "ref_implied_increment",
            "ref_implied_active_share",
            "raw_vs_ref_increment_gap",
            "raw_vs_ref_active_share_gap",
            "raw_formula_statutory_gap",
            "raw_formula_day_weighted_gap",
            "candidate_share_full_month",
            "candidate_share_legal_effective_date",
            "candidate_share_previous_day",
            "candidate_share_next_day",
            "candidate_share_action_month_start",
            "closest_candidate_timing",
            "closest_candidate_abs_gap",
            "diagnosed_stage",
        ]
        empty = pd.DataFrame(columns=empty_cols)
        timing_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_trace", artifact_suffix)
        empty.to_csv(timing_trace_path, index=False)
        for name in ("by_month", "by_rule", "by_stage", "quantiles"):
            pd.DataFrame().to_csv(output_dir / f"raw_replication_china_301_rate_timing_{name}.csv", index=False)
        return {"trace_path": str(timing_trace_path), "rows": 0, "stage_counts": {}, "top_buckets": []}

    trace["cty_code"] = pd.to_numeric(trace["cty_code"], errors="coerce").astype("Int64")
    trace["year"] = pd.to_numeric(trace["year"], errors="coerce").astype("Int64")
    trace["month"] = pd.to_numeric(trace["month"], errors="coerce").astype("Int64")
    trace["ref_m_stattariff1"] = pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce")
    trace["ref_m_stattariff2"] = pd.to_numeric(trace["ref_m_stattariff2"], errors="coerce")
    trace["raw_m_statutory_tariff1"] = pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce")
    trace["raw_m_statutory_tariff2"] = pd.to_numeric(trace["raw_m_statutory_tariff2"], errors="coerce")
    trace["raw_base_statutory_rate_raw"] = pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce")
    trace["raw_panel_increment"] = pd.to_numeric(trace["raw_panel_increment"], errors="coerce")
    trace["raw_tw_active_share_raw"] = pd.to_numeric(trace["raw_tw_active_share_raw"], errors="coerce")
    trace["raw_formula_statutory_gap"] = pd.to_numeric(trace["raw_formula_statutory_gap"], errors="coerce")
    trace["raw_formula_day_weighted_gap"] = pd.to_numeric(trace["raw_formula_day_weighted_gap"], errors="coerce")
    trace["ref_rate_gap_statutory"] = pd.to_numeric(trace["ref_rate_gap_statutory"], errors="coerce")
    trace["ref_rate_gap_day_weighted"] = pd.to_numeric(trace["ref_rate_gap_day_weighted"], errors="coerce")
    trace["benchmark_implied_increment"] = pd.to_numeric(trace["benchmark_implied_increment"], errors="coerce")
    trace["benchmark_implied_active_share"] = pd.to_numeric(trace["benchmark_implied_active_share"], errors="coerce")

    timing_rows: list[dict[str, Any]] = []
    for row in trace.itertuples(index=False):
        hs10 = normalize_hs_code(getattr(row, "hs10"), 10)
        hs8 = normalize_hs_code(getattr(row, "hs8"), 8)
        year = int(getattr(row, "year"))
        month = int(getattr(row, "month"))
        raw_rule_code = normalize_hs_code(getattr(row, "raw_panel_rule_code", pd.NA), 8)
        overlay_rule_code = normalize_hs_code(getattr(row, "overlay_rule_code", pd.NA), 8)
        raw_policy_source = getattr(row, "raw_panel_policy_source", pd.NA)

        ref_base = pd.to_numeric(pd.Series([getattr(row, "raw_base_statutory_rate_raw")]), errors="coerce").iloc[0]
        ref_inc = pd.to_numeric(pd.Series([getattr(row, "ref_m_stattariff1")]), errors="coerce").iloc[0] - ref_base
        ref_share = pd.NA
        if pd.notna(getattr(row, "raw_panel_increment")) and abs(float(getattr(row, "raw_panel_increment"))) > RATE_TOL:
            ref_share = (
                pd.to_numeric(pd.Series([getattr(row, "ref_m_stattariff2")]), errors="coerce").iloc[0] - ref_base
            ) / float(getattr(row, "raw_panel_increment"))

        action_date = _china_301_rule_code_action_date(raw_rule_code)
        legal_effective_share = _month_active_share_from_effective_date(action_date, year, month) if action_date is not None else pd.NA
        previous_day_share = _month_active_share_from_effective_date(action_date - pd.Timedelta(days=1), year, month) if action_date is not None else pd.NA
        next_day_share = _month_active_share_from_effective_date(action_date + pd.Timedelta(days=1), year, month) if action_date is not None else pd.NA
        action_month_start = pd.Timestamp(action_date.year, action_date.month, 1) if action_date is not None else None
        action_month_start_share = _month_active_share_from_effective_date(action_month_start, year, month) if action_month_start is not None else pd.NA

        candidate_values = {
            "full_month": 1.0,
            "legal_effective_date": legal_effective_share,
            "previous_day": previous_day_share,
            "next_day": next_day_share,
            "action_month_start": action_month_start_share,
        }
        candidate_diffs = {
            name: abs(float(ref_share) - float(value))
            for name, value in candidate_values.items()
            if pd.notna(ref_share) and pd.notna(value)
        }
        if candidate_diffs:
            closest_candidate_timing = min(candidate_diffs.items(), key=lambda item: item[1])[0]
            closest_candidate_abs_gap = float(min(candidate_diffs.values()))
        else:
            closest_candidate_timing = "unmapped"
            closest_candidate_abs_gap = pd.NA

        row_dict = {
            "cty_code": int(getattr(row, "cty_code")),
            "hs10": hs10,
            "hs8": hs8,
            "year": year,
            "month": month,
            "discrepancy_type": getattr(row, "discrepancy_type"),
            "ref_m_stattariff1": float(getattr(row, "ref_m_stattariff1")) if pd.notna(getattr(row, "ref_m_stattariff1")) else pd.NA,
            "ref_m_stattariff2": float(getattr(row, "ref_m_stattariff2")) if pd.notna(getattr(row, "ref_m_stattariff2")) else pd.NA,
            "raw_m_statutory_tariff1": float(getattr(row, "raw_m_statutory_tariff1")) if pd.notna(getattr(row, "raw_m_statutory_tariff1")) else pd.NA,
            "raw_m_statutory_tariff2": float(getattr(row, "raw_m_statutory_tariff2")) if pd.notna(getattr(row, "raw_m_statutory_tariff2")) else pd.NA,
            "raw_base_statutory_rate_raw": float(ref_base) if pd.notna(ref_base) else pd.NA,
            "raw_panel_increment": float(getattr(row, "raw_panel_increment")) if pd.notna(getattr(row, "raw_panel_increment")) else pd.NA,
            "raw_tw_active_share_raw": float(getattr(row, "raw_tw_active_share_raw")) if pd.notna(getattr(row, "raw_tw_active_share_raw")) else pd.NA,
            "ref_effective_period": getattr(row, "ref_effective_period"),
            "raw_rule_code": raw_rule_code,
            "overlay_rule_code": overlay_rule_code,
            "raw_policy_source": raw_policy_source,
            "ref_implied_increment": float(ref_inc) if pd.notna(ref_inc) else pd.NA,
            "ref_implied_active_share": float(ref_share) if pd.notna(ref_share) else pd.NA,
            "raw_vs_ref_increment_gap": float(ref_inc - float(getattr(row, "raw_panel_increment"))) if pd.notna(ref_share) and pd.notna(getattr(row, "raw_panel_increment")) and pd.notna(ref_inc) else pd.NA,
            "raw_vs_ref_active_share_gap": float(ref_share - float(getattr(row, "raw_tw_active_share_raw"))) if pd.notna(ref_share) and pd.notna(getattr(row, "raw_tw_active_share_raw")) else pd.NA,
            "raw_formula_statutory_gap": float(getattr(row, "raw_formula_statutory_gap")) if pd.notna(getattr(row, "raw_formula_statutory_gap")) else pd.NA,
            "raw_formula_day_weighted_gap": float(getattr(row, "raw_formula_day_weighted_gap")) if pd.notna(getattr(row, "raw_formula_day_weighted_gap")) else pd.NA,
            "candidate_share_full_month": 1.0,
            "candidate_share_legal_effective_date": float(legal_effective_share) if pd.notna(legal_effective_share) else pd.NA,
            "candidate_share_previous_day": float(previous_day_share) if pd.notna(previous_day_share) else pd.NA,
            "candidate_share_next_day": float(next_day_share) if pd.notna(next_day_share) else pd.NA,
            "candidate_share_action_month_start": float(action_month_start_share) if pd.notna(action_month_start_share) else pd.NA,
            "closest_candidate_timing": closest_candidate_timing,
            "closest_candidate_abs_gap": float(closest_candidate_abs_gap) if pd.notna(closest_candidate_abs_gap) else pd.NA,
        }
        row_dict["diagnosed_stage"] = _classify_china_301_rate_timing_stage(pd.Series(row_dict))
        timing_rows.append(row_dict)

    timing_trace = pd.DataFrame(timing_rows)
    timing_trace = timing_trace.loc[
        :,
        [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "discrepancy_type",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "raw_m_statutory_tariff1",
            "raw_m_statutory_tariff2",
            "raw_base_statutory_rate_raw",
            "raw_panel_increment",
            "raw_tw_active_share_raw",
            "ref_effective_period",
            "raw_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "ref_implied_increment",
            "ref_implied_active_share",
            "raw_vs_ref_increment_gap",
            "raw_vs_ref_active_share_gap",
            "raw_formula_statutory_gap",
            "raw_formula_day_weighted_gap",
            "candidate_share_full_month",
            "candidate_share_legal_effective_date",
            "candidate_share_previous_day",
            "candidate_share_next_day",
            "candidate_share_action_month_start",
            "closest_candidate_timing",
            "closest_candidate_abs_gap",
            "diagnosed_stage",
        ],
    ].copy()

    abs_stat = (pd.to_numeric(timing_trace["ref_m_stattariff1"], errors="coerce") - pd.to_numeric(timing_trace["raw_m_statutory_tariff1"], errors="coerce")).abs() * 100.0
    abs_day = (pd.to_numeric(timing_trace["ref_m_stattariff2"], errors="coerce") - pd.to_numeric(timing_trace["raw_m_statutory_tariff2"], errors="coerce")).abs() * 100.0
    inc_gap_pp = pd.to_numeric(timing_trace["raw_vs_ref_increment_gap"], errors="coerce").abs() * 100.0
    share_gap_pp = pd.to_numeric(timing_trace["raw_vs_ref_active_share_gap"], errors="coerce").abs() * 100.0
    timing_trace["abs_statutory_gap_pp"] = abs_stat
    timing_trace["abs_day_weighted_gap_pp"] = abs_day
    timing_trace["raw_vs_ref_increment_gap_pp"] = inc_gap_pp
    timing_trace["raw_vs_ref_active_share_gap_pp"] = share_gap_pp

    timing_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_trace", artifact_suffix)
    write_parquet(timing_trace, timing_trace_path.with_suffix(".parquet"), overwrite=True)
    timing_trace.to_csv(timing_trace_path, index=False)

    by_month = (
        timing_trace.groupby(["year", "month", "discrepancy_type", "diagnosed_stage"], dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            mean_abs_statutory_gap_pp=("abs_statutory_gap_pp", "mean"),
            median_abs_statutory_gap_pp=("abs_statutory_gap_pp", "median"),
            p90_abs_statutory_gap_pp=("abs_statutory_gap_pp", lambda s: s.quantile(0.9)),
            mean_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", "mean"),
            median_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", "median"),
            p90_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", lambda s: s.quantile(0.9)),
        )
        .reset_index()
        .sort_values(["rows", "year", "month"], ascending=[False, True, True])
    )
    by_rule = (
        timing_trace.groupby(["raw_rule_code", "overlay_rule_code", "discrepancy_type", "diagnosed_stage"], dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            mean_abs_statutory_gap_pp=("abs_statutory_gap_pp", "mean"),
            median_abs_statutory_gap_pp=("abs_statutory_gap_pp", "median"),
            p90_abs_statutory_gap_pp=("abs_statutory_gap_pp", lambda s: s.quantile(0.9)),
            mean_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", "mean"),
            median_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", "median"),
            p90_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", lambda s: s.quantile(0.9)),
        )
        .reset_index()
        .sort_values(["rows", "raw_rule_code"], ascending=[False, True])
    )
    by_stage = (
        timing_trace.groupby(["diagnosed_stage"], dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            mean_abs_statutory_gap_pp=("abs_statutory_gap_pp", "mean"),
            median_abs_statutory_gap_pp=("abs_statutory_gap_pp", "median"),
            p90_abs_statutory_gap_pp=("abs_statutory_gap_pp", lambda s: s.quantile(0.9)),
            mean_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", "mean"),
            median_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", "median"),
            p90_abs_day_weighted_gap_pp=("abs_day_weighted_gap_pp", lambda s: s.quantile(0.9)),
        )
        .reset_index()
        .sort_values(["rows", "diagnosed_stage"], ascending=[False, True])
    )
    by_stage["share_rows"] = by_stage["rows"] / max(len(timing_trace), 1)

    quantiles_rows: list[dict[str, Any]] = []
    for name, series in {
        "abs_statutory_gap_pp": timing_trace["abs_statutory_gap_pp"],
        "abs_day_weighted_gap_pp": timing_trace["abs_day_weighted_gap_pp"],
        "raw_vs_ref_increment_gap_pp": timing_trace["raw_vs_ref_increment_gap_pp"],
        "raw_vs_ref_active_share_gap_pp": timing_trace["raw_vs_ref_active_share_gap_pp"],
    }.items():
        clean = pd.to_numeric(series, errors="coerce").dropna()
        if clean.empty:
            quantiles_rows.append({"gap_name": name})
            continue
        quantiles_rows.append(
            {
                "gap_name": name,
                "q00": float(clean.quantile(0.0)),
                "q01": float(clean.quantile(0.01)),
                "q05": float(clean.quantile(0.05)),
                "q10": float(clean.quantile(0.10)),
                "q25": float(clean.quantile(0.25)),
                "q50": float(clean.quantile(0.50)),
                "q75": float(clean.quantile(0.75)),
                "q90": float(clean.quantile(0.90)),
                "q95": float(clean.quantile(0.95)),
                "q99": float(clean.quantile(0.99)),
                "q100": float(clean.quantile(1.0)),
            }
        )
    quantiles = pd.DataFrame(quantiles_rows)

    by_month_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_by_month", artifact_suffix)
    by_rule_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_by_rule", artifact_suffix)
    by_stage_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_by_stage", artifact_suffix)
    quantiles_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_quantiles", artifact_suffix)
    by_month.to_csv(by_month_path, index=False)
    by_rule.to_csv(by_rule_path, index=False)
    by_stage.to_csv(by_stage_path, index=False)
    quantiles.to_csv(quantiles_path, index=False)

    stage_counts = timing_trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict()
    top_buckets = timing_trace.sort_values(["rows"] if "rows" in timing_trace.columns else ["year", "month"], ascending=False).head(20).to_dict(orient="records")
    return {
        "trace_path": str(timing_trace_path),
        "by_month_path": str(by_month_path),
        "by_rule_path": str(by_rule_path),
        "by_stage_path": str(by_stage_path),
        "quantiles_path": str(quantiles_path),
        "rows": int(len(timing_trace)),
        "stage_counts": stage_counts,
        "top_buckets": top_buckets,
    }


def _classify_china_301_benchmark_definition_stage(row: pd.Series) -> str:
    if int(row.get("duplicate_reference_key_rows") or 0) > 1:
        return "duplicate_reference_key"
    if int(row.get("duplicate_raw_key_rows") or 0) > 1:
        return "duplicate_raw_key"
    if not bool(row.get("ref_active")) or int(row.get("ref_m_china_hit") or 0) != 1 or int(row.get("cty_code") or 0) != 5700:
        return "non_china_or_inactive_reference_leak"
    if not bool(row.get("raw_key_present")):
        return "raw_key_absent"

    discrepancy_type = str(row.get("discrepancy_type") or "")
    statutory_gap = abs(pd.to_numeric(row.get("ref_rate_gap_statutory"), errors="coerce") or 0.0)
    day_gap = abs(pd.to_numeric(row.get("ref_rate_gap_day_weighted"), errors="coerce") or 0.0)
    rule_gap = abs(pd.to_numeric(row.get("benchmark_implied_vs_rule_attribute_gap_pp"), errors="coerce") or 0.0)
    share_gap = abs(pd.to_numeric(row.get("benchmark_implied_active_share_gap_pp"), errors="coerce") or 0.0)

    if discrepancy_type == "statutory_rate_mismatch":
        if rule_gap > PP_GAP_TOL:
            return "benchmark_increment_definition_difference"
        if statutory_gap <= PP_TOL and day_gap <= PP_TOL:
            return "raw_formula_matches_reference"
        return "statutory_rate_aligned_to_raw_formula"
    if discrepancy_type == "day_weighted_rate_mismatch":
        if share_gap > PP_GAP_TOL:
            return "benchmark_timing_convention_difference"
        if statutory_gap <= PP_TOL and day_gap <= PP_TOL and rule_gap <= PP_GAP_TOL:
            return "raw_formula_matches_reference"
        return "day_weighted_rate_aligned_to_raw_formula"
    if statutory_gap <= PP_TOL and day_gap <= PP_TOL and rule_gap <= PP_GAP_TOL:
        return "raw_formula_matches_reference"
    if rule_gap > PP_GAP_TOL:
        return "benchmark_increment_definition_difference"
    if share_gap > PP_GAP_TOL:
        return "benchmark_timing_convention_difference"
    return "requires_full_model_review"


def build_china_301_benchmark_definition_trace_from_artifacts(
    config: PipelineConfig,
    top_n: int = 400,
    artifact_suffix: str = "",
) -> dict[str, Any]:
    """Compare raw China 301 formulas against the paper's benchmark rate definitions."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    rate_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_trace", artifact_suffix)
    if not rate_trace_path.exists():
        raise FileNotFoundError(f"Missing China 301 rate trace artifact: {rate_trace_path}")

    rate_columns = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_active",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_effective_period",
        "raw_key_present",
        "raw_panel_hs10_present",
        "raw_panel_hs8_month_present",
        "raw_panel_increment",
        "raw_panel_rule_code",
        "raw_panel_policy_source",
        "raw_mfn_ad_val_rate",
        "raw_base_pref_rate_raw",
        "raw_base_statutory_rate_raw",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_formula_statutory_rate",
        "raw_formula_day_weighted_rate",
        "raw_formula_statutory_gap",
        "raw_formula_day_weighted_gap",
        "ref_rate_gap_statutory",
        "ref_rate_gap_day_weighted",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "benchmark_implied_active_share_gap",
        "benchmark_implied_increment_gap_pp",
        "overlay_hs8_month_present",
        "overlay_increment",
        "overlay_rule_code",
        "discrepancy_type",
        "duplicate_reference_key_rows",
        "duplicate_raw_key_rows",
        "diagnosed_stage",
    ]
    if rate_trace_path.suffix.lower() == ".csv":
        rate_trace = pd.read_csv(rate_trace_path, usecols=lambda col: col in set(rate_columns))
    else:
        rate_trace = read_table(rate_trace_path, columns=rate_columns)

    component_path = output_dir / _artifact_name("raw_replication_china_301_statutory_component_trace", artifact_suffix)
    component_trace = pd.DataFrame()
    if component_path.exists():
        component_columns = [
            "cty_code",
            "hs10",
            "year",
            "month",
            "raw_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "rule_attribute_increment",
            "raw_panel_increment",
            "raw_m_statutory_tariff1",
            "raw_m_statutory_tariff2",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "benchmark_implied_increment",
            "raw_vs_rule_attribute_increment_gap_pp",
            "benchmark_implied_vs_rule_attribute_gap_pp",
            "diagnosed_component",
            "duplicate_rule_attribute_rows",
        ]
        component_trace = pd.read_csv(component_path, usecols=lambda col: col in set(component_columns))

    trace_path = output_dir / _artifact_name("raw_replication_china_301_benchmark_definition_trace", artifact_suffix)
    empty_cols = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_effective_period",
        "discrepancy_type",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_tw_active_share_raw",
        "raw_m_statutory_tariff1",
        "raw_m_statutory_tariff2",
        "raw_formula_statutory_rate",
        "raw_formula_day_weighted_rate",
        "ref_rate_gap_statutory",
        "ref_rate_gap_day_weighted",
        "benchmark_implied_increment",
        "benchmark_implied_active_share",
        "benchmark_implied_active_share_gap_pp",
        "raw_panel_rule_code",
        "overlay_rule_code",
        "raw_policy_source",
        "rule_attribute_increment",
        "raw_vs_rule_attribute_increment_gap_pp",
        "benchmark_implied_vs_rule_attribute_gap_pp",
        "raw_key_present",
        "raw_panel_hs10_present",
        "raw_panel_hs8_month_present",
        "overlay_hs8_month_present",
        "duplicate_reference_key_rows",
        "duplicate_raw_key_rows",
        "diagnosed_stage",
        "closest_reference_formulation",
        "closest_reference_gap_pp",
    ]
    if rate_trace.empty:
        empty = pd.DataFrame(columns=empty_cols)
        empty.to_csv(trace_path, index=False)
        for name in ("by_rule", "by_month", "by_stage", "quantiles"):
            pd.DataFrame().to_csv(
                output_dir / _artifact_name(f"raw_replication_china_301_benchmark_definition_{name}", artifact_suffix),
                index=False,
            )
        return {"trace_path": str(trace_path), "rows": 0, "stage_counts": {}, "top_buckets": []}

    trace = rate_trace.copy()
    trace["hs10"] = trace["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    trace["hs8"] = trace["hs8"].map(lambda value: normalize_hs_code(value, 8) if pd.notna(value) else pd.NA).astype("string")
    trace["cty_code"] = pd.to_numeric(trace["cty_code"], errors="coerce").astype("Int64")
    trace["year"] = pd.to_numeric(trace["year"], errors="coerce").astype("Int64")
    trace["month"] = pd.to_numeric(trace["month"], errors="coerce").astype("Int64")
    trace["ref_m_stattariff1"] = pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce")
    trace["ref_m_stattariff2"] = pd.to_numeric(trace["ref_m_stattariff2"], errors="coerce")
    trace["raw_base_statutory_rate_raw"] = pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce")
    trace["raw_panel_increment"] = pd.to_numeric(trace["raw_panel_increment"], errors="coerce")
    trace["raw_tw_active_share_raw"] = pd.to_numeric(trace["raw_tw_active_share_raw"], errors="coerce")
    trace["raw_m_statutory_tariff1"] = pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce")
    trace["raw_m_statutory_tariff2"] = pd.to_numeric(trace["raw_m_statutory_tariff2"], errors="coerce")
    trace["raw_formula_statutory_rate"] = pd.to_numeric(trace["raw_formula_statutory_rate"], errors="coerce")
    trace["raw_formula_day_weighted_rate"] = pd.to_numeric(trace["raw_formula_day_weighted_rate"], errors="coerce")
    trace["ref_rate_gap_statutory"] = pd.to_numeric(trace["ref_rate_gap_statutory"], errors="coerce")
    trace["ref_rate_gap_day_weighted"] = pd.to_numeric(trace["ref_rate_gap_day_weighted"], errors="coerce")
    trace["benchmark_implied_increment"] = pd.to_numeric(trace["benchmark_implied_increment"], errors="coerce")
    trace["benchmark_implied_active_share"] = pd.to_numeric(trace["benchmark_implied_active_share"], errors="coerce")
    trace["benchmark_implied_active_share_gap_pp"] = pd.to_numeric(trace.get("benchmark_implied_active_share_gap"), errors="coerce").abs() * 100.0 if "benchmark_implied_active_share_gap" in trace.columns else pd.NA
    trace["duplicate_reference_key_rows"] = pd.to_numeric(trace["duplicate_reference_key_rows"], errors="coerce").fillna(0).astype("Int64")
    trace["duplicate_raw_key_rows"] = pd.to_numeric(trace["duplicate_raw_key_rows"], errors="coerce").fillna(0).astype("Int64")

    if not component_trace.empty:
        component_trace = component_trace.copy()
        component_trace["hs10"] = component_trace["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        component_trace["cty_code"] = pd.to_numeric(component_trace["cty_code"], errors="coerce").astype("Int64")
        component_trace["year"] = pd.to_numeric(component_trace["year"], errors="coerce").astype("Int64")
        component_trace["month"] = pd.to_numeric(component_trace["month"], errors="coerce").astype("Int64")
        component_trace["rule_attribute_increment"] = pd.to_numeric(component_trace["rule_attribute_increment"], errors="coerce")
        component_trace["raw_vs_rule_attribute_increment_gap_pp"] = pd.to_numeric(component_trace["raw_vs_rule_attribute_increment_gap_pp"], errors="coerce")
        component_trace["benchmark_implied_vs_rule_attribute_gap_pp"] = pd.to_numeric(component_trace["benchmark_implied_vs_rule_attribute_gap_pp"], errors="coerce")
        component_trace["duplicate_rule_attribute_rows"] = pd.to_numeric(component_trace["duplicate_rule_attribute_rows"], errors="coerce").fillna(0).astype("Int64")
        component_trace = component_trace.rename(
            columns={
                "raw_rule_code": "component_raw_rule_code",
                "overlay_rule_code": "component_overlay_rule_code",
                "raw_policy_source": "component_raw_policy_source",
            }
        )
        trace = trace.merge(
            component_trace.loc[
                :,
                [
                    "cty_code",
                    "hs10",
                    "year",
                    "month",
                    "component_raw_rule_code",
                    "component_overlay_rule_code",
                    "component_raw_policy_source",
                    "rule_attribute_increment",
                    "raw_vs_rule_attribute_increment_gap_pp",
                    "benchmark_implied_vs_rule_attribute_gap_pp",
                    "duplicate_rule_attribute_rows",
                    "diagnosed_component",
                ],
            ],
            on=["cty_code", "hs10", "year", "month"],
            how="left",
        )
    else:
        trace["component_raw_rule_code"] = pd.NA
        trace["component_overlay_rule_code"] = pd.NA
        trace["component_raw_policy_source"] = pd.NA
        trace["rule_attribute_increment"] = pd.NA
        trace["raw_vs_rule_attribute_increment_gap_pp"] = pd.NA
        trace["benchmark_implied_vs_rule_attribute_gap_pp"] = pd.NA
        trace["duplicate_rule_attribute_rows"] = 0
        trace["diagnosed_component"] = pd.NA

    trace["raw_policy_source"] = trace["raw_panel_policy_source"].where(trace["raw_panel_policy_source"].notna(), trace["component_raw_policy_source"])
    trace["raw_rule_code"] = trace["raw_panel_rule_code"].where(trace["raw_panel_rule_code"].notna(), trace["component_raw_rule_code"])
    trace["overlay_rule_code"] = trace["overlay_rule_code"].where(trace["overlay_rule_code"].notna(), trace["component_overlay_rule_code"])
    trace["raw_rule_code"] = _normalize_rule_code(trace["raw_rule_code"])
    trace["overlay_rule_code"] = _normalize_rule_code(trace["overlay_rule_code"])
    trace["raw_policy_source"] = trace["raw_policy_source"].astype("string")
    trace["rule_attribute_increment"] = pd.to_numeric(trace["rule_attribute_increment"], errors="coerce")
    trace["raw_vs_rule_attribute_increment_gap_pp"] = pd.to_numeric(trace["raw_vs_rule_attribute_increment_gap_pp"], errors="coerce")
    trace["benchmark_implied_vs_rule_attribute_gap_pp"] = pd.to_numeric(trace["benchmark_implied_vs_rule_attribute_gap_pp"], errors="coerce")
    if "benchmark_implied_increment_gap_pp" in trace.columns:
        trace["benchmark_implied_increment_gap_pp"] = pd.to_numeric(trace["benchmark_implied_increment_gap_pp"], errors="coerce")
    else:
        trace["benchmark_implied_increment_gap_pp"] = (pd.to_numeric(trace["benchmark_implied_increment"], errors="coerce") - pd.to_numeric(trace["rule_attribute_increment"], errors="coerce")).abs() * 100.0
    trace["duplicate_rule_attribute_rows"] = pd.to_numeric(trace["duplicate_rule_attribute_rows"], errors="coerce").fillna(0).astype("Int64")
    trace["raw_vs_ref_total_gap_pp"] = (pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce") - pd.to_numeric(trace["raw_m_statutory_tariff1"], errors="coerce")).abs() * 100.0
    trace["raw_vs_ref_day_weighted_gap_pp"] = (pd.to_numeric(trace["ref_m_stattariff2"], errors="coerce") - pd.to_numeric(trace["raw_m_statutory_tariff2"], errors="coerce")).abs() * 100.0
    trace["raw_vs_base_gap_pp"] = (pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce") - pd.to_numeric(trace["raw_base_statutory_rate_raw"], errors="coerce")).abs() * 100.0
    trace["raw_vs_increment_only_gap_pp"] = (pd.to_numeric(trace["ref_m_stattariff1"], errors="coerce") - pd.to_numeric(trace["raw_panel_increment"], errors="coerce")).abs() * 100.0
    trace["raw_vs_formula_statutory_gap_pp"] = pd.to_numeric(trace["raw_formula_statutory_gap"], errors="coerce").abs() * 100.0
    trace["raw_vs_formula_day_weighted_gap_pp"] = pd.to_numeric(trace["raw_formula_day_weighted_gap"], errors="coerce").abs() * 100.0
    trace["benchmark_implied_increment_gap_pp"] = (pd.to_numeric(trace["benchmark_implied_increment"], errors="coerce") - pd.to_numeric(trace["rule_attribute_increment"], errors="coerce")).abs() * 100.0
    if "benchmark_implied_active_share_gap_pp" not in trace.columns or trace["benchmark_implied_active_share_gap_pp"].isna().all():
        trace["benchmark_implied_active_share_gap_pp"] = (pd.to_numeric(trace["benchmark_implied_active_share"], errors="coerce") - pd.to_numeric(trace["raw_tw_active_share_raw"], errors="coerce")).abs() * 100.0
    else:
        trace["benchmark_implied_active_share_gap_pp"] = pd.to_numeric(trace["benchmark_implied_active_share_gap_pp"], errors="coerce")

    candidate_gap_columns = {
        "raw_total_statutory_rate": "raw_vs_ref_total_gap_pp",
        "raw_increment_only": "raw_vs_increment_only_gap_pp",
        "raw_base_only": "raw_vs_base_gap_pp",
        "raw_day_weighted_rate": "raw_vs_ref_day_weighted_gap_pp",
    }
    candidate_gap_frame = trace.loc[:, list(candidate_gap_columns.values())].copy().apply(pd.to_numeric, errors="coerce").fillna(np.inf)
    trace["closest_reference_formulation"] = candidate_gap_frame.idxmin(axis=1).map({value: key for key, value in candidate_gap_columns.items()})
    trace["closest_reference_gap_pp"] = candidate_gap_frame.min(axis=1)
    trace["benchmark_definition_score_pp"] = trace[["raw_vs_formula_statutory_gap_pp", "raw_vs_formula_day_weighted_gap_pp", "benchmark_implied_increment_gap_pp", "benchmark_implied_active_share_gap_pp"]].min(axis=1)
    trace["diagnosed_stage"] = trace.apply(_classify_china_301_benchmark_definition_stage, axis=1)

    trace = trace.loc[
        :,
        [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_effective_period",
            "discrepancy_type",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "raw_base_statutory_rate_raw",
            "raw_panel_increment",
            "raw_tw_active_share_raw",
            "raw_m_statutory_tariff1",
            "raw_m_statutory_tariff2",
            "raw_formula_statutory_rate",
            "raw_formula_day_weighted_rate",
            "ref_rate_gap_statutory",
            "ref_rate_gap_day_weighted",
            "benchmark_implied_increment",
            "benchmark_implied_active_share",
            "benchmark_implied_active_share_gap_pp",
            "benchmark_implied_increment_gap_pp",
            "raw_panel_rule_code",
            "overlay_rule_code",
            "raw_policy_source",
            "rule_attribute_increment",
            "raw_vs_rule_attribute_increment_gap_pp",
            "benchmark_implied_vs_rule_attribute_gap_pp",
            "raw_key_present",
            "raw_panel_hs10_present",
            "raw_panel_hs8_month_present",
            "overlay_hs8_month_present",
            "duplicate_reference_key_rows",
            "duplicate_raw_key_rows",
            "diagnosed_stage",
            "closest_reference_formulation",
            "closest_reference_gap_pp",
            "raw_vs_ref_total_gap_pp",
            "raw_vs_ref_day_weighted_gap_pp",
            "raw_vs_base_gap_pp",
            "raw_vs_increment_only_gap_pp",
            "raw_vs_formula_statutory_gap_pp",
            "raw_vs_formula_day_weighted_gap_pp",
            "benchmark_definition_score_pp",
        ],
    ].copy()
    trace = trace.fillna({"duplicate_reference_key_rows": 0, "duplicate_raw_key_rows": 0})
    trace = trace.sort_values(
        ["discrepancy_type", "diagnosed_stage", "closest_reference_gap_pp", "year", "month", "hs10"],
        ascending=[True, True, False, True, True, True],
    ).reset_index(drop=True)

    write_parquet(trace, trace_path.with_suffix(".parquet"), overwrite=True)
    trace.to_csv(trace_path, index=False)
    by_rule = (
        trace.groupby(["raw_panel_rule_code", "overlay_rule_code", "discrepancy_type", "diagnosed_stage"], dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            mean_raw_vs_ref_total_gap_pp=("raw_vs_ref_total_gap_pp", "mean"),
            median_raw_vs_ref_total_gap_pp=("raw_vs_ref_total_gap_pp", "median"),
            mean_raw_vs_ref_day_weighted_gap_pp=("raw_vs_ref_day_weighted_gap_pp", "mean"),
            median_raw_vs_ref_day_weighted_gap_pp=("raw_vs_ref_day_weighted_gap_pp", "median"),
            mean_benchmark_implied_increment_gap_pp=("benchmark_implied_increment_gap_pp", "mean"),
            median_benchmark_implied_increment_gap_pp=("benchmark_implied_increment_gap_pp", "median"),
        )
        .reset_index()
        .sort_values(["rows", "raw_panel_rule_code"], ascending=[False, True])
    )
    by_month = (
        trace.groupby(["year", "month", "discrepancy_type", "diagnosed_stage"], dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            mean_raw_vs_ref_total_gap_pp=("raw_vs_ref_total_gap_pp", "mean"),
            median_raw_vs_ref_total_gap_pp=("raw_vs_ref_total_gap_pp", "median"),
            mean_raw_vs_ref_day_weighted_gap_pp=("raw_vs_ref_day_weighted_gap_pp", "mean"),
            median_raw_vs_ref_day_weighted_gap_pp=("raw_vs_ref_day_weighted_gap_pp", "median"),
            mean_benchmark_implied_increment_gap_pp=("benchmark_implied_increment_gap_pp", "mean"),
            median_benchmark_implied_increment_gap_pp=("benchmark_implied_increment_gap_pp", "median"),
        )
        .reset_index()
        .sort_values(["rows", "year", "month"], ascending=[False, True, True])
    )
    by_stage = (
        trace.groupby(["diagnosed_stage"], dropna=False, observed=True)
        .agg(
            rows=("hs10", "size"),
            mean_raw_vs_ref_total_gap_pp=("raw_vs_ref_total_gap_pp", "mean"),
            median_raw_vs_ref_total_gap_pp=("raw_vs_ref_total_gap_pp", "median"),
            mean_raw_vs_ref_day_weighted_gap_pp=("raw_vs_ref_day_weighted_gap_pp", "mean"),
            median_raw_vs_ref_day_weighted_gap_pp=("raw_vs_ref_day_weighted_gap_pp", "median"),
            mean_benchmark_implied_increment_gap_pp=("benchmark_implied_increment_gap_pp", "mean"),
            median_benchmark_implied_increment_gap_pp=("benchmark_implied_increment_gap_pp", "median"),
        )
        .reset_index()
        .sort_values(["rows", "diagnosed_stage"], ascending=[False, True])
    )
    by_stage["share_rows"] = by_stage["rows"] / max(len(trace), 1)

    quantiles_rows: list[dict[str, Any]] = []
    for name, series in {
        "raw_vs_ref_total_gap_pp": trace["raw_vs_ref_total_gap_pp"],
        "raw_vs_ref_day_weighted_gap_pp": trace["raw_vs_ref_day_weighted_gap_pp"],
        "raw_vs_base_gap_pp": trace["raw_vs_base_gap_pp"],
        "raw_vs_increment_only_gap_pp": trace["raw_vs_increment_only_gap_pp"],
        "benchmark_implied_increment_gap_pp": trace["benchmark_implied_increment_gap_pp"],
        "benchmark_implied_active_share_gap_pp": trace["benchmark_implied_active_share_gap_pp"],
    }.items():
        clean = pd.to_numeric(series, errors="coerce").dropna()
        if clean.empty:
            quantiles_rows.append({"gap_name": name})
            continue
        quantiles_rows.append(
            {
                "gap_name": name,
                "q00": float(clean.quantile(0.0)),
                "q01": float(clean.quantile(0.01)),
                "q05": float(clean.quantile(0.05)),
                "q10": float(clean.quantile(0.10)),
                "q25": float(clean.quantile(0.25)),
                "q50": float(clean.quantile(0.50)),
                "q75": float(clean.quantile(0.75)),
                "q90": float(clean.quantile(0.90)),
                "q95": float(clean.quantile(0.95)),
                "q99": float(clean.quantile(0.99)),
                "q100": float(clean.quantile(1.0)),
            }
        )
    quantiles = pd.DataFrame(quantiles_rows)
    by_rule_path = output_dir / _artifact_name("raw_replication_china_301_benchmark_definition_by_rule", artifact_suffix)
    by_month_path = output_dir / _artifact_name("raw_replication_china_301_benchmark_definition_by_month", artifact_suffix)
    by_stage_path = output_dir / _artifact_name("raw_replication_china_301_benchmark_definition_by_stage", artifact_suffix)
    quantiles_path = output_dir / _artifact_name("raw_replication_china_301_benchmark_definition_quantiles", artifact_suffix)
    by_rule.to_csv(by_rule_path, index=False)
    by_month.to_csv(by_month_path, index=False)
    by_stage.to_csv(by_stage_path, index=False)
    quantiles.to_csv(quantiles_path, index=False)

    stage_counts = trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict()
    top_buckets = trace.head(20).to_dict(orient="records")
    return {
        "trace_path": str(trace_path),
        "by_rule_path": str(by_rule_path),
        "by_month_path": str(by_month_path),
        "by_stage_path": str(by_stage_path),
        "quantiles_path": str(quantiles_path),
        "rows": int(len(trace)),
        "stage_counts": stage_counts,
        "top_buckets": top_buckets,
    }


def build_china_301_rate_provenance_from_artifacts(config: PipelineConfig, artifact_suffix: str = "") -> dict[str, Any]:
    """Build a row-level provenance diagnostic for the remaining China 301 rate gaps."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    rate_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_trace", artifact_suffix)
    if not rate_trace_path.exists():
        raise FileNotFoundError(f"Missing China 301 rate trace artifact: {rate_trace_path}")
    rate_trace = pd.read_csv(rate_trace_path)

    timing_trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_timing_trace", artifact_suffix)
    timing_trace = pd.DataFrame()
    if timing_trace_path.exists():
        timing_trace = pd.read_csv(timing_trace_path)

    provenance = _build_china_301_rate_provenance(rate_trace, timing_trace)
    trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_provenance", artifact_suffix)
    provenance.to_csv(trace_path, index=False)
    stage_counts = provenance["rate_provenance_stage"].value_counts(dropna=False).sort_index().to_dict() if not provenance.empty else {}
    top_buckets = provenance.head(20).to_dict(orient="records")
    return {"trace_path": str(trace_path), "rows": int(len(provenance)), "stage_counts": stage_counts, "top_buckets": top_buckets}


def build_china_301_rate_mismatch_decomposition_from_artifacts(config: PipelineConfig, artifact_suffix: str = "") -> dict[str, Any]:
    """Summarize the remaining China 301 rate mismatches by common rate tuple."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    provenance_path = output_dir / _artifact_name("raw_replication_china_301_rate_provenance", artifact_suffix)
    if not provenance_path.exists():
        build_china_301_rate_provenance_from_artifacts(config, artifact_suffix=artifact_suffix)
    provenance = pd.read_csv(provenance_path) if provenance_path.exists() else pd.DataFrame()

    decomposition = _build_china_301_rate_mismatch_decomposition(provenance)
    trace_path = output_dir / _artifact_name("raw_replication_china_301_rate_mismatch_decomposition", artifact_suffix)
    decomposition.to_csv(trace_path, index=False)
    stage_counts = decomposition["rate_provenance_stage"].value_counts(dropna=False).sort_index().to_dict() if not decomposition.empty else {}
    top_buckets = decomposition.head(20).to_dict(orient="records")
    return {"trace_path": str(trace_path), "rows": int(len(decomposition)), "stage_counts": stage_counts, "top_buckets": top_buckets}


def _classify_china_301_key_stage(row: pd.Series) -> str:
    if int(row.get("duplicate_reference_key_rows") or 0) > 1:
        return "duplicate_reference_key"
    if int(row.get("duplicate_raw_key_rows") or 0) > 1:
        return "duplicate_raw_key"
    if not bool(row.get("ref_active")) or int(row.get("ref_m_china_hit") or 0) != 1 or int(row.get("cty_code") or 0) != 5700:
        return "non_china_or_inactive_reference_leak"
    if not bool(row.get("raw_key_present")):
        if bool(row.get("overlay_hs8_month_present")):
            return "hs8_overlay_present_exact_hs10_absent"
        return "raw_key_absent"
    if pd.isna(row.get("raw_panel_increment")):
        return "raw_key_present_no_increment"
    discrepancy_type = str(row.get("discrepancy_type") or "")
    if discrepancy_type == "statutory_rate_mismatch":
        return "statutory_rate_mismatch"
    if discrepancy_type == "day_weighted_rate_mismatch":
        return "day_weighted_rate_mismatch"
    if discrepancy_type in {"missing_raw_policy_scope", "missing_raw_key"}:
        return "panel_increment_present_but_validation_mismatch"
    return "requires_full_model_review"


def build_china_301_key_trace_from_artifacts(config: PipelineConfig) -> dict[str, Any]:
    """Build an exact-key China 301 trace from the current validation artifacts."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    disc_path = output_dir / "raw_replication_discrepancies.parquet"
    if not disc_path.exists():
        raise FileNotFoundError(f"Missing raw replication discrepancies artifact: {disc_path}")

    columns = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "ref_active",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_m_effective_mdate2",
        "discrepancy_type",
    ]
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        con = duckdb.connect()
        try:
            escaped_path = str(disc_path).replace("'", "''")
            query = f"""
                SELECT
                    CAST(cty_code AS BIGINT) AS cty_code,
                    CAST(hs10 AS VARCHAR) AS hs10,
                    CAST(year AS BIGINT) AS year,
                    CAST(month AS BIGINT) AS month,
                    CAST(ref_m_status2 AS DOUBLE) AS ref_m_status2,
                    CAST(ref_m_china_hit AS DOUBLE) AS ref_m_china_hit,
                    CAST(ref_m_stattariff1 AS DOUBLE) AS ref_m_stattariff1,
                    CAST(ref_m_stattariff2 AS DOUBLE) AS ref_m_stattariff2,
                    CAST(ref_m_effective_mdate2 AS VARCHAR) AS ref_m_effective_mdate2,
                    CAST(discrepancy_type AS VARCHAR) AS discrepancy_type
                FROM read_parquet('{escaped_path}')
                WHERE CAST(cty_code AS BIGINT) = 5700
                  AND CAST(ref_m_china_hit AS DOUBLE) = 1
                  AND CAST(ref_m_status2 AS DOUBLE) > 0
                  AND CAST(discrepancy_type AS VARCHAR) IN (
                      'missing_raw_policy_scope',
                      'missing_raw_key',
                      'statutory_rate_mismatch',
                      'day_weighted_rate_mismatch'
                  )
            """
            frame = con.execute(query).fetchdf()
        finally:
            con.close()
    else:
        frame = read_table(disc_path, columns=columns)
        frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
        frame["ref_m_status2"] = pd.to_numeric(frame["ref_m_status2"], errors="coerce")
        frame["ref_m_china_hit"] = pd.to_numeric(frame["ref_m_china_hit"], errors="coerce")
        frame = frame.loc[
            frame["cty_code"].eq(5700)
            & frame["ref_m_china_hit"].eq(1)
            & frame["ref_m_status2"].gt(0)
            & frame["discrepancy_type"].isin(
                [
                    "missing_raw_policy_scope",
                    "missing_raw_key",
                    "statutory_rate_mismatch",
                    "day_weighted_rate_mismatch",
                ]
            )
        ].copy()

    trace_path = output_dir / "raw_replication_china_301_key_trace.csv"
    empty_cols = [
        "cty_code",
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_active",
        "ref_m_status2",
        "ref_m_china_hit",
        "ref_m_stattariff1",
        "ref_m_stattariff2",
        "ref_effective_period",
        "raw_key_present",
        "raw_panel_hs10_present",
        "raw_panel_hs8_month_present",
        "raw_panel_increment",
        "raw_panel_rule_code",
        "raw_panel_policy_source",
        "overlay_hs8_month_present",
        "overlay_increment",
        "overlay_rule_code",
        "discrepancy_type",
        "duplicate_reference_key_rows",
        "duplicate_raw_key_rows",
        "diagnosed_stage",
    ]
    if frame.empty:
        empty = pd.DataFrame(columns=empty_cols)
        empty.to_csv(trace_path, index=False)
        return {"trace_path": str(trace_path), "rows": 0, "stage_counts": {}, "top_buckets": []}

    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    frame["ref_m_status2"] = pd.to_numeric(frame["ref_m_status2"], errors="coerce")
    frame["ref_m_china_hit"] = pd.to_numeric(frame["ref_m_china_hit"], errors="coerce")
    frame["ref_m_stattariff1"] = pd.to_numeric(frame["ref_m_stattariff1"], errors="coerce")
    frame["ref_m_stattariff2"] = pd.to_numeric(frame["ref_m_stattariff2"], errors="coerce")
    frame["ref_active"] = frame["ref_m_status2"].gt(0).fillna(False)
    frame["ref_effective_period"] = _effective_period(frame["ref_m_effective_mdate2"])

    priority = {
        "missing_raw_policy_scope": 0,
        "missing_raw_key": 1,
        "statutory_rate_mismatch": 2,
        "day_weighted_rate_mismatch": 3,
    }
    grouped_rows: list[dict[str, Any]] = []
    for _, bucket in frame.groupby(["cty_code", "hs10", "year", "month"], dropna=False, observed=True):
        bucket = bucket.copy()
        discrepancy_counts = bucket["discrepancy_type"].value_counts(dropna=False)
        discrepancy_type = sorted(discrepancy_counts.index.tolist(), key=lambda value: priority.get(str(value), 99))[0]
        grouped_rows.append(
            {
                "cty_code": int(bucket["cty_code"].iloc[0]),
                "hs10": str(bucket["hs10"].iloc[0]),
                "hs8": str(bucket["hs8"].iloc[0]),
                "year": int(bucket["year"].iloc[0]),
                "month": int(bucket["month"].iloc[0]),
                "ref_active": bool(bucket["ref_active"].iloc[0]),
                "ref_m_status2": float(bucket["ref_m_status2"].iloc[0]) if pd.notna(bucket["ref_m_status2"].iloc[0]) else pd.NA,
                "ref_m_china_hit": float(bucket["ref_m_china_hit"].iloc[0]) if pd.notna(bucket["ref_m_china_hit"].iloc[0]) else pd.NA,
                "ref_m_stattariff1": float(bucket["ref_m_stattariff1"].iloc[0]) if pd.notna(bucket["ref_m_stattariff1"].iloc[0]) else pd.NA,
                "ref_m_stattariff2": float(bucket["ref_m_stattariff2"].iloc[0]) if pd.notna(bucket["ref_m_stattariff2"].iloc[0]) else pd.NA,
                "ref_effective_period": str(bucket["ref_effective_period"].iloc[0]),
                "discrepancy_type": str(discrepancy_type),
                "duplicate_reference_key_rows": int(len(bucket)),
                "duplicate_raw_key_rows": 0,
            }
        )

    trace = pd.DataFrame(grouped_rows)
    key_frame = trace.loc[:, ["cty_code", "hs10", "year", "month"]].drop_duplicates().copy()
    overlay_buckets = trace.loc[:, ["hs8", "year", "month"]].drop_duplicates().copy()

    exact_panel = _load_china_301_key_panel_slice(config, key_frame)
    if exact_panel is None:
        exact_panel = pd.DataFrame(columns=["cty_code", "cty_name", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw", "hs8"])
    hs10_panel = _load_china_301_key_hs10_slice(config, key_frame)
    if hs10_panel is None:
        hs10_panel = pd.DataFrame(columns=["cty_code", "cty_name", "hs10", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw", "hs8"])
    overlay = _load_china_301_key_overlay_slice(config, overlay_buckets)
    if overlay is None:
        overlay = pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"])
    hs8_panel = _load_china_301_panel_slice(config, trace.loc[:, ["hs8", "year", "month"]].drop_duplicates())
    if hs8_panel is None:
        hs8_panel = pd.DataFrame(columns=["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"])

    exact_counts = exact_panel.groupby(["cty_code", "hs10", "year", "month"], dropna=False, observed=True).size() if not exact_panel.empty else pd.Series(dtype="int64")
    hs10_counts = hs10_panel.groupby(["hs10", "year", "month"], dropna=False, observed=True).size() if not hs10_panel.empty else pd.Series(dtype="int64")
    hs8_counts = hs8_panel.groupby(["hs8", "year", "month"], dropna=False, observed=True).size() if not hs8_panel.empty else pd.Series(dtype="int64")
    overlay_counts = overlay.groupby(["hs8", "year", "month"], dropna=False, observed=True).size() if not overlay.empty else pd.Series(dtype="int64")

    exact_index = pd.MultiIndex.from_frame(trace.loc[:, ["cty_code", "hs10", "year", "month"]])
    hs10_index = pd.MultiIndex.from_frame(trace.loc[:, ["hs10", "year", "month"]])
    hs8_index = pd.MultiIndex.from_frame(trace.loc[:, ["hs8", "year", "month"]])
    trace["raw_key_present"] = exact_index.isin(exact_counts.index)
    trace["duplicate_raw_key_rows"] = exact_index.map(exact_counts).fillna(0).astype("Int64")
    trace["raw_panel_hs10_present"] = hs10_index.isin(hs10_counts.index)
    trace["raw_panel_hs8_month_present"] = hs8_index.isin(hs8_counts.index)
    trace["overlay_hs8_month_present"] = hs8_index.isin(overlay_counts.index)

    def _lookup(frame: pd.DataFrame, key_cols: list[str], value_col: str) -> pd.Series:
        if frame.empty or value_col not in frame.columns:
            return pd.Series([pd.NA] * len(trace), index=trace.index, dtype="object")
        mapping = frame.set_index(key_cols)[value_col].to_dict()
        return pd.Series(
            [mapping.get(tuple(values), pd.NA) for values in trace.loc[:, key_cols].itertuples(index=False, name=None)],
            index=trace.index,
        )

    trace["raw_panel_increment"] = _lookup(exact_panel, ["cty_code", "hs10", "year", "month"], "tw_increment_rate_raw")
    trace["raw_panel_rule_code"] = _lookup(exact_panel, ["cty_code", "hs10", "year", "month"], "tw_rule_code_raw")
    trace["raw_panel_policy_source"] = _lookup(exact_panel, ["cty_code", "hs10", "year", "month"], "tw_scope_source_raw")
    trace["raw_panel_hs10_increment"] = _lookup(hs10_panel, ["hs10", "year", "month"], "tw_increment_rate_raw")
    trace["raw_panel_hs10_rule_code"] = _lookup(hs10_panel, ["hs10", "year", "month"], "tw_rule_code_raw")
    trace["raw_panel_hs10_policy_source"] = _lookup(hs10_panel, ["hs10", "year", "month"], "tw_scope_source_raw")
    trace["raw_panel_hs8_increment"] = _lookup(hs8_panel, ["hs8", "year", "month"], "tw_increment_rate_raw")
    trace["raw_panel_hs8_rule_code"] = _lookup(hs8_panel, ["hs8", "year", "month"], "tw_rule_code_raw")
    trace["raw_panel_hs8_policy_source"] = _lookup(hs8_panel, ["hs8", "year", "month"], "tw_scope_source_raw")
    trace["overlay_increment"] = _lookup(overlay, ["hs8", "year", "month"], "tw_increment_rate_raw")
    trace["overlay_rule_code"] = _lookup(overlay, ["hs8", "year", "month"], "tw_rule_code_raw")

    trace["raw_key_present"] = trace["raw_key_present"].fillna(False)
    trace["raw_panel_hs10_present"] = trace["raw_panel_hs10_present"].fillna(False)
    trace["raw_panel_hs8_month_present"] = trace["raw_panel_hs8_month_present"].fillna(False)
    trace["overlay_hs8_month_present"] = trace["overlay_hs8_month_present"].fillna(False)
    trace["duplicate_raw_key_rows"] = pd.to_numeric(trace["duplicate_raw_key_rows"], errors="coerce").fillna(0).astype("Int64")
    trace["raw_panel_increment"] = pd.to_numeric(trace["raw_panel_increment"], errors="coerce")
    trace["overlay_increment"] = pd.to_numeric(trace["overlay_increment"], errors="coerce")
    trace["diagnosed_stage"] = trace.apply(_classify_china_301_key_stage, axis=1)
    trace = trace.loc[
        :,
        [
            "cty_code",
            "hs10",
            "hs8",
            "year",
            "month",
            "ref_active",
            "ref_m_status2",
            "ref_m_china_hit",
            "ref_m_stattariff1",
            "ref_m_stattariff2",
            "ref_effective_period",
            "raw_key_present",
            "raw_panel_hs10_present",
            "raw_panel_hs8_month_present",
            "raw_panel_increment",
            "raw_panel_rule_code",
            "raw_panel_policy_source",
            "overlay_hs8_month_present",
            "overlay_increment",
            "overlay_rule_code",
            "discrepancy_type",
            "duplicate_reference_key_rows",
            "duplicate_raw_key_rows",
            "diagnosed_stage",
        ],
    ].copy()
    trace = trace.fillna({"duplicate_reference_key_rows": 0, "duplicate_raw_key_rows": 0}).sort_values(
        ["discrepancy_type", "duplicate_reference_key_rows", "year", "month", "hs10"],
        ascending=[True, False, True, True, True],
    ).reset_index(drop=True)
    trace.to_csv(trace_path, index=False)
    stage_counts = trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict()
    top_buckets = trace.head(20).to_dict(orient="records")
    summary = {
        "trace_path": str(trace_path),
        "rows": int(len(trace)),
        "stage_counts": stage_counts,
        "top_buckets": top_buckets,
        "problem_shares": {
            "exact_raw_key_absent": float((trace["diagnosed_stage"].eq("raw_key_absent")).mean()) if len(trace) else 0.0,
            "exact_raw_key_present_no_increment": float((trace["diagnosed_stage"].eq("raw_key_present_no_increment")).mean()) if len(trace) else 0.0,
            "exact_validation_mismatch": float((trace["diagnosed_stage"].eq("panel_increment_present_but_validation_mismatch")).mean()) if len(trace) else 0.0,
            "duplicate_reference_key": float((trace["diagnosed_stage"].eq("duplicate_reference_key")).mean()) if len(trace) else 0.0,
            "duplicate_raw_key": float((trace["diagnosed_stage"].eq("duplicate_raw_key")).mean()) if len(trace) else 0.0,
        },
    }
    return summary


def _artifact_mtime(path: Path) -> datetime | None:
    if not path.exists():
        return None
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return None


def _count_rows_for_artifact(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            return int(len(read_table(path)))
        if suffix == ".csv":
            return int(len(pd.read_csv(path)))
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return int(len(payload))
            if isinstance(payload, dict):
                return 1
            return None
    except Exception:
        return None
    return None


def _load_china_301_rule_assignment_base(output_dir: Path) -> pd.DataFrame:
    """Load the benchmark-definition rows used to build the China 301 rule-assignment audit."""
    candidates = (
        output_dir / "raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv",
        output_dir / "raw_replication_china_301_benchmark_definition_trace.csv",
    )
    for path in candidates:
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _load_china_301_wave_provenance(output_dir: Path, config: PipelineConfig) -> pd.DataFrame:
    """Load the provenance rows from the current China 301 wave audit, building it if needed."""
    audit_path = output_dir / "raw_replication_china_301_wave_link_audit.csv"
    if not audit_path.exists():
        try:
            build_china_301_wave_link_audit_from_artifacts(config)
        except Exception:
            pass
    if not audit_path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(audit_path)
    except Exception:
        return pd.DataFrame()
    if frame.empty:
        return frame
    for column in ("release_start_date", "release_end_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    for column in ("hs8", "rule_code"):
        if column in frame.columns:
            frame[column] = frame[column].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    for column in ("rule_found_in_same_row", "rule_found_only_in_context"):
        if column in frame.columns:
            frame[column] = frame[column].map(
                lambda value: str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
                if pd.notna(value)
                else False
            ).astype(bool)
    for column in ("source_page", "source_row"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _parse_structural_note_identifier(
    text: object,
    source_file: object = None,
    source_page: object = None,
    source_row: object = None,
) -> str | pd.NA:
    """Extract a machine-readable structural note identifier from source text."""
    value = "" if pd.isna(text) else str(text).strip()
    if value:
        patterns = (
            re.compile(r"(U\.S\.\s*note\s+\d+(?:\([a-z0-9]+\))?)", re.I),
            re.compile(r"(\bnote\s+\d+(?:\([a-z0-9]+\))?)", re.I),
            re.compile(r"(\bsubparagraph\s+[a-z0-9]+\b)", re.I),
        )
        for pattern in patterns:
            match = pattern.search(value)
            if match:
                return re.sub(r"\s+", " ", match.group(1)).strip()
    if pd.notna(source_file) or pd.notna(source_page) or pd.notna(source_row):
        parts = [str(part) for part in (source_file, source_page, source_row) if pd.notna(part)]
        if parts:
            return ":".join(parts)
    return pd.NA


def _collect_rule_codes(values: pd.Series | list[object]) -> list[str]:
    codes: list[str] = []
    for value in values:
        code = normalize_hs_code(value, 8)
        if code:
            codes.append(str(code))
    return sorted(set(codes))


def _policy_family_for_rule_code(rule_code: object) -> str:
    code = normalize_hs_code(rule_code, 8)
    if not code:
        return "unknown"
    code = str(code)
    if code.startswith("990388"):
        return "section_301"
    if code.startswith("990380"):
        return "section_232_steel"
    if code.startswith("990385"):
        return "section_232_aluminum"
    if code.startswith("990345"):
        return "section_201_washers"
    if code.startswith("990346"):
        return "section_201_solar"
    return "other"


def _build_china_301_rule_assignment_candidates(frame: pd.DataFrame, provenance: pd.DataFrame) -> pd.DataFrame:
    """Expand the audit selection into one row per provenance candidate."""
    columns = [
        "hs8",
        "candidate_rule_code",
        "policy_family",
        "release_name",
        "release_start_date",
        "source_file",
        "source_page",
        "source_row",
        "structural_note_identifier",
        "extraction_method",
        "matched_rule_text",
        "rule_found_in_same_row",
        "rule_found_only_in_context",
        "same_structural_note_block",
        "cross_family_candidate",
        "source_priority",
    ]
    if frame.empty or provenance.empty:
        return pd.DataFrame(columns=columns)

    prov = provenance.copy()
    if "hs8" in prov.columns:
        prov["hs8"] = prov["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    if "rule_code" in prov.columns:
        prov["candidate_rule_code"] = prov["rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    else:
        prov["candidate_rule_code"] = pd.NA
    if "release_start_date" in prov.columns:
        prov["release_start_date"] = pd.to_datetime(prov["release_start_date"], errors="coerce")
    else:
        prov["release_start_date"] = pd.NaT
    for column in ("rule_found_in_same_row", "rule_found_only_in_context"):
        if column in prov.columns:
            prov[column] = prov[column].map(
                lambda value: str(value).strip().lower() in {"1", "true", "t", "yes", "y"} if pd.notna(value) else False
            ).astype(bool)
        else:
            prov[column] = False
    for column in ("release_name", "source_file", "source_page", "source_row", "matched_rule_text", "extraction_method", "structural_note_identifier"):
        if column not in prov.columns:
            prov[column] = pd.NA
    prov["source_page"] = pd.to_numeric(prov["source_page"], errors="coerce")
    prov["source_row"] = pd.to_numeric(prov["source_row"], errors="coerce")

    selected_hs8 = {str(value) for value in frame["hs8"].dropna().astype(str)}
    prov = prov.loc[prov["hs8"].isin(selected_hs8)].copy()
    if prov.empty:
        return pd.DataFrame(columns=columns)

    prov["policy_family"] = prov["candidate_rule_code"].map(_policy_family_for_rule_code).astype("string")
    prov["same_structural_note_block"] = (prov["rule_found_in_same_row"] | prov["rule_found_only_in_context"]).astype(bool)
    prov["cross_family_candidate"] = prov["policy_family"].ne("section_301")
    prov["source_priority"] = np.select(
        [prov["rule_found_in_same_row"], prov["rule_found_only_in_context"], prov["cross_family_candidate"]],
        [0, 1, 2],
        default=3,
    ).astype("int64")

    return (
        prov.loc[
            :,
            [
                "hs8",
                "candidate_rule_code",
                "policy_family",
                "release_name",
                "release_start_date",
                "source_file",
                "source_page",
                "source_row",
                "structural_note_identifier",
                "extraction_method",
                "matched_rule_text",
                "rule_found_in_same_row",
                "rule_found_only_in_context",
                "same_structural_note_block",
                "cross_family_candidate",
                "source_priority",
            ],
        ]
        .drop_duplicates()
        .sort_values(["hs8", "source_priority", "candidate_rule_code", "release_name", "source_page", "source_row"], ascending=[True, True, True, True, True, True])
        .reset_index(drop=True)
    )


def _classify_china_301_rule_assignment_stage(row: pd.Series) -> str:
    candidate_count = int(pd.to_numeric(pd.Series([row.get("candidate_rule_count")]), errors="coerce").fillna(0).iloc[0])
    cross_family_count = int(pd.to_numeric(pd.Series([row.get("cross_family_candidate_count")]), errors="coerce").fillna(0).iloc[0])
    raw_increment = pd.to_numeric(pd.Series([row.get("raw_panel_increment")]), errors="coerce").iloc[0]
    raw_rule = normalize_hs_code(row.get("raw_panel_rule_code"), 8)
    overlay_rule = normalize_hs_code(row.get("overlay_rule_code"), 8)
    earliest_rule = normalize_hs_code(row.get("earliest_301_rule"), 8)
    latest_rule = normalize_hs_code(row.get("latest_301_rule"), 8)
    matched_text_value = row.get("matched_rule_text")
    matched_text = "" if pd.isna(matched_text_value) else str(matched_text_value).lower()
    extraction_method_value = row.get("extraction_method")
    extraction_method = "" if pd.isna(extraction_method_value) else str(extraction_method_value).lower()
    year = int(pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").fillna(0).iloc[0])
    month = int(pd.to_numeric(pd.Series([row.get("month")]), errors="coerce").fillna(0).iloc[0])
    discrepancy_type = str(row.get("discrepancy_type") or "")

    if candidate_count <= 0 or (pd.isna(row.get("source_file")) and pd.isna(row.get("source_page"))):
        return "absent_from_official_extract"
    if cross_family_count > 0:
        return "parser_cross_family_context_bleed"
    if candidate_count > 1 and pd.notna(raw_rule) and pd.notna(overlay_rule) and str(raw_rule) != str(overlay_rule):
        if earliest_rule and latest_rule and earliest_rule != latest_rule:
            return "later_wave_overwrite" if str(raw_rule) == str(latest_rule) else "parser_cross_rule_context_bleed"
        return "parser_cross_rule_context_bleed"
    if pd.isna(raw_increment):
        if year == 2018 and month in {7, 8} and earliest_rule:
            return "early_wave_link_missing"
        if "u.s. note" in matched_text and "990388" in matched_text:
            return "early_wave_link_missing"
        return "requires_full_model_review"
    if discrepancy_type == "day_weighted_rate_mismatch":
        return "timing_convention_only"
    if "except as provided" in matched_text or "exclusion" in matched_text or "granted by the u.s. trade representative" in matched_text:
        return "legal_interpretation_required"
    if "context" in extraction_method and (str(raw_rule or overlay_rule).startswith("990388") or "990388" in matched_text):
        return "parser_cross_rule_context_bleed"
    if raw_rule and earliest_rule and str(raw_rule) == str(earliest_rule):
        return "valid_earliest_wave_persistence"
    if raw_rule and latest_rule and str(raw_rule) == str(latest_rule) and earliest_rule and str(earliest_rule) != str(latest_rule):
        return "valid_source_rule_change"
    return "requires_full_model_review"


def build_china_301_rule_assignment_trace_from_artifacts(config: PipelineConfig) -> dict[str, Any]:
    """Build a rule-assignment trace for the remaining China 301 benchmark gap."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    base = _load_china_301_rule_assignment_base(output_dir)
    trace_path = output_dir / "raw_replication_china_301_rule_assignment_trace.csv"
    empty_cols = [
        "hs10",
        "hs8",
        "year",
        "month",
        "ref_effective_period",
        "ref_m_status2",
        "ref_m_stattariff1",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_panel_rule_code",
        "overlay_increment",
        "overlay_rule_code",
        "machine_rule_codes",
        "pdf_rule_codes",
        "official_release_name",
        "official_release_start_date",
        "source_file",
        "source_page",
        "source_row",
        "extraction_method",
        "matched_rule_text",
        "rule_found_in_same_row",
        "rule_found_only_in_context",
        "structural_note_identifier",
        "candidate_rule_count",
        "cross_family_candidate_count",
        "earliest_301_rule",
        "latest_301_rule",
        "diagnosed_stage",
    ]
    if base.empty:
        empty = pd.DataFrame(columns=empty_cols)
        empty.to_csv(trace_path, index=False)
        candidate_path = output_dir / "raw_replication_china_301_rule_assignment_candidates.csv"
        pd.DataFrame(columns=[
            "hs8",
            "candidate_rule_code",
            "policy_family",
            "release_name",
            "release_start_date",
            "source_file",
            "source_page",
            "source_row",
            "structural_note_identifier",
            "extraction_method",
            "matched_rule_text",
            "rule_found_in_same_row",
            "rule_found_only_in_context",
            "same_structural_note_block",
            "cross_family_candidate",
            "source_priority",
        ]).to_csv(candidate_path, index=False)
        return {"trace_path": str(trace_path), "candidate_path": str(candidate_path), "rows": 0, "stage_counts": {}, "top_buckets": []}

    required = [
        "hs10",
        "year",
        "month",
        "ref_effective_period",
        "ref_m_status2",
        "ref_m_stattariff1",
        "raw_base_statutory_rate_raw",
        "raw_panel_increment",
        "raw_panel_rule_code",
        "overlay_increment",
        "overlay_rule_code",
        "discrepancy_type",
    ]
    for column in required:
        if column not in base.columns:
            base[column] = pd.NA

    if "diagnosed_stage" not in base.columns:
        base["diagnosed_stage"] = pd.NA

    if "cty_code" in base.columns:
        base["cty_code"] = pd.to_numeric(base["cty_code"], errors="coerce").astype("Int64")
    else:
        base["cty_code"] = pd.Series([5700] * len(base), index=base.index, dtype="Int64")
    if "hs10" in base.columns:
        base["hs10"] = base["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    if "year" in base.columns:
        base["year"] = pd.to_numeric(base["year"], errors="coerce").astype("Int64")
    if "month" in base.columns:
        base["month"] = pd.to_numeric(base["month"], errors="coerce").astype("Int64")

    status_lookup = pd.DataFrame()
    status_path = output_dir / "raw_replication_china_301_key_trace.csv"
    if status_path.exists():
        try:
            status_lookup = pd.read_csv(status_path, usecols=lambda col: col in {"cty_code", "hs10", "year", "month", "ref_m_status2"})
        except Exception:
            status_lookup = pd.DataFrame()
    if status_lookup.empty:
        disc_path = output_dir / "raw_replication_discrepancies.parquet"
        if disc_path.exists():
            try:
                status_lookup = read_table(disc_path, columns=["cty_code", "hs10", "year", "month", "ref_m_status2"])
            except Exception:
                status_lookup = pd.DataFrame()
    if not status_lookup.empty:
        status_lookup = status_lookup.copy()
        status_lookup["cty_code"] = pd.to_numeric(status_lookup["cty_code"], errors="coerce").astype("Int64")
        status_lookup["hs10"] = status_lookup["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        status_lookup["year"] = pd.to_numeric(status_lookup["year"], errors="coerce").astype("Int64")
        status_lookup["month"] = pd.to_numeric(status_lookup["month"], errors="coerce").astype("Int64")
        status_lookup["ref_m_status2"] = pd.to_numeric(status_lookup["ref_m_status2"], errors="coerce")
        status_lookup = status_lookup.dropna(subset=KEYS).drop_duplicates(KEYS, keep="first")
        base = base.merge(status_lookup, on=KEYS, how="left", suffixes=("", "_status"))
        if "ref_m_status2_status" in base.columns:
            base["ref_m_status2"] = base["ref_m_status2"].where(base["ref_m_status2"].notna(), base["ref_m_status2_status"])
            base = base.drop(columns=["ref_m_status2_status"])

    frame = base.loc[
        base["discrepancy_type"].eq("missing_raw_policy_scope")
        | base["diagnosed_stage"].eq("benchmark_increment_definition_difference")
    ].copy()
    if frame.empty:
        empty = pd.DataFrame(columns=empty_cols)
        empty.to_csv(trace_path, index=False)
        return {"trace_path": str(trace_path), "rows": 0, "stage_counts": {}, "top_buckets": []}

    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["hs8"] = frame["hs10"].astype("string").str.slice(0, 8)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["ref_m_status2"] = pd.to_numeric(frame["ref_m_status2"], errors="coerce")
    frame["ref_m_stattariff1"] = pd.to_numeric(frame["ref_m_stattariff1"], errors="coerce")
    frame["raw_base_statutory_rate_raw"] = pd.to_numeric(frame["raw_base_statutory_rate_raw"], errors="coerce")
    frame["raw_panel_increment"] = pd.to_numeric(frame["raw_panel_increment"], errors="coerce")
    frame["overlay_increment"] = pd.to_numeric(frame["overlay_increment"], errors="coerce")
    frame["raw_panel_rule_code"] = frame["raw_panel_rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    frame["overlay_rule_code"] = frame["overlay_rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    if "ref_m_effective_mdate2" in frame.columns:
        frame["ref_effective_period"] = _effective_period(frame["ref_m_effective_mdate2"])
    else:
        frame["ref_effective_period"] = frame["ref_effective_period"].astype("string")

    machine_links = _load_tradewar_machine_links(config)
    pdf_links = _load_tradewar_pdf_links(config)
    provenance = _load_china_301_wave_provenance(output_dir, config)

    if not machine_links.empty and {"hs8", "rule_code"}.issubset(machine_links.columns):
        machine_links = machine_links.copy()
        machine_links["hs8"] = machine_links["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        machine_links["rule_code"] = machine_links["rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    if not pdf_links.empty and {"hs8", "rule_code"}.issubset(pdf_links.columns):
        pdf_links = pdf_links.copy()
        pdf_links["hs8"] = pdf_links["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        pdf_links["rule_code"] = pdf_links["rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    if not provenance.empty and {"hs8", "rule_code"}.issubset(provenance.columns):
        provenance = provenance.copy()
        provenance["hs8"] = provenance["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        provenance["rule_code"] = provenance["rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")

    candidate_path = output_dir / "raw_replication_china_301_rule_assignment_candidates.csv"
    candidate_frame = _build_china_301_rule_assignment_candidates(frame, provenance)
    candidate_frame.to_csv(candidate_path, index=False)

    machine_map = (
        machine_links.groupby("hs8", dropna=False, observed=True)["rule_code"].apply(_collect_rule_codes).to_dict()
        if not machine_links.empty and {"hs8", "rule_code"}.issubset(machine_links.columns)
        else {}
    )
    pdf_map = (
        pdf_links.groupby("hs8", dropna=False, observed=True)["rule_code"].apply(_collect_rule_codes).to_dict()
        if not pdf_links.empty and {"hs8", "rule_code"}.issubset(pdf_links.columns)
        else {}
    )
    provenance_map = {
        str(hs8): group.copy()
        for hs8, group in provenance.groupby("hs8", dropna=False, observed=True)
        if not group.empty
    } if not provenance.empty and {"hs8", "rule_code"}.issubset(provenance.columns) else {}

    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        hs8 = str(row.get("hs8") or "")
        provenance_frame = provenance_map.get(hs8, pd.DataFrame())
        candidate_codes = sorted(set(machine_map.get(hs8, [])) | set(pdf_map.get(hs8, [])))
        if not provenance_frame.empty and "rule_code" in provenance_frame.columns:
            candidate_codes = sorted(set(candidate_codes) | set(_collect_rule_codes(provenance_frame["rule_code"])))
        cross_family_codes = [code for code in candidate_codes if not str(code).startswith("990388")]
        china_301_codes = [code for code in candidate_codes if str(code).startswith("990388")]

        chosen_rule = normalize_hs_code(row.get("raw_panel_rule_code"), 8)
        if (pd.isna(chosen_rule) or not str(chosen_rule).strip()) and pd.notna(row.get("overlay_rule_code")):
            chosen_rule = normalize_hs_code(row.get("overlay_rule_code"), 8)
        if (pd.isna(chosen_rule) or not str(chosen_rule).strip()) and china_301_codes:
            chosen_rule = china_301_codes[0]

        provenance_subset = provenance_frame
        if not provenance_subset.empty:
            if pd.notna(chosen_rule) and str(chosen_rule).strip() and "rule_code" in provenance_subset.columns:
                exact = provenance_subset.loc[provenance_subset["rule_code"].eq(str(chosen_rule))].copy()
                if not exact.empty:
                    provenance_subset = exact
            elif china_301_codes:
                exact = provenance_subset.loc[provenance_subset["rule_code"].isin(china_301_codes)].copy()
                if not exact.empty:
                    provenance_subset = exact
            provenance_subset = provenance_subset.sort_values(
                [
                    "rule_found_in_same_row",
                    "rule_found_only_in_context",
                    "release_start_date",
                    "source_page",
                    "source_row",
                ],
                ascending=[False, True, True, True, True],
                na_position="last",
            )
        provenance_row = provenance_subset.iloc[0] if not provenance_subset.empty else pd.Series(dtype="object")

        machine_rule_codes = ";".join(machine_map.get(hs8, []))
        pdf_rule_codes = ";".join(pdf_map.get(hs8, []))
        candidate_rule_count = len(candidate_codes)
        cross_family_candidate_count = len(cross_family_codes)
        earliest_301_rule = china_301_codes[0] if china_301_codes else pd.NA
        latest_301_rule = china_301_codes[-1] if china_301_codes else pd.NA
        source_file = provenance_row.get("source_file", pd.NA) if not provenance_row.empty else pd.NA
        source_page = provenance_row.get("source_page", pd.NA) if not provenance_row.empty else pd.NA
        source_row = provenance_row.get("source_row", pd.NA) if not provenance_row.empty else pd.NA
        extraction_method = provenance_row.get("extraction_method", pd.NA) if not provenance_row.empty else pd.NA
        matched_rule_text = provenance_row.get("matched_rule_text", pd.NA) if not provenance_row.empty else pd.NA
        rule_found_in_same_row = bool(provenance_row.get("rule_found_in_same_row", False)) if not provenance_row.empty else False
        rule_found_only_in_context = bool(provenance_row.get("rule_found_only_in_context", False)) if not provenance_row.empty else False
        structural_note_identifier = _parse_structural_note_identifier(matched_rule_text, source_file, source_page, source_row)

        row_dict = {
            "hs10": str(row.get("hs10")),
            "hs8": hs8,
            "year": int(pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").fillna(0).iloc[0]),
            "month": int(pd.to_numeric(pd.Series([row.get("month")]), errors="coerce").fillna(0).iloc[0]),
            "ref_effective_period": str(row.get("ref_effective_period") or pd.NA),
            "ref_m_status2": float(row.get("ref_m_status2")) if pd.notna(row.get("ref_m_status2")) else pd.NA,
            "ref_m_stattariff1": float(row.get("ref_m_stattariff1")) if pd.notna(row.get("ref_m_stattariff1")) else pd.NA,
            "raw_base_statutory_rate_raw": float(row.get("raw_base_statutory_rate_raw")) if pd.notna(row.get("raw_base_statutory_rate_raw")) else pd.NA,
            "raw_panel_increment": float(row.get("raw_panel_increment")) if pd.notna(row.get("raw_panel_increment")) else pd.NA,
            "raw_panel_rule_code": row.get("raw_panel_rule_code", pd.NA),
            "overlay_increment": float(row.get("overlay_increment")) if pd.notna(row.get("overlay_increment")) else pd.NA,
            "overlay_rule_code": row.get("overlay_rule_code", pd.NA),
            "machine_rule_codes": machine_rule_codes,
            "pdf_rule_codes": pdf_rule_codes,
            "official_release_name": provenance_row.get("release_name", pd.NA) if not provenance_row.empty else pd.NA,
            "official_release_start_date": provenance_row.get("release_start_date", pd.NaT) if not provenance_row.empty else pd.NaT,
            "source_file": source_file,
            "source_page": source_page,
            "source_row": source_row,
            "extraction_method": extraction_method,
            "matched_rule_text": matched_rule_text,
            "rule_found_in_same_row": rule_found_in_same_row,
            "rule_found_only_in_context": rule_found_only_in_context,
            "structural_note_identifier": structural_note_identifier,
            "candidate_rule_count": candidate_rule_count,
            "cross_family_candidate_count": cross_family_candidate_count,
            "earliest_301_rule": earliest_301_rule,
            "latest_301_rule": latest_301_rule,
        }
        row_dict["diagnosed_stage"] = _classify_china_301_rule_assignment_stage(pd.Series({**row.to_dict(), **row_dict}))
        rows.append(row_dict)

    trace = pd.DataFrame(rows, columns=empty_cols)
    trace.to_csv(trace_path, index=False)
    stage_counts = trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict() if not trace.empty else {}
    top_buckets = trace.head(20).to_dict(orient="records") if not trace.empty else []
    return {"trace_path": str(trace_path), "candidate_path": str(candidate_path), "rows": int(len(trace)), "stage_counts": stage_counts, "top_buckets": top_buckets}


def _raw_replication_artifact_specs() -> tuple[tuple[str, str], ...]:
    return (
        ("raw_replication_metrics.csv", "metrics"),
        ("raw_replication_discrepancies.parquet", "discrepancies"),
        ("raw_replication_by_family.csv", "by_family"),
        ("raw_replication_by_country.csv", "by_country"),
        ("raw_replication_by_hs2.csv", "by_hs2"),
        ("raw_replication_by_rate_bucket.csv", "by_rate_bucket"),
        ("raw_replication_by_type.csv", "by_type"),
        ("raw_replication_by_year_month.csv", "by_year_month"),
        ("raw_replication_source_health.csv", "source_health"),
        ("raw_replication_source_health.json", "source_health_summary"),
        ("raw_replication_china_301_source_audit.csv", "china_301_source_audit"),
        ("raw_replication_china_301_source_audit_errors.json", "china_301_source_audit_errors"),
        ("raw_replication_china_301_trace.csv", "china_301_trace"),
        ("raw_replication_china_301_rule_assignment_trace.csv", "china_301_rule_assignment_trace"),
        ("raw_replication_china_301_key_trace.csv", "china_301_key_trace"),
        ("raw_replication_china_301_rate_trace.csv", "china_301_rate_trace"),
        ("raw_replication_china_301_rate_timing_trace.csv", "china_301_rate_timing_trace"),
        ("raw_replication_china_301_rate_timing_by_month.csv", "china_301_rate_timing_by_month"),
        ("raw_replication_china_301_rate_timing_by_rule.csv", "china_301_rate_timing_by_rule"),
        ("raw_replication_china_301_rate_timing_by_stage.csv", "china_301_rate_timing_by_stage"),
        ("raw_replication_china_301_rate_timing_quantiles.csv", "china_301_rate_timing_quantiles"),
        ("raw_replication_china_301_rate_provenance.csv", "china_301_rate_provenance"),
        ("raw_replication_china_301_rate_mismatch_decomposition.csv", "china_301_rate_mismatch_decomposition"),
        ("raw_replication_china_301_statutory_component_trace.csv", "china_301_statutory_component_trace"),
        ("raw_replication_china_301_statutory_component_summary.csv", "china_301_statutory_component_summary"),
        ("raw_replication_china_301_statutory_component_top_clusters.csv", "china_301_statutory_component_top_clusters"),
        ("raw_replication_china_301_benchmark_definition_trace.csv", "china_301_benchmark_definition_trace"),
        ("raw_replication_china_301_benchmark_definition_by_rule.csv", "china_301_benchmark_definition_by_rule"),
        ("raw_replication_china_301_benchmark_definition_by_month.csv", "china_301_benchmark_definition_by_month"),
        ("raw_replication_china_301_benchmark_definition_by_stage.csv", "china_301_benchmark_definition_by_stage"),
        ("raw_replication_china_301_benchmark_definition_quantiles.csv", "china_301_benchmark_definition_quantiles"),
        ("raw_replication_china_301_universe_audit.csv", "china_301_universe_audit"),
        ("raw_replication_china_301_metric_denominators.csv", "china_301_metric_denominators"),
        ("raw_replication_china_301_raw_only_keys.csv", "china_301_raw_only_keys"),
        ("raw_replication_china_301_residual_current.csv", "china_301_residual_current"),
        ("raw_replication_china_301_rate_difference_quantiles.csv", "china_301_rate_difference_quantiles"),
        ("raw_replication_22042150_panel_trace.csv", "china_22042150_panel_trace"),
        ("raw_replication_china_301_rule_assignment_candidates.csv", "china_301_rule_assignment_candidates"),
        ("raw_replication_china_301_variable_semantics.csv", "china_301_variable_semantics"),
        ("raw_replication_china_301_universe_trace.csv", "china_301_universe_trace"),
        ("raw_replication_china_301_universe_by_country.csv", "china_301_universe_by_country"),
        ("raw_replication_china_301_universe_by_month.csv", "china_301_universe_by_month"),
        ("raw_replication_china_301_universe_by_status.csv", "china_301_universe_by_status"),
        ("raw_replication_china_301_universe_by_semantics.csv", "china_301_universe_by_semantics"),
        ("raw_replication_metrics_china_301_semantics_corrected.csv", "china_301_metrics_semantics_corrected"),
        ("raw_replication_discrepancies_china_301_semantics_corrected.parquet", "china_301_discrepancies_semantics_corrected"),
        ("raw_replication_release_gate_china_301_semantics_corrected.json", "china_301_release_gate_semantics_corrected"),
        ("raw_replication_china_301_metric_denominators_semantics_corrected.csv", "china_301_metric_denominators_semantics_corrected"),
        ("raw_replication_artifact_freshness_china_301_semantics_corrected.csv", "china_301_artifact_freshness_semantics_corrected"),
        ("raw_replication_china_301_rate_trace_china_301_semantics_corrected.csv", "china_301_rate_trace_semantics_corrected"),
        ("raw_replication_china_301_rate_timing_trace_china_301_semantics_corrected.csv", "china_301_rate_timing_trace_semantics_corrected"),
        ("raw_replication_china_301_rate_timing_by_month_china_301_semantics_corrected.csv", "china_301_rate_timing_by_month_semantics_corrected"),
        ("raw_replication_china_301_rate_timing_by_rule_china_301_semantics_corrected.csv", "china_301_rate_timing_by_rule_semantics_corrected"),
        ("raw_replication_china_301_rate_timing_by_stage_china_301_semantics_corrected.csv", "china_301_rate_timing_by_stage_semantics_corrected"),
        ("raw_replication_china_301_rate_timing_quantiles_china_301_semantics_corrected.csv", "china_301_rate_timing_quantiles_semantics_corrected"),
        ("raw_replication_china_301_rate_provenance_china_301_semantics_corrected.csv", "china_301_rate_provenance_semantics_corrected"),
        ("raw_replication_china_301_rate_mismatch_decomposition_china_301_semantics_corrected.csv", "china_301_rate_mismatch_decomposition_semantics_corrected"),
        ("raw_replication_china_301_statutory_component_trace_china_301_semantics_corrected.csv", "china_301_statutory_component_trace_semantics_corrected"),
        ("raw_replication_china_301_statutory_component_summary_china_301_semantics_corrected.csv", "china_301_statutory_component_summary_semantics_corrected"),
        ("raw_replication_china_301_statutory_component_top_clusters_china_301_semantics_corrected.csv", "china_301_statutory_component_top_clusters_semantics_corrected"),
        ("raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv", "china_301_benchmark_definition_trace_semantics_corrected"),
        ("raw_replication_china_301_benchmark_definition_by_rule_china_301_semantics_corrected.csv", "china_301_benchmark_definition_by_rule_semantics_corrected"),
        ("raw_replication_china_301_benchmark_definition_by_month_china_301_semantics_corrected.csv", "china_301_benchmark_definition_by_month_semantics_corrected"),
        ("raw_replication_china_301_benchmark_definition_by_stage_china_301_semantics_corrected.csv", "china_301_benchmark_definition_by_stage_semantics_corrected"),
        ("raw_replication_china_301_benchmark_definition_quantiles_china_301_semantics_corrected.csv", "china_301_benchmark_definition_quantiles_semantics_corrected"),
        ("raw_replication_release_gate.json", "release_gate"),
    )


def build_raw_replication_artifact_freshness(
    config: PipelineConfig,
    artifact_suffix: str | None = None,
) -> pd.DataFrame:
    """Record whether validator outputs are stale relative to the current overlay and panel."""
    output_dir = config.verification_dir / "raw_replication_imports"
    current_inputs = [
        config.analysis_dir / "tradewar_overlay_raw.parquet",
        config.analysis_dir / "us_products_partner_hs10_monthly.parquet",
    ]
    current_input_mtimes = [mtime for mtime in (_artifact_mtime(path) for path in current_inputs) if mtime is not None]
    current_input_label = json.dumps([str(path) for path in current_inputs])
    current_threshold = max(current_input_mtimes) if current_input_mtimes else None
    specs = _raw_replication_artifact_specs()
    if artifact_suffix is not None:
        suffix_token = artifact_suffix.lower()
        if suffix_token:
            specs = tuple(spec for spec in specs if spec[0].lower().endswith(f"{suffix_token}.csv") or spec[0].lower().endswith(f"{suffix_token}.json") or spec[0].lower().endswith(f"{suffix_token}.parquet"))
        else:
            specs = tuple(spec for spec in specs if "_china_301_semantics_corrected" not in spec[0].lower() and "_china_301_current" not in spec[0].lower())
    rows: list[dict[str, Any]] = []
    for artifact, label in specs:
        path = output_dir / artifact
        exists = path.exists()
        readable = False
        row_count = None
        error: str | None = None
        if exists:
            row_count = _count_rows_for_artifact(path)
            readable = row_count is not None
            if not readable:
                try:
                    _ = path.read_bytes()
                    readable = True
                    row_count = 1 if path.suffix.lower() == ".json" else None
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
        else:
            error = "missing"
        artifact_mtime = _artifact_mtime(path)
        stale = True
        if current_threshold is not None and artifact_mtime is not None:
            stale = artifact_mtime < current_threshold
        rows.append(
            {
                "artifact": artifact,
                "path": str(path),
                "exists": bool(exists),
                "readable": bool(readable),
                "row_count": row_count,
                "modified_time": None if artifact_mtime is None else artifact_mtime.isoformat(),
                "current_input": current_input_label,
                "stale_relative_to_inputs": bool(stale),
                "error": error,
            }
        )
    return pd.DataFrame(rows, columns=["artifact", "path", "exists", "readable", "row_count", "modified_time", "current_input", "stale_relative_to_inputs", "error"])


def build_22042150_panel_trace_from_artifacts(config: PipelineConfig) -> dict[str, Any]:
    """Diagnose the remaining Jan-2019 22042150 China loss using saved artifacts."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    disc_path = output_dir / "raw_replication_discrepancies.parquet"
    if not disc_path.exists():
        raise FileNotFoundError(f"Missing raw replication discrepancies artifact: {disc_path}")
    discrepancies = read_table(disc_path, columns=["hs10", "year", "month", "ref_m_effective_mdate2", "ref_m_china_hit", "discrepancy_type"])
    discrepancies = discrepancies.loc[pd.to_numeric(discrepancies["ref_m_china_hit"], errors="coerce").eq(1)].copy()
    discrepancies["hs8"] = _normalize_hs8(discrepancies["hs10"])
    target = discrepancies.loc[
        discrepancies["hs8"].eq("22042150")
        & pd.to_numeric(discrepancies["year"], errors="coerce").eq(2019)
        & pd.to_numeric(discrepancies["month"], errors="coerce").eq(1)
    ].copy()
    trace_path = output_dir / "raw_replication_22042150_panel_trace.csv"
    empty_cols = [
        "hs10",
        "hs8",
        "cty_code",
        "cty_name",
        "year",
        "month",
        "raw_trade_key_present",
        "overlay_hs8_present",
        "overlay_increment",
        "panel_key_present",
        "panel_hs8",
        "panel_increment",
        "panel_rule_code",
        "panel_policy_source",
        "discrepancy_type",
        "diagnosed_stage",
    ]
    if target.empty:
        empty = pd.DataFrame(columns=empty_cols)
        empty.to_csv(trace_path, index=False)
        return {"trace_path": str(trace_path), "rows": 0, "stage_counts": {}, "diagnosed_stage": "no_raw_trade_key"}

    overlay = pd.DataFrame()
    overlay_path = config.analysis_dir / "tradewar_overlay_raw.parquet"
    if overlay_path.exists():
        try:
            overlay = read_table(overlay_path, columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"])
        except Exception:
            overlay = pd.DataFrame()
    if not overlay.empty:
        overlay["cty_name"] = overlay["cty_name"].astype("string").str.upper()
        overlay["hs8"] = overlay["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        overlay["year"] = pd.to_numeric(overlay["year"], errors="coerce").astype("Int64")
        overlay["month"] = pd.to_numeric(overlay["month"], errors="coerce").astype("Int64")
        overlay["tw_increment_rate_raw"] = pd.to_numeric(overlay["tw_increment_rate_raw"], errors="coerce")

    panel = _load_china_301_panel_trace_slice(config, target["hs10"], 2019, 1)
    panel_lookup = pd.DataFrame(columns=["hs10", "hs8", "cty_code", "cty_name", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_scope_source_raw"])
    if panel is not None and not panel.empty:
        panel_lookup = panel.drop_duplicates(["hs10", "year", "month"], keep="first").copy()

    overlay_rows = overlay.loc[
        overlay["cty_name"].eq("CHINA") & overlay["hs8"].eq("22042150") & overlay["year"].eq(2019) & overlay["month"].eq(1)
    ].copy() if not overlay.empty else pd.DataFrame()
    overlay_present = not overlay_rows.empty
    overlay_increment = pd.to_numeric(overlay_rows["tw_increment_rate_raw"], errors="coerce").iloc[0] if overlay_present else pd.NA

    rows: list[dict[str, Any]] = []
    for _, row in target.sort_values(["hs10"]).iterrows():
        hs10 = normalize_hs_code(row["hs10"], 10)
        panel_match = panel_lookup.loc[panel_lookup["hs10"].astype("string").eq(hs10)].copy() if not panel_lookup.empty else pd.DataFrame()
        raw_trade_key_present = bool(len(panel_match))
        panel_key_present = raw_trade_key_present
        panel_increment = pd.NA
        panel_hs8 = pd.NA
        panel_rule_code = pd.NA
        panel_policy_source = pd.NA
        if raw_trade_key_present:
            panel_row = panel_match.iloc[0]
            panel_increment = pd.to_numeric(pd.Series([panel_row.get("tw_increment_rate_raw")]), errors="coerce").iloc[0]
            panel_hs8 = str(panel_row.get("hs8") or hs10[:8])
            panel_rule_code = panel_row.get("tw_rule_code_raw")
            panel_policy_source = panel_row.get("tw_scope_source_raw")

        if not raw_trade_key_present:
            diagnosed_stage = "no_raw_trade_key"
        elif overlay_present and not panel_key_present:
            diagnosed_stage = "overlay_present_panel_missing"
        elif overlay_present and pd.notna(overlay_increment) and pd.isna(panel_increment):
            diagnosed_stage = "panel_increment_overwritten"
        elif str(panel_hs8 or "").startswith("22042150") is False and raw_trade_key_present:
            diagnosed_stage = "hs8_prefix_mismatch"
        else:
            diagnosed_stage = "no_mechanical_bug_found"

        rows.append(
            {
                "hs10": hs10,
                "hs8": hs10[:8],
                "cty_code": 5700,
                "cty_name": "CHINA",
                "year": 2019,
                "month": 1,
                "raw_trade_key_present": bool(raw_trade_key_present),
                "overlay_hs8_present": bool(overlay_present),
                "overlay_increment": overlay_increment,
                "panel_key_present": bool(panel_key_present),
                "panel_hs8": panel_hs8,
                "panel_increment": panel_increment,
                "panel_rule_code": panel_rule_code,
                "panel_policy_source": panel_policy_source,
                "discrepancy_type": row.get("discrepancy_type"),
                "diagnosed_stage": diagnosed_stage,
            }
        )

    trace = pd.DataFrame(rows, columns=empty_cols)
    trace.to_csv(trace_path, index=False)
    stage_counts = trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict()
    return {"trace_path": str(trace_path), "rows": int(len(trace)), "stage_counts": stage_counts, "diagnosed_stage": trace["diagnosed_stage"].mode().iloc[0] if not trace.empty else "no_raw_trade_key"}


def build_china_301_trace_from_artifacts(config: PipelineConfig, top_n: int = 400) -> dict[str, Any]:
    """Build the China 301 trace directly from saved validation artifacts."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    discrepancies = _load_china_301_discrepancies(output_dir)
    if discrepancies.empty:
        trace_path = output_dir / "raw_replication_china_301_trace.csv"
        empty = pd.DataFrame(
            columns=[
                "hs8",
                "year",
                "month",
                "ref_effective_period",
                "missing_scope_rows",
                "machine_link_rows",
                "pdf_link_rows",
                "raw_link_rows",
                "rule_attr_rows",
                "overlay_rows",
                "overlay_increment_rows",
                "panel_rows",
                "panel_increment_rows",
                "diagnosed_stage",
            ]
        )
        empty.to_csv(trace_path, index=False)
        rate_trace = build_china_301_rate_trace_from_artifacts(config)
        rate_timing_trace = build_china_301_rate_timing_trace_from_artifacts(config)
        rate_provenance = build_china_301_rate_provenance_from_artifacts(config)
        rate_mismatch = build_china_301_rate_mismatch_decomposition_from_artifacts(config)
        statutory_component = build_china_301_statutory_component_trace_from_artifacts(config)
        benchmark_definition = build_china_301_benchmark_definition_trace_from_artifacts(config)
        rule_assignment = build_china_301_rule_assignment_trace_from_artifacts(config)
        wave_audit = build_china_301_wave_link_audit_from_artifacts(config)
        freshness = build_raw_replication_artifact_freshness(config)
        freshness_path = output_dir / "raw_replication_artifact_freshness.csv"
        freshness.to_csv(freshness_path, index=False)
        return {
            "trace_path": str(trace_path),
            "rows": 0,
            "stage_counts": {},
            "top_buckets": [],
            "rate_trace_path": rate_trace.get("trace_path"),
            "rate_trace_stage_counts": rate_trace.get("stage_counts", {}),
            "rate_timing_trace_path": rate_timing_trace.get("trace_path"),
            "rate_timing_stage_counts": rate_timing_trace.get("stage_counts", {}),
            "rate_provenance_path": rate_provenance.get("trace_path"),
            "rate_provenance_stage_counts": rate_provenance.get("stage_counts", {}),
            "rate_mismatch_decomposition_path": rate_mismatch.get("trace_path"),
            "statutory_component_trace_path": statutory_component.get("trace_path"),
            "statutory_component_summary_path": statutory_component.get("summary_path"),
            "statutory_component_clusters_path": statutory_component.get("clusters_path"),
            "statutory_component_stage_counts": statutory_component.get("stage_counts", {}),
            "benchmark_definition_trace_path": benchmark_definition.get("trace_path"),
            "benchmark_definition_by_rule_path": benchmark_definition.get("by_rule_path"),
            "benchmark_definition_by_month_path": benchmark_definition.get("by_month_path"),
            "benchmark_definition_by_stage_path": benchmark_definition.get("by_stage_path"),
            "benchmark_definition_quantiles_path": benchmark_definition.get("quantiles_path"),
            "benchmark_definition_stage_counts": benchmark_definition.get("stage_counts", {}),
            "rule_assignment_trace_path": rule_assignment.get("trace_path"),
            "rule_assignment_stage_counts": rule_assignment.get("stage_counts", {}),
            "wave_audit_path": wave_audit.get("audit_path"),
            "wave_conflicts_path": wave_audit.get("conflicts_path"),
            "wave_materiality_path": wave_audit.get("materiality_path"),
            "artifact_freshness_path": str(freshness_path),
        }

    top = (
        discrepancies.groupby(["hs8", "year", "month", "ref_effective_period"], dropna=False, observed=True)
        .size()
        .reset_index(name="missing_scope_rows")
        .sort_values(["missing_scope_rows", "year", "month", "hs8"], ascending=[False, False, False, True])
        .head(top_n)
        .reset_index(drop=True)
    )

    source_health, source_health_summary, source_tables, source_errors = build_raw_source_health_report(config)
    panel = _load_china_301_panel_slice(config, top)
    audit, china_source_errors = build_china_301_source_audit(
        discrepancies,
        machine_links=source_tables["machine_links"],
        pdf_links=source_tables["pdf_links"],
        rule_attrs=source_tables["rule_attrs"],
        overlay=source_tables["overlay"],
        panel=panel,
        source_health_summary=source_health_summary,
    )

    trace = audit.loc[
        :,
        [
            "hs8",
            "year",
            "month",
            "ref_effective_period",
            "missing_scope_rows",
            "machine_links_rows",
            "pdf_links_rows",
            "raw_link_rows",
            "rule_attr_rows",
            "overlay_rows",
            "overlay_increment_rows",
            "panel_rows",
            "panel_increment_rows",
            "diagnosed_stage",
        ],
    ].copy()
    trace = trace.rename(columns={"machine_links_rows": "machine_link_rows", "pdf_links_rows": "pdf_link_rows"})
    trace_path = output_dir / "raw_replication_china_301_trace.csv"
    trace.to_csv(trace_path, index=False)
    focused_22042150 = build_22042150_panel_trace_from_artifacts(config)
    key_trace = build_china_301_key_trace_from_artifacts(config)
    rate_trace = build_china_301_rate_trace_from_artifacts(config)
    rate_timing_trace = build_china_301_rate_timing_trace_from_artifacts(config)
    rate_provenance = build_china_301_rate_provenance_from_artifacts(config)
    rate_mismatch = build_china_301_rate_mismatch_decomposition_from_artifacts(config)
    statutory_component = build_china_301_statutory_component_trace_from_artifacts(config)
    benchmark_definition = build_china_301_benchmark_definition_trace_from_artifacts(config)
    rule_assignment = build_china_301_rule_assignment_trace_from_artifacts(config)
    wave_audit = build_china_301_wave_link_audit_from_artifacts(config)
    freshness = build_raw_replication_artifact_freshness(config)
    freshness_path = output_dir / "raw_replication_artifact_freshness.csv"
    freshness.to_csv(freshness_path, index=False)
    stage_counts = trace["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict()
    top_buckets = trace.head(10).to_dict(orient="records")
    errors_path = output_dir / "raw_replication_china_301_trace_errors.json"
    write_metadata_json(
        errors_path,
        {
            "source_errors": source_errors,
            "china_source_errors": china_source_errors,
            "source_health_summary": source_health_summary,
        },
    )
    return {
        "trace_path": str(trace_path),
        "errors_path": str(errors_path),
        "rows": int(len(trace)),
        "stage_counts": stage_counts,
        "top_buckets": top_buckets,
        "source_unavailable": bool(source_health_summary.get("blocked_by_source_availability")),
        "focused_22042150_trace_path": focused_22042150.get("trace_path"),
        "key_trace_path": key_trace.get("trace_path"),
        "key_trace_stage_counts": key_trace.get("stage_counts", {}),
        "rate_trace_path": rate_trace.get("trace_path"),
        "rate_trace_stage_counts": rate_trace.get("stage_counts", {}),
        "rate_timing_trace_path": rate_timing_trace.get("trace_path"),
        "rate_timing_stage_counts": rate_timing_trace.get("stage_counts", {}),
        "rate_provenance_path": rate_provenance.get("trace_path"),
        "rate_provenance_stage_counts": rate_provenance.get("stage_counts", {}),
        "rate_mismatch_decomposition_path": rate_mismatch.get("trace_path"),
        "statutory_component_trace_path": statutory_component.get("trace_path"),
        "statutory_component_summary_path": statutory_component.get("summary_path"),
        "statutory_component_clusters_path": statutory_component.get("clusters_path"),
        "statutory_component_stage_counts": statutory_component.get("stage_counts", {}),
        "benchmark_definition_trace_path": benchmark_definition.get("trace_path"),
        "benchmark_definition_by_rule_path": benchmark_definition.get("by_rule_path"),
        "benchmark_definition_by_month_path": benchmark_definition.get("by_month_path"),
        "benchmark_definition_by_stage_path": benchmark_definition.get("by_stage_path"),
        "benchmark_definition_quantiles_path": benchmark_definition.get("quantiles_path"),
        "benchmark_definition_stage_counts": benchmark_definition.get("stage_counts", {}),
        "rule_assignment_trace_path": rule_assignment.get("trace_path"),
        "rule_assignment_stage_counts": rule_assignment.get("stage_counts", {}),
        "wave_audit_path": wave_audit.get("audit_path"),
        "wave_conflicts_path": wave_audit.get("conflicts_path"),
        "wave_materiality_path": wave_audit.get("materiality_path"),
        "artifact_freshness_path": str(freshness_path),
    }


def build_china_301_source_audit(
    cells: pd.DataFrame,
    machine_links: pd.DataFrame | None = None,
    pdf_links: pd.DataFrame | None = None,
    rule_attrs: pd.DataFrame | None = None,
    overlay: pd.DataFrame | None = None,
    panel: pd.DataFrame | None = None,
    source_health_summary: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, str | None]]:
    """Compare the top China 301 missing-scope buckets against raw source layers.

    The audit is intentionally tolerant of unavailable cached parquet files. When a source
    layer cannot be read in the current environment, the audit records the failure and
    still writes the final-panel coverage fields.
    """
    discrepancy = cells.loc[cells["discrepancy_type"].eq("missing_raw_policy_scope") & cells["ref_m_china_hit"].eq(1)].copy()
    if discrepancy.empty:
        empty_cols = [
            "hs8",
            "year",
            "month",
            "ref_effective_period",
            "missing_scope_rows",
            "machine_link_rows",
            "pdf_link_rows",
            "raw_link_rows",
            "rule_attr_rows",
            "overlay_rows",
            "overlay_increment_rows",
            "panel_rows",
            "panel_increment_rows",
            "diagnosed_stage",
        ]
        empty = pd.DataFrame(columns=empty_cols)
        return empty, {"machine_links": "empty", "pdf_links": "empty", "rule_attrs": "empty", "overlay": "empty", "panel": "empty"}

    discrepancy["hs8"] = _normalize_hs8(discrepancy["hs10"])
    discrepancy["ref_effective_period"] = _effective_period(discrepancy["ref_m_effective_mdate2"])
    top = discrepancy.groupby(["hs8", "year", "month", "ref_effective_period"], dropna=False, observed=True).size().reset_index(name="missing_scope_rows")
    top = top.sort_values(["missing_scope_rows", "year", "month", "hs8"], ascending=[False, False, False, True]).head(400).reset_index(drop=True)

    source_errors: dict[str, str | None] = {
        "machine_links": None,
        "pdf_links": None,
        "rule_attrs": None,
        "overlay": None,
        "panel": None,
    }
    source_unavailable = False
    if source_health_summary:
        if source_health_summary.get("blocked_by_source_availability"):
            source_errors["audit_blocked"] = "source_availability"
            source_errors["blocking_artifacts"] = ",".join(sorted(map(str, source_health_summary.get("blocking_artifacts", []))))
            source_unavailable = True

    def _prepare_source(frame: pd.DataFrame | None, label: str) -> pd.DataFrame | None:
        if frame is None or frame.empty:
            source_errors[label] = "empty"
            return None
        out = frame.copy()
        if "hs8" in out.columns:
            out["hs8"] = _normalize_hs8(out["hs8"])
        return out

    machine_links = _prepare_source(machine_links, "machine_links")
    pdf_links = _prepare_source(pdf_links, "pdf_links")
    rule_attrs = _prepare_source(rule_attrs, "rule_attrs")
    overlay = _prepare_source(overlay, "overlay")
    panel = _prepare_source(panel, "panel")

    if machine_links is not None and "rule_code" in machine_links.columns:
        machine_links["rule_code"] = _normalize_rule_code(machine_links["rule_code"])
    if pdf_links is not None and "rule_code" in pdf_links.columns:
        pdf_links["rule_code"] = _normalize_rule_code(pdf_links["rule_code"])
    if rule_attrs is not None and "rule_code" in rule_attrs.columns:
        rule_attrs["rule_code"] = _normalize_rule_code(rule_attrs["rule_code"])
        rule_attrs["year"] = pd.to_numeric(rule_attrs.get("year"), errors="coerce").astype("Int64")
        rule_attrs["month"] = pd.to_numeric(rule_attrs.get("month"), errors="coerce").astype("Int64")
    if overlay is not None and "tw_rule_code_raw" in overlay.columns:
        overlay["tw_rule_code_raw"] = _normalize_rule_code(overlay["tw_rule_code_raw"])
    if panel is not None and "tw_rule_code_raw" in panel.columns:
        panel["tw_rule_code_raw"] = _normalize_rule_code(panel["tw_rule_code_raw"])

    audit = top.copy()
    for label, frame in [("machine_links", machine_links), ("pdf_links", pdf_links)]:
        count_col = f"{label}_rows"
        rule_col = f"{label}_rules"
        if frame is None:
            audit[count_col] = pd.NA
            audit[rule_col] = pd.NA
            continue
        if "hs8" not in frame.columns:
            audit[count_col] = pd.NA
            audit[rule_col] = pd.NA
            continue
        subset = frame.loc[frame["hs8"].isin(audit["hs8"])].copy()
        if subset.empty:
            audit[count_col] = 0
            audit[rule_col] = 0
            continue
        counts = subset.groupby("hs8", dropna=False, observed=True).size()
        audit[count_col] = audit["hs8"].map(counts).fillna(0).astype("Int64")
        rule_source_col = None
        for candidate in ("rule_code", "tw_rule_code_raw"):
            if candidate in subset.columns:
                rule_source_col = candidate
                break
        if rule_source_col is None:
            audit[rule_col] = pd.NA
        else:
            rule_counts = subset.groupby("hs8", dropna=False, observed=True)[rule_source_col].nunique()
            audit[rule_col] = audit["hs8"].map(rule_counts).fillna(0).astype("Int64")

    source_cols = [
        "machine_links_rows",
        "machine_links_rules",
        "pdf_links_rows",
        "pdf_links_rules",
        "raw_link_rows",
        "rule_attr_rows",
        "overlay_rows",
        "overlay_increment_rows",
        "panel_rows",
        "panel_increment_rows",
    ]
    for column in source_cols:
        if column not in audit.columns:
            audit[column] = pd.NA

    bucket_rows: list[dict[str, Any]] = []
    for _, bucket in audit.iterrows():
        hs8 = bucket["hs8"]
        year = int(bucket["year"])
        month = int(bucket["month"])

        machine_subset = machine_links.loc[machine_links["hs8"].eq(hs8)] if machine_links is not None and "hs8" in machine_links.columns else None
        pdf_subset = pdf_links.loc[pdf_links["hs8"].eq(hs8)] if pdf_links is not None and "hs8" in pdf_links.columns else None

        raw_link_frames: list[pd.DataFrame] = []
        if machine_subset is not None and not machine_subset.empty:
            raw_link_frames.append(machine_subset[["hs8", "rule_code", "release_name"]].drop_duplicates())
        if pdf_subset is not None and not pdf_subset.empty:
            raw_link_frames.append(pdf_subset[["hs8", "rule_code", "release_name"]].drop_duplicates())
        raw_links = pd.concat(raw_link_frames, ignore_index=True).drop_duplicates() if raw_link_frames else pd.DataFrame(columns=["hs8", "rule_code", "release_name"])
        raw_links["rule_code"] = raw_links.get("rule_code", pd.Series(dtype="string")).astype("string")
        raw_links = raw_links.loc[raw_links["rule_code"].notna()].copy()
        raw_links = raw_links.loc[raw_links["rule_code"].str.startswith("990388", na=False)].copy()

        rule_attr_rows = 0
        if not raw_links.empty and rule_attrs is not None and not rule_attrs.empty:
            rule_attrs_bucket = rule_attrs.loc[rule_attrs["year"].eq(year) & rule_attrs["month"].eq(month)].copy()
            if not rule_attrs_bucket.empty:
                rule_attr_rows = int(len(raw_links.merge(rule_attrs_bucket[["rule_code"]].drop_duplicates(), on="rule_code", how="inner")))

        overlay_rows = 0
        overlay_increment_rows = 0
        if overlay is not None and not overlay.empty and {"hs8", "year", "month", "cty_name", "tw_increment_rate_raw"}.issubset(overlay.columns):
            overlay_sub = overlay.loc[overlay["hs8"].eq(hs8) & overlay["year"].eq(year) & overlay["month"].eq(month) & overlay["cty_name"].astype("string").str.upper().eq("CHINA")].copy()
            overlay_rows = int(len(overlay_sub))
            overlay_increment_rows = int(pd.to_numeric(overlay_sub["tw_increment_rate_raw"], errors="coerce").notna().sum())

        panel_rows = 0
        panel_increment_rows = 0
        if panel is not None and not panel.empty and {"cty_code", "hs10", "year", "month", "tw_increment_rate_raw"}.issubset(panel.columns):
            panel_sub = panel.loc[
                pd.to_numeric(panel["cty_code"], errors="coerce").eq(5700)
                & panel["hs10"].astype("string").str.slice(0, 8).eq(hs8)
                & pd.to_numeric(panel["year"], errors="coerce").eq(year)
                & pd.to_numeric(panel["month"], errors="coerce").eq(month)
            ].copy()
            panel_rows = int(len(panel_sub))
            panel_increment_rows = int(pd.to_numeric(panel_sub["tw_increment_rate_raw"], errors="coerce").notna().sum())

        bucket_rows.append(
            {
                "hs8": hs8,
                "year": year,
                "month": month,
                "ref_effective_period": bucket["ref_effective_period"],
                "missing_scope_rows": int(bucket["missing_scope_rows"]),
                "machine_links_rows": int(len(machine_subset)) if machine_subset is not None else pd.NA,
                "machine_links_rules": int(machine_subset["rule_code"].nunique()) if machine_subset is not None and "rule_code" in machine_subset.columns else pd.NA,
                "pdf_links_rows": int(len(pdf_subset)) if pdf_subset is not None else pd.NA,
                "pdf_links_rules": int(pdf_subset["rule_code"].nunique()) if pdf_subset is not None and "rule_code" in pdf_subset.columns else pd.NA,
                "raw_link_rows": int(len(raw_links)) if not raw_links.empty else 0,
                "rule_attr_rows": int(rule_attr_rows),
                "overlay_rows": int(overlay_rows),
                "overlay_increment_rows": int(overlay_increment_rows),
                "panel_rows": int(panel_rows),
                "panel_increment_rows": int(panel_increment_rows),
                "diagnosed_stage": _trace_stage(
                    int(len(raw_links)) if not raw_links.empty else 0,
                    int(rule_attr_rows),
                    int(overlay_increment_rows),
                    int(panel_increment_rows),
                    source_unavailable,
                ),
            }
        )

    audit = pd.DataFrame(bucket_rows)

    fill_values = {column: 0 for column in audit.columns if column.endswith("_rows") or column.endswith("_codes")}
    if fill_values:
        audit = audit.fillna(fill_values).infer_objects(copy=False)
    sort_col = "missing_scope_rows" if "missing_scope_rows" in audit.columns else "rows"
    return audit.sort_values([sort_col, "hs8", "year", "month"], ascending=[False, True, True, True]).reset_index(drop=True), source_errors


def _load_china_301_wave_focus_buckets(output_dir: Path) -> pd.DataFrame:
    """Load the top China 301 missing-scope HS8 buckets used to focus wave diagnostics."""
    focus_paths = [
        output_dir / "raw_replication_china_301_top_hs8_month_wave.csv",
        output_dir / "raw_replication_china_301_top_hs8.csv",
        output_dir / "raw_replication_china_301_trace.csv",
    ]
    for path in focus_paths:
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty or "hs8" not in frame.columns:
            continue
        frame = frame.copy()
        frame["hs8"] = frame["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        if "missing_scope_rows" in frame.columns:
            frame["missing_scope_rows"] = pd.to_numeric(frame["missing_scope_rows"], errors="coerce").fillna(0).astype("Int64")
            focus = frame.groupby("hs8", dropna=False, observed=True)["missing_scope_rows"].sum().reset_index()
        else:
            focus = frame[["hs8"]].drop_duplicates().assign(missing_scope_rows=pd.NA)
        return focus.dropna(subset=["hs8"]).reset_index(drop=True)
    return pd.DataFrame(columns=["hs8", "missing_scope_rows"])


def _classify_china_301_wave_conflict_stage(frame: pd.DataFrame) -> str:
    """Classify the provenance pattern for a single HS8 wave bucket."""
    if frame.empty:
        return "requires_full_model_review"
    rules = sorted({str(value) for value in frame.get("rule_code", pd.Series(dtype="string")).astype("string") if str(value).strip()})
    releases = sorted({str(value) for value in frame.get("release_name", pd.Series(dtype="string")).astype("string") if str(value).strip()})
    methods = set(frame.get("extraction_method", pd.Series(dtype="string")).astype("string").fillna("").tolist())
    same_row = bool(pd.Series(frame.get("rule_found_in_same_row", pd.Series(dtype="boolean"))).fillna(False).any())
    context_only = bool(pd.Series(frame.get("rule_found_only_in_context", pd.Series(dtype="boolean"))).fillna(False).any())
    chapter99 = any(str(value).startswith("chapter99") for value in methods)

    if len(rules) == 1 and len(releases) <= 1:
        if chapter99:
            return "chapter99_enumeration_link"
        if context_only and not same_row:
            return "product_context_only_link"
        return "single_core_rule"
    if len(rules) == 2 and set(rules).issubset({"99038803", "99038804"}):
        return "rule03_rule04_temporal_pair"
    if len(releases) > 1 and len(rules) > 1:
        return "cross_release_action_rule_change"
    if len(releases) == 1 and len(rules) > 1:
        if chapter99:
            return "chapter99_enumeration_link"
        return "same_release_multiple_action_rules"
    if chapter99:
        return "chapter99_enumeration_link"
    if context_only and not same_row:
        return "product_context_only_link"
    return "requires_full_model_review"


def build_china_301_wave_link_audit_from_artifacts(config: PipelineConfig) -> dict[str, Any]:
    """Write provenance-focused diagnostics for the Section 301 PDF CSV wave links."""
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    provenance = _load_tradewar_pdf_csv_link_provenance(config)
    if provenance.empty:
        empty_audit = pd.DataFrame(
            columns=[
                "release_name",
                "release_start_date",
                "release_end_date",
                "source_file",
                "source_page",
                "source_row",
                "hs8",
                "rule_code",
                "extraction_method",
                "rule_found_in_same_row",
                "rule_found_only_in_context",
                "matched_rule_text",
                "focus_missing_scope_rows",
            ]
        )
        audit_path = output_dir / "raw_replication_china_301_wave_link_audit.csv"
        conflicts_path = output_dir / "raw_replication_china_301_wave_conflicts.csv"
        materiality_path = output_dir / "raw_replication_china_301_wave_materiality.csv"
        empty_audit.to_csv(audit_path, index=False)
        pd.DataFrame(
            columns=[
                "hs8",
                "release_count",
                "rule_count",
                "same_row_link_rows",
                "context_only_link_rows",
                "chapter99_link_rows",
                "missing_scope_rows",
                "diagnosed_stage",
            ]
        ).to_csv(conflicts_path, index=False)
        pd.DataFrame(
            columns=[
                "diagnosed_stage",
                "rows",
                "hs8_count",
                "release_count",
                "same_row_link_rows",
                "context_only_link_rows",
                "chapter99_link_rows",
                "missing_scope_rows",
            ]
        ).to_csv(materiality_path, index=False)
        return {"audit_path": str(audit_path), "conflicts_path": str(conflicts_path), "materiality_path": str(materiality_path), "rows": 0, "stage_counts": {}}

    provenance = provenance.copy()
    provenance["hs8"] = provenance["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    provenance["rule_code"] = provenance["rule_code"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
    provenance["extraction_method"] = provenance.get("extraction_method", pd.Series(dtype="string")).astype("string")
    provenance["rule_found_in_same_row"] = provenance.get("rule_found_in_same_row", pd.Series(dtype="boolean")).fillna(False).astype(bool)
    provenance["rule_found_only_in_context"] = provenance.get("rule_found_only_in_context", pd.Series(dtype="boolean")).fillna(False).astype(bool)

    focus = _load_china_301_wave_focus_buckets(output_dir)
    if not focus.empty:
        focus = focus.copy()
        focus["hs8"] = focus["hs8"].map(lambda value: normalize_hs_code(value, 8)).astype("string")
        focus_rows = focus.rename(columns={"missing_scope_rows": "focus_missing_scope_rows"})
        audit = provenance.merge(focus_rows, on="hs8", how="inner")
    else:
        audit = provenance.copy()
        audit["focus_missing_scope_rows"] = pd.NA

    audit_path = output_dir / "raw_replication_china_301_wave_link_audit.csv"
    audit.to_csv(audit_path, index=False)

    conflict_rows: list[dict[str, Any]] = []
    if not audit.empty:
        for hs8, frame in audit.groupby("hs8", dropna=False, observed=True):
            stage = _classify_china_301_wave_conflict_stage(frame)
            conflict_rows.append(
                {
                    "hs8": hs8,
                    "release_count": int(frame["release_name"].nunique(dropna=True)) if "release_name" in frame.columns else 0,
                    "rule_count": int(frame["rule_code"].nunique(dropna=True)) if "rule_code" in frame.columns else 0,
                    "same_row_link_rows": int(frame["rule_found_in_same_row"].sum()) if "rule_found_in_same_row" in frame.columns else 0,
                    "context_only_link_rows": int(frame["rule_found_only_in_context"].sum()) if "rule_found_only_in_context" in frame.columns else 0,
                    "chapter99_link_rows": int(frame["extraction_method"].astype("string").str.startswith("chapter99", na=False).sum()) if "extraction_method" in frame.columns else 0,
                    "missing_scope_rows": int(frame["focus_missing_scope_rows"].fillna(0).astype("Int64").max()) if "focus_missing_scope_rows" in frame.columns and frame["focus_missing_scope_rows"].notna().any() else pd.NA,
                    "diagnosed_stage": stage,
                }
            )
    conflicts = pd.DataFrame(
        conflict_rows,
        columns=[
            "hs8",
            "release_count",
            "rule_count",
            "same_row_link_rows",
            "context_only_link_rows",
            "chapter99_link_rows",
            "missing_scope_rows",
            "diagnosed_stage",
        ],
    )
    if not conflicts.empty:
        conflicts = conflicts.sort_values(["missing_scope_rows", "release_count", "rule_count", "hs8"], ascending=[False, False, False, True]).reset_index(drop=True)
    conflicts_path = output_dir / "raw_replication_china_301_wave_conflicts.csv"
    conflicts.to_csv(conflicts_path, index=False)

    materiality = (
        conflicts.groupby("diagnosed_stage", dropna=False, observed=True)
        .agg(
            rows=("hs8", "size"),
            hs8_count=("hs8", "nunique"),
            release_count=("release_count", "sum"),
            same_row_link_rows=("same_row_link_rows", "sum"),
            context_only_link_rows=("context_only_link_rows", "sum"),
            chapter99_link_rows=("chapter99_link_rows", "sum"),
            missing_scope_rows=("missing_scope_rows", "sum"),
        )
        .reset_index()
        if not conflicts.empty
        else pd.DataFrame(
            columns=[
                "diagnosed_stage",
                "rows",
                "hs8_count",
                "release_count",
                "same_row_link_rows",
                "context_only_link_rows",
                "chapter99_link_rows",
                "missing_scope_rows",
            ]
        )
    )
    materiality_path = output_dir / "raw_replication_china_301_wave_materiality.csv"
    materiality.to_csv(materiality_path, index=False)

    return {
        "audit_path": str(audit_path),
        "conflicts_path": str(conflicts_path),
        "materiality_path": str(materiality_path),
        "rows": int(len(audit)),
        "stage_counts": conflicts["diagnosed_stage"].value_counts(dropna=False).sort_index().to_dict() if not conflicts.empty else {},
    }


def run_raw_replication_validation(config: PipelineConfig) -> dict[str, Any]:
    """Run the raw reconstruction validation and publish an extension release gate."""
    raw_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    ref_path = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        selected = ", ".join(RAW_COLUMNS)
        escaped_path = str(raw_path).replace("'", "''")
        con = duckdb.connect()
        try:
            raw = con.execute(
                f"SELECT {selected} FROM read_parquet('{escaped_path}') WHERE year BETWEEN 2017 AND 2019"
            ).fetchdf()
        finally:
            con.close()
    else:
        raw = read_table(raw_path, columns=RAW_COLUMNS)
    reference = read_table(ref_path, columns=PACKAGE_COLUMNS)
    # The package's published import regression horizon ends in 2019-04.
    reference = reference.loc[(pd.to_numeric(reference["year"], errors="coerce") >= 2017) & (pd.to_numeric(reference["year"], errors="coerce") <= 2019)].copy()
    raw = raw.loc[(pd.to_numeric(raw["year"], errors="coerce") >= 2017) & (pd.to_numeric(raw["year"], errors="coerce") <= 2019)].copy()
    cells, metrics = compare_raw_reconstruction(reference, raw)
    metrics_path = output_dir / "raw_replication_metrics.csv"
    cells_path = output_dir / "raw_replication_discrepancies.parquet"
    metrics.to_csv(metrics_path, index=False)
    write_parquet(cells.loc[cells["discrepancy_type"].ne("match")], cells_path, overwrite=True)
    summaries = summarize_discrepancies(cells)
    summary_paths: dict[str, str] = {}
    for name, summary in summaries.items():
        path = output_dir / f"raw_replication_{name}.csv"
        summary.to_csv(path, index=False)
        summary_paths[f"{name}_path"] = str(path)

    source_tables = {
        "machine_links": None,
        "pdf_links": None,
        "rule_attrs": None,
        "overlay": None,
    }
    source_health, source_health_summary, source_tables, source_load_errors = build_raw_source_health_report(config)
    source_health_path = output_dir / "raw_replication_source_health.csv"
    source_health.to_csv(source_health_path, index=False)
    source_health_summary_path = output_dir / "raw_replication_source_health.json"
    write_metadata_json(source_health_summary_path, source_health_summary)

    china_audit, china_source_errors = build_china_301_source_audit(
        cells,
        machine_links=source_tables["machine_links"],
        pdf_links=source_tables["pdf_links"],
        rule_attrs=source_tables["rule_attrs"],
        overlay=source_tables["overlay"],
        panel=raw,
        source_health_summary=source_health_summary,
    )
    china_audit_path = output_dir / "raw_replication_china_301_source_audit.csv"
    china_audit.to_csv(china_audit_path, index=False)
    summary_paths["china_301_source_audit_path"] = str(china_audit_path)
    china_trace_path = output_dir / "raw_replication_china_301_trace.csv"
    china_audit.to_csv(china_trace_path, index=False)
    summary_paths["china_301_trace_path"] = str(china_trace_path)
    source_errors_path = output_dir / "raw_replication_china_301_source_audit_errors.json"
    write_metadata_json(
        source_errors_path,
        {
            "source_load_errors": source_load_errors,
            "china_audit_errors": china_source_errors,
            "source_health_summary": source_health_summary,
        },
    )
    summary_paths["china_301_source_audit_errors_path"] = str(source_errors_path)
    summary_paths["source_health_path"] = str(source_health_path)
    summary_paths["source_health_summary_path"] = str(source_health_summary_path)
    matched = cells.loc[cells["_merge"].eq("both")]
    matched_non_sentinel = matched.loc[~matched["is_non_ad_valorem_or_sentinel"]]
    active_reference = cells.loc[cells["ref_active"]]
    active_matched = matched.loc[matched["ref_active"] | matched["raw_active"]]
    active_non_sentinel = active_matched.loc[~active_matched["is_non_ad_valorem_or_sentinel"]]
    gate = {
        "paper_key_coverage_rate": float(len(matched) / max(len(reference), 1)),
        "tariff_active_key_coverage_rate": float(active_reference["_merge"].eq("both").mean()) if len(active_reference) else 1.0,
        "tariff_active_treatment_match_rate": float((active_matched["ref_treated"] == active_matched["raw_treated"]).mean()) if len(active_matched) else 1.0,
        "tariff_active_statutory_rate_match_rate": float(active_non_sentinel["rate1_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0,
        "tariff_active_day_weighted_rate_match_rate": float(active_non_sentinel["rate2_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0,
        "raw_trade_value_match_rate": float(matched["trade_value_abs_diff"].le(TRADE_VALUE_TOL).mean()) if len(matched) else 0.0,
        "statutory_rate_match_rate": float(matched_non_sentinel["rate1_abs_diff"].le(RATE_TOL).mean()) if len(matched_non_sentinel) else 0.0,
        "day_weighted_rate_match_rate": float(matched_non_sentinel["rate2_abs_diff"].le(RATE_TOL).mean()) if len(matched_non_sentinel) else 0.0,
        "non_ad_valorem_or_sentinel_rows": int(cells["is_non_ad_valorem_or_sentinel"].sum()),
    }
    gate["ready_for_extension"] = bool(
        gate["paper_key_coverage_rate"] >= 0.999
        and gate["tariff_active_key_coverage_rate"] == 1.0
        and gate["tariff_active_treatment_match_rate"] == 1.0
        and gate["tariff_active_statutory_rate_match_rate"] == 1.0
        and gate["tariff_active_day_weighted_rate_match_rate"] == 1.0
    )
    gate["reason"] = "passed" if gate["ready_for_extension"] else "raw_reconstruction_not_yet_equivalent_to_reference"
    gate_path = output_dir / "raw_replication_release_gate.json"
    write_metadata_json(gate_path, gate)
    freshness = build_raw_replication_artifact_freshness(config)
    freshness_path = output_dir / "raw_replication_artifact_freshness.csv"
    freshness.to_csv(freshness_path, index=False)
    summary_paths["artifact_freshness_path"] = str(freshness_path)
    return {"metrics_path": str(metrics_path), "discrepancies_path": str(cells_path), "gate_path": str(gate_path), **summary_paths, **gate}


def run_raw_replication_validation_china_current(config: PipelineConfig) -> dict[str, Any]:
    """Run a China-only validation pass without touching the full release gate."""
    raw_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    ref_path = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        selected = ", ".join(RAW_COLUMNS)
        escaped_path = str(raw_path).replace("'", "''")
        con = duckdb.connect()
        try:
            raw = con.execute(
                f"""
                SELECT {selected}
                FROM read_parquet('{escaped_path}')
                WHERE year BETWEEN 2017 AND 2019
                  AND CAST(cty_code AS BIGINT) = 5700
                """
            ).fetchdf()
        finally:
            con.close()
    else:
        raw = read_table(raw_path, columns=RAW_COLUMNS)
        raw = raw.loc[pd.to_numeric(raw["cty_code"], errors="coerce").eq(5700)].copy()
    reference = read_table(ref_path)
    for column in PACKAGE_COLUMNS:
        if column not in reference.columns:
            reference[column] = pd.NA
    reference = reference[PACKAGE_COLUMNS].copy()
    universe = _build_china_301_validation_universe(reference)
    universe_path = output_dir / "raw_replication_china_301_validation_universe.csv"
    universe.to_csv(universe_path, index=False)
    decomposition = _build_china_301_validation_decomposition(reference)
    decomposition_path = output_dir / "raw_replication_china_301_validation_decomposition.csv"
    decomposition.to_csv(decomposition_path, index=False)
    reference = reference.loc[
        pd.to_numeric(reference["cty_code"], errors="coerce").eq(5700)
        & pd.to_numeric(reference["m_china_hit"], errors="coerce").eq(1)
    ].copy()
    reference = reference.loc[
        (pd.to_numeric(reference["year"], errors="coerce") >= 2017)
        & (pd.to_numeric(reference["year"], errors="coerce") <= 2019)
    ].copy()
    cells, metrics = compare_raw_reconstruction(reference, raw)
    metrics_path = output_dir / "raw_replication_metrics_china_301_current.csv"
    cells_path = output_dir / "raw_replication_discrepancies_china_301_current.parquet"
    metrics.to_csv(metrics_path, index=False)
    write_parquet(cells.loc[cells["discrepancy_type"].ne("match")], cells_path, overwrite=True)
    summaries = summarize_discrepancies(cells)
    summary_paths: dict[str, str] = {}
    for name, summary in summaries.items():
        path = output_dir / f"raw_replication_{name}_china_301_current.csv"
        summary.to_csv(path, index=False)
        summary_paths[f"{name}_path"] = str(path)
    targeted_family_path = output_dir / "raw_replication_by_family_country_targeted.csv"
    summaries["by_family"].to_csv(targeted_family_path, index=False)
    summary_paths["by_family_country_targeted_path"] = str(targeted_family_path)
    universe_path = output_dir / "raw_replication_china_301_validation_universe.csv"
    decomposition_path = output_dir / "raw_replication_china_301_validation_decomposition.csv"
    summary_paths["validation_universe_path"] = str(universe_path)
    summary_paths["validation_decomposition_path"] = str(decomposition_path)

    universe_audit = universe
    universe_audit_path = output_dir / "raw_replication_china_301_universe_audit.csv"
    universe_audit.to_csv(universe_audit_path, index=False)

    metric_denominators = _build_china_301_metric_denominators(cells)
    metric_denominators_path = output_dir / "raw_replication_china_301_metric_denominators.csv"
    metric_denominators.to_csv(metric_denominators_path, index=False)

    raw_only_keys = _build_china_301_raw_only_keys(cells)
    raw_only_keys_path = output_dir / "raw_replication_china_301_raw_only_keys.csv"
    raw_only_keys.to_csv(raw_only_keys_path, index=False)

    residual_current = _build_china_301_residual_current(cells)
    residual_current_path = output_dir / "raw_replication_china_301_residual_current.csv"
    residual_current.to_csv(residual_current_path, index=False)

    rate_difference_quantiles = _build_china_301_rate_difference_quantiles(cells)
    rate_difference_quantiles_path = output_dir / "raw_replication_china_301_rate_difference_quantiles.csv"
    rate_difference_quantiles.to_csv(rate_difference_quantiles_path, index=False)

    summary_paths.update(
        {
            "universe_audit_path": str(universe_audit_path),
            "metric_denominators_path": str(metric_denominators_path),
            "raw_only_keys_path": str(raw_only_keys_path),
            "residual_current_path": str(residual_current_path),
            "rate_difference_quantiles_path": str(rate_difference_quantiles_path),
        }
    )

    matched = cells.loc[cells["_merge"].eq("both")]
    active_reference = cells.loc[cells["ref_active"]]
    active_matched = matched.loc[matched["ref_active"] | matched["raw_active"]].copy()
    active_non_sentinel = active_matched.loc[~active_matched["is_non_ad_valorem_or_sentinel"]].copy()
    gate = {
        "paper_key_coverage_rate": float(len(matched) / max(len(reference), 1)),
        "tariff_active_key_coverage_rate": float(active_reference["_merge"].eq("both").mean()) if len(active_reference) else 1.0,
        "tariff_active_treatment_match_rate": float((active_matched["ref_treated"] == active_matched["raw_treated"]).mean()) if len(active_matched) else 1.0,
        "tariff_active_statutory_rate_match_rate": float(active_non_sentinel["rate1_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0,
        "tariff_active_day_weighted_rate_match_rate": float(active_non_sentinel["rate2_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0,
        "raw_trade_value_match_rate": float(matched["trade_value_abs_diff"].le(TRADE_VALUE_TOL).mean()) if len(matched) else 0.0,
        "ready_for_extension": False,
        "reason": "china_301_current_partial_validation_only",
    }
    gate_path = output_dir / "raw_replication_release_gate_china_301_current.json"
    write_metadata_json(gate_path, gate)
    freshness = build_raw_replication_artifact_freshness(config)
    freshness_path = output_dir / "raw_replication_artifact_freshness_china_301_current.csv"
    freshness.to_csv(freshness_path, index=False)
    return {
        "metrics_path": str(metrics_path),
        "discrepancies_path": str(cells_path),
        "gate_path": str(gate_path),
        "artifact_freshness_path": str(freshness_path),
        "validation_universe_path": str(universe_path),
        "validation_decomposition_path": str(decomposition_path),
        **summary_paths,
        **gate,
    }


def run_raw_replication_validation_china_semantics_corrected(config: PipelineConfig) -> dict[str, Any]:
    """Run a China 301 validation pass on the paper-compatible partner-target universe."""
    raw_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    ref_path = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    output_dir = config.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_suffix = "_china_301_semantics_corrected"

    reference = read_table(ref_path, columns=PACKAGE_COLUMNS)
    for column in PACKAGE_COLUMNS:
        if column not in reference.columns:
            reference[column] = pd.NA
    semantics_reference = _build_china_301_reference_semantics_frame(reference)
    semantics_reference["ref_product_target"] = semantics_reference["ref_target_product"].fillna(False)

    semantics_trace = build_china_301_universe_trace_from_artifacts(config)
    semantics_table = build_china_301_variable_semantics_from_artifacts(config)

    reference_corrected = semantics_reference.loc[
        semantics_reference["cty_code"].eq(5700)
        & semantics_reference["m_china_hit"].eq(1)
        & semantics_reference["ref_product_target"]
    ].copy()
    reference_corrected = reference_corrected[PACKAGE_COLUMNS].copy()

    target_crosswalk = semantics_reference.loc[
        semantics_reference["cty_code"].eq(5700) & semantics_reference["m_china_hit"].eq(1) & semantics_reference["ref_product_target"],
        ["cty_code", "hs10"],
    ].drop_duplicates()
    if importlib.util.find_spec("duckdb") is not None:
        import duckdb

        selected = ", ".join(RAW_COLUMNS)
        escaped_path = str(raw_path).replace("'", "''")
        con = duckdb.connect()
        try:
            con.register("china_target_crosswalk", target_crosswalk)
            try:
                raw = con.execute(
                    f"""
                    SELECT {", ".join(f"p.{column}" for column in RAW_COLUMNS)}
                    FROM read_parquet('{escaped_path}') AS p
                    INNER JOIN china_target_crosswalk AS t
                        ON CAST(p.cty_code AS BIGINT) = t.cty_code
                        AND CAST(p.hs10 AS VARCHAR) = t.hs10
                    WHERE CAST(p.cty_code AS BIGINT) = 5700
                    """
                ).fetchdf()
            except Exception:
                raw = _load_table_with_required_columns(raw_path, RAW_COLUMNS)
                raw["cty_code"] = pd.to_numeric(raw["cty_code"], errors="coerce").astype("Int64")
                raw["hs10"] = raw["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
                raw["year"] = pd.to_numeric(raw["year"], errors="coerce").astype("Int64")
                raw["month"] = pd.to_numeric(raw["month"], errors="coerce").astype("Int64")
                raw = raw.loc[raw["cty_code"].eq(5700)].copy()
                raw = raw.merge(target_crosswalk, on=["cty_code", "hs10"], how="inner")
        finally:
            con.close()
    else:
        raw = _load_table_with_required_columns(raw_path, RAW_COLUMNS)
        raw["cty_code"] = pd.to_numeric(raw["cty_code"], errors="coerce").astype("Int64")
        raw["hs10"] = raw["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
        raw["year"] = pd.to_numeric(raw["year"], errors="coerce").astype("Int64")
        raw["month"] = pd.to_numeric(raw["month"], errors="coerce").astype("Int64")
        raw = raw.loc[raw["cty_code"].eq(5700)].copy()
        raw = raw.merge(target_crosswalk, on=["cty_code", "hs10"], how="inner")
    raw = raw.drop(columns=[column for column in raw.columns if column.endswith("_x") or column.endswith("_y") or column == "ref_target_product"], errors="ignore")

    cells, metrics = compare_raw_reconstruction(reference_corrected, raw)
    metrics_path = output_dir / "raw_replication_metrics_china_301_semantics_corrected.csv"
    cells_path = output_dir / "raw_replication_discrepancies_china_301_semantics_corrected.parquet"
    metrics.to_csv(metrics_path, index=False)
    write_parquet(cells.loc[cells["discrepancy_type"].ne("match")], cells_path, overwrite=True)

    summaries = summarize_discrepancies(cells)
    summary_paths: dict[str, str] = {}
    for name, summary in summaries.items():
        path = output_dir / f"raw_replication_{name}_china_301_semantics_corrected.csv"
        summary.to_csv(path, index=False)
        summary_paths[f"{name}_path"] = str(path)

    matched = cells.loc[cells["_merge"].eq("both")]
    active_reference = cells.loc[cells["ref_active"]]
    active_matched = matched.loc[matched["ref_active"] | matched["raw_active"]].copy()
    active_non_sentinel = active_matched.loc[~active_matched["is_non_ad_valorem_or_sentinel"]].copy()
    gate = {
        "paper_key_coverage_rate": float(len(matched) / max(len(reference_corrected), 1)),
        "tariff_active_key_coverage_rate": float(active_reference["_merge"].eq("both").mean()) if len(active_reference) else 1.0,
        "tariff_active_treatment_match_rate": float((active_matched["ref_treated"] == active_matched["raw_treated"]).mean()) if len(active_matched) else 1.0,
        "tariff_active_statutory_rate_match_rate": float(active_non_sentinel["rate1_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0,
        "tariff_active_day_weighted_rate_match_rate": float(active_non_sentinel["rate2_abs_diff"].le(RATE_TOL).mean()) if len(active_non_sentinel) else 1.0,
        "raw_trade_value_match_rate": float(matched["trade_value_abs_diff"].le(TRADE_VALUE_TOL).mean()) if len(matched) else 0.0,
        "ready_for_extension": False,
        "reason": "china_301_semantics_corrected_validation_only",
    }
    gate_path = output_dir / "raw_replication_release_gate_china_301_semantics_corrected.json"
    write_metadata_json(gate_path, gate)

    denominators = _build_china_301_metric_denominators(cells)
    denominators_path = output_dir / "raw_replication_china_301_metric_denominators_semantics_corrected.csv"
    denominators.to_csv(denominators_path, index=False)

    rate_trace = build_china_301_rate_trace_from_artifacts(config, artifact_suffix=artifact_suffix)
    rate_timing_trace = build_china_301_rate_timing_trace_from_artifacts(config, artifact_suffix=artifact_suffix)
    rate_provenance = build_china_301_rate_provenance_from_artifacts(config, artifact_suffix=artifact_suffix)
    rate_mismatch = build_china_301_rate_mismatch_decomposition_from_artifacts(config, artifact_suffix=artifact_suffix)
    statutory_component = build_china_301_statutory_component_trace_from_artifacts(config, artifact_suffix=artifact_suffix)
    benchmark_definition = build_china_301_benchmark_definition_trace_from_artifacts(config, artifact_suffix=artifact_suffix)

    freshness = build_raw_replication_artifact_freshness(config, artifact_suffix=artifact_suffix)
    freshness_path = output_dir / "raw_replication_artifact_freshness_china_301_semantics_corrected.csv"
    freshness.to_csv(freshness_path, index=False)

    return {
        "metrics_path": str(metrics_path),
        "discrepancies_path": str(cells_path),
        "gate_path": str(gate_path),
        "artifact_freshness_path": str(freshness_path),
        "variable_semantics_path": semantics_table["trace_path"],
        "universe_trace_path": semantics_trace["trace_path"],
        "denominators_path": str(denominators_path),
        "rate_trace_path": rate_trace.get("trace_path"),
        "rate_timing_trace_path": rate_timing_trace.get("trace_path"),
        "rate_timing_by_month_path": rate_timing_trace.get("by_month_path"),
        "rate_timing_by_rule_path": rate_timing_trace.get("by_rule_path"),
        "rate_timing_by_stage_path": rate_timing_trace.get("by_stage_path"),
        "rate_timing_quantiles_path": rate_timing_trace.get("quantiles_path"),
        "rate_provenance_path": rate_provenance.get("trace_path"),
        "rate_mismatch_decomposition_path": rate_mismatch.get("trace_path"),
        "statutory_component_trace_path": statutory_component.get("trace_path"),
        "statutory_component_summary_path": statutory_component.get("summary_path"),
        "statutory_component_clusters_path": statutory_component.get("clusters_path"),
        "benchmark_definition_trace_path": benchmark_definition.get("trace_path"),
        "benchmark_definition_by_rule_path": benchmark_definition.get("by_rule_path"),
        "benchmark_definition_by_month_path": benchmark_definition.get("by_month_path"),
        "benchmark_definition_by_stage_path": benchmark_definition.get("by_stage_path"),
        "benchmark_definition_quantiles_path": benchmark_definition.get("quantiles_path"),
        **summary_paths,
        **gate,
    }
