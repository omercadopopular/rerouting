"""Verification utilities for rebuilt passthrough datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json

import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_metadata_json

TARGETS = {
    "hs10_codes": {"built": "reference/hs10_codes.parquet", "reference": "hs10_codes.dta", "keys": ["hs10"], "columns": None},
    "hs6_bec": {"built": "reference/hs6_bec.parquet", "reference": "hs6_bec.dta", "keys": ["hs6"], "columns": None},
    "cpi_hs6x": {"built": "reference/cpi_hs6x.parquet", "reference": "cpi_hs6x.dta", "keys": ["hs6"], "columns": None},
    "m_flow_hs10_fm_new": {"built": "analysis/m_flow_hs10_fm_new.parquet", "reference": "m_flow_hs10_fm_new.dta", "keys": ["cty_code", "cty_name", "hs10", "year", "month"], "columns": ["cty_code", "cty_name", "hs10", "year", "month", "m_val", "m_q1", "m_hit", "m_stattariff1", "m_applied_tariff", "m_ess"]},
    "x_flow_hs10_fm_new": {"built": "analysis/x_flow_hs10_fm_new.parquet", "reference": "x_flow_hs10_fm_new.dta", "keys": ["cty_code", "cty_name", "hs10", "year", "month"], "columns": ["cty_code", "cty_name", "hs10", "year", "month", "x_val", "x_q1", "x_hit", "x_stattariff1", "x_mfn_tariff", "x_ess"]},
}


def _resolve_built_path(config: PipelineConfig, relative: str) -> Path:
    area, name = relative.split("/", 1)
    base = config.reference_dir if area == "reference" else config.analysis_dir
    return base / name


def _load_dataset(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if path.suffix.lower() == ".dta":
        return read_table(path, columns=columns)
    frame = read_table(path)
    if columns is None:
        return frame
    present = [column for column in columns if column in frame.columns]
    return frame[present]


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column, digits in (("hs10", 10), ("hs8", 8), ("hs6", 6), ("hs4", 4), ("hs2", 2)):
        if column in out.columns:
            out[column] = out[column].map(lambda value: normalize_hs_code(value, digits)).astype("string")
    if "cty_name" in out.columns:
        out["cty_name"] = out["cty_name"].astype("string").str.upper()
    if "period" in out.columns:
        out["period"] = out["period"].astype("string")
    return out


def _pair_summary(name: str, built: pd.DataFrame, reference: pd.DataFrame, keys: list[str]) -> dict[str, Any]:
    built = _normalize_frame(built)
    reference = _normalize_frame(reference)
    shared_columns = sorted(set(built.columns) & set(reference.columns))
    key_columns = [column for column in keys if column in shared_columns]
    summary: dict[str, Any] = {
        "dataset": name,
        "built_rows": int(len(built)),
        "reference_rows": int(len(reference)),
        "built_columns": sorted(built.columns.tolist()),
        "reference_columns": sorted(reference.columns.tolist()),
        "shared_columns": shared_columns,
        "built_only_columns": sorted(set(built.columns) - set(reference.columns)),
        "reference_only_columns": sorted(set(reference.columns) - set(built.columns)),
        "duplicate_keys_built": int(built.duplicated(subset=key_columns).sum()) if key_columns else None,
        "duplicate_keys_reference": int(reference.duplicated(subset=key_columns).sum()) if key_columns else None,
    }
    if "year" in built.columns and "month" in built.columns and not built.empty:
        summary["built_period_min"] = f"{int(built['year'].min()):04d}-{int(built['month'].min()):02d}"
        summary["built_period_max"] = f"{int(built['year'].max()):04d}-{int(built['month'].max()):02d}"
    if "year" in reference.columns and "month" in reference.columns and not reference.empty:
        summary["reference_period_min"] = f"{int(reference['year'].min()):04d}-{int(reference['month'].min()):02d}"
        summary["reference_period_max"] = f"{int(reference['year'].max()):04d}-{int(reference['month'].max()):02d}"

    if key_columns:
        built_keys = built[key_columns].drop_duplicates()
        reference_keys = reference[key_columns].drop_duplicates()
        merged_keys = built_keys.merge(reference_keys, on=key_columns, how="inner")
        summary["key_overlap_rows"] = int(len(merged_keys))
        summary["key_overlap_rate_vs_built"] = float(len(merged_keys) / max(len(built_keys), 1))
        summary["key_overlap_rate_vs_reference"] = float(len(merged_keys) / max(len(reference_keys), 1))

    numeric_columns = [column for column in shared_columns if pd.api.types.is_numeric_dtype(built[column]) and pd.api.types.is_numeric_dtype(reference[column])]
    summary["numeric_summaries"] = [
        {
            "column": column,
            "built_sum": float(pd.to_numeric(built[column], errors="coerce").fillna(0).sum()),
            "reference_sum": float(pd.to_numeric(reference[column], errors="coerce").fillna(0).sum()),
            "built_mean": float(pd.to_numeric(built[column], errors="coerce").mean()),
            "reference_mean": float(pd.to_numeric(reference[column], errors="coerce").mean()),
        }
        for column in numeric_columns[:12]
    ]
    return summary


def _soft_master_validation(config: PipelineConfig) -> dict[str, Any]:
    master_path = config.fajgelbaum_analysis_dir / "master_panel_hs10.dta"
    if not master_path.exists():
        return {"status": "reference_unavailable"}
    if master_path.stat().st_size > 1_000_000_000:
        return {
            "status": "skipped_large_reference",
            "path": str(master_path),
            "size_bytes": master_path.stat().st_size,
            "note": "Soft master validation skipped because pandas.read_stata exceeds available memory on this file in the current environment.",
        }
    master_columns = ["cty_code", "cty_name", "hs10", "year", "month"]
    master = _normalize_frame(_load_dataset(master_path, columns=master_columns))
    results = {}
    for dataset_name, built_name in (("imports", "m_flow_hs10_fm_new.parquet"), ("exports", "x_flow_hs10_fm_new.parquet")):
        built_path = config.analysis_dir / built_name
        if not built_path.exists():
            results[dataset_name] = {"status": "built_missing"}
            continue
        built = _normalize_frame(_load_dataset(built_path, columns=master_columns))
        overlap = built[master_columns].drop_duplicates().merge(master[master_columns].drop_duplicates(), on=master_columns, how="inner")
        results[dataset_name] = {"status": "ok", "overlap_rows": int(len(overlap)), "built_unique_keys": int(len(built[master_columns].drop_duplicates()))}
    return results


def run_verification(config: PipelineConfig) -> dict[str, Any]:
    verification_dir = config.verification_dir
    verification_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, Any]] = []

    for target, spec in TARGETS.items():
        built_path = _resolve_built_path(config, spec["built"])
        reference_path = config.fajgelbaum_analysis_dir / spec["reference"]
        if not built_path.exists():
            summaries.append({"dataset": target, "status": "built_missing", "built_path": str(built_path), "reference_path": str(reference_path)})
            continue
        if not reference_path.exists():
            summaries.append({"dataset": target, "status": "reference_missing", "built_path": str(built_path), "reference_path": str(reference_path)})
            continue
        built_df = _load_dataset(built_path, columns=spec["columns"])
        reference_df = _load_dataset(reference_path, columns=spec["columns"])
        summary = _pair_summary(target, built_df, reference_df, spec["keys"])
        summary["status"] = "ok"
        summary["built_path"] = str(built_path)
        summary["reference_path"] = str(reference_path)
        summaries.append(summary)
        write_metadata_json(verification_dir / f"{target}_diagnostics.json", summary)
        pd.DataFrame(summary.get("numeric_summaries", [])).to_csv(verification_dir / f"{target}_numeric_summary.csv", index=False)

    soft_master = _soft_master_validation(config)
    write_metadata_json(verification_dir / "master_panel_soft_validation.json", soft_master)
    summary_df = pd.DataFrame(summaries)
    summary_csv = verification_dir / "verification_summary.csv"
    summary_df.to_csv(summary_csv, index=False)

    markdown_lines = ["# Passthru Data Verification", ""]
    for summary in summaries:
        markdown_lines.append(f"## {summary['dataset']}")
        markdown_lines.append(f"- Status: {summary.get('status', 'ok')}")
        markdown_lines.append(f"- Built rows: {summary.get('built_rows')}")
        markdown_lines.append(f"- Reference rows: {summary.get('reference_rows')}")
        if "key_overlap_rate_vs_reference" in summary:
            markdown_lines.append(f"- Key overlap vs reference: {summary['key_overlap_rate_vs_reference']:.4f}")
        built_only = summary.get("built_only_columns")
        if built_only:
            markdown_lines.append(f"- Built-only columns: {', '.join(built_only)}")
        ref_only = summary.get("reference_only_columns")
        if ref_only:
            markdown_lines.append(f"- Reference-only columns: {', '.join(ref_only)}")
        markdown_lines.append("")
    markdown_lines.append("## Master Panel Soft Validation")
    markdown_lines.append(f"```json\n{json.dumps(soft_master, indent=2)}\n```")
    report_path = verification_dir / "verification_report.md"
    report_path.write_text("\n".join(markdown_lines), encoding="utf-8")

    payload = {"datasets": summaries, "master_panel_soft_validation": soft_master}
    write_metadata_json(verification_dir / "verification_diagnostics.json", payload)
    return {"summary_csv": str(summary_csv), "report": str(report_path), "json": str(verification_dir / 'verification_diagnostics.json')}
