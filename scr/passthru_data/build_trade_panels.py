"""Build import and export passthrough trade panels."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet, write_stata_if_enabled

PANEL_SPECS = {
    "imports": {
        "basename": "m_flow_hs10_fm_new",
        "prefix": "m",
        "reference": "m_flow_hs10_fm_new.dta",
        "minimal_columns": ["cty_code", "cty_name", "hs10", "hs8", "hs6", "hs4", "hs2", "year", "month", "mdate", "m_val", "m_q1"],
    },
    "exports": {
        "basename": "x_flow_hs10_fm_new",
        "prefix": "x",
        "reference": "x_flow_hs10_fm_new.dta",
        "minimal_columns": ["cty_code", "cty_name", "hs10", "hs8", "hs6", "hs4", "hs2", "year", "month", "mdate", "x_val", "x_q1"],
    },
}


def _stage_metadata(config: PipelineConfig, flow: str) -> dict[str, Any]:
    path = config.staging_dir / f"{flow}_trade_staging.metadata.json"
    if not path.exists():
        return {}
    import json
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_reference_panel(df: pd.DataFrame) -> pd.DataFrame:
    for column, digits in (("hs10", 10), ("hs8", 8), ("hs6", 6), ("hs4", 4), ("hs2", 2)):
        if column in df.columns:
            df[column] = df[column].map(lambda value: normalize_hs_code(value, digits)).astype("string")
    if "cty_name" in df.columns:
        df["cty_name"] = df["cty_name"].astype("string").str.upper()
    return df


def _build_minimal_panel(staging_df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    out = staging_df.rename(columns={
        "partner_code": "cty_code",
        "partner_name": "cty_name",
        "trade_value": f"{prefix}_val",
        "quantity": f"{prefix}_q1",
    }).copy()
    out["cty_code"] = pd.to_numeric(out["cty_code"], errors="coerce").fillna(-9999).astype(int)
    out["cty_name"] = out["cty_name"].astype("string")
    out["mdate"] = pd.to_datetime(out["period"] + "-01")
    ordered = [
        "cty_code", "cty_name", "hs10", "hs8", "hs6", "hs4", "hs2", "year", "month", "mdate", f"{prefix}_val", f"{prefix}_q1"
    ]
    return out[ordered].sort_values(["cty_name", "hs10", "year", "month"]).reset_index(drop=True)


def _materialize_panel(config: PipelineConfig, flow: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    spec = PANEL_SPECS[flow]
    metadata = _stage_metadata(config, flow)
    staging_path = config.staging_dir / f"{flow}_trade_staging.parquet"
    if metadata.get("raw_files") and any("reference_fallback" in str(item) for item in metadata.values()):
        reference_path = config.fajgelbaum_analysis_dir / spec["reference"]
        df = _normalize_reference_panel(read_table(reference_path))
        return df, {"build_mode": "reference_fallback", "source": str(reference_path)}

    staging_df = read_table(staging_path)
    panel = _build_minimal_panel(staging_df, spec["prefix"])
    return panel, {"build_mode": "manual_or_raw", "source": str(staging_path)}


def run_trade_panel_build(config: PipelineConfig) -> dict[str, Any]:
    """Build import and export analysis panels."""
    outputs: dict[str, Any] = {}
    for flow, spec in PANEL_SPECS.items():
        panel_df, metadata = _materialize_panel(config, flow)
        panel_df = _normalize_reference_panel(panel_df)
        parquet_path = config.analysis_dir / f"{spec['basename']}.parquet"
        dta_path = config.analysis_dir / f"{spec['basename']}.dta"
        write_parquet(panel_df, parquet_path, overwrite=True)
        write_stata_if_enabled(panel_df, dta_path, enabled=config.export_dta(), overwrite=True)
        write_data_dictionary(panel_df, config.analysis_dir / f"{spec['basename']}.dictionary.json", key_columns=["cty_name", "hs10", "year", "month"])
        dupes = int(panel_df.duplicated(subset=[column for column in ["cty_code", "cty_name", "hs10", "year", "month"] if column in panel_df.columns]).sum())
        write_metadata_json(config.analysis_dir / f"{spec['basename']}.metadata.json", metadata | {"rows": int(len(panel_df)), "duplicate_keys": dupes})
        outputs[flow] = {"rows": int(len(panel_df)), "outputs": {"parquet": str(parquet_path), "dta": str(dta_path) if config.export_dta() else None}, "metadata": metadata}
    return outputs
