"""Materialize regression-specific trade workhorse panels."""

from __future__ import annotations

from typing import Any
import json
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import read_table, write_data_dictionary, write_parquet, write_stata_if_enabled
from .trade_regression_common import (
    WORKHORSE_SPECS,
    filter_to_paper_window,
    filter_to_analysis_window,
    normalize_workhorse_panel,
    panel_has_required_columns,
    workhorse_metadata_path,
    workhorse_output_path,
    write_step_manifest,
)
from .trade_regression_sources import run_trade_regression_source_audit


def _month_index(year: pd.Series, month: pd.Series) -> pd.Series:
    return (pd.to_numeric(year, errors="coerce") * 12 + pd.to_numeric(month, errors="coerce") - 1).astype("Int64")


def _build_imports_from_bilateral_raw(config: PipelineConfig) -> pd.DataFrame:
    source = config.analysis_dir / "us_products_partner_hs10_monthly_regression.parquet"
    if not source.exists():
        source = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not source.exists():
        raise RuntimeError(
            "Raw-only mode: missing bilateral policy panel. "
            f"Expected {config.analysis_dir / 'us_products_partner_hs10_monthly_regression.parquet'}."
        )
    cols = [
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
        "m_statutory_tariff2",
        "tw_increment_rate_raw",
    ]
    schema_cols = set(pq.read_schema(source).names)
    keep = [column for column in cols if column in schema_cols]
    frame = read_table(source, columns=keep)
    frame = frame.loc[pd.to_numeric(frame["cty_code"], errors="coerce").fillna(-9999) > 0].copy()
    frame["m_val"] = pd.to_numeric(frame.get("m_val"), errors="coerce")
    frame["m_q1"] = pd.to_numeric(frame.get("m_q1"), errors="coerce")
    frame["m_stattariff2"] = pd.to_numeric(frame.get("m_statutory_tariff2"), errors="coerce").fillna(0.0)
    valid_qty = frame["m_q1"] > 0
    frame["m_p"] = np.where(valid_qty, frame["m_val"] / frame["m_q1"], np.nan)
    frame["m_pduty"] = frame["m_p"] * (1.0 + frame["m_stattariff2"].fillna(0.0))
    frame["month_index"] = _month_index(frame["year"], frame["month"])
    factorized = pd.factorize(
        frame["cty_code"].astype("Int64").astype(str) + "|" + frame["hs10"].astype("string"),
        sort=False,
    )[0]
    frame["id"] = pd.Series(factorized, index=frame.index, dtype="Int64")
    frame["naics_str"] = frame["hs4"].astype("string").str.zfill(4) + "00"
    tw = pd.to_numeric(frame.get("tw_increment_rate_raw"), errors="coerce").fillna(0.0)
    treated = tw > 0
    first_treat = frame.loc[treated].groupby("id")["month_index"].min()
    frame["m_effective_mdate2"] = frame["id"].map(first_treat).astype("Int64")
    frame["m_ess"] = pd.Series(np.where(frame["m_effective_mdate2"].notna(), 2, 0), index=frame.index, dtype="Int64")
    frame["m_status2"] = pd.Series(
        np.where(
            frame["m_effective_mdate2"].notna() & (frame["month_index"] >= frame["m_effective_mdate2"]),
            2,
            0,
        ),
        index=frame.index,
        dtype="Int64",
    )
    required = WORKHORSE_SPECS["imports"]["required_columns"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Raw-only mode: unable to construct imports workhorse columns: {missing}")
    return frame[required].copy()


def _select_source_panel(config: PipelineConfig, flow: str, audit: dict[str, Any]):
    spec = WORKHORSE_SPECS[flow]
    if flow == "imports" and config.analysis_window == "current":
        section301_path = config.analysis_dir / "section301_imports_hs10.parquet"
        if not section301_path.exists():
            raise RuntimeError(
                "Current Section 301 analysis requires section301_imports_hs10.parquet. "
                "Run --only-step build_section301_import_panel after the raw bilateral policy build."
            )
        metadata_path = config.analysis_dir / "section301_imports_hs10.metadata.json"
        if not metadata_path.exists():
            raise RuntimeError("Current Section 301 analysis requires section301_imports_hs10.metadata.json for freshness validation.")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        raw_trade_path = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
        if raw_trade_path.exists():
            raw_periods = read_table(raw_trade_path, columns=["year", "month"])
            raw_periods = raw_periods[["year", "month"]].dropna().sort_values(["year", "month"])
            if not raw_periods.empty:
                raw_last = raw_periods.iloc[-1]
                raw_max = f"{int(raw_last['year']):04d}-{int(raw_last['month']):02d}"
                if metadata.get("period_max") != raw_max:
                    raise RuntimeError(
                        "Current Section 301 panel is stale: it ends at "
                        f"{metadata.get('period_max')}, while the raw Census import panel ends at {raw_max}. "
                        "Rebuild the bilateral policy panel and Section 301 panel first."
                    )
        frame = read_table(section301_path)
        is_ready, missing = panel_has_required_columns(frame, flow)
        if not is_ready:
            raise RuntimeError(f"Section 301 panel is not regression-ready: {missing}")
        return frame[spec["required_columns"]].copy(), {"build_mode": "raw_section301_current", "source_path": str(section301_path)}
    local_path = config.analysis_dir / f"{spec['basename']}.parquet"
    if local_path.exists():
        local_df = read_table(local_path)
        is_ready, missing = panel_has_required_columns(local_df, flow)
        if is_ready:
            return local_df[spec["required_columns"]].copy(), {"build_mode": "local_analysis_workhorse", "source_path": str(local_path)}
        if flow == "imports":
            built = _build_imports_from_bilateral_raw(config)
            return built, {"build_mode": "constructed_from_raw_bilateral_panel", "source_path": str(config.analysis_dir / "us_products_partner_hs10_monthly.parquet")}
        raise RuntimeError(
            f"Raw-only mode: local panel exists but is not regression-ready for {flow}. "
            f"Missing columns: {missing}. Source: {local_path}. "
            "Run with --trade-flow imports for raw-only policy regressions."
        )
    raise RuntimeError(
        f"Raw-only mode: required local panel for {flow} does not exist at {local_path}. "
        "Reference-package fallback is disabled."
    )


def run_trade_workhorse_panel_build(config: PipelineConfig) -> dict[str, Any]:
    audit = run_trade_regression_source_audit(config)
    outputs: dict[str, Any] = {}
    flows = [config.trade_flow] if config.trade_flow else list(WORKHORSE_SPECS)
    for flow in flows:
        spec = WORKHORSE_SPECS[flow]
        panel_df, metadata = _select_source_panel(config, flow, audit)
        input_rows = int(len(panel_df))
        panel_df = normalize_workhorse_panel(panel_df)
        panel_df = filter_to_analysis_window(panel_df, flow, config.analysis_window)
        parquet_path = workhorse_output_path(config, flow)
        dta_path = parquet_path.with_suffix(".dta")
        write_parquet(panel_df, parquet_path, overwrite=True)
        write_stata_if_enabled(panel_df, dta_path, enabled=config.export_dta(), overwrite=True)
        write_data_dictionary(panel_df, parquet_path.with_suffix(".dictionary.json"), key_columns=["id", "mdate"])
        periods = panel_df[["year", "month"]].drop_duplicates().sort_values(["year", "month"])
        step_meta = metadata | {
            "input_rows_pre_normalization": input_rows,
            "rows": int(len(panel_df)),
            "dropped_rows_nonpositive_cty_code": max(input_rows - int(len(panel_df)), 0),
            "columns": panel_df.columns.tolist(),
            "paper_window_start": spec["paper_start_period"],
            "paper_window_end": spec["paper_end_period"],
            "analysis_window": config.analysis_window,
            "first_period": None if periods.empty else f"{int(periods.iloc[0]['year']):04d}-{int(periods.iloc[0]['month']):02d}",
            "last_period": None if periods.empty else f"{int(periods.iloc[-1]['year']):04d}-{int(periods.iloc[-1]['month']):02d}",
        }
        write_step_manifest(workhorse_metadata_path(config, flow), step_meta)
        outputs[flow] = {
            "rows": int(len(panel_df)),
            "build_mode": metadata["build_mode"],
            "parquet": str(parquet_path),
            "dta": str(dta_path) if config.export_dta() else None,
        }
    return outputs
