"""Build import analysis panels using raw trade flows plus package tariff shocks."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet


def _load_raw_imports(config: PipelineConfig) -> pd.DataFrame:
    path = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
    cols = ["cty_code", "cty_name", "hs10", "hs8", "hs6", "year", "month", "m_val", "m_q1"]
    frame = read_table(path, columns=cols)
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10))
    frame["hs8"] = frame["hs8"].map(lambda value: normalize_hs_code(value, 8))
    frame["hs6"] = frame["hs6"].map(lambda value: normalize_hs_code(value, 6))
    return frame


def _load_package_shocks(config: PipelineConfig) -> pd.DataFrame:
    path = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    cols = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "m_ess",
        "m_status2",
        "m_effective_mdate2",
        "m_stattariff2",
        "m_stattariff1",
    ]
    frame = read_table(path, columns=cols)
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10))
    return frame.drop_duplicates(["cty_code", "hs10", "year", "month"], keep="last").reset_index(drop=True)


def _build_hs10_panel(config: PipelineConfig) -> pd.DataFrame:
    raw_imports = _load_raw_imports(config)
    shocks = _load_package_shocks(config)
    panel = raw_imports.merge(shocks, on=["cty_code", "hs10", "year", "month"], how="left")
    panel["m_p"] = np.where(pd.to_numeric(panel["m_q1"], errors="coerce") > 0, panel["m_val"] / panel["m_q1"], np.nan)
    panel["m_pduty"] = panel["m_p"] * (1.0 + pd.to_numeric(panel["m_stattariff2"], errors="coerce").fillna(0.0))
    panel["mdate"] = pd.to_datetime(
        panel["year"].astype("Int64").astype(str) + "-" + panel["month"].astype("Int64").astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )
    return panel.sort_values(["cty_code", "hs10", "year", "month"]).reset_index(drop=True)


def _build_hs6_panel(hs10_panel: pd.DataFrame) -> pd.DataFrame:
    work = hs10_panel.copy()
    work["m_val"] = pd.to_numeric(work["m_val"], errors="coerce")
    work["m_q1"] = pd.to_numeric(work["m_q1"], errors="coerce")
    work["m_stattariff2"] = pd.to_numeric(work["m_stattariff2"], errors="coerce")
    work["m_stattariff1"] = pd.to_numeric(work["m_stattariff1"], errors="coerce")
    key_cols = ["cty_code", "cty_name", "hs6", "year", "month"]
    work["w_t2"] = work["m_val"].fillna(0.0) * work["m_stattariff2"].fillna(0.0)
    work["w_t2_den"] = np.where(work["m_stattariff2"].notna(), work["m_val"].fillna(0.0), 0.0)
    work["w_t1"] = work["m_val"].fillna(0.0) * work["m_stattariff1"].fillna(0.0)
    work["w_t1_den"] = np.where(work["m_stattariff1"].notna(), work["m_val"].fillna(0.0), 0.0)
    out = (
        work.groupby(key_cols, dropna=False, sort=False)
        .agg(
            m_val=("m_val", "sum"),
            m_q1=("m_q1", "sum"),
            m_ess=("m_ess", "max"),
            m_status2=("m_status2", "max"),
            m_effective_mdate2=("m_effective_mdate2", "min"),
            w_t2=("w_t2", "sum"),
            w_t2_den=("w_t2_den", "sum"),
            w_t1=("w_t1", "sum"),
            w_t1_den=("w_t1_den", "sum"),
        )
        .reset_index()
    )
    out["m_stattariff2"] = np.where(out["w_t2_den"] > 0, out["w_t2"] / out["w_t2_den"], np.nan)
    out["m_stattariff1"] = np.where(out["w_t1_den"] > 0, out["w_t1"] / out["w_t1_den"], np.nan)
    out = out.drop(columns=["w_t2", "w_t2_den", "w_t1", "w_t1_den"])
    out["m_p"] = np.where(out["m_q1"] > 0, out["m_val"] / out["m_q1"], np.nan)
    out["m_pduty"] = out["m_p"] * (1.0 + pd.to_numeric(out["m_stattariff2"], errors="coerce").fillna(0.0))
    out["mdate"] = pd.to_datetime(
        out["year"].astype("Int64").astype(str) + "-" + out["month"].astype("Int64").astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )
    out = out.sort_values(["cty_code", "hs6", "year", "month"]).reset_index(drop=True)
    out["id"] = pd.factorize(out["cty_code"].astype("Int64").astype(str) + "|" + out["hs6"].astype("string"), sort=False)[0].astype("int64")
    return out


def run_build_imports_with_package_shocks(config: PipelineConfig) -> dict[str, Any]:
    hs10_panel = _build_hs10_panel(config)
    hs6_panel = _build_hs6_panel(hs10_panel)

    hs10_path = config.analysis_dir / "imports_hs10_raw_package_shocks.parquet"
    hs6_path = config.analysis_dir / "imports_hs6_raw_package_shocks.parquet"
    write_parquet(hs10_panel, hs10_path, overwrite=True)
    write_parquet(hs6_panel, hs6_path, overwrite=True)
    write_data_dictionary(hs10_panel, hs10_path.with_suffix(".dictionary.json"), key_columns=["cty_code", "hs10", "year", "month"])
    write_data_dictionary(hs6_panel, hs6_path.with_suffix(".dictionary.json"), key_columns=["cty_code", "hs6", "year", "month"])

    meta = {
        "rows_hs10": int(len(hs10_panel)),
        "rows_hs6": int(len(hs6_panel)),
        "period_hs10_min": None if hs10_panel.empty else f"{int(hs10_panel['year'].min()):04d}-{int(hs10_panel['month'].min()):02d}",
        "period_hs10_max": None if hs10_panel.empty else f"{int(hs10_panel['year'].max()):04d}-{int(hs10_panel['month'].max()):02d}",
        "shock_non_null_hs10": int(hs10_panel["m_stattariff2"].notna().sum()),
        "shock_non_null_hs6": int(hs6_panel["m_stattariff2"].notna().sum()),
        "output_hs10": str(hs10_path),
        "output_hs6": str(hs6_path),
    }
    write_metadata_json(config.analysis_dir / "imports_raw_package_shocks.metadata.json", meta)
    return meta
