"""Merge rerouted-share controls into the import HS6 analysis panel."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet


def _load_rerouted_shares(config: PipelineConfig) -> pd.DataFrame:
    path = config.repo_root / "data" / "rerouted_shares" / "data_share_rerouted.dta"
    frame = read_table(path)
    frame["hs6"] = frame["hs_6dig"].map(lambda value: normalize_hs_code(value, 6))
    frame["mdate"] = pd.to_datetime(frame["modate_imports"], errors="coerce")
    frame["year"] = frame["mdate"].dt.year.astype("Int64")
    frame["month"] = frame["mdate"].dt.month.astype("Int64")
    frame["share_rerouted"] = pd.to_numeric(frame["share_rerouted"], errors="coerce")
    frame["tariff_increase"] = pd.to_numeric(frame.get("tariff_increase"), errors="coerce")
    frame = frame.dropna(subset=["hs6", "year", "month"]).copy()
    frame = frame.drop_duplicates(["hs6", "year", "month"], keep="last").reset_index(drop=True)
    init = (
        frame.loc[frame["year"] == 2017, ["hs6", "share_rerouted"]]
        .groupby("hs6", as_index=False)["share_rerouted"]
        .mean()
        .rename(columns={"share_rerouted": "reroute_share_init_2017"})
    )
    frame = frame.merge(init, on="hs6", how="left")
    frame = frame.rename(columns={"share_rerouted": "reroute_share_t", "tariff_increase": "reroute_tariff_increase_t"})
    return frame[["hs6", "year", "month", "mdate", "reroute_share_t", "reroute_share_init_2017", "reroute_tariff_increase_t"]]


def run_build_rerouting_controls(config: PipelineConfig) -> dict[str, Any]:
    imports_hs6_path = config.analysis_dir / "imports_hs6_raw_package_shocks.parquet"
    imports = read_table(imports_hs6_path)
    controls = _load_rerouted_shares(config)
    merged = imports.merge(controls, on=["hs6", "year", "month"], how="left", validate="many_to_one")
    merged["treated"] = (pd.to_numeric(merged["m_ess"], errors="coerce").fillna(0) == 2).astype("int8")
    merged["reroute_treated_t"] = pd.to_numeric(merged["reroute_share_t"], errors="coerce") * merged["treated"]
    merged["reroute_treated_init"] = pd.to_numeric(merged["reroute_share_init_2017"], errors="coerce") * merged["treated"]

    output = config.analysis_dir / "imports_hs6_raw_package_shocks_rerouting.parquet"
    write_parquet(merged, output, overwrite=True)
    write_data_dictionary(merged, output.with_suffix(".dictionary.json"), key_columns=["cty_code", "hs6", "year", "month"])

    meta = {
        "rows_imports_hs6": int(len(imports)),
        "rows_output": int(len(merged)),
        "share_merge_non_null": float(merged["reroute_share_t"].notna().mean()) if len(merged) else 0.0,
        "init_merge_non_null": float(merged["reroute_share_init_2017"].notna().mean()) if len(merged) else 0.0,
        "period_min": None if merged.empty else f"{int(merged['year'].min()):04d}-{int(merged['month'].min()):02d}",
        "period_max": None if merged.empty else f"{int(merged['year'].max()):04d}-{int(merged['month'].max()):02d}",
        "output_path": str(output),
    }
    write_metadata_json(config.analysis_dir / "imports_hs6_rerouting_controls.metadata.json", meta)
    return meta

