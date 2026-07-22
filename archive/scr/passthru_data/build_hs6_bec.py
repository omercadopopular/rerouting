"""Build the canonical HS6-BEC reference table."""

from __future__ import annotations

from typing import Any
import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet, write_stata_if_enabled


def run_hs6_bec_build(config: PipelineConfig) -> dict[str, Any]:
    source_path = config.reference_dir / "hs6_bec_source.parquet"
    if source_path.exists():
        df = read_table(source_path)
    else:
        df = read_table(config.fajgelbaum_analysis_dir / "hs6_bec.dta")

    df = df.rename(columns={"bec_code": "bec"})
    df["hs6"] = df["hs6"].map(lambda value: normalize_hs_code(value, 6))
    df["bec"] = pd.to_numeric(df["bec"], errors="coerce").astype("Int64")
    if "bec_description" not in df.columns:
        df["bec_description"] = pd.NA
    df = df[["hs6", "bec", "bec_description"]].dropna(subset=["hs6", "bec"]).drop_duplicates(subset=["hs6"], keep="first")
    df["source"] = source_path.name if source_path.exists() else "fajgelbaum_reference"
    df = df.sort_values("hs6").reset_index(drop=True)

    parquet_path = config.reference_dir / "hs6_bec.parquet"
    dta_path = config.reference_dir / "hs6_bec.dta"
    write_parquet(df, parquet_path, overwrite=True)
    write_stata_if_enabled(df, dta_path, enabled=config.export_dta(), overwrite=True)
    write_data_dictionary(df, config.reference_dir / "hs6_bec.dictionary.json", key_columns=["hs6"])
    write_metadata_json(config.reference_dir / "hs6_bec.metadata.json", {"rows": int(len(df)), "columns": list(df.columns)})
    return {"rows": int(len(df)), "outputs": {"parquet": str(parquet_path), "dta": str(dta_path) if config.export_dta() else None}}
