"""Build the canonical HS10 description table."""

from __future__ import annotations

from typing import Any
import pandas as pd

from .config import PipelineConfig
from .io_utils import add_hierarchy_codes, normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet, write_stata_if_enabled


def run_hs10_code_build(config: PipelineConfig) -> dict[str, Any]:
    source_path = config.reference_dir / "hs10_concordance_source.parquet"
    if source_path.exists():
        df = read_table(source_path)
    else:
        df = read_table(config.fajgelbaum_analysis_dir / "hs10_codes.dta")

    if "hs10_desc" not in df.columns:
        df = df.rename(columns={"description": "hs10_desc", "desc": "hs10_desc"})
    df["hs10"] = df["hs10"].map(lambda value: normalize_hs_code(value, 10))
    df = add_hierarchy_codes(df[["hs10", "hs10_desc"]].dropna(subset=["hs10"]).drop_duplicates(), "hs10")
    df["hs10_desc"] = df["hs10_desc"].astype("string")
    df["source"] = source_path.name if source_path.exists() else "fajgelbaum_reference"
    df = df[["hs10", "hs8", "hs6", "hs4", "hs2", "hs10_desc", "source"]].sort_values("hs10").reset_index(drop=True)

    parquet_path = config.reference_dir / "hs10_codes.parquet"
    dta_path = config.reference_dir / "hs10_codes.dta"
    write_parquet(df, parquet_path, overwrite=True)
    write_stata_if_enabled(df, dta_path, enabled=config.export_dta(), overwrite=True)
    write_data_dictionary(df, config.reference_dir / "hs10_codes.dictionary.json", key_columns=["hs10"])
    write_metadata_json(config.reference_dir / "hs10_codes.metadata.json", {"rows": int(len(df)), "columns": list(df.columns)})
    return {"rows": int(len(df)), "outputs": {"parquet": str(parquet_path), "dta": str(dta_path) if config.export_dta() else None}}
