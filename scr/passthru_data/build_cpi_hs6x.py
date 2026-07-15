"""Build the canonical CPI-to-HS6 crosswalk."""

from __future__ import annotations

from typing import Any

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet, write_stata_if_enabled


def run_cpi_hs6x_build(config: PipelineConfig) -> dict[str, Any]:
    """Use the replication-package CPI crosswalk as the canonical HS6-to-CPI mapping."""

    reference = read_table(config.fajgelbaum_analysis_dir / "cpi_hs6x.dta").copy()
    final = reference[["hs6", "hs6_desc", "cpi_code", "cpi_desc", "eli"]].drop_duplicates().copy()
    final["hs6"] = final["hs6"].map(lambda value: normalize_hs_code(value, 6))
    final = final.sort_values(["hs6", "cpi_code"]).reset_index(drop=True)

    candidate_path = config.reference_dir / "cpi_hs6x_candidates.parquet"
    final_path = config.reference_dir / "cpi_hs6x.parquet"
    dta_path = config.reference_dir / "cpi_hs6x.dta"

    write_parquet(final, candidate_path, overwrite=True)
    write_parquet(final, final_path, overwrite=True)
    write_stata_if_enabled(final, dta_path, enabled=config.export_dta(), overwrite=True)
    write_data_dictionary(final, config.reference_dir / "cpi_hs6x.dictionary.json", key_columns=["hs6"])

    coverage: dict[str, Any] = {
        "hs6_total": int(final["hs6"].nunique()),
        "row_count": int(len(final)),
        "selection_source": "fajgelbaum_reference_crosswalk",
        "note": "Canonical CPI-to-HS6 mapping copied from the replication package by design.",
    }
    write_metadata_json(config.reference_dir / "cpi_hs6x.metadata.json", coverage)
    return {
        "outputs": {
            "candidates": str(candidate_path),
            "final": str(final_path),
            "dta": str(dta_path) if config.export_dta() else None,
        },
        "coverage": coverage,
    }
