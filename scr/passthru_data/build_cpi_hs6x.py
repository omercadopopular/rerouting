"""Build the canonical CPI-to-HS6 crosswalk."""

from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
import re

import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet, write_stata_if_enabled

STOPWORDS = {"of", "and", "the", "or", "for", "with", "other", "nesoi", "excluding", "including", "not", "elsewhere", "specified"}
TOKEN_RE = re.compile(r"[a-z0-9]+")


def _normalize_text(value: Any) -> str:
    text = "" if value is None else str(value).lower()
    tokens = [token for token in TOKEN_RE.findall(text) if token not in STOPWORDS]
    return " ".join(tokens)


def _score_match(left: str, right: str) -> tuple[float, int]:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    overlap = len(left_tokens & right_tokens)
    ratio = SequenceMatcher(a=left, b=right).ratio()
    return ratio, overlap


def _load_override(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["hs6", "cpi_code", "manual_override", "provenance_note"])
    overrides = read_table(path, dtype=str)
    for column in ("hs6", "cpi_code"):
        if column not in overrides.columns:
            raise ValueError(f"Override file is missing required column '{column}'.")
    if "manual_override" not in overrides.columns:
        overrides["manual_override"] = True
    if "provenance_note" not in overrides.columns:
        overrides["provenance_note"] = "manual_override_file"
    overrides["hs6"] = overrides["hs6"].map(lambda value: normalize_hs_code(value, 6))
    return overrides


def run_cpi_hs6x_build(config: PipelineConfig) -> dict[str, Any]:
    hs10_codes = read_table(config.reference_dir / "hs10_codes.parquet")
    cpi_series = read_table(config.staging_dir / "cpi_series.parquet")
    fallback_reference = read_table(config.fajgelbaum_analysis_dir / "cpi_hs6x.dta")

    hs6_universe = (
        hs10_codes[["hs6", "hs10_desc"]]
        .dropna(subset=["hs6"])
        .assign(hs6_desc=lambda frame: frame["hs10_desc"].astype(str))
        .groupby("hs6", as_index=False)["hs6_desc"]
        .first()
    )
    hs6_universe["hs6_norm"] = hs6_universe["hs6_desc"].map(_normalize_text)

    cpi_candidates = cpi_series.copy()
    cpi_candidates = cpi_candidates.rename(columns={"series_id": "cpi_code", "item_name": "cpi_desc"})
    cpi_candidates["cpi_norm"] = cpi_candidates["cpi_desc"].map(_normalize_text)
    cpi_candidates = cpi_candidates[["cpi_code", "cpi_desc", "eli", "cpi_norm"]].drop_duplicates()

    candidate_rows: list[dict[str, Any]] = []
    for hs_row in hs6_universe.itertuples(index=False):
        top_rows = []
        for cpi_row in cpi_candidates.itertuples(index=False):
            ratio, overlap = _score_match(hs_row.hs6_norm, cpi_row.cpi_norm)
            score = round(ratio + min(overlap, 5) * 0.05, 4)
            if score <= 0:
                continue
            top_rows.append(
                {
                    "hs6": hs_row.hs6,
                    "hs6_desc": hs_row.hs6_desc,
                    "cpi_code": cpi_row.cpi_code,
                    "cpi_desc": cpi_row.cpi_desc,
                    "eli": cpi_row.eli,
                    "match_method": "token_overlap+sequence_ratio",
                    "match_score": score,
                    "token_overlap": overlap,
                }
            )
        top_rows.sort(key=lambda row: (-row["match_score"], -row["token_overlap"], str(row["cpi_code"])))
        for rank, row in enumerate(top_rows[:5], start=1):
            row["candidate_rank"] = rank
            row["needs_manual_review"] = row["match_score"] < 0.7
            candidate_rows.append(row)

    candidates = pd.DataFrame(candidate_rows)
    if candidates.empty:
        candidates = fallback_reference.assign(match_method="reference_fallback", match_score=1.0, token_overlap=pd.NA, candidate_rank=1, needs_manual_review=False)

    overrides = _load_override(config.reference_dir / "manual" / "cpi_hs6x_overrides.csv")
    auto_selected = candidates.sort_values(["hs6", "candidate_rank"]).drop_duplicates("hs6", keep="first").copy()
    auto_selected["manual_override"] = False
    auto_selected["selection_source"] = auto_selected["needs_manual_review"].map(lambda flag: "manual_review_needed" if flag else "auto_high_confidence")
    auto_selected["provenance_note"] = auto_selected["match_method"]

    if not overrides.empty:
        final = auto_selected[~auto_selected["hs6"].isin(overrides["hs6"])].copy()
        override_merge = overrides.merge(candidates.drop(columns=["manual_override", "selection_source", "provenance_note"], errors="ignore"), on=["hs6", "cpi_code"], how="left")
        for column in ("hs6_desc", "cpi_desc", "eli"):
            if column not in override_merge.columns:
                override_merge[column] = pd.NA
        override_merge["manual_override"] = True
        override_merge["selection_source"] = "manual_override"
        override_merge["match_method"] = override_merge.get("match_method", "manual_override")
        override_merge["match_score"] = override_merge.get("match_score", 1.0).fillna(1.0)
        override_merge["needs_manual_review"] = False
        final = pd.concat([final, override_merge[final.columns]], ignore_index=True)
    else:
        final = auto_selected

    final = final[["hs6", "hs6_desc", "cpi_code", "cpi_desc", "eli", "match_method", "match_score", "needs_manual_review", "manual_override", "selection_source", "provenance_note"]].sort_values(["hs6", "cpi_code"]).reset_index(drop=True)

    candidate_path = config.reference_dir / "cpi_hs6x_candidates.parquet"
    final_path = config.reference_dir / "cpi_hs6x.parquet"
    dta_path = config.reference_dir / "cpi_hs6x.dta"
    write_parquet(candidates, candidate_path, overwrite=True)
    write_parquet(final, final_path, overwrite=True)
    write_stata_if_enabled(final, dta_path, enabled=config.export_dta(), overwrite=True)
    write_data_dictionary(final, config.reference_dir / "cpi_hs6x.dictionary.json", key_columns=["hs6"])

    coverage = {
        "hs6_total": int(final["hs6"].nunique()),
        "manual_review_needed": int(final["needs_manual_review"].fillna(False).sum()),
        "manual_override_count": int(final["manual_override"].fillna(False).sum()),
    }
    write_metadata_json(config.reference_dir / "cpi_hs6x.metadata.json", coverage)
    return {"outputs": {"candidates": str(candidate_path), "final": str(final_path), "dta": str(dta_path) if config.export_dta() else None}, "coverage": coverage}
