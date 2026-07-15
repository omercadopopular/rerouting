"""Shared constants and helpers for trade regression replication."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, normalize_period, write_metadata_json

REGRESSION_OUTCOMES = ("val", "q1", "p", "pduty")
PAPER_START_PERIOD = "2017-01"
PAPER_END_PERIOD = "2019-04"

WORKHORSE_SPECS: dict[str, dict[str, Any]] = {
    "imports": {
        "basename": "m_flow_hs10_fm_new",
        "prefix": "m",
        "reference": "m_flow_hs10_fm_new.dta",
        "default_event_period": "2018-02",
        "paper_start_period": PAPER_START_PERIOD,
        "paper_end_period": PAPER_END_PERIOD,
        "reference_figure_event": "fig_02.pdf",
        "reference_figure_dynamic": "fig_04a.pdf",
        "cluster_expr": "hs8 + cty_code",
        "cluster_columns": ["hs8", "cty_code"],
        "required_columns": [
            "id",
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
            "m_p",
            "m_pduty",
            "m_ess",
            "m_status2",
            "m_effective_mdate2",
            "m_stattariff2",
            "naics_str",
        ],
    },
    "exports": {
        "basename": "x_flow_hs10_fm_new",
        "prefix": "x",
        "reference": "x_flow_hs10_fm_new.dta",
        "default_event_period": "2018-04",
        "paper_start_period": PAPER_START_PERIOD,
        "paper_end_period": PAPER_END_PERIOD,
        "reference_figure_event": "fig_03.pdf",
        "reference_figure_dynamic": "fig_04b.pdf",
        "cluster_expr": "hs6 + cty_code",
        "cluster_columns": ["hs6", "cty_code"],
        "required_columns": [
            "id",
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
            "x_val",
            "x_q1",
            "x_p",
            "x_pduty",
            "x_ess",
            "x_status2",
            "x_effective_mdate2",
            "x_stattariff2",
            "naics_str",
        ],
    },
}


def regression_dir(config: PipelineConfig) -> Path:
    path = config.analysis_dir / "trade_regressions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def workhorse_dir(config: PipelineConfig) -> Path:
    path = regression_dir(config) / "workhorse"
    if config.analysis_window == "current":
        path = path / "current"
    path.mkdir(parents=True, exist_ok=True)
    return path


def chart_dir(config: PipelineConfig) -> Path:
    path = regression_dir(config) / "charts"
    if config.analysis_window == "current":
        path = path / "current"
    path.mkdir(parents=True, exist_ok=True)
    return path


def table_dir(config: PipelineConfig) -> Path:
    path = regression_dir(config) / "tables"
    if config.analysis_window == "current":
        path = path / "current"
    path.mkdir(parents=True, exist_ok=True)
    return path


def workhorse_output_path(config: PipelineConfig, flow: str) -> Path:
    spec = WORKHORSE_SPECS[flow]
    return workhorse_dir(config) / f"{spec['basename']}_regression.parquet"


def workhorse_metadata_path(config: PipelineConfig, flow: str) -> Path:
    spec = WORKHORSE_SPECS[flow]
    return workhorse_dir(config) / f"{spec['basename']}_regression.metadata.json"


def package_reference_figure_path(config: PipelineConfig, flow: str, spec_name: str) -> Path:
    spec = WORKHORSE_SPECS[flow]
    if spec_name == "event":
        return config.fajgelbaum_root / "results" / "main" / spec["reference_figure_event"]
    if spec_name == "dynamic":
        return config.fajgelbaum_root / "results" / "main" / spec["reference_figure_dynamic"]
    raise ValueError(f"Unknown regression figure spec: {spec_name}")


def normalize_workhorse_panel(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "cty_code" in out.columns:
        cty_numeric = pd.to_numeric(out["cty_code"], errors="coerce")
        out = out.loc[cty_numeric.fillna(-9999) > 0].copy()
        out["cty_code"] = cty_numeric.loc[out.index].astype("Int64")
    for column, digits in (("hs10", 10), ("hs8", 8), ("hs6", 6), ("hs4", 4), ("hs2", 2)):
        if column in out.columns:
            out[column] = out[column].map(lambda value: normalize_hs_code(value, digits)).astype("string")
    if "cty_name" in out.columns:
        out["cty_name"] = out["cty_name"].astype("string").str.upper()
    if "naics_str" in out.columns:
        out["naics_str"] = out["naics_str"].astype("string")
    return out


def panel_has_required_columns(df: pd.DataFrame, flow: str) -> tuple[bool, list[str]]:
    required = WORKHORSE_SPECS[flow]["required_columns"]
    missing = [column for column in required if column not in df.columns]
    return not missing, missing


def filter_to_paper_window(df: pd.DataFrame, flow: str) -> pd.DataFrame:
    spec = WORKHORSE_SPECS[flow]
    start_period = pd.Period(spec["paper_start_period"], freq="M")
    end_period = pd.Period(spec["paper_end_period"], freq="M")
    month_index = month_index_from_columns(df)
    start_index = int(start_period.year) * 12 + int(start_period.month) - 1
    end_index = int(end_period.year) * 12 + int(end_period.month) - 1
    mask = month_index.between(start_index, end_index, inclusive="both")
    return df.loc[mask.fillna(False)].copy()


def filter_to_analysis_window(df: pd.DataFrame, flow: str, analysis_window: str) -> pd.DataFrame:
    """Keep the published window for benchmark work or all available months for current work."""
    if analysis_window == "benchmark":
        return filter_to_paper_window(df, flow)
    if analysis_window == "current":
        return df.loc[pd.to_numeric(df["year"], errors="coerce") >= 2017].copy()
    raise ValueError(f"Unknown analysis window: {analysis_window}")


def stata_month_period_to_index(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return int(value.year) * 12 + int(value.month) - 1
    if isinstance(value, pd.Period):
        return int(value.year) * 12 + int(value.month) - 1
    text = str(value).strip()
    if not text:
        return None
    try:
        if "-" in text:
            period = pd.Period(normalize_period(text), freq="M")
            return int(period.year) * 12 + int(period.month) - 1
        numeric = int(float(text))
        # Stata monthly dates count months since 1960-01.
        if -1000 <= numeric <= 5000:
            return numeric + (1960 * 12)
        return numeric
    except Exception:
        return None


def month_index_from_columns(df: pd.DataFrame, mdate_column: str = "mdate") -> pd.Series:
    if mdate_column in df.columns:
        parsed = df[mdate_column].map(stata_month_period_to_index)
        if parsed.notna().all():
            return parsed.astype("Int64")
    return (pd.to_numeric(df["year"], errors="coerce") * 12 + pd.to_numeric(df["month"], errors="coerce") - 1).astype("Int64")


def write_markdown_report(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return path


def write_step_manifest(path: Path, payload: dict[str, Any]) -> Path:
    return write_metadata_json(path, payload)
