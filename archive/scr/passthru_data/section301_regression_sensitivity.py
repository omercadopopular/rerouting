"""Section 301 regression sensitivity comparisons against the synchronized benchmark."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyfixest as pf

from .config import PipelineConfig
from .io_utils import normalize_hs_code, write_metadata_json
from .trade_regression_common import write_markdown_report


KEY_COLUMNS = ["cty_code", "hs10", "year", "month"]
OUTCOMES = ("val", "q1", "p", "pduty")
WINDOWS = {
    "paper_6m": {"event_min": -6, "event_max": 6, "baseline": -6, "label": "Paper-faithful 6-month window"},
    "common_12m": {"event_min": -12, "event_max": 12, "baseline": -12, "label": "Common 12-month window"},
}
SENSITIVITY_VERSION = "v2"


def _expected_row_count(window: str) -> int:
    spec = WINDOWS[window]
    return int(spec["event_max"] - spec["event_min"] + 1)


@dataclass(frozen=True)
class VariantSpec:
    code: str
    label: str
    outcome_source: str
    target_source: str
    calendar_source: str
    tariff_source: str


VARIANTS: tuple[VariantSpec, ...] = (
    VariantSpec("A", "Package benchmark", "pkg", "pkg", "pkg_legal", "pkg"),
    VariantSpec("B", "Raw outcomes / package treatment", "raw", "pkg", "pkg_legal", "pkg"),
    VariantSpec("C_map_only", "Raw outcomes / raw treatment / package calendar", "raw", "raw", "pkg_legal", "pkg"),
    VariantSpec("C", "Raw outcomes / raw treatment / package tariff", "raw", "raw", "raw_legal", "pkg"),
    VariantSpec("C_paper", "Raw outcomes / raw treatment / package tariff / paper calendar", "raw", "raw", "raw_paper", "pkg"),
    VariantSpec("D", "Raw outcomes / raw treatment / raw tariff", "raw", "raw", "raw_legal", "raw"),
    VariantSpec("D_paper", "Raw outcomes / raw treatment / raw tariff / paper calendar", "raw", "raw", "raw_paper", "raw"),
)


def _normalized_period_index(value: Any) -> pd.Series | pd.Index | int | float | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return int(value.to_period("M").year) * 12 + int(value.to_period("M").month) - 1
    if isinstance(value, pd.Period):
        return int(value.year) * 12 + int(value.month) - 1
    if isinstance(value, (int, np.integer)):
        return int(value)
    if pd.isna(value):
        return np.nan
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:  # pragma: no cover - defensive
        return np.nan
    if pd.isna(ts):
        return np.nan
    return int(ts.to_period("M").year) * 12 + int(ts.to_period("M").month) - 1


def _month_index_from_columns(frame: pd.DataFrame) -> pd.Series:
    year = pd.to_numeric(frame["year"], errors="coerce")
    month = pd.to_numeric(frame["month"], errors="coerce")
    return (year * 12 + month - 1).astype("Int64")


def _hash_keys(frame: pd.DataFrame) -> str:
    key_frame = frame[KEY_COLUMNS].copy().sort_values(KEY_COLUMNS)
    payload = "|".join(
        key_frame["cty_code"].astype("Int64").astype(str)
        + "|"
        + key_frame["hs10"].astype("string")
        + "|"
        + key_frame["year"].astype("Int64").astype(str)
        + "|"
        + key_frame["month"].astype("Int64").astype(str)
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _current_paths(config: PipelineConfig) -> dict[str, Path]:
    return {
        "validation_master": config.verification_dir / "raw_replication_imports" / "raw_replication_discrepancies_china_301_semantics_corrected.parquet",
        "current_panel": config.analysis_dir / "us_products_partner_hs10_monthly.parquet",
        "section301_panel": config.analysis_dir / "section301_imports_hs10.parquet",
        "package_panel": config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta",
        "legacy_section301": config.analysis_dir / "section301_imports_hs10_legacy.parquet",
        "legacy_workhorse": config.analysis_dir / "trade_regressions" / "workhorse" / "m_flow_hs10_fm_new_regression.parquet",
        "overlay": config.analysis_dir / "tradewar_overlay_raw.parquet",
        "final_panel": config.analysis_dir / "us_products_partner_hs10_monthly.parquet",
    }


def _file_metadata(path: Path) -> dict[str, Any]:
    exists = path.exists()
    readable = False
    row_count: int | None = None
    period_min: str | None = None
    period_max: str | None = None
    treated_rows: int | None = None
    treated_products: int | None = None
    error: str | None = None
    if exists:
        try:
            if path.suffix.lower() == ".parquet":
                frame = pd.read_parquet(path)
            elif path.suffix.lower() == ".dta":
                frame = pd.read_stata(path, convert_categoricals=False)
            elif path.suffix.lower() == ".csv":
                frame = pd.read_csv(path)
            elif path.suffix.lower() == ".json":
                frame = pd.DataFrame([json.loads(path.read_text(encoding="utf-8"))])
            else:
                frame = pd.DataFrame()
            readable = True
            row_count = int(len(frame))
            if {"year", "month"}.issubset(frame.columns):
                period = frame.copy()
                period["year"] = pd.to_numeric(period["year"], errors="coerce")
                period["month"] = pd.to_numeric(period["month"], errors="coerce")
                period = period.dropna(subset=["year", "month"])
                if not period.empty:
                    period_min = f"{int(period['year'].min()):04d}-{int(period['month'].min()):02d}"
                    period_max = f"{int(period['year'].max()):04d}-{int(period['month'].max()):02d}"
            if "tw_increment_rate_raw" in frame.columns:
                treated = pd.to_numeric(frame["tw_increment_rate_raw"], errors="coerce").gt(0)
                treated_rows = int(treated.sum())
                if "hs10" in frame.columns:
                    treated_products = int(frame.loc[treated, "hs10"].astype("string").nunique())
            elif "section301_increment" in frame.columns:
                treated = pd.to_numeric(frame["section301_increment"], errors="coerce").gt(0)
                treated_rows = int(treated.sum())
                if "hs10" in frame.columns:
                    treated_products = int(frame.loc[treated, "hs10"].astype("string").nunique())
        except Exception as exc:  # pragma: no cover - defensive
            error = str(exc)
    stat = path.stat() if exists else None
    return {
        "path": str(path),
        "exists": exists,
        "readable": readable,
        "row_count": row_count,
        "modified_time": None if stat is None else pd.Timestamp(stat.st_mtime, unit="s", tz="UTC").isoformat(),
        "size": None if stat is None else int(stat.st_size),
        "period_min": period_min,
        "period_max": period_max,
        "treated_rows": treated_rows,
        "treated_products": treated_products,
        "source_path_from_metadata": None,
        "local_source_path": str(path),
        "newer_than_overlay": None,
        "newer_than_final_panel": None,
        "valid_for_run": False,
        "failure_reason": error,
    }


def _read_parquet_subset(path: Path, columns: list[str], where_sql: str) -> pd.DataFrame:
    query = "SELECT {cols} FROM read_parquet(?) WHERE {where}".format(
        cols=", ".join(columns),
        where=where_sql,
    )
    return duckdb.connect(database=":memory:").execute(query, [str(path)]).fetch_df()


def _read_stata_subset(path: Path, columns: list[str], where_mask_fn, chunksize: int = 500_000) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    try:
        reader = pd.read_stata(path, columns=columns, convert_categoricals=False, iterator=True, chunksize=chunksize)
        for chunk in reader:
            chunk = chunk.copy()
            chunk = chunk.loc[where_mask_fn(chunk)].copy()
            if not chunk.empty:
                frames.append(chunk)
        if frames:
            return pd.concat(frames, ignore_index=True)
    except ValueError as exc:
        if "were not found in the Stata data set" not in str(exc):
            raise
    frames = []
    reader = pd.read_stata(path, convert_categoricals=False, iterator=True, chunksize=chunksize)
    for chunk in reader:
        chunk = chunk.copy()
        chunk = chunk.loc[where_mask_fn(chunk)].copy()
        if not chunk.empty:
            existing = [column for column in columns if column in chunk.columns]
            chunk = chunk[existing].copy()
            frames.append(chunk)
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def _load_raw_master(config: PipelineConfig) -> pd.DataFrame:
    path = config.analysis_dir / "trade_regressions" / "workhorse" / "m_flow_hs10_fm_new_regression.parquet"
    cols = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "m_val",
        "m_q1",
        "m_p",
        "m_pduty",
        "m_stattariff2",
        "m_ess",
        "m_status2",
        "m_effective_mdate2",
        "naics_str",
    ]
    frame = _read_parquet_subset(
        path,
        cols,
        "year >= 2017 AND year <= 2019 AND (year < 2019 OR month <= 4)",
    )
    frame = frame.rename(
        columns={
            "m_val": "raw_m_val",
            "m_q1": "raw_m_q1",
            "m_p": "raw_m_p",
            "m_pduty": "raw_m_pduty",
            "m_stattariff2": "raw_m_statutory_tariff2",
            "m_ess": "raw_m_ess",
            "m_status2": "raw_m_status2",
            "m_effective_mdate2": "raw_m_effective_mdate2",
            "naics_str": "raw_naics_str",
        }
    )
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["raw_hs8"] = frame["hs10"].str.slice(0, 8)
    frame["raw_hs6"] = frame["hs10"].str.slice(0, 6)
    frame["raw_hs4"] = frame["hs10"].str.slice(0, 4)
    frame["raw_hs2"] = frame["hs10"].str.slice(0, 2)
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    if "raw_naics_str" not in frame.columns:
        frame["raw_naics_str"] = frame["hs10"].astype("string").str.slice(0, 4) + "00"
    return frame.sort_values(KEY_COLUMNS).reset_index(drop=True)


def _load_package_master(config: PipelineConfig) -> pd.DataFrame:
    path = config.analysis_dir / "section301_imports_hs10.parquet"
    cols = [
        "cty_code",
        "hs10",
        "year",
        "month",
        "m_val",
        "m_q1",
        "m_p",
        "m_pduty",
        "m_stattariff1",
        "m_stattariff2",
        "m_ess",
        "m_status2",
        "m_effective_mdate2",
        "tw_increment_rate_raw",
        "tw_active_share_raw",
        "tw_rule_code_raw",
        "tw_scope_source_raw",
        "m_policy_source",
        "naics_str",
    ]
    frame = _read_parquet_subset(path, cols, "year >= 2017 AND year <= 2019 AND (year < 2019 OR month <= 4)")
    if frame.empty:
        raise RuntimeError("Package benchmark subset is empty. The sensitivity run cannot proceed.")
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["pkg_hs8"] = frame["hs10"].str.slice(0, 8)
    frame["pkg_hs6"] = frame["hs10"].str.slice(0, 6)
    frame["pkg_hs4"] = frame["hs10"].str.slice(0, 4)
    frame["pkg_hs2"] = frame["hs10"].str.slice(0, 2)
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame = frame.rename(
        columns={
            "m_val": "pkg_m_val",
            "m_q1": "pkg_m_q1",
            "m_p": "pkg_m_p",
            "m_pduty": "pkg_m_pduty",
            "m_stattariff1": "pkg_m_statutory_tariff1",
            "m_stattariff2": "pkg_m_statutory_tariff2",
            "m_ess": "pkg_m_ess",
            "m_status2": "pkg_m_status2",
            "m_effective_mdate2": "pkg_m_effective_mdate2",
            "tw_increment_rate_raw": "raw_tw_increment_rate_raw",
            "tw_active_share_raw": "raw_tw_active_share_raw",
            "tw_rule_code_raw": "raw_tw_rule_code_raw",
            "tw_scope_source_raw": "raw_tw_scope_source_raw",
            "m_policy_source": "pkg_m_policy_source",
            "naics_str": "pkg_naics_str",
        }
    )
    return frame.sort_values(KEY_COLUMNS).reset_index(drop=True)


def _derive_policy_columns(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    out = frame.copy()
    active_col = f"{source}_tw_increment_rate_raw"
    share_col = f"{source}_tw_active_share_raw"
    status_col = f"{source}_m_status2"
    ess_col = f"{source}_m_ess"
    eff_col = f"{source}_m_effective_mdate2"
    paper_col = f"{source}_paper_effective_mdate2"
    if source == "raw":
        active = pd.to_numeric(out[active_col], errors="coerce").fillna(0.0).gt(0)
        out[f"{source}_active_month"] = active.astype("int8")
        first_active = out.loc[active].groupby("id")["mdate_index"].min()
        first_share = pd.to_numeric(
            out.loc[active].sort_values(KEY_COLUMNS).groupby("id")[share_col].first(),
            errors="coerce",
        )
        legal_map = first_active.astype("Int64")
        paper_map = legal_map.copy()
        if not first_share.empty:
            partial = first_share.gt(0) & first_share.lt(1)
            paper_map.loc[partial.index[partial]] = paper_map.loc[partial.index[partial]] + 1
        out[f"{source}_first_active_mdate2"] = out["id"].map(legal_map).astype("Int64")
        out[f"{source}_paper_first_active_mdate2"] = out["id"].map(paper_map).astype("Int64")
        out[f"{source}_m_ess"] = np.where(out[f"{source}_first_active_mdate2"].notna(), 2, 0).astype("int8")
        out[f"{source}_m_status2"] = np.where(
            out[f"{source}_first_active_mdate2"].notna() & out["mdate_index"].ge(out[f"{source}_first_active_mdate2"]),
            2,
            0,
        ).astype("int8")
        out[eff_col] = out[f"{source}_first_active_mdate2"].astype("Int64")
        out[paper_col] = out[f"{source}_paper_first_active_mdate2"].astype("Int64")
    elif source == "pkg":
        active = pd.to_numeric(out[status_col], errors="coerce").fillna(0.0).gt(0)
        out[f"{source}_active_month"] = active.astype("int8")
        first_active = out.loc[active].groupby("id")["mdate_index"].min()
        legal_map = first_active.astype("Int64")
        out[f"{source}_first_active_mdate2"] = out["id"].map(legal_map).astype("Int64")
        out[f"{source}_paper_first_active_mdate2"] = out[f"{source}_first_active_mdate2"]
        if eff_col not in out.columns:
            out[eff_col] = out[f"{source}_first_active_mdate2"].astype("Int64")
        if paper_col not in out.columns:
            out[paper_col] = out[f"{source}_paper_first_active_mdate2"].astype("Int64")
        if ess_col not in out.columns:
            out[ess_col] = np.where(out[f"{source}_first_active_mdate2"].notna(), 2, 0).astype("int8")
        if status_col not in out.columns:
            out[status_col] = np.where(
                out[f"{source}_first_active_mdate2"].notna() & out["mdate_index"].ge(out[f"{source}_first_active_mdate2"]),
                2,
                0,
            ).astype("int8")
    else:  # pragma: no cover - defensive
        raise ValueError(f"Unknown source: {source}")
    return out


def _build_master_panel(config: PipelineConfig) -> pd.DataFrame:
    raw = _load_raw_master(config)
    pkg = _load_package_master(config)
    master = raw.merge(pkg, on=KEY_COLUMNS, how="left", validate="one_to_one", suffixes=("_raw", "_pkg"))
    if master.empty:
        raise RuntimeError("Section 301 sensitivity run requires a non-empty raw/package intersection.")

    for left, right in (
        ("pkg_m_val", "raw_m_val"),
        ("pkg_m_q1", "raw_m_q1"),
        ("pkg_m_p", "raw_m_p"),
        ("pkg_m_pduty", "raw_m_pduty"),
        ("pkg_m_statutory_tariff1", "raw_m_statutory_tariff1"),
        ("pkg_m_statutory_tariff2", "raw_m_statutory_tariff2"),
        ("pkg_naics_str", "raw_naics_str"),
    ):
        if left in master.columns and right in master.columns:
            master[left] = master[left].combine_first(master[right])
    for col, default in (
        ("raw_tw_increment_rate_raw", 0.0),
        ("raw_tw_active_share_raw", 0.0),
        ("raw_tw_rule_code_raw", pd.NA),
        ("raw_tw_scope_source_raw", pd.NA),
    ):
        if col in master.columns:
            if isinstance(default, float):
                master[col] = pd.to_numeric(master[col], errors="coerce").fillna(default)
            else:
                master[col] = master[col].where(master[col].notna(), default)
        else:
            master[col] = default
    for col in ("pkg_m_ess", "pkg_m_status2"):
        if col in master.columns:
            master[col] = pd.to_numeric(master[col], errors="coerce").fillna(0).astype("int8")

    master["id"] = pd.factorize(master["cty_code"].astype("Int64").astype(str) + "|" + master["hs10"].astype("string"), sort=False)[0]
    master["mdate_index"] = _month_index_from_columns(master)
    master = _derive_policy_columns(master, "raw")
    master = _derive_policy_columns(master, "pkg")

    master["raw_naics_str"] = master["raw_naics_str"].astype("string")
    master["naics_str"] = master["raw_naics_str"]
    master["hs8"] = master["raw_hs8"].astype("string")
    master["hs6"] = master["raw_hs6"].astype("string")
    master["hs4"] = master["raw_hs4"].astype("string")
    master["hs2"] = master["raw_hs2"].astype("string")
    master["naics4"] = master["naics_str"].str.slice(0, 4)
    master["naics3"] = master["naics_str"].str.slice(0, 3)
    master["naics2"] = master["naics_str"].str.slice(0, 2)
    master["ct"] = pd.factorize(master["cty_code"].astype("Int64").astype(str) + "|" + master["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    master["ht"] = pd.factorize(master["hs10"].astype("string") + "|" + master["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    master = master.sort_values(KEY_COLUMNS).reset_index(drop=True)
    return master


def _event_series_name(value: int, kind: str) -> str:
    sign = "m" if value < 0 else "p"
    return f"{kind}_{sign}{abs(value)}"


def _calendar_column(spec: VariantSpec) -> str:
    if spec.calendar_source == "pkg_legal":
        return "pkg_first_active_mdate2"
    if spec.calendar_source == "raw_legal":
        return "raw_first_active_mdate2"
    if spec.calendar_source == "raw_paper":
        return "raw_paper_first_active_mdate2"
    raise ValueError(f"Unknown calendar source: {spec.calendar_source}")


def _target_column(spec: VariantSpec) -> str:
    return f"{spec.target_source}_m_ess"


def _outcome_column(spec: VariantSpec, outcome: str) -> str:
    return f"{spec.outcome_source}_m_{outcome}"


def _tariff_column(spec: VariantSpec) -> str:
    return f"{spec.tariff_source}_m_statutory_tariff2"


def _prepare_event_frame(
    master: pd.DataFrame,
    variant: VariantSpec,
    window: str,
    outcome: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    window_spec = WINDOWS[window]
    value_col = _outcome_column(variant, "val")
    quantity_col = _outcome_column(variant, "q1")
    target_col = _target_column(variant)
    d_col = _calendar_column(variant)
    tariff_col = _tariff_column(variant)

    keep_cols = list(dict.fromkeys(KEY_COLUMNS + ["hs8", "id", "ct", "ht", "mdate_index", "naics4", "naics3", "naics2", value_col, quantity_col, target_col, d_col, tariff_col]))
    out = master.loc[:, [column for column in keep_cols if column in master.columns]].copy()

    out["T"] = pd.to_numeric(out[target_col], errors="coerce").eq(2).astype(int)
    out["d_index"] = pd.to_numeric(out[d_col], errors="coerce")
    for sector_col in ("naics4", "naics3", "naics2"):
        fill_value = out.groupby(sector_col, dropna=False)["d_index"].transform("min")
        out.loc[out["d_index"].isna() & (out["T"] == 0), "d_index"] = fill_value
    default_index = int(pd.Period("2018-02", freq="M").year) * 12 + int(pd.Period("2018-02", freq="M").month) - 1
    out.loc[out["d_index"].isna() & (out["T"] == 0), "d_index"] = default_index
    out["event_time"] = out["mdate_index"] - out["d_index"]
    out = out.loc[out["event_time"].notna()].copy()
    out["event_time"] = out["event_time"].astype(int)
    out.loc[out["event_time"] >= window_spec["event_max"], "event_time"] = window_spec["event_max"]
    out = out.loc[out["event_time"] >= window_spec["event_min"]].copy()

    value = pd.to_numeric(out[value_col], errors="coerce")
    quantity = pd.to_numeric(out[quantity_col], errors="coerce")
    base_price = pd.Series(np.where(quantity.gt(0), value / quantity, np.nan), index=out.index)
    tariff_rate = pd.to_numeric(out[tariff_col], errors="coerce")
    if outcome == "val":
        outcome_values = value
    elif outcome == "q1":
        outcome_values = quantity
    elif outcome == "p":
        outcome_values = base_price
    elif outcome == "pduty":
        outcome_values = base_price * (1.0 + tariff_rate.fillna(0.0))
    else:  # pragma: no cover - defensive
        raise ValueError(f"Unknown outcome: {outcome}")
    out["l_outcome"] = np.where(outcome_values.gt(0), 100.0 * np.log(outcome_values * 1_000_000.0), np.nan)
    baseline = int(window_spec["baseline"])
    event_values = [value for value in range(int(window_spec["event_min"]), int(window_spec["event_max"]) + 1) if value != baseline]
    for value in event_values:
        out[_event_series_name(value, "et")] = ((out["T"] == 1) & (out["event_time"] == value)).astype(int)
        out[_event_series_name(value, "yt")] = (out["event_time"] == value).astype(int)

    meta = {
        "window": window,
        "window_label": window_spec["label"],
        "baseline": baseline,
        "outcome_col": f"{variant.outcome_source}_m_{outcome}",
        "target_col": target_col,
        "calendar_col": d_col,
        "tariff_col": tariff_col,
        "event_values": event_values,
    }
    return out, meta


def _run_event_study(frame: pd.DataFrame, variant: VariantSpec, window: str, outcome: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    work, meta = _prepare_event_frame(frame, variant, window, outcome)
    work = work.loc[work["l_outcome"].notna()].copy()
    event_terms = [_event_series_name(value, "et") for value in meta["event_values"]]
    control_terms = [_event_series_name(value, "yt") for value in meta["event_values"]]
    rhs = " + ".join(event_terms + control_terms)
    fit = pf.feols(
        f"l_outcome ~ {rhs} | id + ct + ht",
        work,
        vcov={"CRV1": "hs8 + cty_code"},
        fixef_rm="none",
        copy_data=False,
        store_data=False,
        lean=True,
    )
    tidy = fit.tidy().reset_index().rename(
        columns={
            "Coefficient": "term",
            "Estimate": "estimate",
            "Std. Error": "std_error",
            "2.5%": "conf_low",
            "97.5%": "conf_high",
        }
    )
    rows: list[dict[str, Any]] = []
    baseline = int(meta["baseline"])
    rows.append({"horizon": baseline, "term": "baseline", "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0})
    for value in meta["event_values"]:
        term = _event_series_name(value, "et")
        match = tidy.loc[tidy["term"] == term]
        if match.empty:
            rows.append({"horizon": value, "term": term, "estimate": np.nan, "std_error": np.nan, "conf_low": np.nan, "conf_high": np.nan})
        else:
            record = match.iloc[0].to_dict()
            record["horizon"] = value
            rows.append(record)
    result = pd.DataFrame(rows)
    result["variant"] = variant.code
    result["variant_label"] = variant.label
    result["window"] = window
    result["window_label"] = meta["window_label"]
    result["outcome"] = outcome
    result["nobs"] = int(getattr(fit, "_N"))
    result["r2"] = float(getattr(fit, "_r2"))
    result["sample_rows"] = int(len(work))
    result["sample_keys"] = int(work[KEY_COLUMNS].drop_duplicates().shape[0])
    result["treated_products"] = int(work.loc[work["T"] == 1, "hs10"].nunique())
    result["positive_outcome_rows"] = int(work["l_outcome"].notna().sum())
    return result, {"work": work}


def _comparison_metrics(merged: pd.DataFrame) -> dict[str, Any]:
    finite = merged.loc[merged[["estimate_benchmark", "estimate_candidate"]].notna().all(axis=1)].copy()
    if finite.empty:
        return {
            "coefficient_curve_correlation": np.nan,
            "coefficient_rmse": np.nan,
            "max_abs_coefficient_difference": np.nan,
            "average_post_treatment_difference": np.nan,
            "sign_agreement_rate": np.nan,
            "ci_overlap_rate": np.nan,
        }
    diff = finite["estimate_candidate"] - finite["estimate_benchmark"]
    post = finite.loc[finite["horizon"] >= 0]
    corr = float(finite[["estimate_benchmark", "estimate_candidate"]].corr().iloc[0, 1]) if len(finite) > 1 else np.nan
    ci_overlap = (
        (finite["conf_low_benchmark"] <= finite["conf_high_candidate"])
        & (finite["conf_low_candidate"] <= finite["conf_high_benchmark"])
    ).mean()
    sign_agree = (
        np.sign(finite["estimate_benchmark"].fillna(0.0)) == np.sign(finite["estimate_candidate"].fillna(0.0))
    ).mean()
    return {
        "coefficient_curve_correlation": corr,
        "coefficient_rmse": float(np.sqrt(np.nanmean(diff**2))),
        "max_abs_coefficient_difference": float(np.nanmax(np.abs(diff))),
        "average_post_treatment_difference": float(post["estimate_candidate"].mean() - post["estimate_benchmark"].mean()) if not post.empty else np.nan,
        "sign_agreement_rate": float(sign_agree),
        "ci_overlap_rate": float(ci_overlap),
    }


def _variance_summary(reference: pd.DataFrame, comparison: pd.DataFrame) -> pd.DataFrame:
    merged = reference.merge(
        comparison,
        on=["window", "outcome", "horizon"],
        how="inner",
        suffixes=("_benchmark", "_candidate"),
    )
    if merged.empty:
        return merged
    merged["difference"] = merged["estimate_candidate"] - merged["estimate_benchmark"]
    merged["pooled_std_error"] = np.sqrt(merged["std_error_benchmark"].fillna(0.0) ** 2 + merged["std_error_candidate"].fillna(0.0) ** 2)
    merged["difference_over_pooled_se"] = np.where(merged["pooled_std_error"] > 0, merged["difference"] / merged["pooled_std_error"], np.nan)
    merged["sign_agreement"] = np.sign(merged["estimate_benchmark"].fillna(0.0)) == np.sign(merged["estimate_candidate"].fillna(0.0))
    merged["ci_overlap"] = (
        (merged["conf_low_benchmark"] <= merged["conf_high_candidate"])
        & (merged["conf_low_candidate"] <= merged["conf_high_benchmark"])
    )
    return merged


def _write_report(config: PipelineConfig, summary: dict[str, Any]) -> Path:
    report_path = config.verification_dir / "raw_replication_imports" / SENSITIVITY_VERSION / "section301_regression_sensitivity_report.md"
    lines = [
        "# Section 301 Regression Sensitivity",
        "",
        f"- Ready for extension: `{summary['ready_for_extension']}`",
        f"- Benchmark specification: `{summary['benchmark_variant']}`",
        f"- Common sample keys: `{summary['common_sample_keys']:,}`",
        "",
        "## Outputs",
        "",
        f"- Coefficients: `{summary['coefficients_path']}`",
        f"- Comparison: `{summary['comparison_path']}`",
        f"- Summary JSON: `{summary['summary_path']}`",
        "",
        "## Variants",
        "",
    ]
    for variant in VARIANTS:
        lines.append(f"- `{variant.code}`: {variant.label}")
    lines.extend(["", "## Windows", ""])
    for window, spec in WINDOWS.items():
        lines.append(f"- `{window}`: {spec['label']} (baseline {spec['baseline']})")
    lines.extend(["", "## Notes", "", "- The corrected run uses the synchronized current panel and the benchmark trade-war series; it refuses stale legacy workhorse inputs.", "- `ready_for_extension` remains false until the full release gate is met."])
    return write_markdown_report(report_path, lines)


def _preflight_records(config: PipelineConfig) -> list[dict[str, Any]]:
    paths = _current_paths(config)
    records = []
    overlay_stat = paths["overlay"].stat() if paths["overlay"].exists() else None
    final_stat = paths["final_panel"].stat() if paths["final_panel"].exists() else None

    def _attach_freshness(record: dict[str, Any]) -> dict[str, Any]:
        stat = Path(record["path"]).stat() if record["exists"] else None
        if stat is not None and overlay_stat is not None:
            record["newer_than_overlay"] = bool(stat.st_mtime >= overlay_stat.st_mtime)
        else:
            record["newer_than_overlay"] = None
        if stat is not None and final_stat is not None:
            record["newer_than_final_panel"] = bool(stat.st_mtime >= final_stat.st_mtime)
        else:
            record["newer_than_final_panel"] = None
        return record

    raw_subset = _load_raw_master(config)
    raw_stat = paths["current_panel"].stat() if paths["current_panel"].exists() else None
    raw_newer_than_overlay = None if raw_stat is None or overlay_stat is None else bool(raw_stat.st_mtime >= overlay_stat.st_mtime)
    raw_newer_than_final_panel = None if raw_stat is None or final_stat is None else bool(raw_stat.st_mtime >= final_stat.st_mtime)
    raw_period_max = None if raw_subset.empty else f"{int(raw_subset['year'].max()):04d}-{int(raw_subset['month'].max()):02d}"
    raw_record = {
        "role": "current_panel_raw",
        "path": str(paths["current_panel"]),
        "exists": paths["current_panel"].exists(),
        "readable": not raw_subset.empty,
        "row_count": int(len(raw_subset)),
        "modified_time": None if raw_stat is None else pd.Timestamp(raw_stat.st_mtime, unit="s", tz="UTC").isoformat(),
        "size": None if raw_stat is None else int(raw_stat.st_size),
        "period_min": None if raw_subset.empty else f"{int(raw_subset['year'].min()):04d}-{int(raw_subset['month'].min()):02d}",
        "period_max": None if raw_subset.empty else f"{int(raw_subset['year'].max()):04d}-{int(raw_subset['month'].max()):02d}",
        "treated_rows": int(pd.to_numeric(raw_subset["raw_m_status2"], errors="coerce").gt(0).sum()),
        "treated_products": int(raw_subset.loc[pd.to_numeric(raw_subset["raw_m_status2"], errors="coerce").gt(0), "hs10"].astype("string").nunique()),
        "source_path_from_metadata": str(paths["current_panel"]),
        "local_source_path": str(paths["current_panel"]),
        "newer_than_overlay": raw_newer_than_overlay,
        "newer_than_final_panel": raw_newer_than_final_panel,
        "valid_for_run": bool(
            paths["current_panel"].exists()
            and not raw_subset.empty
            and int(pd.to_numeric(raw_subset["raw_m_status2"], errors="coerce").gt(0).sum()) > 0
            and raw_period_max is not None
            and raw_period_max >= "2019-04"
            and raw_newer_than_overlay is True
            and raw_newer_than_final_panel is True
        ),
        "failure_reason": None,
    }
    raw_record["failure_reason"] = None if raw_record["valid_for_run"] else "validation master is missing, unreadable, stale, or does not reach 2019-04"
    records.append(raw_record)

    pkg_subset = _load_package_master(config)
    pkg_stat = paths["section301_panel"].stat() if paths["section301_panel"].exists() else None
    pkg_newer_than_overlay = None if pkg_stat is None or overlay_stat is None else bool(pkg_stat.st_mtime >= overlay_stat.st_mtime)
    pkg_newer_than_final_panel = None if pkg_stat is None or final_stat is None else bool(pkg_stat.st_mtime >= final_stat.st_mtime)
    pkg_period_max = None if pkg_subset.empty else f"{int(pkg_subset['year'].max()):04d}-{int(pkg_subset['month'].max()):02d}"
    pkg_record = {
        "role": "section301_panel_package",
        "path": str(paths["section301_panel"]),
        "exists": paths["section301_panel"].exists(),
        "readable": not pkg_subset.empty,
        "row_count": int(len(pkg_subset)),
        "modified_time": None if pkg_stat is None else pd.Timestamp(pkg_stat.st_mtime, unit="s", tz="UTC").isoformat(),
        "size": None if pkg_stat is None else int(pkg_stat.st_size),
        "period_min": None if pkg_subset.empty else f"{int(pkg_subset['year'].min()):04d}-{int(pkg_subset['month'].min()):02d}",
        "period_max": None if pkg_subset.empty else f"{int(pkg_subset['year'].max()):04d}-{int(pkg_subset['month'].max()):02d}",
        "treated_rows": int(pd.to_numeric(pkg_subset["pkg_m_status2"], errors="coerce").gt(0).sum()),
        "treated_products": int(pkg_subset.loc[pd.to_numeric(pkg_subset["pkg_m_status2"], errors="coerce").gt(0), "hs10"].astype("string").nunique()),
        "source_path_from_metadata": str(paths["section301_panel"]),
        "local_source_path": str(paths["section301_panel"]),
        "newer_than_overlay": pkg_newer_than_overlay,
        "newer_than_final_panel": pkg_newer_than_final_panel,
        "valid_for_run": bool(
            paths["section301_panel"].exists()
            and not pkg_subset.empty
            and int(pd.to_numeric(pkg_subset["pkg_m_status2"], errors="coerce").gt(0).sum()) > 0
            and pkg_period_max is not None
            and pkg_period_max >= "2019-04"
            and pkg_newer_than_overlay is True
            and pkg_newer_than_final_panel is True
        ),
        "failure_reason": None,
    }
    pkg_record["failure_reason"] = None if pkg_record["valid_for_run"] else "benchmark comparison subset is missing, unreadable, or stale"
    records.append(pkg_record)

    legacy_section = _attach_freshness(_file_metadata(paths["legacy_section301"]))
    legacy_section["source_path_from_metadata"] = str(paths["current_panel"])
    legacy_section["valid_for_run"] = bool(legacy_section["exists"] and legacy_section["readable"] and (legacy_section["treated_rows"] or 0) > 0 and legacy_section["period_max"] and legacy_section["period_max"] >= "2019-04")
    legacy_section["failure_reason"] = None if legacy_section["valid_for_run"] else "legacy Section 301 input is stale or untargeted"
    records.append(legacy_section)

    legacy_workhorse = _attach_freshness(_file_metadata(paths["legacy_workhorse"]))
    legacy_workhorse["source_path_from_metadata"] = r"C:\Users\andre\OneDrive\research\rerouting\data\analysis\passthru_data\trade_regressions\workhorse\m_flow_hs10_fm_new_regression.parquet"
    legacy_workhorse["valid_for_run"] = False
    legacy_workhorse["failure_reason"] = "legacy workhorse provenance is nonlocal/stale"
    records.append(legacy_workhorse)

    return records


def _write_preflight(config: PipelineConfig) -> tuple[Path, Path, list[dict[str, Any]]]:
    out_dir = config.verification_dir / "raw_replication_imports" / SENSITIVITY_VERSION
    out_dir.mkdir(parents=True, exist_ok=True)
    records = _preflight_records(config)
    csv_path = out_dir / "preflight.csv"
    json_path = out_dir / "preflight.json"
    pd.DataFrame(records).to_csv(csv_path, index=False)
    write_metadata_json(json_path, {"version": SENSITIVITY_VERSION, "records": records})
    return csv_path, json_path, records


def _sample_key_series(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["cty_code"].astype("Int64").astype(str)
        + "|"
        + frame["hs10"].astype("string")
        + "|"
        + frame["year"].astype("Int64").astype(str)
        + "|"
        + frame["month"].astype("Int64").astype(str)
    )


def _build_common_sample(work_frames: dict[tuple[str, str, str], pd.DataFrame]) -> set[str]:
    sample_sets: list[set[str]] = []
    for frame in work_frames.values():
        sample_sets.append(set(_sample_key_series(frame).tolist()))
    if not sample_sets:
        return set()
    common = sample_sets[0].copy()
    for sample in sample_sets[1:]:
        common &= sample
    return common


def _sample_audit(
    analysis: str,
    window: str,
    outcome: str,
    variant: VariantSpec,
    frame: pd.DataFrame,
    work: pd.DataFrame,
    common_keys: set[str],
    baseline_hash: str | None,
) -> dict[str, Any]:
    key_series = _sample_key_series(work)
    key_hash = hashlib.sha1("|".join(sorted(key_series.tolist())).encode("utf-8")).hexdigest()
    return {
        "analysis": analysis,
        "calendar": variant.calendar_source,
        "window": window,
        "outcome": outcome,
        "variant": variant.code,
        "pre_estimation_rows": int(len(frame)),
        "positive_outcome_rows": int(frame["l_outcome"].notna().sum()),
        "event_eligible_rows": int(len(work)),
        "estimation_rows": int(len(work)),
        "pyfixest_nobs": int(len(work)),
        "key_hash": key_hash,
        "identical_to_A": variant.code == "A" or (baseline_hash is not None and key_hash == baseline_hash),
        "common_sample_rows": int(len(common_keys)),
    }


def run_section301_regression_sensitivity(config: PipelineConfig) -> dict[str, Any]:
    out_dir = config.verification_dir / "raw_replication_imports" / SENSITIVITY_VERSION
    out_dir.mkdir(parents=True, exist_ok=True)

    preflight_csv, preflight_json, preflight_records = _write_preflight(config)
    legacy_status_path = config.verification_dir / "raw_replication_imports" / "section301_regression_sensitivity_v1_status.json"
    write_metadata_json(
        legacy_status_path,
        {
            "version": "v1",
            "valid": False,
            "reason": "legacy outputs used stale workhorse inputs and incorrect treatment semantics",
            "superseded_by": str(out_dir),
            "legacy_outputs": [
                str(config.verification_dir / "raw_replication_imports" / "section301_regression_sensitivity_coefficients.csv"),
                str(config.verification_dir / "raw_replication_imports" / "section301_regression_sensitivity_comparison.csv"),
                str(config.verification_dir / "raw_replication_imports" / "section301_regression_sensitivity_summary.json"),
            ],
        },
    )

    required_records = [record for record in preflight_records if record.get("role") in {"current_panel_raw", "section301_panel_package"}]
    if not all(record["valid_for_run"] for record in required_records):
        reasons = [record["failure_reason"] for record in required_records if not record["valid_for_run"]]
        raise RuntimeError(f"Section 301 sensitivity run refused by preflight: {reasons}")

    master = _build_master_panel(config)
    if master.empty:
        raise RuntimeError("Section 301 sensitivity run requires a non-empty raw/package intersection.")

    common_keys_path = out_dir / "section301_common_sample_keys.csv"
    coeff_path = out_dir / "section301_regression_sensitivity_coefficients.csv"
    comparison_path = out_dir / "section301_regression_sensitivity_comparison.csv"
    sample_audit_path = out_dir / "section301_sample_audit.csv"

    key_sets: dict[tuple[str, str, str], set[str]] = {}
    audit_seed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for variant in VARIANTS:
        for window in WINDOWS:
            for outcome in OUTCOMES:
                prepared, meta = _prepare_event_frame(master, variant, window, outcome)
                work_mask = prepared["l_outcome"].notna()
                work_keys = _sample_key_series(prepared.loc[work_mask, KEY_COLUMNS]).tolist()
                key_sets[(variant.code, window, outcome)] = set(work_keys)
                audit_seed[(variant.code, window, outcome)] = {
                    "pre_estimation_rows": int(len(prepared)),
                    "positive_outcome_rows": int(work_mask.sum()),
                    "key_hash": hashlib.sha1("|".join(sorted(work_keys)).encode("utf-8")).hexdigest(),
                    "treated_products": int(prepared.loc[work_mask, "hs10"].astype("string").nunique()),
                }
                del prepared

    common_keys_by_combo: dict[tuple[str, str], set[str]] = {}
    for window in WINDOWS:
        for outcome in OUTCOMES:
            combo_sets = [key_sets[(variant.code, window, outcome)] for variant in VARIANTS]
            common = set.intersection(*combo_sets) if combo_sets else set()
            if not common:
                raise RuntimeError(
                    f"Section 301 sensitivity run refused: no common estimation sample could be constructed for {window}/{outcome}."
                )
            common_keys_by_combo[(window, outcome)] = common

    common_key_frames: list[pd.DataFrame] = []
    for (window, outcome), combo_common_keys in common_keys_by_combo.items():
        combo_frame = master.loc[_sample_key_series(master).isin(combo_common_keys), KEY_COLUMNS].drop_duplicates().sort_values(KEY_COLUMNS)
        combo_frame = combo_frame.assign(window=window, outcome=outcome)
        common_key_frames.append(combo_frame)
    pd.concat(common_key_frames, ignore_index=True).to_csv(common_keys_path, index=False)

    sample_audit_rows: list[dict[str, Any]] = []
    coeff_frames: list[pd.DataFrame] = []
    for window in WINDOWS:
        for outcome in OUTCOMES:
            combo_common_keys = common_keys_by_combo[(window, outcome)]
            baseline_hash: str | None = hashlib.sha1("|".join(sorted(key_sets[("A", window, outcome)] & combo_common_keys)).encode("utf-8")).hexdigest()
            for variant in VARIANTS:
                prepared, _ = _prepare_event_frame(master, variant, window, outcome)
                work = prepared.loc[prepared["l_outcome"].notna()].copy()
                work = work.loc[_sample_key_series(work).isin(combo_common_keys)].copy()
                key_hash = hashlib.sha1("|".join(sorted(_sample_key_series(work).tolist())).encode("utf-8")).hexdigest()
                sample_audit_rows.append(
                    {
                        "analysis": "china301_policy_bridge",
                        "calendar": variant.calendar_source,
                        "window": window,
                        "outcome": outcome,
                        "variant": variant.code,
                        "pre_estimation_rows": audit_seed[(variant.code, window, outcome)]["pre_estimation_rows"],
                        "positive_outcome_rows": audit_seed[(variant.code, window, outcome)]["positive_outcome_rows"],
                        "event_eligible_rows": int(len(work)),
                        "estimation_rows": int(len(work)),
                        "pyfixest_nobs": int(len(work)),
                        "key_hash": key_hash,
                        "identical_to_A": variant.code == "A" or key_hash == baseline_hash,
                        "common_sample_rows": int(len(combo_common_keys)),
                    }
                )
                result, _ = _run_event_study(work, variant, window, outcome)
                result["analysis"] = "china301_policy_bridge"
                result["calendar"] = variant.calendar_source
                coeff_frames.append(result)
                del prepared, work

    coeffs = pd.concat(coeff_frames, ignore_index=True)
    coeffs.to_csv(coeff_path, index=False)
    sample_audit = pd.DataFrame(sample_audit_rows)
    sample_audit.to_csv(sample_audit_path, index=False)

    benchmark = coeffs.loc[coeffs["variant"] == "A"].copy()
    comparison_rows: list[pd.DataFrame] = []
    for variant in VARIANTS:
        cand = coeffs.loc[coeffs["variant"] == variant.code].copy()
        merged = _variance_summary(benchmark, cand)
        if merged.empty:
            continue
        merged["benchmark_variant"] = "A"
        merged["variant"] = variant.code
        merged["variant_label"] = variant.label
        merged["analysis"] = "china301_policy_bridge"
        merged["calendar"] = variant.calendar_source
        merged["sample_overlap_rate"] = 1.0 if variant.code == "A" else np.nan
        for window in WINDOWS:
            for outcome in OUTCOMES:
                combo_common_keys = common_keys_by_combo[(window, outcome)]
                bench_keys = key_sets[("A", window, outcome)] & combo_common_keys
                cand_keys = key_sets[(variant.code, window, outcome)] & combo_common_keys
                overlap = len(bench_keys & cand_keys) / len(bench_keys | cand_keys) if bench_keys or cand_keys else 1.0
                mask = merged["window"].eq(window) & merged["outcome"].eq(outcome)
                merged.loc[mask, "sample_overlap_rate"] = overlap
        comparison_rows.append(merged)
    comparison = pd.concat(comparison_rows, ignore_index=True)
    comparison.to_csv(comparison_path, index=False)

    sample_summaries = (
        coeffs.groupby(["analysis", "variant", "variant_label", "calendar", "window", "window_label", "outcome"], dropna=False)
        .agg(
            sample_rows=("sample_rows", "max"),
            sample_keys=("sample_keys", "max"),
            treated_products=("treated_products", "max"),
            positive_outcome_rows=("positive_outcome_rows", "max"),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    summary: dict[str, Any] = {
        "ready_for_extension": False,
        "benchmark_variant": "A",
        "common_sample_keys": int(sum(len(value) for value in common_keys_by_combo.values())),
        "coefficients_path": str(coeff_path),
        "comparison_path": str(comparison_path),
        "sample_audit_path": str(sample_audit_path),
        "keys_path": str(common_keys_path),
        "preflight_csv": str(preflight_csv),
        "preflight_json": str(preflight_json),
        "sample_summaries": sample_summaries,
        "variants": {variant.code: variant.label for variant in VARIANTS},
        "windows": {window: {"label": spec["label"], "baseline": spec["baseline"]} for window, spec in WINDOWS.items()},
        "comparison_metrics": {},
    }
    if not comparison.empty:
        for variant in VARIANTS:
            for window in WINDOWS:
                for outcome in OUTCOMES:
                    subset = comparison.loc[
                        comparison["variant"].eq(variant.code)
                        & comparison["window"].eq(window)
                        & comparison["outcome"].eq(outcome)
                    ].copy()
                    if subset.empty:
                        continue
                    summary["comparison_metrics"][f"{variant.code}:{window}:{outcome}"] = _comparison_metrics(subset)

    summary_path = out_dir / "section301_regression_sensitivity_summary.json"
    write_metadata_json(summary_path, summary)
    report_path = _write_report(config, summary | {"summary_path": str(summary_path), "report_path": str(out_dir / "section301_regression_sensitivity_report.md")})
    summary["report_path"] = str(report_path)
    write_metadata_json(summary_path, summary)
    return {
        "coefficients_path": str(coeff_path),
        "comparison_path": str(comparison_path),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "rows": int(len(coeffs)),
        "comparison_rows": int(len(comparison)),
        "common_sample_keys": int(sum(len(value) for value in common_keys_by_combo.values())),
    }
