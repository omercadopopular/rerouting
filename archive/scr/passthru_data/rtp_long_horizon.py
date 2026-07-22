"""Long-horizon import regressions anchored to the published 2018 RTP treatment map."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import gc
import importlib.util

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet
from .trade_regression_common import stata_month_period_to_index


RTP_DIRNAME = "rtp_long_horizon"
PACKAGE_TREATMENT_COLUMNS = [
    "cty_code", "hs10", "m_ess", "m_effective_mdate2", "naics_str",
    "m_china_hit", "m_steel_hit", "m_alum_hit", "m_washer_hit", "m_solar_hit",
]
LEDGER_REQUIRED_COLUMNS = {
    "cty_code", "hs10", "year", "month", "applicable_total_ad_valorem_duty",
    "is_non_ad_valorem", "is_unresolved", "source_url", "policy_panel_version",
}
IEEPA_REQUIRED_COLUMNS = LEDGER_REQUIRED_COLUMNS | {"is_china_ieepa_treated"}


def rtp_dir(config: PipelineConfig) -> Path:
    path = config.analysis_dir / "trade_regressions" / RTP_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def treatment_crosswalk_path(config: PipelineConfig) -> Path:
    return rtp_dir(config) / "2018_treatment_crosswalk.parquet"


def frozen_panel_path(config: PipelineConfig) -> Path:
    return rtp_dir(config) / "frozen_universe" / "imports_2018_treatment_through_latest.parquet"


def _event_index(value: Any) -> int | None:
    return stata_month_period_to_index(value)


def build_2018_treatment_crosswalk(config: PipelineConfig) -> dict[str, Any]:
    """Extract the package's original treatment definition without extending its tariffs."""
    source = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    frame = read_table(source, columns=PACKAGE_TREATMENT_COLUMNS)
    frame["cty_code"] = pd.to_numeric(frame["cty_code"], errors="coerce").astype("Int64")
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    frame["m_ess"] = pd.to_numeric(frame["m_ess"], errors="coerce").fillna(0)
    frame["event_index_raw"] = frame["m_effective_mdate2"].map(_event_index).astype("Int64")
    frame["treated_2018"] = frame["m_ess"].eq(2)
    frame["naics_str"] = frame["naics_str"].astype("string")
    frame["naics4"] = frame["naics_str"].str.slice(0, 4)
    keys = ["cty_code", "hs10"]
    treated = frame.loc[frame["treated_2018"], keys + ["event_index_raw"]].groupby(keys, as_index=False)["event_index_raw"].min()
    crosswalk = frame[keys + ["naics_str", "naics4"]].drop_duplicates(keys).merge(treated, on=keys, how="left")
    crosswalk["treated_2018"] = crosswalk["event_index_raw"].notna().astype("int8")
    # Paper controls receive their nearest sector's first treatment date, with the published
    # February-2018 fallback if their sector has no treated code.
    sector_dates = crosswalk.loc[crosswalk["treated_2018"].eq(1)].groupby("naics4", as_index=False)["event_index_raw"].min()
    crosswalk = crosswalk.merge(sector_dates.rename(columns={"event_index_raw": "sector_event_index"}), on="naics4", how="left")
    default_index = 2018 * 12 + 2 - 1
    crosswalk["event_index"] = crosswalk["event_index_raw"].where(crosswalk["treated_2018"].eq(1), crosswalk["sector_event_index"].fillna(default_index)).astype("int64")
    for column in ("m_china_hit", "m_steel_hit", "m_alum_hit", "m_washer_hit", "m_solar_hit"):
        if column in frame.columns:
            flags = frame.groupby(keys, as_index=False)[column].max()
            crosswalk = crosswalk.merge(flags, on=keys, how="left")
    crosswalk = crosswalk.sort_values(keys).reset_index(drop=True)
    path = treatment_crosswalk_path(config)
    write_parquet(crosswalk, path, overwrite=True)
    write_data_dictionary(crosswalk, path.with_suffix(".dictionary.json"), key_columns=keys)
    metadata = {
        "source_path": str(source), "output_path": str(path), "rows": int(len(crosswalk)),
        "treated_pairs": int(crosswalk["treated_2018"].sum()), "event_default_period": "2018-02",
        "policy_source": "fajgelbaum_package_treatment_only",
    }
    write_metadata_json(path.with_suffix(".metadata.json"), metadata)
    del frame
    gc.collect()
    return metadata


def build_frozen_long_horizon_panel(config: PipelineConfig) -> dict[str, Any]:
    """Join raw Census outcomes to the frozen package universe with DuckDB on disk."""
    if importlib.util.find_spec("duckdb") is None:
        raise RuntimeError("DuckDB is required for the long-horizon panel build. Install duckdb in the estimation environment.")
    crosswalk = treatment_crosswalk_path(config)
    if not crosswalk.exists():
        build_2018_treatment_crosswalk(config)
    raw = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
    if not raw.exists():
        raise FileNotFoundError(f"Missing raw Census import panel: {raw}")
    import duckdb

    output = frozen_panel_path(config)
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        raw_sql = str(raw).replace("'", "''")
        crosswalk_sql = str(crosswalk).replace("'", "''")
        output_sql = str(output).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    r.cty_code, r.cty_name, r.hs10, r.hs8, r.hs6, r.hs4, r.hs2,
                    r.year, r.month, r.m_val, r.m_q1,
                    t.naics_str, t.naics4, t.treated_2018, t.event_index,
                    CASE WHEN r.m_q1 > 0 THEN r.m_val / r.m_q1 ELSE NULL END AS m_p,
                    make_date(CAST(r.year AS INTEGER), CAST(r.month AS INTEGER), 1) AS mdate
                FROM read_parquet('{raw_sql}') AS r
                INNER JOIN read_parquet('{crosswalk_sql}') AS t
                    ON CAST(r.cty_code AS BIGINT) = CAST(t.cty_code AS BIGINT)
                    AND CAST(r.hs10 AS VARCHAR) = CAST(t.hs10 AS VARCHAR)
                WHERE r.year >= 2017 AND r.cty_code > 0
            ) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        row = con.execute(f"SELECT count(*), min(mdate), max(mdate), sum(treated_2018) FROM read_parquet('{output_sql}')").fetchone()
    finally:
        con.close()
    metadata = {
        "output_path": str(output), "raw_outcomes_path": str(raw), "treatment_crosswalk_path": str(crosswalk),
        "rows": int(row[0]), "period_min": str(row[1])[:7], "period_max": str(row[2])[:7],
        "treated_observations": int(row[3] or 0), "sample": "frozen_paper_universe",
    }
    write_data_dictionary(read_table(output, columns=["cty_code", "hs10", "year", "month", "treated_2018", "event_index"]), output.with_suffix(".dictionary.json"), key_columns=["cty_code", "hs10", "year", "month"])
    write_metadata_json(output.with_suffix(".metadata.json"), metadata)
    return metadata


def validate_public_tariff_ledger(config: PipelineConfig) -> dict[str, Any]:
    """Gate public-policy outcomes until the ledger is complete and current."""
    ledger = config.analysis_dir / "public_tariff_ledger_hs10_monthly.parquet"
    result: dict[str, Any] = {"ledger_path": str(ledger), "ready": False, "reasons": []}
    if not ledger.exists():
        result["reasons"].append("missing_public_tariff_ledger")
        return result
    columns = set(read_table(ledger).columns)
    missing = sorted(LEDGER_REQUIRED_COLUMNS - columns)
    if missing:
        result["reasons"].append(f"missing_columns:{','.join(missing)}")
        return result
    periods = read_table(ledger, columns=["year", "month"])[["year", "month"]].dropna().sort_values(["year", "month"])
    raw_periods = read_table(config.analysis_dir / "m_flow_hs10_fm_new.parquet", columns=["year", "month"])[["year", "month"]].dropna().sort_values(["year", "month"])
    if periods.empty or raw_periods.empty:
        result["reasons"].append("empty_policy_or_trade_panel")
        return result
    last_ledger = f"{int(periods.iloc[-1]['year']):04d}-{int(periods.iloc[-1]['month']):02d}"
    last_trade = f"{int(raw_periods.iloc[-1]['year']):04d}-{int(raw_periods.iloc[-1]['month']):02d}"
    result.update({"ledger_period_max": last_ledger, "trade_period_max": last_trade})
    if last_ledger != last_trade:
        result["reasons"].append("ledger_does_not_reach_trade_cutoff")
        return result
    result["ready"] = True
    return result


def build_2025_ieepa_event_panel(config: PipelineConfig) -> dict[str, Any]:
    """Build the February-2025 -12/+12 comparison input after ledger validation.

    This intentionally refuses to use the package tariff variables or the incomplete
    raw-policy artifact. The ledger is the sole source for 2025 treatment and duties.
    """
    gate = validate_public_tariff_ledger(config)
    if not gate["ready"]:
        raise RuntimeError(f"2025 event panel blocked by public tariff ledger gate: {gate['reasons']}")
    ledger_path = Path(gate["ledger_path"])
    columns = set(read_table(ledger_path).columns)
    missing = sorted(IEEPA_REQUIRED_COLUMNS - columns)
    if missing:
        raise RuntimeError(f"2025 event panel blocked: tariff ledger is missing {missing}")
    if importlib.util.find_spec("duckdb") is None:
        raise RuntimeError("DuckDB is required for the 2025 event panel build.")
    import duckdb

    raw = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
    output = rtp_dir(config) / "public_policy" / "imports_2025_ieepa_event_panel.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        raw_sql, ledger_sql, output_sql = (str(path).replace("'", "''") for path in (raw, ledger_path, output))
        con.execute(
            f"""
            COPY (
                SELECT r.cty_code, r.cty_name, r.hs10, r.hs8, r.hs6, r.hs4, r.hs2, r.year, r.month,
                    r.m_val, r.m_q1, l.is_china_ieepa_treated,
                    l.applicable_total_ad_valorem_duty, l.is_non_ad_valorem, l.is_unresolved,
                    make_date(CAST(r.year AS INTEGER), CAST(r.month AS INTEGER), 1) AS mdate,
                    CASE WHEN r.m_q1 > 0 THEN r.m_val / r.m_q1 ELSE NULL END AS m_p,
                    CASE WHEN r.m_q1 > 0 AND NOT l.is_non_ad_valorem AND NOT l.is_unresolved
                         THEN (r.m_val / r.m_q1) * (1 + l.applicable_total_ad_valorem_duty) END AS m_pduty
                FROM read_parquet('{raw_sql}') r
                INNER JOIN read_parquet('{ledger_sql}') l
                  ON CAST(r.cty_code AS BIGINT) = CAST(l.cty_code AS BIGINT)
                 AND CAST(r.hs10 AS VARCHAR) = CAST(l.hs10 AS VARCHAR)
                 AND r.year = l.year AND r.month = l.month
                WHERE make_date(CAST(r.year AS INTEGER), CAST(r.month AS INTEGER), 1)
                      BETWEEN DATE '2024-01-01' AND DATE '2026-02-28'
            ) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        row = con.execute(f"SELECT count(*), min(mdate), max(mdate), sum(is_china_ieepa_treated) FROM read_parquet('{output_sql}')").fetchone()
    finally:
        con.close()
    metadata = {
        "output_path": str(output), "ledger_path": str(ledger_path), "event_anchor": "2025-02",
        "window": "-12/+12", "available_period_min": str(row[1])[:7], "available_period_max": str(row[2])[:7],
        "rows": int(row[0]), "treated_observations": int(row[3] or 0),
        "unavailable_horizons": [11, 12], "estimator": "paired_comparison_country_hs10_and_hs10_month_fe",
    }
    write_metadata_json(output.with_suffix(".metadata.json"), metadata)
    return metadata


def run_long_horizon_2018_event(config: PipelineConfig, min_event: int = -12) -> dict[str, Any]:
    """Estimate every available monthly post-event coefficient for the frozen 2018 universe."""
    if importlib.util.find_spec("pyfixest") is None:
        raise RuntimeError("pyfixest is required for long-horizon event estimation.")
    panel_path = frozen_panel_path(config)
    if not panel_path.exists():
        build_frozen_long_horizon_panel(config)
    import pyfixest as pf

    panel = read_table(panel_path)
    panel["mdate_index"] = pd.to_numeric(panel["year"], errors="coerce") * 12 + pd.to_numeric(panel["month"], errors="coerce") - 1
    panel["event_time"] = panel["mdate_index"] - pd.to_numeric(panel["event_index"], errors="coerce")
    max_event = int(panel["event_time"].max())
    panel = panel.loc[panel["event_time"].between(min_event, max_event)].copy()
    panel["id"] = pd.factorize(panel["cty_code"].astype("string") + "|" + panel["hs10"].astype("string"), sort=False)[0]
    panel["ct"] = pd.factorize(panel["cty_code"].astype("string") + "|" + panel["mdate_index"].astype("string"), sort=False)[0]
    panel["ht"] = pd.factorize(panel["hs10"].astype("string") + "|" + panel["mdate_index"].astype("string"), sort=False)[0]
    terms = []
    for event_time in range(min_event + 1, max_event + 1):
        term = f"event_{'m' + str(abs(event_time)) if event_time < 0 else 'p' + str(event_time)}"
        panel[term] = ((panel["treated_2018"] == 1) & (panel["event_time"] == event_time)).astype("int8")
        terms.append((event_time, term))
    requested_outcomes = (config.regression_outcome,) if config.regression_outcome else ("val", "q1", "p")
    if "pduty" in requested_outcomes:
        raise RuntimeError("Long-horizon duty-inclusive outcomes are blocked pending the validated public tariff ledger.")
    output_rows = []
    for outcome in requested_outcomes:
        column = f"m_{outcome}"
        work = panel.loc[pd.to_numeric(panel[column], errors="coerce") > 0].copy()
        work["log_outcome"] = 100 * np.log(pd.to_numeric(work[column], errors="coerce"))
        fit = pf.feols(f"log_outcome ~ {' + '.join(term for _, term in terms)} | id + ct + ht", data=work, vcov={"CRV1": "hs8 + cty_code"}, copy_data=False, store_data=False, lean=True)
        tidy = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
        output_rows.append(pd.DataFrame([{ "event_time": min_event, "term": "baseline", "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0, "outcome": outcome, "nobs": int(getattr(fit, "_N")) }]))
        extract = pd.DataFrame(terms, columns=["event_time", "term"]).merge(tidy[["term", "estimate", "std_error", "conf_low", "conf_high"]], on="term", how="left")
        extract["outcome"] = outcome
        extract["nobs"] = int(getattr(fit, "_N"))
        output_rows.append(extract)
    output = pd.concat(output_rows, ignore_index=True).sort_values(["outcome", "event_time"])
    path = rtp_dir(config) / "frozen_universe" / "original_2018_monthly_event_coefficients.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(path, index=False)
    metadata = {"output_path": str(path), "panel_path": str(panel_path), "min_event": min_event, "max_event": max_event, "outcomes": list(requested_outcomes), "duty_inclusive_status": "blocked_pending_public_tariff_ledger"}
    write_metadata_json(path.with_suffix(".metadata.json"), metadata)
    return metadata
