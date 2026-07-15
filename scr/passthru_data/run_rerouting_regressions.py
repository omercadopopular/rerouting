"""Estimate baseline and rerouting-controlled import regressions."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pyfixest as pf

from .config import PipelineConfig
from .io_utils import read_table, write_metadata_json
from .trade_regression_common import month_index_from_columns, stata_month_period_to_index


def _prepare_panel(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out = out.loc[pd.to_numeric(out["cty_code"], errors="coerce").fillna(-9999) > 0].copy()
    out = out.loc[(out["year"] > 2016) & ((out["year"] < 2019) | ((out["year"] == 2019) & (out["month"] <= 4)))].copy()
    out["mdate_index"] = month_index_from_columns(out)
    out["id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
    out["ct"] = pd.factorize(out["cty_code"].astype("Int64").astype(str) + "|" + out["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    out["ht"] = pd.factorize(out["hs6"].astype("string") + "|" + out["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    out["hs2"] = out["hs6"].astype("string").str.slice(0, 2)
    out["cs"] = pd.factorize(out["cty_code"].astype("Int64").astype(str) + "|" + out["hs2"].astype("string"), sort=False)[0]
    out["m_val"] = pd.to_numeric(out["m_val"], errors="coerce")
    out["m_q1"] = pd.to_numeric(out["m_q1"], errors="coerce")
    out["m_p"] = pd.to_numeric(out["m_p"], errors="coerce")
    out["m_pduty"] = pd.to_numeric(out["m_pduty"], errors="coerce")
    out["m_stattariff2"] = pd.to_numeric(out["m_stattariff2"], errors="coerce")
    out["m_effective_mdate2"] = out["m_effective_mdate2"].map(stata_month_period_to_index)
    out["m_ess"] = pd.to_numeric(out["m_ess"], errors="coerce")
    out["treated"] = (out["m_ess"] == 2).astype(int)
    out["reroute_treated_t"] = pd.to_numeric(out["reroute_treated_t"], errors="coerce")
    out["reroute_treated_init"] = pd.to_numeric(out["reroute_treated_init"], errors="coerce")
    return out


def _event_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    default_index = 2018 * 12 + 2 - 1
    out["d_index"] = out["m_effective_mdate2"].astype("Float64")
    out.loc[out["d_index"].isna() & (out["m_ess"] == 0), "d_index"] = default_index
    out["event_time"] = out["mdate_index"] - out["d_index"]
    out = out.loc[out["event_time"].notna()].copy()
    out["event_time"] = out["event_time"].astype(int)
    out = out.loc[(out["event_time"] >= -6) & (out["event_time"] <= 6)].copy()
    out.loc[out["event_time"] >= 6, "event_time"] = 6
    for v in range(-5, 7):
        tag = f"et_m{abs(v)}" if v < 0 else f"et_p{v}"
        ytag = f"yt_m{abs(v)}" if v < 0 else f"yt_p{v}"
        out[tag] = ((out["treated"] == 1) & (out["event_time"] == v)).astype(int)
        out[ytag] = (out["event_time"] == v).astype(int)
    return out


def _run_event(frame: pd.DataFrame, controlled: bool) -> pd.DataFrame:
    data = frame.loc[frame["m_val"] > 0].copy()
    data["l_val"] = 100.0 * np.log(data["m_val"] * 1_000_000.0)
    et_cols = [f"et_m{abs(v)}" if v < 0 else f"et_p{v}" for v in range(-5, 7)]
    yt_cols = [f"yt_m{abs(v)}" if v < 0 else f"yt_p{v}" for v in range(-5, 7)]
    rhs = et_cols + yt_cols
    if controlled:
        rhs = rhs + ["reroute_treated_t", "reroute_treated_init"]
    fit = pf.feols(
        f"l_val ~ {' + '.join(rhs)} | id + ct + ht",
        data=data,
        vcov={"CRV1": "hs6 + cty_code"},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    tidy = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
    rows = [{"horizon": -6, "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0, "term": "baseline"}]
    for v in range(-5, 7):
        term = f"et_m{abs(v)}" if v < 0 else f"et_p{v}"
        match = tidy.loc[tidy["term"] == term]
        if match.empty:
            rows.append({"horizon": v, "estimate": np.nan, "std_error": np.nan, "conf_low": np.nan, "conf_high": np.nan, "term": term})
        else:
            rec = match.iloc[0][["estimate", "std_error", "conf_low", "conf_high", "term"]].to_dict()
            rec["horizon"] = v
            rows.append(rec)
    out = pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
    out["spec"] = "event"
    out["model"] = "controlled" if controlled else "baseline"
    out["nobs"] = int(getattr(fit, "_N"))
    return out


def _dynamic_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["id", "mdate_index"]).copy()
    out["lstattf"] = np.log1p(out["m_stattariff2"])
    out["x"] = out.groupby("id", sort=False)["lstattf"].diff()
    out["log_val"] = np.where(out["m_val"] > 0, np.log(out["m_val"] * 1_000_000.0), np.nan)
    out["dl_val"] = out.groupby("id", sort=False)["log_val"].diff()
    for lead in range(1, 7):
        out[f"F{lead}x"] = out.groupby("id", sort=False)["x"].shift(-lead).fillna(0.0)
        out[f"DUMMYF{lead}"] = out.groupby("id", sort=False)["x"].shift(-lead).isna().astype(int)
    for lag in range(1, 7):
        out[f"L{lag}x"] = out.groupby("id", sort=False)["x"].shift(lag).fillna(0.0)
        out[f"DUMMYL{lag}"] = out.groupby("id", sort=False)["x"].shift(lag).isna().astype(int)
    return out


def _run_dynamic(frame: pd.DataFrame, controlled: bool) -> pd.DataFrame:
    data = frame.loc[frame["dl_val"].notna() & frame["x"].notna()].copy()
    rhs = [f"F{k}x" for k in range(6, 0, -1)] + ["x"] + [f"L{k}x" for k in range(1, 7)] + [f"DUMMYF{k}" for k in range(1, 7)] + [f"DUMMYL{k}" for k in range(1, 7)]
    if controlled:
        rhs = rhs + ["reroute_treated_t", "reroute_treated_init"]
    fit = pf.feols(
        f"dl_val ~ {' + '.join(rhs)} | ht + ct + cs",
        data=data,
        vcov={"CRV1": "hs6 + cty_code"},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    coef = fit.coef()
    vcov = np.asarray(fit._vcov)
    names = list(fit._coefnames)
    name_to_idx = {n: i for i, n in enumerate(names)}
    terms = []
    current: list[str] = []
    for lead in range(6, 0, -1):
        current = current + [f"F{lead}x"]
        terms.append((-lead, current.copy()))
    current = current + ["x"]
    terms.append((0, current.copy()))
    for lag in range(1, 7):
        current = current + [f"L{lag}x"]
        terms.append((lag, current.copy()))
    rows = []
    for h, pieces in terms:
        idx = [name_to_idx[p] for p in pieces if p in name_to_idx]
        est = float(sum(float(coef[p]) for p in pieces if p in coef.index))
        if idx:
            sub = vcov[np.ix_(idx, idx)]
            se = float(np.sqrt(max(float(np.ones(len(idx)) @ sub @ np.ones(len(idx))), 0.0)))
        else:
            se = np.nan
        rows.append({"horizon": h, "estimate": est, "std_error": se, "conf_low": est - 1.96 * se if pd.notna(se) else np.nan, "conf_high": est + 1.96 * se if pd.notna(se) else np.nan})
    out = pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
    out["spec"] = "dynamic"
    out["model"] = "controlled" if controlled else "baseline"
    out["nobs"] = int(getattr(fit, "_N"))
    return out


def _run_rerouting_outcome(frame: pd.DataFrame) -> pd.DataFrame:
    base = frame[["hs6", "year", "month", "mdate_index", "m_stattariff2", "reroute_share_t"]].drop_duplicates(["hs6", "year", "month"]).copy()
    base = base.sort_values(["hs6", "mdate_index"]).reset_index(drop=True)
    base["l_tarf"] = np.log1p(pd.to_numeric(base["m_stattariff2"], errors="coerce"))
    base["dl_tarf"] = base.groupby("hs6", sort=False)["l_tarf"].diff()
    base["dl_reroute"] = base.groupby("hs6", sort=False)["reroute_share_t"].diff()
    base["tt"] = pd.factorize(base["year"].astype("Int64").astype(str) + "-" + base["month"].astype("Int64").astype(str), sort=False)[0]
    work = base.loc[base["dl_reroute"].notna() & base["dl_tarf"].notna()].copy()
    fit = pf.feols("dl_reroute ~ dl_tarf | hs6 + tt", data=work, vcov={"CRV1": "hs6"}, copy_data=False, store_data=False, lean=True)
    tidy = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
    return tidy


def run_rerouting_regressions(config: PipelineConfig) -> dict[str, Any]:
    panel_path = config.analysis_dir / "imports_hs6_raw_package_shocks_rerouting.parquet"
    panel = _prepare_panel(read_table(panel_path))
    event_panel = _event_frame(panel)
    dynamic_panel = _dynamic_frame(panel)

    out_dir = config.analysis_dir / "trade_regressions" / "rerouting_extension"
    out_dir.mkdir(parents=True, exist_ok=True)

    event_base = _run_event(event_panel, controlled=False)
    event_ctrl = _run_event(event_panel, controlled=True)
    dyn_base = _run_dynamic(dynamic_panel, controlled=False)
    dyn_ctrl = _run_dynamic(dynamic_panel, controlled=True)
    reroute_out = _run_rerouting_outcome(panel)

    event = pd.concat([event_base, event_ctrl], ignore_index=True)
    dynamic = pd.concat([dyn_base, dyn_ctrl], ignore_index=True)
    event_path = out_dir / "imports_event_val_baseline_vs_rerouting_controls.csv"
    dynamic_path = out_dir / "imports_dynamic_val_baseline_vs_rerouting_controls.csv"
    reroute_path = out_dir / "rerouting_outcome_hs6_diff_regression.csv"
    event.to_csv(event_path, index=False)
    dynamic.to_csv(dynamic_path, index=False)
    reroute_out.to_csv(reroute_path, index=False)

    meta = {
        "event_rows": int(len(event)),
        "dynamic_rows": int(len(dynamic)),
        "reroute_outcome_rows": int(len(reroute_out)),
        "event_output": str(event_path),
        "dynamic_output": str(dynamic_path),
        "reroute_output": str(reroute_path),
        "sample_period_min": None if panel.empty else f"{int(panel['year'].min()):04d}-{int(panel['month'].min()):02d}",
        "sample_period_max": None if panel.empty else f"{int(panel['year'].max()):04d}-{int(panel['month'].max()):02d}",
    }
    write_metadata_json(out_dir / "rerouting_regressions.metadata.json", meta)
    return meta
