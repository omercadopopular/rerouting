"""Stepwise replication and rerouting-control analysis (imports, value outcome)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyfixest as pf

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_metadata_json
from .trade_regression_common import month_index_from_columns, stata_month_period_to_index


def _load_package_imports(config: PipelineConfig) -> pd.DataFrame:
    path = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    cols = [
        "cty_code",
        "cty_name",
        "hs10",
        "year",
        "month",
        "mdate",
        "m_val",
        "m_q1",
        "m_ess",
        "m_status2",
        "m_effective_mdate2",
        "m_stattariff2",
    ]
    frame = read_table(path, columns=cols)
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10))
    frame["hs6"] = frame["hs10"].str.slice(0, 6)
    frame["mdate_index"] = month_index_from_columns(frame)
    frame["id"] = pd.to_numeric(frame.get("id"), errors="coerce")
    if frame["id"].isna().all():
        frame["id"] = pd.factorize(frame["cty_code"].astype("Int64").astype(str) + "|" + frame["hs10"].astype("string"), sort=False)[0]
    frame["id"] = pd.to_numeric(frame["id"], errors="coerce").astype("Int64")
    frame["ct"] = pd.factorize(frame["cty_code"].astype("Int64").astype(str) + "|" + frame["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    frame["ht"] = pd.factorize(frame["hs10"].astype("string") + "|" + frame["mdate_index"].astype("Int64").astype(str), sort=False)[0]
    frame["hs8"] = frame["hs10"].str.slice(0, 8)
    frame["hs2"] = frame["hs10"].str.slice(0, 2)
    frame["cs"] = pd.factorize(frame["cty_code"].astype("Int64").astype(str) + "|" + frame["hs2"].astype("string"), sort=False)[0]
    frame["m_val"] = pd.to_numeric(frame["m_val"], errors="coerce")
    frame["m_q1"] = pd.to_numeric(frame["m_q1"], errors="coerce")
    frame["m_stattariff2"] = pd.to_numeric(frame["m_stattariff2"], errors="coerce")
    frame["m_effective_mdate2"] = frame["m_effective_mdate2"].map(stata_month_period_to_index)
    frame["treated"] = (pd.to_numeric(frame["m_ess"], errors="coerce").fillna(0) == 2).astype(int)
    frame = frame.loc[(frame["year"] >= 2017) & ((frame["year"] < 2019) | ((frame["year"] == 2019) & (frame["month"] <= 4)))].copy()
    return frame.reset_index(drop=True)


def _load_rerouting_controls(config: PipelineConfig) -> pd.DataFrame:
    path = config.repo_root / "data" / "rerouted_shares" / "data_share_rerouted.dta"
    frame = read_table(path)
    frame["hs6"] = frame["hs_6dig"].map(lambda value: normalize_hs_code(value, 6))
    frame["mdate"] = pd.to_datetime(frame["modate_imports"], errors="coerce")
    frame["year"] = frame["mdate"].dt.year.astype("Int64")
    frame["month"] = frame["mdate"].dt.month.astype("Int64")
    frame["share_rerouted"] = pd.to_numeric(frame["share_rerouted"], errors="coerce")
    frame = frame.dropna(subset=["hs6", "year", "month"]).drop_duplicates(["hs6", "year", "month"], keep="last").copy()
    init = (
        frame.loc[frame["year"] == 2017, ["hs6", "share_rerouted"]]
        .groupby("hs6", as_index=False)["share_rerouted"]
        .mean()
        .rename(columns={"share_rerouted": "reroute_share_init_2017"})
    )
    frame = frame.merge(init, on="hs6", how="left")
    frame = frame.rename(columns={"share_rerouted": "reroute_share_t"})
    return frame[["hs6", "year", "month", "reroute_share_t", "reroute_share_init_2017"]]


def _build_event(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    default_index = 2018 * 12 + 2 - 1
    out["d_index"] = pd.to_numeric(out["m_effective_mdate2"], errors="coerce")
    out.loc[out["d_index"].isna() & (pd.to_numeric(out["m_ess"], errors="coerce") == 0), "d_index"] = default_index
    out["event_time"] = out["mdate_index"] - out["d_index"]
    out = out.loc[out["event_time"].notna()].copy()
    out["event_time"] = out["event_time"].astype(int)
    out.loc[out["event_time"] >= 6, "event_time"] = 6
    out = out.loc[out["event_time"] >= -6].copy()
    for v in range(-5, 7):
        et = f"et_m{abs(v)}" if v < 0 else f"et_p{v}"
        yt = f"yt_m{abs(v)}" if v < 0 else f"yt_p{v}"
        out[et] = ((out["treated"] == 1) & (out["event_time"] == v)).astype(int)
        out[yt] = (out["event_time"] == v).astype(int)
    out = out.sort_values(["id", "mdate_index"]).copy()
    out["baseline_dtau"] = out.groupby("id", sort=False)["m_stattariff2"].transform(lambda s: np.log1p(s).diff())
    out["baseline_dtau"] = out["baseline_dtau"].fillna(0.0)
    return out


def _run_event(frame: pd.DataFrame, controlled: bool) -> pd.DataFrame:
    data = frame.loc[frame["m_val"] > 0].copy()
    data["l_val"] = 100.0 * np.log(data["m_val"] * 1_000_000.0)
    et_cols = [f"et_m{abs(v)}" if v < 0 else f"et_p{v}" for v in range(-5, 7)]
    yt_cols = [f"yt_m{abs(v)}" if v < 0 else f"yt_p{v}" for v in range(-5, 7)]
    rhs = et_cols + yt_cols
    if controlled:
        rhs += ["reroute_treated_init", "baseline_dtau"]
    fit = pf.feols(
        f"l_val ~ {' + '.join(rhs)} | id + ct + ht",
        data=data,
        vcov={"CRV1": "hs8 + cty_code"},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    tidy = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
    rows = [{"horizon": -6, "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0}]
    for v in range(-5, 7):
        term = f"et_m{abs(v)}" if v < 0 else f"et_p{v}"
        hit = tidy.loc[tidy["term"] == term]
        if hit.empty:
            rows.append({"horizon": v, "estimate": np.nan, "std_error": np.nan, "conf_low": np.nan, "conf_high": np.nan})
        else:
            rec = hit.iloc[0][["estimate", "std_error", "conf_low", "conf_high"]].to_dict()
            rec["horizon"] = v
            rows.append(rec)
    out = pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
    out["model"] = "controlled" if controlled else "baseline"
    out["nobs"] = int(getattr(fit, "_N"))
    return out


def _build_dynamic(frame: pd.DataFrame) -> pd.DataFrame:
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
        rhs += ["reroute_treated_init"]
    fit = pf.feols(
        f"dl_val ~ {' + '.join(rhs)} | ht + ct + cs",
        data=data,
        vcov={"CRV1": "hs8 + cty_code"},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    coef = fit.coef()
    vcov = np.asarray(fit._vcov)
    names = list(fit._coefnames)
    idx_map = {n: i for i, n in enumerate(names)}
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
    for h, tlist in terms:
        valid = [t for t in tlist if t in idx_map]
        est = float(sum(float(coef[t]) for t in valid))
        if valid:
            i = [idx_map[t] for t in valid]
            sub = vcov[np.ix_(i, i)]
            se = float(np.sqrt(max(float(np.ones(len(i)) @ sub @ np.ones(len(i))), 0.0)))
        else:
            se = np.nan
        rows.append({"horizon": h, "estimate": est, "std_error": se, "conf_low": est - 1.96 * se if pd.notna(se) else np.nan, "conf_high": est + 1.96 * se if pd.notna(se) else np.nan})
    out = pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
    out["model"] = "controlled" if controlled else "baseline"
    out["nobs"] = int(getattr(fit, "_N"))
    return out


def _plot_overlay(df: pd.DataFrame, xcol: str, title: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    for model, color in (("baseline", "navy"), ("controlled", "darkorange")):
        sub = df.loc[df["model"] == model].sort_values(xcol)
        if sub.empty:
            continue
        ax.plot(sub[xcol], sub["estimate"], marker="o", color=color, label=model)
        ax.fill_between(sub[xcol], sub["conf_low"], sub["conf_high"], color=color, alpha=0.18)
    ax.axhline(0.0, color="0.6", linestyle="--", linewidth=1)
    ax.set_title(title)
    ax.set_xlabel("Horizon")
    ax.set_ylabel("Percent")
    ax.legend(frameon=False)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=220)
    fig.savefig(path.with_suffix(".pdf"))
    plt.close(fig)


def _rerouting_on_tariff_chart(coef_row: pd.Series, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6, 5))
    est = float(coef_row["estimate"])
    lo = float(coef_row["conf_low"])
    hi = float(coef_row["conf_high"])
    ax.errorbar([0], [est], yerr=[[est - lo], [hi - est]], fmt="o", color="firebrick", capsize=6)
    ax.axhline(0.0, color="0.6", linestyle="--", linewidth=1)
    ax.set_xticks([0])
    ax.set_xticklabels(["d ln(1+tau)"])
    ax.set_ylabel("Effect on d rerouted share")
    ax.set_title("Rerouted Share on Tariff Changes")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=220)
    fig.savefig(path.with_suffix(".pdf"))
    plt.close(fig)


def run_stepwise_analysis(config: PipelineConfig) -> dict[str, Any]:
    out_dir = config.analysis_dir / "trade_regressions" / "rerouting_stepwise"
    chart_dir = out_dir / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)

    base = _load_package_imports(config)
    event = _build_event(base)
    dyn = _build_dynamic(base)
    event_base = _run_event(event, controlled=False)
    dyn_base = _run_dynamic(dyn, controlled=False)
    event_base.to_csv(out_dir / "step1_exact_replication_event_val.csv", index=False)
    dyn_base.to_csv(out_dir / "step1_exact_replication_dynamic_val.csv", index=False)
    _plot_overlay(event_base, "horizon", "Step 1: Exact Replication (Event, imports val)", chart_dir / "step1_exact_event_val.png")
    _plot_overlay(dyn_base, "horizon", "Step 1: Exact Replication (Dynamic, imports val)", chart_dir / "step1_exact_dynamic_val.png")

    controls = _load_rerouting_controls(config)
    merged = base.merge(controls, on=["hs6", "year", "month"], how="left")
    merged["reroute_treated_init"] = pd.to_numeric(merged["reroute_share_init_2017"], errors="coerce") * merged["treated"]
    event_c = _build_event(merged)
    dyn_c = _build_dynamic(merged)
    event_ctrl = _run_event(event_c, controlled=True)
    dyn_ctrl = _run_dynamic(dyn_c, controlled=True)
    event_step2 = pd.concat([event_base, event_ctrl], ignore_index=True)
    dyn_step2 = pd.concat([dyn_base, dyn_ctrl], ignore_index=True)
    event_step2.to_csv(out_dir / "step2_controlled_event_val.csv", index=False)
    dyn_step2.to_csv(out_dir / "step2_controlled_dynamic_val.csv", index=False)
    _plot_overlay(event_step2, "horizon", "Step 2: Event (Baseline vs Rerouting-Controlled)", chart_dir / "step2_event_overlay_val.png")
    _plot_overlay(dyn_step2, "horizon", "Step 2: Dynamic (Baseline vs Rerouting-Controlled)", chart_dir / "step2_dynamic_overlay_val.png")

    hs6 = merged[["hs6", "year", "month", "mdate_index", "m_stattariff2", "reroute_share_t"]].drop_duplicates(["hs6", "year", "month"]).sort_values(["hs6", "mdate_index"]).copy()
    hs6["dl_tarf"] = hs6.groupby("hs6", sort=False)["m_stattariff2"].transform(lambda s: np.log1p(pd.to_numeric(s, errors="coerce")).diff())
    hs6["dl_reroute"] = hs6.groupby("hs6", sort=False)["reroute_share_t"].diff()
    hs6["tt"] = pd.factorize(hs6["year"].astype("Int64").astype(str) + "-" + hs6["month"].astype("Int64").astype(str), sort=False)[0]
    work = hs6.loc[hs6["dl_tarf"].notna() & hs6["dl_reroute"].notna()].copy()
    fit = pf.feols("dl_reroute ~ dl_tarf | hs6 + tt", data=work, vcov={"CRV1": "hs6"}, copy_data=False, store_data=False, lean=True)
    rer = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
    rer.to_csv(out_dir / "step3_reroute_on_tariff_changes.csv", index=False)
    coef = rer.loc[rer["term"] == "dl_tarf"].iloc[0]
    _rerouting_on_tariff_chart(coef, chart_dir / "step3_reroute_on_tariff_changes.png")

    meta = {
        "step1_event": str(out_dir / "step1_exact_replication_event_val.csv"),
        "step1_dynamic": str(out_dir / "step1_exact_replication_dynamic_val.csv"),
        "step2_event": str(out_dir / "step2_controlled_event_val.csv"),
        "step2_dynamic": str(out_dir / "step2_controlled_dynamic_val.csv"),
        "step3": str(out_dir / "step3_reroute_on_tariff_changes.csv"),
        "chart_dir": str(chart_dir),
    }
    write_metadata_json(out_dir / "stepwise_analysis.metadata.json", meta)
    return meta


if __name__ == "__main__":
    cfg = PipelineConfig.default()
    cfg.ensure_directories()
    result = run_stepwise_analysis(cfg)
    print(result)
