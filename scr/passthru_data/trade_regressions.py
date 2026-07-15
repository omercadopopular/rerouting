"""Run trade-regression replications for the paper-window trade analyses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyfixest as pf

from .config import PipelineConfig
from .io_utils import read_table, write_metadata_json
from .trade_regression_common import (
    REGRESSION_OUTCOMES,
    WORKHORSE_SPECS,
    chart_dir,
    month_index_from_columns,
    package_reference_figure_path,
    regression_dir,
    write_markdown_report,
    stata_month_period_to_index,
    table_dir,
    workhorse_metadata_path,
    workhorse_output_path,
)


@dataclass(frozen=True)
class RegressionResult:
    flow: str
    spec: str
    outcome: str
    frame: pd.DataFrame
    nobs: int
    r2: float | None
    source_mode: str
    input_path: str


def _load_workhorse(config: PipelineConfig, flow: str, columns: list[str] | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    panel_path = workhorse_output_path(config, flow)
    metadata = {}
    metadata_path = workhorse_metadata_path(config, flow)
    if metadata_path.exists():
        import json
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    df = read_table(panel_path, columns=columns) if columns else read_table(panel_path)
    return df, metadata


def _factorize_columns(frame: pd.DataFrame, columns: list[str], name: str) -> pd.Series:
    key_frame = frame[columns].copy()
    for column in columns:
        key_frame[column] = key_frame[column].astype("string")
    return pd.MultiIndex.from_frame(key_frame).factorize(sort=False)[0]


def _prepare_base_frame(frame: pd.DataFrame, flow: str) -> pd.DataFrame:
    prefix = WORKHORSE_SPECS[flow]["prefix"]
    out = frame.copy()
    out["mdate_index"] = month_index_from_columns(out)
    out = out.loc[pd.to_numeric(out["cty_code"], errors="coerce").fillna(-9999) > 0].copy()
    if "id" not in out.columns:
        out["id"] = _factorize_columns(out, ["cty_code", "hs10"], "id")
    out["naics_str"] = out["naics_str"].astype("string")
    out["naics4"] = out["naics_str"].str.slice(0, 4)
    out["naics3"] = out["naics_str"].str.slice(0, 3)
    out["naics2"] = out["naics_str"].str.slice(0, 2)
    out["ht"] = _factorize_columns(out, ["hs10", "mdate_index"], "ht")
    out["ct"] = _factorize_columns(out, ["cty_code", "mdate_index"], "ct")
    out["cs"] = _factorize_columns(out, ["cty_code", "naics4"], "cs")
    for suffix in REGRESSION_OUTCOMES:
        column = f"{prefix}_{suffix}"
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    tariff_col = f"{prefix}_stattariff2"
    out[tariff_col] = pd.to_numeric(out[tariff_col], errors="coerce")
    return out


def _event_series_name(prefix: str, value: int, kind: str) -> str:
    sign = "m" if value < 0 else "p"
    return f"{kind}_{sign}{abs(value)}"


def _prepare_event_study(flow: str, frame: pd.DataFrame) -> pd.DataFrame:
    spec = WORKHORSE_SPECS[flow]
    prefix = spec["prefix"]
    out = _prepare_base_frame(frame, flow)
    out = out.loc[out["year"] >= 2017].copy()
    d_col = f"{prefix}_effective_mdate2"
    ess_col = f"{prefix}_ess"
    out["d_index"] = out[d_col].map(stata_month_period_to_index).astype("Int64")
    out[ess_col] = pd.to_numeric(out[ess_col], errors="coerce")
    for sector_col in ("naics4", "naics3", "naics2"):
        fill_value = out.groupby(sector_col, dropna=False)["d_index"].transform("min")
        out.loc[out["d_index"].isna() & (out[ess_col] == 0), "d_index"] = fill_value
    default_period = pd.Period(spec["default_event_period"], freq="M")
    default_index = int(default_period.year) * 12 + int(default_period.month) - 1
    out.loc[out["d_index"].isna() & (out[ess_col] == 0), "d_index"] = default_index
    out["event_time"] = out["mdate_index"] - out["d_index"]
    out = out.loc[out["event_time"].notna()].copy()
    out["event_time"] = out["event_time"].astype(int)
    out.loc[out["event_time"] >= 6, "event_time"] = 6
    out = out.loc[out["event_time"] >= -6].copy()
    out["T"] = (out[ess_col] == 2).astype(int)
    for event_value in range(-5, 7):
        et_name = _event_series_name(prefix, event_value, "et")
        yt_name = _event_series_name(prefix, event_value, "yt")
        out[et_name] = ((out["T"] == 1) & (out["event_time"] == event_value)).astype(int)
        out[yt_name] = (out["event_time"] == event_value).astype(int)
    return out


def _run_event_study_one(config: PipelineConfig, flow: str, outcome: str, frame: pd.DataFrame, source_mode: str, input_path: str) -> RegressionResult:
    prefix = WORKHORSE_SPECS[flow]["prefix"]
    ycol = f"{prefix}_{outcome}"
    work = frame.loc[frame[ycol] > 0].copy()
    work[f"l_{outcome}"] = 100 * np.log(work[ycol] * 1_000_000.0)
    event_columns = [_event_series_name(prefix, value, "et") for value in range(-5, 7)]
    control_columns = [_event_series_name(prefix, value, "yt") for value in range(-5, 7)]
    rhs = " + ".join(event_columns + control_columns)
    formula = f"l_{outcome} ~ {rhs} | id + ct + ht"
    fit = pf.feols(
        formula,
        work,
        vcov={"CRV1": WORKHORSE_SPECS[flow]["cluster_expr"]},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    tidy = fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})
    rows = []
    for event_value in range(-6, 7):
        if event_value == -6:
            rows.append({"event_time": -6, "term": "baseline", "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0})
            continue
        term = _event_series_name(prefix, event_value, "et")
        match = tidy.loc[tidy["term"] == term]
        if match.empty:
            rows.append({"event_time": event_value, "term": term, "estimate": 0.0, "std_error": np.nan, "conf_low": np.nan, "conf_high": np.nan})
        else:
            record = match.iloc[0].to_dict()
            record["event_time"] = event_value
            rows.append(record)
    result = pd.DataFrame(rows)
    result["flow"] = flow
    result["spec"] = "event"
    result["outcome"] = outcome
    result["nobs"] = int(getattr(fit, "_N"))
    result["r2"] = float(getattr(fit, "_r2"))
    result["source_mode"] = source_mode
    result["input_path"] = input_path
    return RegressionResult(flow=flow, spec="event", outcome=outcome, frame=result, nobs=int(getattr(fit, "_N")), r2=float(getattr(fit, "_r2")), source_mode=source_mode, input_path=input_path)


def _prepare_dynamic(flow: str, frame: pd.DataFrame) -> pd.DataFrame:
    prefix = WORKHORSE_SPECS[flow]["prefix"]
    out = _prepare_base_frame(frame, flow)
    out = out.loc[out["year"] >= 2017].copy()
    out = out.sort_values(["id", "mdate_index"]).reset_index(drop=True)
    tariff_col = f"{prefix}_stattariff2"
    out["lstattf"] = np.log1p(out[tariff_col])
    out["x"] = out.groupby("id", sort=False)["lstattf"].diff()
    for outcome in REGRESSION_OUTCOMES:
        ycol = f"{prefix}_{outcome}"
        valid = out[ycol] > 0
        out[f"log_{outcome}"] = np.where(valid, np.log(out[ycol] * 1_000_000.0), np.nan)
        out[f"dl_{outcome}"] = out.groupby("id", sort=False)[f"log_{outcome}"].diff()
    for lead in range(1, 7):
        out[f"F{lead}x"] = out.groupby("id", sort=False)["x"].shift(-lead)
        out[f"DUMMYF{lead}"] = out[f"F{lead}x"].isna().astype(int)
        out[f"F{lead}x"] = out[f"F{lead}x"].fillna(0.0)
    for lag in range(1, 7):
        out[f"L{lag}x"] = out.groupby("id", sort=False)["x"].shift(lag)
        out[f"DUMMYL{lag}"] = out[f"L{lag}x"].isna().astype(int)
        out[f"L{lag}x"] = out[f"L{lag}x"].fillna(0.0)
    return out


def _dynamic_cumulative_terms() -> list[tuple[int, list[str]]]:
    terms: list[tuple[int, list[str]]] = []
    current: list[str] = []
    for lead in range(6, 0, -1):
        current = current + [f"F{lead}x"]
        terms.append((-lead, current.copy()))
    current = current + ["x"]
    terms.append((0, current.copy()))
    for lag in range(1, 7):
        current = current + [f"L{lag}x"]
        terms.append((lag, current.copy()))
    return terms


def _run_dynamic_one(config: PipelineConfig, flow: str, outcome: str, frame: pd.DataFrame, source_mode: str, input_path: str) -> RegressionResult:
    ycol = f"dl_{outcome}"
    work = frame.loc[frame[ycol].notna() & frame["x"].notna()].copy()
    lead_terms = [f"F{lead}x" for lead in range(6, 0, -1)]
    lag_terms = [f"L{lag}x" for lag in range(1, 7)]
    missing_terms = [f"DUMMYF{lead}" for lead in range(1, 7)] + [f"DUMMYL{lag}" for lag in range(1, 7)]
    rhs = " + ".join(lead_terms + ["x"] + lag_terms + missing_terms)
    formula = f"{ycol} ~ {rhs} | ht + ct + cs"
    fit = pf.feols(
        formula,
        work,
        vcov={"CRV1": WORKHORSE_SPECS[flow]["cluster_expr"]},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    coef = fit.coef()
    vcov = np.asarray(fit._vcov)
    coef_names = list(fit._coefnames)
    name_to_idx = {name: idx for idx, name in enumerate(coef_names)}
    rows = []
    for horizon, terms in _dynamic_cumulative_terms():
        available = [term for term in terms if term in name_to_idx]
        estimate = float(sum(float(coef[term]) for term in available)) if available else 0.0
        if available:
            idx = [name_to_idx[term] for term in available]
            vcov_sub = vcov[np.ix_(idx, idx)]
            variance = float(np.ones(len(idx)) @ vcov_sub @ np.ones(len(idx)))
            std_error = float(np.sqrt(max(variance, 0.0)))
        else:
            std_error = np.nan
        rows.append(
            {
                "horizon": horizon,
                "term": " + ".join(terms),
                "estimate": estimate,
                "std_error": std_error,
                "conf_low": estimate - 1.96 * std_error if pd.notna(std_error) else np.nan,
                "conf_high": estimate + 1.96 * std_error if pd.notna(std_error) else np.nan,
            }
        )
    result = pd.DataFrame(rows)
    result["flow"] = flow
    result["spec"] = "dynamic"
    result["outcome"] = outcome
    result["nobs"] = int(getattr(fit, "_N"))
    result["r2"] = float(getattr(fit, "_r2"))
    result["source_mode"] = source_mode
    result["input_path"] = input_path
    return RegressionResult(flow=flow, spec="dynamic", outcome=outcome, frame=result, nobs=int(getattr(fit, "_N")), r2=float(getattr(fit, "_r2")), source_mode=source_mode, input_path=input_path)


def _collect_existing_regression_outputs(config: PipelineConfig) -> dict[str, Any]:
    outputs: dict[str, Any] = {"event": {}, "dynamic": {}}
    for flow in WORKHORSE_SPECS:
        event_path = table_dir(config) / f"{flow}_event_study_coefficients.parquet"
        dynamic_path = table_dir(config) / f"{flow}_dynamic_coefficients.parquet"
        if event_path.exists():
            event_df = read_table(event_path)
            outputs["event"][flow] = {
                "rows": int(len(event_df)),
                "parquet": str(event_path),
                "csv": str(event_path.with_suffix(".csv")),
            }
        if dynamic_path.exists():
            dynamic_df = read_table(dynamic_path)
            outputs["dynamic"][flow] = {
                "rows": int(len(dynamic_df)),
                "parquet": str(dynamic_path),
                "csv": str(dynamic_path.with_suffix(".csv")),
            }
    return outputs


def _collect_existing_chart_outputs(config: PipelineConfig) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    for flow in WORKHORSE_SPECS:
        for spec_name in ("event_study", "dynamic"):
            png_path = chart_dir(config) / f"{flow}_{spec_name}.png"
            pdf_path = chart_dir(config) / f"{flow}_{spec_name}.pdf"
            if png_path.exists() and pdf_path.exists():
                outputs[f"{flow}_{spec_name}"] = {"png": str(png_path), "pdf": str(pdf_path)}
    return outputs


def run_trade_regressions(config: PipelineConfig) -> dict[str, Any]:
    outputs: dict[str, Any] = {"event": {}, "dynamic": {}}
    flows = [config.trade_flow] if config.trade_flow else list(WORKHORSE_SPECS)
    requested_specs = [config.regression_spec] if config.regression_spec else ["event", "dynamic"]
    requested_outcomes = [config.regression_outcome] if config.regression_outcome else list(REGRESSION_OUTCOMES)
    for flow in flows:
        spec = WORKHORSE_SPECS[flow]
        panel_path = workhorse_output_path(config, flow)
        workhorse, metadata = _load_workhorse(config, flow)
        source_mode = metadata.get("build_mode", "unknown")
        source_path = metadata.get("source_path", str(panel_path))
        if "event" in requested_specs:
            event_frame = _prepare_event_study(flow, workhorse)
            event_results = []
            for outcome in requested_outcomes:
                event_results.append(_run_event_study_one(config, flow, outcome, event_frame, source_mode, source_path).frame)
            event_df = pd.concat(event_results, ignore_index=True)
            event_path = table_dir(config) / f"{flow}_event_study_coefficients.parquet"
            if event_path.exists():
                existing = read_table(event_path)
                existing = existing.loc[~existing["outcome"].isin(requested_outcomes)].copy()
                event_df = pd.concat([existing, event_df], ignore_index=True)
            event_df = event_df.sort_values(["outcome", "event_time"]).reset_index(drop=True)
            event_df.to_parquet(event_path, index=False)
            event_df.to_csv(event_path.with_suffix(".csv"), index=False)
            outputs["event"][flow] = {"rows": int(len(event_df)), "parquet": str(event_path), "csv": str(event_path.with_suffix(".csv"))}
        if "dynamic" in requested_specs:
            dynamic_frame = _prepare_dynamic(flow, workhorse)
            dynamic_results = []
            for outcome in requested_outcomes:
                dynamic_results.append(_run_dynamic_one(config, flow, outcome, dynamic_frame, source_mode, source_path).frame)
            dynamic_df = pd.concat(dynamic_results, ignore_index=True)
            dynamic_path = table_dir(config) / f"{flow}_dynamic_coefficients.parquet"
            if dynamic_path.exists():
                existing = read_table(dynamic_path)
                existing = existing.loc[~existing["outcome"].isin(requested_outcomes)].copy()
                dynamic_df = pd.concat([existing, dynamic_df], ignore_index=True)
            dynamic_df = dynamic_df.sort_values(["outcome", "horizon"]).reset_index(drop=True)
            dynamic_df.to_parquet(dynamic_path, index=False)
            dynamic_df.to_csv(dynamic_path.with_suffix(".csv"), index=False)
            outputs["dynamic"][flow] = {"rows": int(len(dynamic_df)), "parquet": str(dynamic_path), "csv": str(dynamic_path.with_suffix(".csv"))}

    all_outputs = _collect_existing_regression_outputs(config)
    manifest_path = regression_dir(config) / "trade_regression_manifest.json"
    write_metadata_json(manifest_path, all_outputs)
    _write_regression_report(config, all_outputs)
    outputs["manifest"] = str(manifest_path)
    return outputs


def _plot_event(ax, frame: pd.DataFrame, title: str) -> None:
    ax.axhline(0.0, color="0.7", linestyle="--", linewidth=1)
    ax.errorbar(frame["event_time"], frame["estimate"], yerr=1.96 * frame["std_error"].fillna(0.0), fmt="o", color="firebrick", ecolor="0.4", capsize=3)
    ax.set_title(title)
    ax.set_xlabel("Months Relative to Tariff Enactment")
    ax.set_ylabel("Percent")
    ax.set_xticks(list(range(-6, 7)))


def _plot_dynamic(ax, frame: pd.DataFrame, title: str) -> None:
    ax.axhline(0.0, color="0.7", linestyle="--", linewidth=1)
    ax.plot(frame["horizon"], frame["estimate"], color="midnightblue", marker="o")
    ax.plot(frame["horizon"], frame["conf_high"], color="midnightblue", linestyle="--", linewidth=1)
    ax.plot(frame["horizon"], frame["conf_low"], color="midnightblue", linestyle="--", linewidth=1)
    ax.set_title(title)
    ax.set_xlabel("Months Relative to Tariff Increase")
    ax.set_ylabel("Percent")
    ax.set_xticks(list(range(-6, 7)))


def plot_trade_regressions(config: PipelineConfig) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    outcome_titles = {
        "val": "Log Value",
        "q1": "Log Quantity",
        "p": "Log Unit Value",
        "pduty": "Log Duty-Inclusive Unit Value",
    }
    flows = [config.trade_flow] if config.trade_flow else list(WORKHORSE_SPECS)
    requested_specs = [config.regression_spec] if config.regression_spec else ["event", "dynamic"]
    requested_outcomes = [config.regression_outcome] if config.regression_outcome else list(REGRESSION_OUTCOMES)
    for flow in flows:
        for spec_name, plotter in (("event_study", _plot_event), ("dynamic", _plot_dynamic)):
            if spec_name == "event_study" and "event" not in requested_specs:
                continue
            if spec_name == "dynamic" and "dynamic" not in requested_specs:
                continue
            input_path = table_dir(config) / f"{flow}_{'event_study' if spec_name == 'event_study' else 'dynamic'}_coefficients.parquet"
            df = read_table(input_path)
            fig, axes = plt.subplots(2, 2, figsize=(11, 8.5), constrained_layout=True)
            axes = axes.ravel()
            for idx, outcome in enumerate(REGRESSION_OUTCOMES):
                subset = df.loc[df["outcome"] == outcome].copy()
                if outcome not in requested_outcomes and config.regression_outcome is not None:
                    axes[idx].axis("off")
                    continue
                if subset.empty:
                    axes[idx].axis("off")
                    axes[idx].text(0.5, 0.5, f"Missing outcome: {outcome}", ha="center", va="center")
                    continue
                subset = subset.sort_values("event_time" if spec_name == "event_study" else "horizon")
                plotter(axes[idx], subset, outcome_titles[outcome])
            fig.suptitle(f"{flow.title()} {'Event Study' if spec_name == 'event_study' else 'Dynamic Lead-Lag'}")
            png_path = chart_dir(config) / f"{flow}_{spec_name}.png"
            pdf_path = chart_dir(config) / f"{flow}_{spec_name}.pdf"
            fig.savefig(png_path, dpi=200)
            fig.savefig(pdf_path)
            plt.close(fig)
            outputs[f"{flow}_{spec_name}"] = {"png": str(png_path), "pdf": str(pdf_path)}
    all_outputs = _collect_existing_chart_outputs(config)
    manifest_path = regression_dir(config) / "trade_regression_chart_manifest.json"
    write_metadata_json(manifest_path, all_outputs)
    outputs["manifest"] = str(manifest_path)
    return outputs


def _write_regression_report(config: PipelineConfig, outputs: dict[str, Any]) -> Path:
    report_path = config.verification_dir / "trade_regression_package_policy_report.md"
    lines = [
        "# Trade Regression Package-Policy Replication",
        "",
        "This run uses locally built raw-source workhorse panels (raw-only mode, no package-policy input).",
        "",
        "## Output Tables",
        "",
    ]
    for spec_name in ("event", "dynamic"):
        for flow, payload in outputs.get(spec_name, {}).items():
            lines.append(f"- `{flow}` `{spec_name}` coefficients: `{payload['csv']}`")
    lines.extend(["", "## Package Reference Figures", ""])
    for flow in WORKHORSE_SPECS:
        event_path = package_reference_figure_path(config, flow, "event")
        dynamic_path = package_reference_figure_path(config, flow, "dynamic")
        lines.append(f"- `{flow}` event reference: `{event_path}`")
        lines.append(f"- `{flow}` dynamic reference: `{dynamic_path}`")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- The package does not ship the underlying numeric chart series for Figures 2, 3, and 4, so the automated benchmark here is script-faithful reconstruction plus side-by-side figure comparison.",
            "- Policy/tariff regressors are sourced from the local raw-construction pipeline and compared to package figures only as an external benchmark.",
        ]
    )
    return write_markdown_report(report_path, lines)
