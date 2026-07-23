"""Estimate and plot the February-2025 pass-through extension.

The public runner refuses empirical estimation until the independent 2025
policy manifest authorizes it.  This keeps a complete Census trade build from
being mistaken for a verified statutory tariff treatment.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyfixest as pf

from scr.data_construction.extension_2025 import (
    CHINA_HK_CODES,
    EVENT_PERIOD,
    SHORT_POST_HORIZON,
    TARGET_POST_HORIZON,
    supported_post_horizon,
)
from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet

VERSION = "pass_through_extension_2025_v1"
OUTCOMES = ("val", "q1", "p", "pduty")
ANALYSES = ("china_hk", "all_actions")
SPECS = ("event", "dynamic")
# The estimable 2025 grid uses the bilateral design.  The historical fixed
# effects absorb a common China-by-month intervention and the historical
# dynamic sector effect requires a current HS10-to-NAICS concordance.  Those
# cases remain explicit rank/provenance diagnostics; they are not fabricated
# as regression checkpoints.
DESIGNS = ("bilateral",)
DIAGNOSTIC_DESIGNS = ("locked",)
HORIZONS = ("short", "long")
OUTCOME_COLUMNS = {outcome: f"m_{outcome}" for outcome in OUTCOMES}
OUTCOME_LABELS = {
    "val": "Import value",
    "q1": "Quantity",
    "p": "Pre-duty unit value",
    "pduty": "Duty-inclusive unit value",
}


@dataclass(frozen=True)
class FitSpec:
    analysis: str
    specification: str
    outcome: str
    design: str
    clock: str
    horizon_kind: str
    pre_horizon: int
    post_horizon: int

    @property
    def fit_id(self) -> str:
        return "|".join((self.analysis, self.specification, self.outcome, self.design, self.clock, self.horizon_kind, f"m{self.pre_horizon}_p{self.post_horizon}"))


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def _eligible_hash(frame: pd.DataFrame, spec: FitSpec) -> tuple[str, str]:
    if spec.specification == "event":
        eligible = frame.loc[pd.to_numeric(frame[OUTCOME_COLUMNS[spec.outcome]], errors="coerce") > 0]
        treatment_columns = ["exposure", "event_time"]
    else:
        eligible = frame.loc[frame[f"dl_{spec.outcome}"].notna() & frame["x"].notna()]
        treatment_columns = ["statutory_total_rate", "x"]
    keys = ["partner_code", "hs10", "year", "month"]
    sample_bytes = pd.util.hash_pandas_object(eligible[keys].astype("string"), index=False).to_numpy().tobytes()
    treatment_bytes = pd.util.hash_pandas_object(eligible[treatment_columns], index=False).to_numpy().tobytes()
    return hashlib.sha256(sample_bytes).hexdigest(), hashlib.sha256(treatment_bytes).hexdigest()


def _formula_description(spec: FitSpec) -> str:
    if spec.specification == "event":
        fixed = "id + ht + country_year"
        return f"100*log(m_{spec.outcome}) ~ exposure x event[-5,+{spec.post_horizon}] | {fixed}"
    return f"D.log(m_{spec.outcome}) ~ F6..F1,D.log(1+tariff),L1..L{spec.post_horizon},missing dummies | id + ht + country_year"


def estimator_fingerprint() -> str:
    sources = [
        inspect.getsource(build_event_design),
        inspect.getsource(build_dynamic_design),
        inspect.getsource(fit_event),
        inspect.getsource(fit_dynamic),
    ]
    normalized = "\n".join(source.replace("\r\n", "\n").rstrip() for source in sources)
    return hashlib.sha256(normalized.encode()).hexdigest()


def horizon_contract(latest_period: str) -> dict[str, Any]:
    maximum = supported_post_horizon(latest_period, EVENT_PERIOD, TARGET_POST_HORIZON)
    return {
        "event_period": EVENT_PERIOD,
        "latest_trade_period": latest_period,
        "short": {"pre": 6, "post": SHORT_POST_HORIZON, "complete": maximum >= SHORT_POST_HORIZON, "topcoded": False},
        "long": {"pre": 6, "target_post": TARGET_POST_HORIZON, "actual_post": maximum, "complete_target": maximum >= TARGET_POST_HORIZON, "topcoded": False},
        "right_censored": maximum < TARGET_POST_HORIZON,
    }


def fit_grid(latest_period: str) -> list[FitSpec]:
    contract = horizon_contract(latest_period)
    rows: list[FitSpec] = []
    for analysis in ANALYSES:
        for spec in SPECS:
            if spec == "dynamic":
                clocks = ("legal_path",)
            else:
                clocks = ("common_feb",) if analysis == "china_hk" else ("common_feb", "first_increase")
            for outcome in OUTCOMES:
                for design in DESIGNS:
                    for clock in clocks:
                        for kind in HORIZONS:
                            post = contract[kind]["post"] if kind == "short" else contract[kind]["actual_post"]
                            if post >= 0:
                                rows.append(FitSpec(analysis, spec, outcome, design, clock, kind, 6, int(post)))
    return rows


def rank_preflight(spec: FitSpec) -> dict[str, Any]:
    broad_common_clock = spec.clock == "common_feb" and spec.analysis == "china_hk"
    absorbed = spec.design == "locked" and broad_common_clock and spec.specification == "event"
    missing_sector = spec.design == "locked" and spec.specification == "dynamic"
    identified = not absorbed and not missing_sector
    if absorbed:
        reason = "common China-by-event-month treatment is absorbed by country-month fixed effects"
    elif missing_sector:
        reason = "historical dynamic design requires a current HS10-to-NAICS4 concordance for the country-sector fixed effect"
    else:
        reason = "not structurally absorbed by declared fixed effects"
    return {
        "fit_id": spec.fit_id,
        "identified": identified,
        "reason": reason,
        "fixed_effects": "id + ct + ht" if spec.design == "locked" and spec.specification == "event" else ("ht + ct + cs" if spec.design == "locked" else "id + ht + country_year"),
        "cluster": "partner_code^hs8",
    }


def diagnostic_grid(latest_period: str) -> list[FitSpec]:
    """Return non-estimable historical-design cases for explicit preflight."""
    contract = horizon_contract(latest_period)
    rows: list[FitSpec] = []
    for analysis in ANALYSES:
        for specification in SPECS:
            clock = "common_feb" if specification == "event" else "legal_path"
            for kind in HORIZONS:
                post = contract[kind]["post"] if kind == "short" else contract[kind]["actual_post"]
                if post >= 0:
                    rows.append(FitSpec(analysis, specification, "val", "locked", clock, kind, 6, int(post)))
    return rows


def _month_index(frame: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(frame["year"], errors="raise").astype(int) * 12 + pd.to_numeric(frame["month"], errors="raise").astype(int) - 1


def _factorize(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    keys = frame[columns].astype("string")
    return pd.MultiIndex.from_frame(keys).factorize(sort=False)[0]


def _base_design(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["partner_code"] = out["partner_code"].astype("string").str.zfill(4)
    out["hs10"] = out["hs10"].astype("string").str.zfill(10)
    out["hs8"] = out["hs10"].str[:8]
    out["hs4"] = out["hs10"].str[:4]
    out["mdate_index"] = _month_index(out)
    out["id"] = _factorize(out, ["partner_code", "hs10"])
    out["ct"] = _factorize(out, ["partner_code", "mdate_index"])
    out["ht"] = _factorize(out, ["hs10", "mdate_index"])
    out["country_year"] = _factorize(out, ["partner_code", "year"])
    out["cluster"] = _factorize(out, ["partner_code", "hs8"])
    return out


def build_event_design(frame: pd.DataFrame, spec: FitSpec) -> pd.DataFrame:
    out = _base_design(frame)
    if spec.clock == "common_feb":
        event_index = pd.Period(EVENT_PERIOD, freq="M").year * 12 + pd.Period(EVENT_PERIOD, freq="M").month - 1
        out["event_index"] = event_index
    elif spec.clock == "first_increase":
        if "first_increase_period" not in out:
            raise ValueError("first_increase clock requires first_increase_period")
        first = pd.PeriodIndex(out["first_increase_period"].astype(str), freq="M")
        out["event_index"] = first.year * 12 + first.month - 1
    else:
        raise ValueError(f"Unsupported event clock {spec.clock}")
    out["event_time"] = out["mdate_index"] - out["event_index"]
    out = out.loc[out["event_time"].between(-spec.pre_horizon, spec.post_horizon)].copy()
    if spec.analysis == "china_hk":
        out["exposure"] = out["partner_code"].isin(CHINA_HK_CODES).astype(float)
    else:
        if "new_admin_treatment_intensity" not in out:
            raise ValueError("all_actions requires new_admin_treatment_intensity")
        out["exposure"] = pd.to_numeric(out["new_admin_treatment_intensity"], errors="coerce")
    for horizon in range(-spec.pre_horizon + 1, spec.post_horizon + 1):
        name = f"event_{'m' if horizon < 0 else 'p'}{abs(horizon)}"
        out[name] = out["exposure"] * out["event_time"].eq(horizon).astype(float)
    return out


def _exact_shift(frame: pd.DataFrame, column: str, offset: int) -> pd.Series:
    lookup = frame[["id", "mdate_index", column]].rename(columns={column: "_value"})
    target = frame[["id", "mdate_index"]].copy()
    target["mdate_index"] += int(offset)
    return target.merge(lookup, on=["id", "mdate_index"], how="left", sort=False)["_value"]


def build_dynamic_design(frame: pd.DataFrame, spec: FitSpec) -> pd.DataFrame:
    out = _base_design(frame).sort_values(["id", "mdate_index"], kind="mergesort").reset_index(drop=True)
    if out.duplicated(["id", "mdate_index"]).any():
        raise ValueError("dynamic panel requires unique partner-HS10-month keys")
    if "statutory_total_rate" not in out:
        raise ValueError("dynamic design requires independently constructed statutory_total_rate")
    out["log_tariff"] = np.log1p(pd.to_numeric(out["statutory_total_rate"], errors="coerce"))
    out["x"] = out["log_tariff"] - _exact_shift(out, "log_tariff", -1)
    for outcome, column in OUTCOME_COLUMNS.items():
        values = pd.to_numeric(out[column], errors="coerce")
        out[f"log_{outcome}"] = np.log(values.where(values > 0))
        out[f"dl_{outcome}"] = out[f"log_{outcome}"] - _exact_shift(out, f"log_{outcome}", -1)
    for lead in range(1, spec.pre_horizon + 1):
        out[f"F{lead}x"] = _exact_shift(out, "x", lead)
        out[f"DUMMYF{lead}"] = out[f"F{lead}x"].isna().astype(int)
        out[f"F{lead}x"] = out[f"F{lead}x"].fillna(0.0)
    for lag in range(1, spec.post_horizon + 1):
        out[f"L{lag}x"] = _exact_shift(out, "x", -lag)
        out[f"DUMMYL{lag}"] = out[f"L{lag}x"].isna().astype(int)
        out[f"L{lag}x"] = out[f"L{lag}x"].fillna(0.0)
    return out


def _tidy(fit: Any) -> pd.DataFrame:
    return fit.tidy().reset_index().rename(columns={"Coefficient": "term", "Estimate": "estimate", "Std. Error": "std_error", "2.5%": "conf_low", "97.5%": "conf_high"})


def fit_event(frame: pd.DataFrame, spec: FitSpec) -> pd.DataFrame:
    y = OUTCOME_COLUMNS[spec.outcome]
    work = frame.loc[pd.to_numeric(frame[y], errors="coerce") > 0].copy()
    work["log_y"] = 100.0 * np.log(pd.to_numeric(work[y], errors="raise"))
    terms = [f"event_{'m' if h < 0 else 'p'}{abs(h)}" for h in range(-spec.pre_horizon + 1, spec.post_horizon + 1)]
    fixed_effects = "id + ct + ht" if spec.design == "locked" else "id + ht + country_year"
    fit = pf.feols(f"log_y ~ {' + '.join(terms)} | {fixed_effects}", work, vcov={"CRV1": "cluster"}, copy_data=False, store_data=False, lean=True)
    tidy = _tidy(fit)
    rows = [{"horizon": -spec.pre_horizon, "term": "baseline", "estimate": 0.0, "std_error": 0.0, "conf_low": 0.0, "conf_high": 0.0}]
    for horizon, term in zip(range(-spec.pre_horizon + 1, spec.post_horizon + 1), terms):
        match = tidy.loc[tidy["term"] == term]
        if match.empty:
            rows.append({"horizon": horizon, "term": term, "estimate": np.nan, "std_error": np.nan, "conf_low": np.nan, "conf_high": np.nan})
        else:
            record = match.iloc[0][["term", "estimate", "std_error", "conf_low", "conf_high"]].to_dict()
            record["horizon"] = horizon
            rows.append(record)
    result = pd.DataFrame(rows)
    result["nobs"] = int(getattr(fit, "_N"))
    return result


def fit_dynamic(frame: pd.DataFrame, spec: FitSpec) -> pd.DataFrame:
    y = f"dl_{spec.outcome}"
    work = frame.loc[frame[y].notna() & frame["x"].notna()].copy()
    leads = [f"F{i}x" for i in range(spec.pre_horizon, 0, -1)]
    lags = [f"L{i}x" for i in range(1, spec.post_horizon + 1)]
    missing = [f"DUMMYF{i}" for i in range(1, spec.pre_horizon + 1)] + [f"DUMMYL{i}" for i in range(1, spec.post_horizon + 1)]
    fixed_effects = "ht + ct + id" if spec.design == "locked" else "id + ht + country_year"
    fit = pf.feols(f"{y} ~ {' + '.join(leads + ['x'] + lags + missing)} | {fixed_effects}", work, vcov={"CRV1": "cluster"}, copy_data=False, store_data=False, lean=True)
    coef = fit.coef()
    names = list(fit._coefnames)
    index = {name: i for i, name in enumerate(names)}
    vcov = np.asarray(fit._vcov)
    cumulative: list[str] = []
    rows = []
    for horizon, term in [(h, f"F{abs(h)}x") for h in range(-spec.pre_horizon, 0)] + [(0, "x")] + [(h, f"L{h}x") for h in range(1, spec.post_horizon + 1)]:
        cumulative.append(term)
        available = [value for value in cumulative if value in index]
        if not available:
            estimate = std_error = np.nan
        else:
            estimate = float(sum(float(coef[value]) for value in available))
            idx = [index[value] for value in available]
            variance = float(np.ones(len(idx)) @ vcov[np.ix_(idx, idx)] @ np.ones(len(idx)))
            std_error = float(np.sqrt(max(variance, 0.0)))
        rows.append({"horizon": horizon, "term": " + ".join(cumulative), "estimate": estimate, "std_error": std_error, "conf_low": estimate - 1.96 * std_error if pd.notna(std_error) else np.nan, "conf_high": estimate + 1.96 * std_error if pd.notna(std_error) else np.nan})
    result = pd.DataFrame(rows)
    result["nobs"] = int(getattr(fit, "_N"))
    return result


def horizon_support(frame: pd.DataFrame, spec: FitSpec) -> pd.DataFrame:
    if "event_time" not in frame:
        return pd.DataFrame()
    base = frame.loc[frame["event_time"] == 0]
    base_products = max(1, int(base["hs10"].nunique()))
    rows = []
    for horizon, group in frame.groupby("event_time", sort=True):
        rows.append({
            "fit_id": spec.fit_id,
            "horizon": int(horizon),
            "rows": len(group),
            "products": int(group["hs10"].nunique()),
            "partners": int(group["partner_code"].nunique()),
            "clusters": int(group["cluster"].nunique()),
            "product_support_share_of_h0": float(group["hs10"].nunique() / base_products),
        })
    return pd.DataFrame(rows)


def _paths(config: PipelineConfig, spec: FitSpec) -> tuple[Path, Path, Path]:
    root = config.processed_trade_dir / "regressions" / "extension_2025" / "checkpoints" / spec.fit_id.replace("|", "__")
    return root / "coefficients.parquet", root / "sample_audit.parquet", root / "manifest.json"


def validate_checkpoint(config: PipelineConfig, spec: FitSpec, source_hash: str, policy_hash: str) -> tuple[bool, str]:
    coefficient_path, audit_path, manifest_path = _paths(config, spec)
    if not all(path.exists() for path in (coefficient_path, audit_path, manifest_path)):
        return False, "missing_checkpoint_component"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {"fit_id": spec.fit_id, "source_hash": source_hash, "policy_hash": policy_hash, "estimator_fingerprint": estimator_fingerprint(), "specification_fingerprint": _hash_payload(asdict(spec))}
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False, f"mismatch:{key}"
    for required in ("eligible_sample_hash", "treatment_hash", "observations", "coefficient_path", "sample_audit_path"):
        if manifest.get(required) in (None, ""):
            return False, f"missing_manifest_field:{required}"
    coefficient = read_table(coefficient_path)
    required_columns = {"horizon", "estimate", "std_error", "conf_low", "conf_high", "nobs", "fit_id"}
    if not required_columns.issubset(coefficient.columns):
        return False, "invalid_coefficient_schema"
    expected_horizons = set(range(-spec.pre_horizon, spec.post_horizon + 1))
    observed_horizons = set(pd.to_numeric(coefficient["horizon"], errors="raise").astype(int))
    if observed_horizons != expected_horizons or coefficient["horizon"].duplicated().any():
        return False, "incomplete_horizon_grid"
    if coefficient["fit_id"].nunique() != 1 or coefficient["fit_id"].iloc[0] != spec.fit_id:
        return False, "coefficient_fit_id_mismatch"
    if int(coefficient["nobs"].iloc[0]) != int(manifest["observations"]):
        return False, "observation_count_mismatch"
    return True, "valid"


def preflight(config: PipelineConfig) -> dict[str, Any]:
    trade_manifest_path = config.processed_trade_dir / "extension_2025" / "trade_extension_manifest.json"
    policy_manifest_path = config.processed_tariff_dir / "extension_2025" / "policy_extension_manifest.json"
    trade = json.loads(trade_manifest_path.read_text(encoding="utf-8")) if trade_manifest_path.exists() else {"status": "missing"}
    policy = json.loads(policy_manifest_path.read_text(encoding="utf-8")) if policy_manifest_path.exists() else {"status": "missing", "policy_gate": "failed"}
    latest = trade.get("end_period") or "2025-12"
    contract = horizon_contract(latest)
    grid = fit_grid(latest)
    rank = [rank_preflight(spec) for spec in diagnostic_grid(latest)]
    result = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "trade_status": trade.get("status"),
        "policy_status": policy.get("status"),
        "policy_gate": policy.get("policy_gate", "failed"),
        "event_estimation_authorized": bool(trade.get("status") == "passed" and policy.get("event_estimation_authorized") is True),
        "horizon_contract": contract,
        "expected_fit_count": len(grid),
        "rank_preflight": rank,
        "diagnostic_nonidentified_fit_count": sum(not row["identified"] for row in rank),
        "diagnostic_cases_are_not_expected_fits": True,
    }
    root = config.processed_trade_dir / "regressions" / "extension_2025"
    write_metadata_json(root / "preflight.json", result)
    return result


def run_fits(config: PipelineConfig, specs: Iterable[FitSpec], *, resume: bool = True) -> dict[str, Any]:
    gate = preflight(config)
    if not gate["event_estimation_authorized"]:
        raise RuntimeError("2025 regression is blocked: the independent product/date/rate/exclusion/stacking ledger has not passed")
    panel_path = config.processed_trade_dir / "extension_2025" / "final_event_panel.parquet"
    policy_path = config.processed_tariff_dir / "extension_2025" / "final_tariff_panel.parquet"
    if not panel_path.exists() or not policy_path.exists():
        raise FileNotFoundError("validated final event and tariff panels are required")
    source_hash, policy_hash = sha256_file(panel_path), sha256_file(policy_path)
    frame = read_table(panel_path)
    current = config.processed_trade_dir / "regressions" / "extension_2025" / "current_fit.json"
    records = []
    for spec in specs:
        valid, reason = validate_checkpoint(config, spec, source_hash, policy_hash)
        if resume and valid:
            records.append({"fit_id": spec.fit_id, "status": "resumed"})
            continue
        prepared = build_event_design(frame, spec) if spec.specification == "event" else build_dynamic_design(frame, spec)
        sample_hash, treatment_hash = _eligible_hash(prepared, spec)
        marker = {
            "fit_id": spec.fit_id,
            "source_mode": spec.analysis,
            "specification": spec.specification,
            "outcome": spec.outcome,
            "row_count": len(prepared),
            "estimated_memory_bytes": int(prepared.memory_usage(deep=True).sum()),
            "formula": _formula_description(spec),
            "fixed_effects": rank_preflight(spec)["fixed_effects"],
            "clusters": "partner_code^hs8",
            "start_time": datetime.now(timezone.utc).isoformat(),
        }
        write_metadata_json(current, marker)
        coefficient_path, audit_path, manifest_path = _paths(config, spec)
        try:
            coefficient = fit_event(prepared, spec) if spec.specification == "event" else fit_dynamic(prepared, spec)
            coefficient["fit_id"] = spec.fit_id
            coefficient["analysis"] = spec.analysis
            coefficient["specification"] = spec.specification
            coefficient["outcome"] = spec.outcome
            coefficient["design"] = spec.design
            coefficient["clock"] = spec.clock
            coefficient["horizon_kind"] = spec.horizon_kind
            audit = horizon_support(prepared, spec)
            if audit.empty:
                audit = pd.DataFrame([{"fit_id": spec.fit_id, "rows": len(prepared), "products": prepared["hs10"].nunique(), "partners": prepared["partner_code"].nunique(), "clusters": prepared["cluster"].nunique()}])
            write_parquet(coefficient, coefficient_path, overwrite=True)
            write_parquet(audit, audit_path, overwrite=True)
            manifest = {
                "version": VERSION,
                "fit_id": spec.fit_id,
                "source_hash": source_hash,
                "policy_hash": policy_hash,
                "estimator_fingerprint": estimator_fingerprint(),
                "specification_fingerprint": _hash_payload(asdict(spec)),
                "eligible_sample_hash": sample_hash,
                "treatment_hash": treatment_hash,
                "observations": int(coefficient["nobs"].iloc[0]),
                "horizons": int(coefficient["horizon"].nunique()),
                "coefficient_path": coefficient_path.resolve().relative_to(config.repo_root.resolve()).as_posix(),
                "sample_audit_path": audit_path.resolve().relative_to(config.repo_root.resolve()).as_posix(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            write_metadata_json(manifest_path, manifest)
            valid, reason = validate_checkpoint(config, spec, source_hash, policy_hash)
            if not valid:
                raise RuntimeError(f"checkpoint validation failed: {reason}")
            current.unlink(missing_ok=True)
            records.append({"fit_id": spec.fit_id, "status": "fitted"})
        except Exception as exc:
            failure = manifest_path.with_name("failure.json")
            write_metadata_json(failure, {**marker, "exception_type": type(exc).__name__, "exception_message": str(exc)})
            raise
        finally:
            del prepared
    return {"version": VERSION, "fits": records}


def _plot_one(config: PipelineConfig, coefficients: pd.DataFrame, analysis: str, specification: str, horizon_kind: str) -> Path:
    figure_clock = "common_feb" if specification == "event" else "legal_path"
    subset = coefficients.loc[(coefficients["analysis"] == analysis) & (coefficients["specification"] == specification) & (coefficients["horizon_kind"] == horizon_kind) & (coefficients["design"] == "bilateral") & (coefficients["clock"] == figure_clock)]
    if subset.empty:
        raise ValueError("No coefficients for requested main figure")
    fig, axes = plt.subplots(2, 2, figsize=(13, 8.5), sharex=True)
    for axis, outcome in zip(axes.flat, OUTCOMES):
        line = subset.loc[subset["outcome"] == outcome].sort_values("horizon")
        x = line["horizon"].to_numpy(float)
        estimate = line["estimate"].to_numpy(float)
        axis.fill_between(x, line["conf_low"].to_numpy(float), line["conf_high"].to_numpy(float), color="#2563eb", alpha=.16, linewidth=0)
        axis.plot(x, estimate, color="#2563eb", marker="o", markersize=3, linewidth=1.7)
        axis.axhline(0, color="0.3", linewidth=.7)
        axis.axvline(0, color="0.55", linewidth=.7, linestyle="--")
        axis.set_title(OUTCOME_LABELS[outcome])
        axis.grid(alpha=.2)
    post = int(subset["horizon"].max())
    fig.suptitle(f"2025 tariff episode: {analysis.replace('_', ' ').title()} {specification.title()} [-6,+{post}]")
    fig.tight_layout(rect=(0, 0, 1, .95))
    root = config.repo_root / "figs" / "extension_2025"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{analysis}_{specification}_{horizon_kind}.pdf"
    fig.savefig(path)
    plt.close(fig)
    return path


def finalize(config: PipelineConfig) -> dict[str, Any]:
    gate = preflight(config)
    if not gate["event_estimation_authorized"]:
        raise RuntimeError("Cannot finalize: independent 2025 policy gate failed")
    grid = fit_grid(gate["horizon_contract"]["latest_trade_period"])
    expected_ids = {spec.fit_id for spec in grid}
    checkpoint_root = config.processed_trade_dir / "regressions" / "extension_2025" / "checkpoints"
    discovered_ids: list[str] = []
    checkpoint_manifests = list(checkpoint_root.glob("*/manifest.json")) if checkpoint_root.exists() else []
    for manifest_path in checkpoint_manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("fit_id"):
            discovered_ids.append(str(manifest["fit_id"]))
    duplicate_ids = sorted({fit_id for fit_id in discovered_ids if discovered_ids.count(fit_id) > 1})
    extra_ids = sorted(set(discovered_ids).difference(expected_ids))
    if duplicate_ids or extra_ids:
        raise RuntimeError(f"Cannot finalize checkpoint namespace: duplicate={duplicate_ids}, extra={extra_ids}")
    frames = []
    missing = []
    panel_hash = sha256_file(config.processed_trade_dir / "extension_2025" / "final_event_panel.parquet")
    policy_hash = sha256_file(config.processed_tariff_dir / "extension_2025" / "final_tariff_panel.parquet")
    for spec in grid:
        valid, reason = validate_checkpoint(config, spec, panel_hash, policy_hash)
        if not valid:
            missing.append({"fit_id": spec.fit_id, "reason": reason})
        else:
            frames.append(read_table(_paths(config, spec)[0]))
    if missing:
        raise RuntimeError(f"Cannot finalize incomplete grid: {len(missing)} invalid fits")
    coefficients = pd.concat(frames, ignore_index=True)
    root = config.processed_trade_dir / "regressions" / "extension_2025"
    write_parquet(coefficients, root / "coefficients.parquet", overwrite=True)
    figures = [_plot_one(config, coefficients, analysis, specification, kind) for analysis in ANALYSES for specification in SPECS for kind in HORIZONS]
    manifest = {"version": VERSION, "status": "complete", "fits": len(grid), "horizon_contract": gate["horizon_contract"], "figures": [path.resolve().relative_to(config.repo_root.resolve()).as_posix() for path in figures]}
    write_metadata_json(root / "manifest.json", manifest)
    (root / "current_fit.json").unlink(missing_ok=True)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--analysis", choices=(*ANALYSES, "all"), default="all")
    parser.add_argument("--spec", choices=(*SPECS, "all"), default="all")
    parser.add_argument("--outcome", choices=(*OUTCOMES, "all"), default="all")
    parser.add_argument("--design", choices=(*DESIGNS, "all"), default="all")
    parser.add_argument("--horizon", choices=(*HORIZONS, "all"), default="all")
    parser.add_argument("--clock", choices=("common_feb", "first_increase", "legal_path", "all"), default="all")
    parser.add_argument("--only-fit")
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args(argv)
    config = PipelineConfig.default()
    gate = preflight(config)
    if args.preflight_only or not (args.run or args.finalize_only):
        print(json.dumps(gate, indent=2))
        return 0
    if args.run:
        grid = fit_grid(gate["horizon_contract"]["latest_trade_period"])
        filters = {"analysis": args.analysis, "specification": args.spec, "outcome": args.outcome, "design": args.design, "horizon_kind": args.horizon, "clock": args.clock}
        selected = [spec for spec in grid if all(value == "all" or getattr(spec, key) == value for key, value in filters.items())]
        if args.only_fit:
            selected = [spec for spec in selected if spec.fit_id == args.only_fit]
        print(json.dumps(run_fits(config, selected, resume=not args.no_resume), indent=2))
    if args.finalize_only:
        print(json.dumps(finalize(config), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
