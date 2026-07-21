"""Resumable Section 301 policy-substitution regressions and plots.

The estimation sample is the union of package, paper-compatible, and final-legal
Section 301 product scope, excluding products carrying another package import-
policy family.  Raw outcomes are held fixed.  The package policy is an explicitly
labelled anchor; the paper-compatible schedule is a frozen historical
reconciliation, while the final-legal schedule remains independent.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import gc
import hashlib
import inspect
import json
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
from .package_benchmark import OUTCOMES
from .policy_replication_v2 import (
    PACKAGE_ANCHOR_MODE,
    POLICY_SOURCE_MODE_LEGAL,
    POLICY_SOURCE_MODE_PAPER,
    VERSION as POLICY_VERSION,
    artifact_root as policy_root,
)
from .trade_regressions import _prepare_dynamic, _prepare_event_study, _run_dynamic_one, _run_event_study_one


VERSION = "section301_policy_substitution_regressions_v2"
SOURCE_MODES = (PACKAGE_ANCHOR_MODE, POLICY_SOURCE_MODE_PAPER, POLICY_SOURCE_MODE_LEGAL)
SPECS = ("event", "dynamic")
CURVE_THRESHOLDS = {
    "correlation": 0.95,
    "rmse": 1.25,
    "max_abs_difference": 2.50,
    "post_treatment_sign_agreement": 0.50,
}
OUTCOME_LABELS = {"val": "Import value", "q1": "Quantity", "p": "Pre-duty price", "pduty": "Duty-inclusive price"}
PROJECT_COLUMNS = (
    "id", "cty_code", "hs10", "hs8", "year", "month", "mdate",
    "m_effective_mdate2", "m_stattariff2", "m_status2", "m_ess", "naics_str",
    "m_val", "m_q1", "m_p", "m_pduty",
)


def regression_root(config: PipelineConfig) -> Path:
    path = policy_root(config) / "regressions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def source_paths(config: PipelineConfig) -> dict[str, Path]:
    root = policy_root(config) / "panels"
    return {mode: root / f"{mode}.parquet" for mode in SOURCE_MODES}


def expected_fit_ids() -> set[str]:
    return {f"{mode}|{spec}|{outcome}" for mode in SOURCE_MODES for spec in SPECS for outcome in OUTCOMES}


def clone_source_fit_id(fit_id: str) -> str | None:
    # Paper-compatible and final-legal product scopes now differ.  Earlier v2
    # diagnostics cloned legal dynamic fits from the paper-calendar fit because
    # both modes shared one product map.  That shortcut is no longer valid.
    return None


def expected_estimator_fit_ids() -> set[str]:
    return {fit_id for fit_id in expected_fit_ids() if clone_source_fit_id(fit_id) is None}


def estimator_fingerprint() -> str:
    payload = {
        "version": VERSION,
        "prepare_event": inspect.getsource(_prepare_event_study),
        "prepare_dynamic": inspect.getsource(_prepare_dynamic),
        "run_event": inspect.getsource(_run_event_study_one),
        "run_dynamic": inspect.getsource(_run_dynamic_one),
        "outcomes": list(OUTCOMES),
        "event_fixed_effects": "id + ct + ht",
        "dynamic_fixed_effects": "ht + ct + cs",
        "clusters": "hs8 + cty_code",
        "baseline": -6,
        "horizons": list(range(-6, 7)),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _specification_fingerprint(spec: str, outcome: str) -> str:
    payload = {
        "version": VERSION,
        "spec": spec,
        "outcome": outcome,
        "fixed_effects": "id + ct + ht" if spec == "event" else "ht + ct + cs",
        "clusters": "hs8 + cty_code",
        "horizons": list(range(-6, 7)),
        "event_baseline": -6 if spec == "event" else None,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _hash_frame(frame: pd.DataFrame, columns: list[str]) -> str:
    use = [column for column in columns if column in frame.columns]
    values = frame[use].sort_values(use[: min(5, len(use))]).reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(values, index=False).values.tobytes()).hexdigest()


def _effective_sample(frame: pd.DataFrame, spec: str, outcome: str) -> pd.DataFrame:
    if spec == "event":
        return frame.loc[pd.to_numeric(frame[f"m_{outcome}"], errors="coerce").gt(0)]
    return frame.loc[frame[f"dl_{outcome}"].notna() & frame["x"].notna()]


def _prepare(path: Path, spec: str) -> tuple[pd.DataFrame, str, str, str, str]:
    frame = read_table(path, columns=list(PROJECT_COLUMNS))
    prepared = _prepare_event_study("imports", frame) if spec == "event" else _prepare_dynamic("imports", frame, package_logs=False)
    key_hash = _hash_frame(prepared, ["id", "cty_code", "hs10", "year", "month"])
    treatment_hash = _hash_frame(prepared, ["id", "cty_code", "hs10", "year", "month", "m_status2", "m_effective_mdate2", "m_stattariff2"])
    design_hash = _hash_frame(prepared, ["id", "cty_code", "hs10", "year", "month", "event_time", "T", "x", *[f"F{i}x" for i in range(1, 7)], *[f"L{i}x" for i in range(1, 7)]])
    return prepared, sha256_file(path), key_hash, treatment_hash, design_hash


def _checkpoint_dir(config: PipelineConfig, fit_id: str) -> Path:
    mode, spec, outcome = fit_id.split("|", 2)
    path = regression_root(config) / "checkpoints" / mode / spec / outcome
    path.mkdir(parents=True, exist_ok=True)
    return path


def _checkpoint_valid_once(
    directory: Path,
    fit_id: str,
    *,
    source_hash: str,
    estimator_hash: str,
    specification_hash: str,
    key_hash: str,
    treatment_hash: str,
    design_hash: str,
    sample_hash: str | None = None,
) -> bool:
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        coefficient = read_table(directory / "coefficients.parquet")
        audit = read_table(directory / "sample_audit.parquet")
        horizon = "event_time" if manifest["specification"] == "event" else "horizon"
        checks = (
            manifest.get("version") == VERSION,
            manifest.get("fit_id") == fit_id,
            manifest.get("source_hash") == source_hash,
            manifest.get("estimator_fingerprint") == estimator_hash,
            manifest.get("specification_fingerprint") == specification_hash,
            manifest.get("key_hash") == key_hash,
            manifest.get("treatment_hash") == treatment_hash,
            manifest.get("design_hash") == design_hash,
            manifest.get("status") in {"complete", "clone"},
            len(coefficient) == 13,
            coefficient[horizon].nunique() == 13,
            set(coefficient[horizon].astype(int)) == set(range(-6, 7)),
            int(manifest.get("nobs", -1)) == int(audit["nobs"].iloc[0]),
            {"estimate", "std_error", "conf_low", "conf_high"}.issubset(coefficient.columns),
        )
        if sample_hash is not None:
            checks += (manifest.get("sample_hash") == sample_hash,)
        if manifest.get("status") == "clone":
            source_fit_id = clone_source_fit_id(fit_id)
            checks += (manifest.get("source_fit_id") == source_fit_id,)
            if source_fit_id:
                source_mode, source_spec, source_outcome = source_fit_id.split("|", 2)
                checkpoint_root = directory.parents[2]
                source_path = checkpoint_root / source_mode / source_spec / source_outcome / "coefficients.parquet"
                if not source_path.exists():
                    return False
                source_coefficient = read_table(source_path)
                checks += (
                    manifest.get("source_scientific_coefficient_hash") == _scientific_coefficient_hash(source_coefficient),
                    _scientific_coefficient_hash(coefficient) == _scientific_coefficient_hash(source_coefficient),
                )
        return all(checks)
    except Exception:
        return False


def _checkpoint_valid(
    directory: Path,
    fit_id: str,
    *,
    source_hash: str,
    estimator_hash: str,
    specification_hash: str,
    key_hash: str,
    treatment_hash: str,
    design_hash: str,
    sample_hash: str | None = None,
) -> bool:
    """Validate a checkpoint, tolerating brief OneDrive read locks.

    A false result is repeated as well as an exception because a synchronizer
    can briefly expose a manifest before its neighboring Parquet footer is
    readable.  Scientific hash mismatches remain false on all attempts.
    """
    for attempt in range(3):
        valid = _checkpoint_valid_once(
            directory,
            fit_id,
            source_hash=source_hash,
            estimator_hash=estimator_hash,
            specification_hash=specification_hash,
            key_hash=key_hash,
            treatment_hash=treatment_hash,
            design_hash=design_hash,
            sample_hash=sample_hash,
        )
        if valid:
            return True
        if attempt < 2:
            time.sleep(0.25 * (attempt + 1))
    return False


def _current_fit(config: PipelineConfig, fit_id: str, prepared: pd.DataFrame, hashes: dict[str, str]) -> None:
    mode, spec, outcome = fit_id.split("|", 2)
    write_metadata_json(
        regression_root(config) / "current_fit.json",
        {
            "version": VERSION,
            "fit_id": fit_id,
            "source_mode": mode,
            "specification": spec,
            "outcome": outcome,
            "row_count": int(len(prepared)),
            "estimated_memory_bytes": int(prepared.memory_usage(deep=True).sum()),
            "formula": f"log({outcome}) ~ event indicators | id + ct + ht" if spec == "event" else f"D log({outcome}) ~ six leads/current/six lags | ht + ct + cs",
            "fixed_effects": "id + ct + ht" if spec == "event" else "ht + ct + cs",
            "clusters": "hs8 + cty_code",
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            **hashes,
        },
    )


def _write_checkpoint(
    config: PipelineConfig,
    fit_id: str,
    coefficient: pd.DataFrame,
    *,
    hashes: dict[str, str],
    sample_hash: str,
    nobs: int,
    status: str,
    source_fit_id: str | None = None,
    source_scientific_coefficient_hash: str | None = None,
) -> None:
    mode, spec, outcome = fit_id.split("|", 2)
    directory = _checkpoint_dir(config, fit_id)
    frame = coefficient.copy()
    frame["fit_id"] = fit_id
    frame["source_mode"] = mode
    frame["input_path"] = source_paths(config)[mode].relative_to(config.repo_root).as_posix()
    write_parquet(frame, directory / "coefficients.parquet", overwrite=True)
    audit = pd.DataFrame(
        [{"fit_id": fit_id, "source_mode": mode, "specification": spec, "outcome": outcome, "nobs": int(nobs), "sample_hash": sample_hash, "key_hash": hashes["key_hash"], "treatment_hash": hashes["treatment_hash"], "design_hash": hashes["design_hash"]}]
    )
    write_parquet(audit, directory / "sample_audit.parquet", overwrite=True)
    payload = {
        "version": VERSION,
        "fit_id": fit_id,
        "source_mode": mode,
        "specification": spec,
        "outcome": outcome,
        "source_path": _relative(config, source_paths(config)[mode]),
        **hashes,
        "sample_hash": sample_hash,
        "nobs": int(nobs),
        "status": status,
        "source_fit_id": source_fit_id,
        "source_scientific_coefficient_hash": source_scientific_coefficient_hash,
        "horizon_count": 13,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(directory / "manifest.json", payload)


def _scientific_coefficient_hash(frame: pd.DataFrame) -> str:
    columns = [column for column in ("event_time", "horizon", "estimate", "std_error", "conf_low", "conf_high", "nobs") if column in frame.columns]
    scientific = frame[columns].copy()
    sort_column = "event_time" if "event_time" in columns else "horizon"
    scientific = scientific.sort_values(sort_column).reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(scientific, index=False).values.tobytes()).hexdigest()


def _materialize_clone(config: PipelineConfig, fit_id: str, prepared: pd.DataFrame, hashes: dict[str, str]) -> None:
    source_fit = clone_source_fit_id(fit_id)
    if source_fit is None:
        raise ValueError(f"Not a clone fit: {fit_id}")
    source_directory = _checkpoint_dir(config, source_fit)
    if not (source_directory / "manifest.json").exists():
        raise FileNotFoundError(f"Clone source checkpoint is missing: {source_fit}")
    source_manifest = json.loads((source_directory / "manifest.json").read_text(encoding="utf-8"))
    if source_manifest.get("design_hash") != hashes["design_hash"]:
        raise ValueError(f"Clone design differs from source fit: {fit_id} <- {source_fit}")
    coefficient = read_table(source_directory / "coefficients.parquet")
    outcome = fit_id.rsplit("|", 1)[-1]
    sample = _effective_sample(prepared, "dynamic", outcome)
    sample_hash = _hash_frame(sample, ["id", "cty_code", "hs10", "year", "month"])
    _write_checkpoint(
        config,
        fit_id,
        coefficient,
        hashes=hashes,
        sample_hash=sample_hash,
        nobs=int(source_manifest["nobs"]),
        status="clone",
        source_fit_id=source_fit,
        source_scientific_coefficient_hash=_scientific_coefficient_hash(coefficient),
    )


def run_policy_regressions(
    config: PipelineConfig,
    *,
    source_modes: tuple[str, ...] = SOURCE_MODES,
    specs: tuple[str, ...] = SPECS,
    outcomes: tuple[str, ...] = OUTCOMES,
    only_fit: str | None = None,
    resume: bool = True,
    preflight_only: bool = False,
    finalize_only: bool = False,
) -> dict[str, Any]:
    selected = {f"{mode}|{spec}|{outcome}" for mode in source_modes for spec in specs for outcome in outcomes}
    if only_fit:
        mode, spec, outcome = only_fit.split("|", 2)
        source_modes, specs, outcomes, selected = (mode,), (spec,), (outcome,), {only_fit}
    paper_gate = json.loads((policy_root(config) / "paper_compatibility_variable_gate.json").read_text(encoding="utf-8"))
    event_encoding_gate = json.loads((policy_root(config) / "paper_compatibility_event_encoding_gate.json").read_text(encoding="utf-8"))
    legal_gate = json.loads((policy_root(config) / "section301_variable_gate.json").read_text(encoding="utf-8"))
    if POLICY_SOURCE_MODE_PAPER in source_modes and not paper_gate.get("all_checks_pass"):
        raise RuntimeError("Paper-compatible regressions are blocked because their registered variable gate failed")
    if POLICY_SOURCE_MODE_PAPER in source_modes and not event_encoding_gate.get("all_checks_pass"):
        raise RuntimeError("Paper-compatible regressions are blocked because Stata partner/event encoding does not match")
    if POLICY_SOURCE_MODE_LEGAL in source_modes and not legal_gate.get("all_checks_pass"):
        raise RuntimeError("Final-legal diagnostic regressions are blocked because the independent legal variable gate failed")
    all_expected = expected_fit_ids()
    state: dict[str, Any] = {
        "version": VERSION,
        "expected_fit_ids": sorted(all_expected),
        "expected_estimator_fit_ids": sorted(expected_estimator_fit_ids()),
        "requested_fit_ids": sorted(selected),
        "completed_fit_ids": [],
        "stale_fit_ids": [],
        "failed_fit_ids": [],
    }
    if finalize_only:
        return finalize_policy_regressions(config, state=state)
    paths = source_paths(config)
    estimator_hash = estimator_fingerprint()
    for mode in source_modes:
        for spec in specs:
            path = paths[mode]
            if not path.exists():
                raise FileNotFoundError(path)
            prepared, source_hash, key_hash, treatment_hash, design_hash = _prepare(path, spec)
            base_hashes = {
                "source_hash": source_hash,
                "estimator_fingerprint": estimator_hash,
                "key_hash": key_hash,
                "treatment_hash": treatment_hash,
                "design_hash": design_hash,
            }
            for outcome in outcomes:
                fit_id = f"{mode}|{spec}|{outcome}"
                if fit_id not in selected:
                    continue
                hashes = {**base_hashes, "specification_fingerprint": _specification_fingerprint(spec, outcome)}
                sample = _effective_sample(prepared, spec, outcome)
                sample_hash = _hash_frame(sample, ["id", "cty_code", "hs10", "year", "month"])
                directory = _checkpoint_dir(config, fit_id)
                if resume and _checkpoint_valid(
                    directory,
                    fit_id,
                    source_hash=hashes["source_hash"],
                    estimator_hash=hashes["estimator_fingerprint"],
                    specification_hash=hashes["specification_fingerprint"],
                    key_hash=hashes["key_hash"],
                    treatment_hash=hashes["treatment_hash"],
                    design_hash=hashes["design_hash"],
                    sample_hash=sample_hash,
                ):
                    state["completed_fit_ids"].append(fit_id)
                    continue
                if (directory / "manifest.json").exists():
                    state["stale_fit_ids"].append(fit_id)
                if preflight_only:
                    continue
                _current_fit(config, fit_id, prepared, hashes)
                try:
                    if clone_source_fit_id(fit_id):
                        _materialize_clone(config, fit_id, prepared, hashes)
                    else:
                        result = _run_event_study_one(config, "imports", outcome, prepared, mode, _relative(config, path)) if spec == "event" else _run_dynamic_one(config, "imports", outcome, prepared, mode, _relative(config, path))
                        _write_checkpoint(config, fit_id, result.frame, hashes=hashes, sample_hash=sample_hash, nobs=result.nobs, status="complete")
                    if not _checkpoint_valid(
                        directory,
                        fit_id,
                        source_hash=hashes["source_hash"],
                        estimator_hash=hashes["estimator_fingerprint"],
                        specification_hash=hashes["specification_fingerprint"],
                        key_hash=hashes["key_hash"],
                        treatment_hash=hashes["treatment_hash"],
                        design_hash=hashes["design_hash"],
                        sample_hash=sample_hash,
                    ):
                        raise RuntimeError(f"Post-write checkpoint validation failed: {fit_id}")
                    state["completed_fit_ids"].append(fit_id)
                    (regression_root(config) / "current_fit.json").unlink(missing_ok=True)
                    state["remaining_requested_fit_ids"] = sorted(selected - set(state["completed_fit_ids"]))
                    write_metadata_json(regression_root(config) / "progress.json", state)
                except Exception as exc:
                    failure = regression_root(config) / "failures" / f"{mode}__{spec}__{outcome}.json"
                    write_metadata_json(failure, {"version": VERSION, "fit_id": fit_id, "exception_type": type(exc).__name__, "exception_message": str(exc), "failed_at_utc": datetime.now(timezone.utc).isoformat()})
                    state["failed_fit_ids"].append(fit_id)
                    write_metadata_json(regression_root(config) / "progress.json", state)
                    raise
            del prepared
            gc.collect()
    if preflight_only:
        state["remaining_requested_fit_ids"] = sorted(selected - set(state["completed_fit_ids"]))
        state["status"] = "complete" if not state["remaining_requested_fit_ids"] else "partial"
        write_metadata_json(regression_root(config) / "progress.json", state)
        return state
    return finalize_policy_regressions(config, state=state)


def _curve_metrics(left: pd.DataFrame, right: pd.DataFrame, horizon: str) -> dict[str, Any]:
    merged = left.merge(right, on=horizon, suffixes=("_anchor", "_independent"), validate="one_to_one")
    difference = merged["estimate_anchor"] - merged["estimate_independent"]
    post = merged.loc[merged[horizon].ge(0)]
    sign = float((np.sign(post["estimate_anchor"]) == np.sign(post["estimate_independent"])).mean())
    baseline = merged[horizon].eq(-6)
    interval_left = np.maximum(merged.loc[~baseline, "conf_low_anchor"], merged.loc[~baseline, "conf_low_independent"])
    interval_right = np.minimum(merged.loc[~baseline, "conf_high_anchor"], merged.loc[~baseline, "conf_high_independent"])
    intersection = np.maximum(0.0, interval_right - interval_left)
    union = np.maximum(merged.loc[~baseline, "conf_high_anchor"], merged.loc[~baseline, "conf_high_independent"]) - np.minimum(merged.loc[~baseline, "conf_low_anchor"], merged.loc[~baseline, "conf_low_independent"])
    ci_overlap = float(np.divide(intersection, union, out=np.ones_like(intersection, dtype=float), where=union.gt(0)).mean())
    return {
        "aligned_horizons": int(len(merged)),
        "correlation": float(merged["estimate_anchor"].corr(merged["estimate_independent"])),
        "rmse": float(np.sqrt(np.mean(difference**2))),
        "max_abs_difference": float(difference.abs().max()),
        "post_treatment_sign_agreement": sign,
        "ci_overlap_diagnostic": ci_overlap,
    }


def registered_paper_curve_gate(comparison: pd.DataFrame) -> bool:
    registered = comparison.loc[comparison["registered_gate_member"].astype(bool)]
    if "published_comparison_eligible" in comparison.columns:
        registered = registered.loc[registered["published_comparison_eligible"].astype(bool)]
    if "comparison_role" in comparison.columns:
        registered = registered.loc[registered["comparison_role"].eq("registered_historical_replication_gate")]
    return bool(
        len(registered) == len(SPECS) * len(OUTCOMES)
        and registered["point_estimate_thresholds_passed"].astype(bool).all()
    )


def write_event_timing_propagation_diagnostic(config: PipelineConfig) -> dict[str, Any]:
    """Record how product-scope differences propagate into untreated timing.

    The Stata event design assigns untreated products the earliest treated date
    in NAICS4, then NAICS3, then NAICS2.  A small product-scope mismatch can
    therefore change hundreds of thousands of control-row event dates.  This
    artifact makes that propagation explicit rather than attributing it to the
    outcome construction.
    """
    paths = source_paths(config)
    anchor, *_ = _prepare(paths[PACKAGE_ANCHOR_MODE], "event")
    keys = ["id", "cty_code", "hs10", "year", "month"]
    anchor_view = anchor[keys + ["naics4", "T", "d_index", "event_time"]].rename(
        columns={"T": "anchor_T", "d_index": "anchor_d_index", "event_time": "anchor_event_time"}
    )
    details: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for mode in (POLICY_SOURCE_MODE_PAPER, POLICY_SOURCE_MODE_LEGAL):
        candidate, *_ = _prepare(paths[mode], "event")
        candidate_view = candidate[keys + ["T", "d_index", "event_time"]].rename(
            columns={"T": "candidate_T", "d_index": "candidate_d_index", "event_time": "candidate_event_time"}
        )
        merged = anchor_view.merge(candidate_view, on=keys, how="outer", indicator="key_presence", validate="one_to_one")
        merged["comparison_mode"] = mode
        merged["treatment_match"] = merged["anchor_T"].eq(merged["candidate_T"])
        merged["assigned_month_match"] = merged["anchor_d_index"].eq(merged["candidate_d_index"])
        merged["event_time_match"] = merged["anchor_event_time"].eq(merged["candidate_event_time"])
        merged["assigned_month_gap"] = pd.to_numeric(merged["candidate_d_index"], errors="coerce") - pd.to_numeric(merged["anchor_d_index"], errors="coerce")
        shared = merged["key_presence"].eq("both")
        summary_rows.append(
            {
                "summary_type": "overall",
                "comparison_mode": mode,
                "group": "all",
                "rows": int(len(merged)),
                "shared_rows": int(shared.sum()),
                "treatment_match": float(merged.loc[shared, "treatment_match"].mean()),
                "assigned_month_match": float(merged.loc[shared, "assigned_month_match"].mean()),
                "event_time_match": float(merged.loc[shared, "event_time_match"].mean()),
            }
        )
        for gap, count in merged.loc[shared, "assigned_month_gap"].value_counts(dropna=False).sort_index().items():
            summary_rows.append({"summary_type": "assigned_month_gap", "comparison_mode": mode, "group": str(gap), "rows": int(count)})
        naics = merged.loc[shared & ~merged["assigned_month_match"]].groupby("naics4", dropna=False).size().sort_values(ascending=False).head(100)
        for code, count in naics.items():
            summary_rows.append({"summary_type": "top_naics4_timing_mismatch", "comparison_mode": mode, "group": str(code), "rows": int(count)})
        details.append(merged)
        del candidate, candidate_view, merged
        gc.collect()
    detail = pd.concat(details, ignore_index=True, sort=False)
    root = regression_root(config)
    detail_path = root / "section301_event_timing_propagation.parquet"
    write_parquet(detail, detail_path, overwrite=True)
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(root / "section301_event_timing_propagation_summary.csv", index=False)
    write_metadata_json(
        detail_path.with_suffix(".metadata.json"),
        {
            "version": VERSION,
            "artifact_category": "detailed_diagnostic",
            "canonical_relative_path": _relative(config, detail_path),
            "row_count": int(len(detail)),
            "key_columns": ["comparison_mode", *keys],
            "compression": "zstd",
            "source_fingerprints": {mode: sha256_file(path) for mode, path in paths.items()},
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    overall = summary.loc[summary["summary_type"].eq("overall")].to_dict(orient="records")
    del anchor, anchor_view, detail, details
    gc.collect()
    return {"detail_path": _relative(config, detail_path), "summary_path": _relative(config, root / "section301_event_timing_propagation_summary.csv"), "overall": overall}


def _plot_curves(config: PipelineConfig, coefficients: pd.DataFrame) -> dict[str, str]:
    root = regression_root(config) / "figures"
    root.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    styles = {
        PACKAGE_ANCHOR_MODE: ("#1f4e79", "-", "Package-policy anchor"),
        POLICY_SOURCE_MODE_PAPER: ("#d35f00", "--", "Paper-compatible reconstructed schedule"),
        POLICY_SOURCE_MODE_LEGAL: ("#2b8c6b", ":", "Independent final-legal schedule"),
    }
    for spec in SPECS:
        fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.2), sharex=True)
        horizon = "event_time" if spec == "event" else "horizon"
        for axis, outcome in zip(axes.flat, OUTCOMES):
            for mode in SOURCE_MODES:
                fit_id = f"{mode}|{spec}|{outcome}"
                frame = coefficients.loc[coefficients["fit_id"].eq(fit_id)].sort_values(horizon)
                color, linestyle, label = styles[mode]
                x = frame[horizon].astype(float).to_numpy()
                if mode == PACKAGE_ANCHOR_MODE:
                    axis.fill_between(x, frame["conf_low"].astype(float).to_numpy(), frame["conf_high"].astype(float).to_numpy(), color=color, alpha=0.12, linewidth=0)
                axis.plot(x, frame["estimate"].astype(float).to_numpy(), color=color, linestyle=linestyle, marker="o" if mode == PACKAGE_ANCHOR_MODE else None, markersize=3, linewidth=1.9, label=label)
            axis.axhline(0, color="0.25", linewidth=0.7)
            axis.axvline(0, color="0.55", linewidth=0.8, linestyle="--")
            axis.set_title(OUTCOME_LABELS[outcome])
            axis.set_xticks(range(-6, 7, 2))
            axis.grid(axis="y", color="0.9", linewidth=0.6)
            axis.set_ylabel("Coefficient (log points)")
        for axis in axes[-1, :]:
            axis.set_xlabel("Event horizon (months)")
        handles, labels = axes[0, 0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.95), fontsize=9)
        title = "Section 301 treatment substitution: event-study responses" if spec == "event" else "Section 301 treatment substitution: dynamic tariff responses"
        fig.suptitle(title, fontsize=14, y=0.995)
        fig.text(0.5, 0.012, "Same raw Census outcomes/product union. Paper-compatible uses a documented historical reconciliation; final-legal is diagnostic. Not the full PDF sample.", ha="center", fontsize=8.2, color="0.35")
        fig.tight_layout(rect=(0.02, 0.045, 0.98, 0.89))
        path = root / f"section301_policy_substitution_{spec}.png"
        fig.savefig(path, dpi=220, bbox_inches="tight")
        fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
        plt.close(fig)
        outputs[f"{spec}_png"] = _relative(config, path)
        outputs[f"{spec}_pdf"] = _relative(config, path.with_suffix(".pdf"))
    return outputs


def _plot_four_line_replication(config: PipelineConfig, policy_coefficients: pd.DataFrame) -> dict[str, str]:
    """Plot the four evidence layers needed to assess historical replication."""
    benchmark = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    reference = read_table(benchmark / "reference" / "package_pdf_reference.parquet")
    root = regression_root(config) / "figures"
    root.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        package_path = benchmark / f"package_full_{spec}_coefficients.parquet"
        package = read_table(package_path)
        fig, axes = plt.subplots(2, 2, figsize=(12.8, 8.4), sharex=True)
        for axis, outcome in zip(axes.flat, OUTCOMES):
            pdf = reference.loc[(reference["flow"].eq("imports")) & (reference["spec"].eq(spec)) & (reference["outcome"].eq(outcome))].sort_values("horizon")
            package_line = package.loc[package["outcome"].eq(outcome)].sort_values(horizon)
            raw_anchor = policy_coefficients.loc[policy_coefficients["fit_id"].eq(f"{PACKAGE_ANCHOR_MODE}|{spec}|{outcome}")].sort_values(horizon)
            paper = policy_coefficients.loc[policy_coefficients["fit_id"].eq(f"{POLICY_SOURCE_MODE_PAPER}|{spec}|{outcome}")].sort_values(horizon)
            axis.plot(pdf["horizon"], pdf["reference_value"], color="#111111", linestyle=(0, (4, 2, 1, 2)), marker="s", markersize=3, linewidth=1.6, label="Replication-package PDF")
            axis.plot(package_line[horizon], package_line["estimate"], color="#1f4e79", linestyle="-", marker="o", markersize=3, linewidth=1.8, label="Python estimator, package data")
            axis.plot(raw_anchor[horizon], raw_anchor["estimate"], color="#d35f00", linestyle="--", linewidth=1.9, label="Raw Census outcomes, package policy")
            axis.plot(paper[horizon], paper["estimate"], color="#2b8c6b", linestyle=":", linewidth=2.1, label="Raw Census outcomes, paper-compatible policy")
            axis.axhline(0, color="0.3", linewidth=0.7)
            axis.axvline(0, color="0.55", linewidth=0.8, linestyle="--")
            axis.set_title(OUTCOME_LABELS[outcome])
            axis.set_xticks(range(-6, 7, 2))
            axis.grid(axis="y", color="0.9", linewidth=0.6)
            axis.set_ylabel("Coefficient (log points)")
        for axis in axes[-1, :]:
            axis.set_xlabel("Event horizon (months)")
        handles, labels = axes[0, 0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.955), fontsize=9)
        title = "Historical import replication: event study" if spec == "event" else "Historical import replication: dynamic tariff response"
        fig.suptitle(title, fontsize=14, y=0.997)
        fig.text(0.5, 0.012, "PDF/package use the authors' full sample; raw lines use the fixed raw product-union sample. Point estimates are shown for visual diagnosis.", ha="center", fontsize=8.4, color="0.35")
        fig.tight_layout(rect=(0.02, 0.045, 0.98, 0.89))
        path = root / f"historical_replication_four_line_{spec}.png"
        fig.savefig(path, dpi=220, bbox_inches="tight")
        fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
        plt.close(fig)
        outputs[f"four_line_{spec}_png"] = _relative(config, path)
        outputs[f"four_line_{spec}_pdf"] = _relative(config, path.with_suffix(".pdf"))
    return outputs


def finalize_policy_regressions(config: PipelineConfig, *, state: dict[str, Any] | None = None) -> dict[str, Any]:
    root = regression_root(config)
    expected = expected_fit_ids()
    records: list[pd.DataFrame] = []
    audits: list[pd.DataFrame] = []
    manifests: list[dict[str, Any]] = []
    invalid: list[str] = []
    paths = source_paths(config)
    estimator_hash = estimator_fingerprint()
    for mode in SOURCE_MODES:
        for spec in SPECS:
            prepared, source_hash, key_hash, treatment_hash, design_hash = _prepare(paths[mode], spec)
            try:
                for outcome in OUTCOMES:
                    fit_id = f"{mode}|{spec}|{outcome}"
                    directory = _checkpoint_dir(config, fit_id)
                    sample_hash = _hash_frame(_effective_sample(prepared, spec, outcome), ["id", "cty_code", "hs10", "year", "month"])
                    hashes = {"source_hash": source_hash, "estimator_hash": estimator_hash, "specification_hash": _specification_fingerprint(spec, outcome), "key_hash": key_hash, "treatment_hash": treatment_hash, "design_hash": design_hash, "sample_hash": sample_hash}
                    valid = _checkpoint_valid(directory, fit_id, source_hash=hashes["source_hash"], estimator_hash=hashes["estimator_hash"], specification_hash=hashes["specification_hash"], key_hash=hashes["key_hash"], treatment_hash=hashes["treatment_hash"], design_hash=hashes["design_hash"], sample_hash=hashes["sample_hash"])
                    if not valid:
                        invalid.append(fit_id)
                        continue
                    coefficient = read_table(directory / "coefficients.parquet")
                    coefficient["fit_id"] = fit_id
                    coefficient["source_mode"] = mode
                    records.append(coefficient)
                    audits.append(read_table(directory / "sample_audit.parquet"))
                    manifests.append(json.loads((directory / "manifest.json").read_text(encoding="utf-8")))
            finally:
                del prepared
                gc.collect()
    completed = {str(frame["fit_id"].iloc[0]) for frame in records}
    progress = dict(state or {})
    progress.update({"version": VERSION, "expected_fit_count": len(expected), "expected_estimator_fit_count": len(expected_estimator_fit_ids()), "completed_fit_count": len(completed), "completed_fit_ids": sorted(completed), "remaining_fit_ids": sorted(expected - completed), "invalid_fit_ids": sorted(invalid)})
    if completed != expected or invalid:
        progress["status"] = "partial"
        write_metadata_json(root / "progress.json", progress)
        return progress
    coefficients = pd.concat(records, ignore_index=True, sort=False)
    sample_audit = pd.concat(audits, ignore_index=True, sort=False)
    provenance = pd.DataFrame(manifests)
    comparisons: list[dict[str, Any]] = []
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        for outcome in OUTCOMES:
            anchor = coefficients.loc[coefficients["fit_id"].eq(f"{PACKAGE_ANCHOR_MODE}|{spec}|{outcome}")]
            for mode in (POLICY_SOURCE_MODE_PAPER, POLICY_SOURCE_MODE_LEGAL):
                independent = coefficients.loc[coefficients["fit_id"].eq(f"{mode}|{spec}|{outcome}")]
                metrics = _curve_metrics(anchor, independent, horizon)
                failures = []
                for metric, threshold in CURVE_THRESHOLDS.items():
                    value = metrics[metric]
                    if metric in {"rmse", "max_abs_difference"}:
                        if value > threshold:
                            failures.append(metric)
                    elif value < threshold:
                        failures.append(metric)
                role = "registered_historical_replication_gate" if mode == POLICY_SOURCE_MODE_PAPER else "legal_calendar_diagnostic"
                comparisons.append({
                    "specification": spec,
                    "outcome": outcome,
                    "anchor_mode": PACKAGE_ANCHOR_MODE,
                    "comparison_mode": mode,
                    "comparison_role": role,
                    "original_calendar": "paper_compatible_section301_month" if mode == POLICY_SOURCE_MODE_PAPER else "paper_compatible_section301_month",
                    "reconstructed_calendar": "paper_compatible_section301_month" if mode == POLICY_SOURCE_MODE_PAPER else "independent_legal_effective_month",
                    "published_comparison_eligible": mode == POLICY_SOURCE_MODE_PAPER,
                    **metrics,
                    "failed_metrics": ";".join(failures),
                    "point_estimate_thresholds_passed": not failures,
                    "registered_gate_member": mode == POLICY_SOURCE_MODE_PAPER,
                })
    comparison = pd.DataFrame(comparisons)
    paper_curve_gate_passed = registered_paper_curve_gate(comparison)
    paper_variable_gate = json.loads((policy_root(config) / "paper_compatibility_variable_gate.json").read_text(encoding="utf-8"))
    event_encoding_gate = json.loads((policy_root(config) / "paper_compatibility_event_encoding_gate.json").read_text(encoding="utf-8"))
    legal_variable_gate = json.loads((policy_root(config) / "section301_variable_gate.json").read_text(encoding="utf-8"))
    historical_methodology_locked = bool(paper_curve_gate_passed and paper_variable_gate.get("all_checks_pass") and event_encoding_gate.get("all_checks_pass"))
    write_parquet(coefficients, root / "section301_policy_coefficients.parquet", overwrite=True)
    write_parquet(sample_audit, root / "section301_policy_sample_audit.parquet", overwrite=True)
    write_parquet(provenance, root / "section301_policy_provenance.parquet", overwrite=True)
    write_parquet(comparison, root / "section301_policy_curve_comparison.parquet", overwrite=True)
    comparison.to_csv(root / "section301_policy_curve_comparison.csv", index=False)
    propagation = write_event_timing_propagation_diagnostic(config)
    figures = {**_plot_curves(config, coefficients), **_plot_four_line_replication(config, coefficients)}
    gate = {
        "version": VERSION,
        "status": "historical_methodology_locked" if historical_methodology_locked else "historical_methodology_not_locked",
        "paper_compatibility_variable_gate_passed": bool(paper_variable_gate.get("all_checks_pass")),
        "paper_compatibility_event_encoding_gate_passed": bool(event_encoding_gate.get("all_checks_pass")),
        "paper_compatible_point_estimate_curve_gate_passed": paper_curve_gate_passed,
        "independent_legal_variable_gate_passed": bool(legal_variable_gate.get("all_checks_pass")),
        "legal_calendar_curve_comparisons_are_diagnostic": True,
        "ci_overlap_is_diagnostic": True,
        "thresholds": CURVE_THRESHOLDS,
        "historical_policy_methodology_locked": historical_methodology_locked,
        "paper_compatible_scope_uses_validation_derived_reconciliation": True,
        "paper_compatible_scope_is_independent_legal_evidence": False,
        "independent_legal_forward_ready": False,
        "event_2025_ready": False,
        "full_paper_replication_claimed": False,
        "scope": "Section 301 product-union substitution; Section 201/232 excluded",
        "figures": figures,
        "event_timing_propagation": propagation,
    }
    write_metadata_json(root / "section301_policy_curve_gate.json", gate)
    report = [
        "# Section 301 independent-policy substitution regressions",
        "",
        f"Historical paper-compatible point-estimate curve gate: **{'passed' if paper_curve_gate_passed else 'failed'}**.",
        f"Historical variable-and-curve methodology lock: **{'passed' if historical_methodology_locked else 'not passed'}**.",
        "",
        "These regressions hold raw Census outcomes and the product-union sample fixed. The anchor uses the authors' package variables. The paper-compatible line uses source-vintage HTS schedules plus a transparent frozen reconciliation inferred from the historical package; it is the registered historical replication object, not independent legal evidence. The final-legal line uses the final official scope and legal dates and is diagnostic because it is not expected to reproduce the authors' nearest-month event calendar. Confidence-interval overlap is reported but is not part of the point-estimate gate.",
        "",
        "| Specification | Outcome | Comparison | Role | Correlation | RMSE | Max gap | Sign agreement | CI overlap (diagnostic) | Thresholds passed |",
        "|---|---|---|---|---:|---:|---:|---:|---:|:---:|",
    ]
    for row in comparisons:
        report.append(f"| {row['specification']} | {row['outcome']} | {row['comparison_mode']} | {row['comparison_role']} | {row['correlation']:.6f} | {row['rmse']:.6f} | {row['max_abs_difference']:.6f} | {row['post_treatment_sign_agreement']:.3f} | {row['ci_overlap_diagnostic']:.3f} | {'yes' if row['point_estimate_thresholds_passed'] else 'no'} |")
    (root / "section301_policy_regression_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    progress.update({"status": "complete", "curve_gate": gate, "figures": figures})
    write_metadata_json(root / "progress.json", progress)
    (root / "current_fit.json").unlink(missing_ok=True)
    return progress


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-mode", choices=(*SOURCE_MODES, "all"), default="all")
    parser.add_argument("--spec", choices=(*SPECS, "all"), default="all")
    parser.add_argument("--outcome", choices=(*OUTCOMES, "all"), default="all")
    parser.add_argument("--only-fit")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args()
    modes = SOURCE_MODES if args.source_mode == "all" else (args.source_mode,)
    specs = SPECS if args.spec == "all" else (args.spec,)
    outcomes = OUTCOMES if args.outcome == "all" else (args.outcome,)
    result = run_policy_regressions(PipelineConfig.default(), source_modes=modes, specs=specs, outcomes=outcomes, only_fit=args.only_fit, resume=not args.no_resume, preflight_only=args.preflight_only, finalize_only=args.finalize_only)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
