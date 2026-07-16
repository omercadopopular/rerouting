"""Resumable package/common versus raw-outcome bridge regressions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import hashlib
import json

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
from .package_benchmark import OUTCOMES
from .trade_regressions import _prepare_dynamic, _prepare_event_study, _run_dynamic_one, _run_event_study_one


VERSION = "bridge_v1"
SOURCE_MODES = ("package_common_sample_anchor", "raw_outcomes_package_policy")
SPECS = ("event", "dynamic")


def bridge_root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample" / "bridge_resumable"
    path.mkdir(parents=True, exist_ok=True)
    return path


def expected_fit_ids(source_modes: tuple[str, ...] = SOURCE_MODES, specs: tuple[str, ...] = SPECS, outcomes: tuple[str, ...] = OUTCOMES) -> set[str]:
    return {f"{mode}|{spec}|{outcome}" for mode in source_modes for spec in specs for outcome in outcomes}


def _hash_keys(frame: pd.DataFrame) -> str:
    columns = [column for column in ("id", "cty_code", "hs10", "year", "month") if column in frame.columns]
    keys = frame[columns].drop_duplicates().sort_values(columns).reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(keys, index=False).values.tobytes()).hexdigest()


def _hash_treatment(frame: pd.DataFrame) -> str:
    columns = [column for column in ("id", "cty_code", "hs10", "year", "month", "m_status2", "m_effective_mdate2", "m_stattariff2") if column in frame.columns]
    values = frame[columns].sort_values(columns[:5]).reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(values, index=False).values.tobytes()).hexdigest()


def _fingerprint(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def _specification_hash(spec: str, outcome: str) -> str:
    payload = {"version": VERSION, "spec": spec, "outcome": outcome, "event_baseline": -6, "horizons": list(range(-6, 7)), "clusters": "hs8 + cty_code", "fixed_effects": "id + ct + ht" if spec == "event" else "ht + ct + cs"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _source_paths(config: PipelineConfig) -> dict[str, Path]:
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample"
    return {
        "package_common_sample_anchor": root / "package_common_sample_hs10fixed.parquet",
        "raw_outcomes_package_policy": root / "raw_outcomes_package_policy_hs10fixed.parquet",
    }


def _checkpoint_valid(directory: Path, fit_id: str, source_hash: str, sample_hash: str, treatment_hash: str, spec_hash: str) -> bool:
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        coefficient = read_table(directory / "coefficients.parquet")
        audit = read_table(directory / "sample_audit.parquet")
        horizon = "event_time" if "event_time" in coefficient.columns else "horizon"
        return (
            manifest.get("version") == VERSION
            and manifest.get("fit_id") == fit_id
            and manifest.get("source_hash") == source_hash
            and manifest.get("sample_hash") == sample_hash
            and manifest.get("treatment_hash") == treatment_hash
            and manifest.get("specification_hash") == spec_hash
            and len(coefficient) >= 13
            and coefficient[horizon].nunique() == 13
            and int(manifest.get("nobs", -1)) == int(audit["nobs"].iloc[0])
        )
    except Exception:
        return False


def _write_current_fit(root: Path, fit_id: str, rows: int, formula: str, fixed_effects: str, clusters: str) -> None:
    write_metadata_json(root / "current_fit.json", {"version": VERSION, "fit_id": fit_id, "rows": int(rows), "formula": formula, "fixed_effects": fixed_effects, "clusters": clusters, "started_at_utc": datetime.now(timezone.utc).isoformat()})


def _load_mode_frame(path: Path, spec: str) -> tuple[pd.DataFrame, str, str, str]:
    frame = read_table(path)
    prepared = _prepare_event_study("imports", frame) if spec == "event" else _prepare_dynamic("imports", frame)
    return prepared, _fingerprint(path), _hash_keys(prepared), _hash_treatment(prepared)


def run_bridge(config: PipelineConfig, *, source_modes: tuple[str, ...] = SOURCE_MODES, specs: tuple[str, ...] = SPECS, outcomes: tuple[str, ...] = OUTCOMES, only_fit: str | None = None, resume: bool = True, preflight_only: bool = False, finalize_only: bool = False) -> dict[str, Any]:
    root = bridge_root(config)
    paths = _source_paths(config)
    selected = expected_fit_ids(source_modes, specs, outcomes)
    if only_fit:
        selected = {only_fit}
    state: dict[str, Any] = {"version": VERSION, "expected_fit_ids": sorted(selected), "completed_fit_ids": [], "stale_fit_ids": [], "failed_fit_ids": []}
    frames: dict[tuple[str, str], tuple[pd.DataFrame, str, str, str]] = {}
    for mode in source_modes:
        for spec in specs:
            if mode not in paths or not paths[mode].exists():
                raise FileNotFoundError(f"Missing bridge source for {mode}: {paths.get(mode)}")
            frames[(mode, spec)] = _load_mode_frame(paths[mode], spec)
    if preflight_only:
        for fit_id in sorted(selected):
            mode, spec, outcome = fit_id.split("|")
            _, source_hash, sample_hash, treatment_hash = frames[(mode, spec)]
            directory = root / mode / spec / outcome
            if _checkpoint_valid(directory, fit_id, source_hash, sample_hash, _hash_treatment(frames[(mode, spec)][0]), _specification_hash(spec, outcome)):
                state["completed_fit_ids"].append(fit_id)
            elif (directory / "manifest.json").exists():
                state["stale_fit_ids"].append(fit_id)
        state["remaining_fit_ids"] = sorted(selected - set(state["completed_fit_ids"]))
        write_metadata_json(root / "progress.json", state)
        return state
    if not finalize_only:
        for fit_id in sorted(selected):
            mode, spec, outcome = fit_id.split("|")
            prepared, source_hash, sample_hash, treatment_hash = frames[(mode, spec)]
            directory = root / mode / spec / outcome
            directory.mkdir(parents=True, exist_ok=True)
            spec_hash = _specification_hash(spec, outcome)
            if resume and _checkpoint_valid(directory, fit_id, source_hash, sample_hash, treatment_hash, spec_hash):
                state["completed_fit_ids"].append(fit_id)
                continue
            _write_current_fit(root, fit_id, len(prepared), f"{outcome} ~ event_horizon" if spec == "event" else f"{outcome} ~ dynamic_horizon", "id + ct + ht" if spec == "event" else "ht + ct + cs", "hs8 + cty_code")
            try:
                result = _run_event_study_one(config, "imports", outcome, prepared, mode, str(paths[mode])) if spec == "event" else _run_dynamic_one(config, "imports", outcome, prepared, mode, str(paths[mode]))
                coefficient = result.frame
                write_parquet(coefficient, directory / "coefficients.parquet", overwrite=True)
                nobs = int(coefficient["nobs"].iloc[0])
                write_parquet(pd.DataFrame([{"fit_id": fit_id, "source_mode": mode, "spec": spec, "outcome": outcome, "nobs": nobs, "sample_hash": sample_hash, "treatment_hash": treatment_hash}]), directory / "sample_audit.parquet", overwrite=True)
                write_metadata_json(directory / "manifest.json", {"version": VERSION, "fit_id": fit_id, "source_mode": mode, "source_path": str(paths[mode]), "source_hash": source_hash, "sample_hash": sample_hash, "treatment_hash": treatment_hash, "specification_hash": spec_hash, "nobs": nobs, "status": "complete", "horizons": 13})
                if not _checkpoint_valid(directory, fit_id, source_hash, sample_hash, treatment_hash, spec_hash):
                    raise RuntimeError("bridge checkpoint failed post-write validation")
                state["completed_fit_ids"].append(fit_id)
                (root / "current_fit.json").unlink(missing_ok=True)
            except Exception as exc:
                write_metadata_json(root / "failure_manifest.json", {"version": VERSION, "fit_id": fit_id, "exception_type": type(exc).__name__, "exception_message": str(exc)})
                state["failed_fit_ids"].append(fit_id)
                raise
    return finalize_bridge(config, selected=selected, state=state)


def finalize_bridge(config: PipelineConfig, *, selected: set[str] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    root = bridge_root(config)
    selected = selected or expected_fit_ids()
    records: list[pd.DataFrame] = []
    audits: list[pd.DataFrame] = []
    provenance: list[dict[str, Any]] = []
    for fit_id in sorted(selected):
        mode, spec, outcome = fit_id.split("|")
        directory = root / mode / spec / outcome
        if not (directory / "coefficients.parquet").exists() or not (directory / "manifest.json").exists():
            continue
        coefficient = read_table(directory / "coefficients.parquet")
        coefficient["fit_id"] = fit_id
        coefficient["source_mode"] = mode
        records.append(coefficient)
        audit = read_table(directory / "sample_audit.parquet")
        audits.append(audit)
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        provenance.append(manifest)
    completed = {str(frame["fit_id"].iloc[0]) for frame in records}
    payload = dict(state or {})
    payload.update({"version": VERSION, "expected_fit_count": len(selected), "completed_fit_count": len(completed), "completed_fit_ids": sorted(completed), "remaining_fit_ids": sorted(selected - completed)})
    if completed != selected:
        payload["status"] = "partial"
        write_metadata_json(root / "progress.json", payload)
        return payload
    coefficients = pd.concat(records, ignore_index=True)
    sample_audit = pd.concat(audits, ignore_index=True)
    provenance_frame = pd.DataFrame(provenance)
    write_parquet(coefficients, root / "bridge_coefficients.parquet", overwrite=True)
    write_parquet(sample_audit, root / "bridge_sample_audit.parquet", overwrite=True)
    write_parquet(provenance_frame, root / "bridge_provenance.parquet", overwrite=True)
    comparisons: list[dict[str, Any]] = []
    for spec in SPECS:
        for outcome in OUTCOMES:
            left = coefficients.loc[(coefficients["source_mode"] == SOURCE_MODES[0]) & (coefficients["fit_id"].str.contains(f"|{spec}|{outcome}", regex=False))].copy()
            right = coefficients.loc[(coefficients["source_mode"] == SOURCE_MODES[1]) & (coefficients["fit_id"].str.contains(f"|{spec}|{outcome}", regex=False))].copy()
            horizon = "event_time" if "event_time" in left.columns else "horizon"
            merged = left.merge(right, on=[horizon], suffixes=("_package", "_raw"))
            differences = merged["estimate_package"] - merged["estimate_raw"]
            comparisons.append({"spec": spec, "outcome": outcome, "n_points": len(merged), "correlation": float(merged["estimate_package"].corr(merged["estimate_raw"])), "rmse": float(np.sqrt(np.mean(differences**2))), "max_abs_difference": float(differences.abs().max())})
    comparison_frame = pd.DataFrame(comparisons)
    write_parquet(comparison_frame, root / "bridge_comparison.parquet", overwrite=True)
    comparison_frame.to_csv(root / "bridge_comparison.csv", index=False)
    write_metadata_json(root / "bridge_gate.json", {"version": VERSION, "status": "diagnostic", "bridge_gate": "requires_registered_ci_and_sign_checks", "v5_ready": False})
    payload["status"] = "complete"
    write_metadata_json(root / "progress.json", payload)
    (root / "bridge_report.md").write_text("# Resumable bridge v1\n\nThis bridge is diagnostic and does not alter Section 301 policy semantics.\n", encoding="utf-8")
    (root / "current_fit.json").unlink(missing_ok=True)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-mode", choices=(*SOURCE_MODES, "all"), default="all")
    parser.add_argument("--spec", choices=(*SPECS, "all"), default="all")
    parser.add_argument("--outcome", choices=(*OUTCOMES, "all"), default="all")
    parser.add_argument("--only-fit")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args(argv)
    modes = SOURCE_MODES if args.source_mode == "all" else (args.source_mode,)
    specs = SPECS if args.spec == "all" else (args.spec,)
    outcomes = OUTCOMES if args.outcome == "all" else (args.outcome,)
    print(run_bridge(PipelineConfig.default(), source_modes=modes, specs=specs, outcomes=outcomes, only_fit=args.only_fit, resume=args.resume, preflight_only=args.preflight_only, finalize_only=args.finalize_only))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
