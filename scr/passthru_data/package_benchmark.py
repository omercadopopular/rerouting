"""Package-only benchmark for the Fajgelbaum import regressions.

This module deliberately never joins the package estimation file to a raw
Census panel.  It is therefore the gold estimator benchmark; raw/common
sample comparisons belong to the Section 301 sensitivity path.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import hashlib
import json
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, sha256_file, write_metadata_json, write_parquet
from .trade_regression_common import PAPER_END_PERIOD, PAPER_START_PERIOD
from .trade_regressions import _prepare_dynamic, _prepare_event_study, _run_dynamic_one, _run_event_study_one


OUTCOMES = ("val", "q1", "p", "pduty")

PACKAGE_COLUMNS = [
    "id", "cty_code", "cty_name", "hs10", "hs8", "hs6", "hs4", "hs2",
    "year", "month", "mdate", "m_val", "m_q1", "m_p", "m_pduty",
    "m_effective_mdate2", "m_stattariff2", "m_status2", "m_ess", "naics_str",
    "lm_p", "lm_pduty", "lm_q1", "lm_val",
]


def package_benchmark_dir(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_pdf_reference(config: PipelineConfig) -> dict[str, Any]:
    """Freeze the existing local vector-extraction reference without re-reading PDFs."""
    source = config.verification_dir / "trade_chart_value_comparison.csv"
    reference_dir = package_benchmark_dir(config) / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    required = {("event", outcome) for outcome in OUTCOMES} | {("dynamic", outcome) for outcome in OUTCOMES}
    status = "blocked"
    reason = None
    if not source.exists():
        reason = "missing existing local PDF comparison table"
        reference = pd.DataFrame()
    else:
        raw = pd.read_csv(source)
        columns = ["flow", "spec", "outcome", "horizon", "reference_value", "reference_conf_low", "reference_conf_high", "reference_source"]
        reference = raw.loc[(raw["flow"] == "imports") & raw["spec"].isin(["event", "dynamic"]), columns].copy()
        counts = reference.groupby(["spec", "outcome"], dropna=False).size().to_dict()
        observed = set(counts)
        if observed != required or any(counts[key] != 13 for key in required):
            reason = f"incomplete import PDF reference grid: observed={sorted(observed)}, counts={counts}"
        elif reference[["flow", "spec", "outcome", "horizon"]].duplicated().any():
            reason = "duplicate PDF reference keys"
        else:
            status = "passed"
    reference_path = reference_dir / "package_pdf_reference.parquet"
    write_parquet(reference, reference_path, overwrite=True)
    pdf_paths = {
        "fig_02": config.fajgelbaum_root / "results" / "main" / "fig_02.pdf",
        "fig_04a": config.fajgelbaum_root / "results" / "main" / "fig_04a.pdf",
    }
    manifest = {
        "version": "v5",
        "status": status,
        "reason": reason,
        "source_comparison_table": _repo_relative(config, source),
        "source_comparison_table_sha256": _fingerprint(source),
        "pdf_sha256": {name: _fingerprint(path) for name, path in pdf_paths.items()},
        "reference_path": _repo_relative(config, reference_path),
        "reference_rows": int(len(reference)),
        "horizons_per_import_spec_outcome": 13 if status == "passed" else None,
        "extraction_provenance": "existing_local_vector_extraction",
        "reextracted_in_this_invocation": False,
        "missing_export_fig_04b": True,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(reference_dir / "package_pdf_reference_manifest.json", manifest)
    return manifest


def finalize_package_pdf_comparison(config: PipelineConfig) -> dict[str, Any]:
    """Compare the complete package fit grid to the frozen local reference."""
    out_dir = package_benchmark_dir(config)
    reference_path = out_dir / "reference" / "package_pdf_reference.parquet"
    event_path = out_dir / "package_full_event_coefficients.parquet"
    dynamic_path = out_dir / "package_full_dynamic_coefficients.parquet"
    if not reference_path.exists() or not event_path.exists() or not dynamic_path.exists():
        return {"status": "blocked", "reason": "missing reference or complete package coefficient artifact"}
    reference = read_table(reference_path)
    event = read_table(event_path).rename(columns={"event_time": "horizon"})
    dynamic = read_table(dynamic_path)
    generated = pd.concat([event, dynamic], ignore_index=True)
    merged = reference.merge(generated, on=["flow", "spec", "outcome", "horizon"], how="left", validate="one_to_one")
    merged["difference"] = merged["estimate"] - merged["reference_value"]
    merged["abs_difference"] = merged["difference"].abs()
    comparison_path = out_dir / "package_pdf_comparison.parquet"
    write_parquet(merged, comparison_path, overwrite=True)
    merged.to_csv(out_dir / "package_pdf_comparison.csv", index=False)
    summary = merged.groupby(["spec", "outcome"], dropna=False).agg(
        n_points=("horizon", "size"), missing_estimates=("estimate", lambda values: int(values.isna().sum())),
        max_abs_difference=("abs_difference", "max"), mean_abs_difference=("abs_difference", "mean"),
    ).reset_index()
    required = {(spec, outcome) for spec in ("event", "dynamic") for outcome in OUTCOMES}
    observed = set(zip(summary["spec"], summary["outcome"]))
    gate_passed = observed == required and not summary["missing_estimates"].any() and (summary["n_points"] == 13).all() and (summary["max_abs_difference"] <= 1.10).all()
    payload = {"status": "passed" if gate_passed else "failed", "max_abs_difference": float(merged["abs_difference"].max()), "required_threshold": 1.10, "comparisons": summary.to_dict(orient="records"), "reference_path": _repo_relative(config, reference_path), "comparison_path": _repo_relative(config, comparison_path), "missing_export_fig_04b": True}
    write_metadata_json(out_dir / "package_pdf_comparison_manifest.json", payload)
    manifest_path = out_dir / "package_full_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["package_pdf_gate"] = payload["status"]
        manifest["package_pdf_comparison_manifest"] = _repo_relative(config, out_dir / "package_pdf_comparison_manifest.json")
        write_metadata_json(manifest_path, manifest)
    (out_dir / "package_benchmark_report.md").write_text(
        "# Package-full benchmark v5\n\n"
        "The package-only benchmark uses the authors' estimation data and does not join raw Census keys.\n\n"
        f"- PDF gate: **{payload['status']}**\n- Maximum absolute difference: `{payload['max_abs_difference']:.6f}` log points\n"
        "- Export Figure 4b: unavailable in the local package and excluded from this gate.\n",
        encoding="utf-8",
    )
    return payload


def build_raw_outcomes_package_policy(config: PipelineConfig) -> dict[str, Any]:
    """Join raw outcomes to package treatment/design columns on corrected keys."""
    import duckdb
    package_path, _ = _package_cache(config, overwrite=False)
    raw_path = config.analysis_dir / "imports_hs10_raw_package_shocks.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw outcome/package-policy panel: {raw_path}")
    out_dir = package_benchmark_dir(config) / "common_sample"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "raw_outcomes_package_policy_hs10fixed.parquet"
    con = duckdb.connect(database=":memory:")
    try:
        query = """
            SELECT p.* EXCLUDE (m_val, m_q1, m_p, m_pduty),
                   r.m_val AS m_val, r.m_q1 AS m_q1, r.m_p AS m_p, r.m_pduty AS m_pduty
            FROM read_parquet(?) p
            INNER JOIN read_parquet(?) r USING (cty_code, hs10, year, month)
        """
        con.execute("COPY (" + query.replace("?", "'{}'", 2).format(str(package_path).replace("'", "''"), str(raw_path).replace("'", "''")) + ") TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(out_path)])
        rows = int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out_path)]).fetchone()[0])
    finally:
        con.close()
    manifest = {"version": "v5", "source_mode": "raw_outcomes_package_policy", "package_design_path": _repo_relative(config, package_path), "raw_outcome_path": _repo_relative(config, raw_path), "output_path": _repo_relative(config, out_path), "rows": rows, "status": "bridge_input"}
    write_metadata_json(out_dir / "raw_outcomes_package_policy_manifest.json", manifest)
    return manifest


def run_common_sample_bridge(config: PipelineConfig) -> dict[str, Any]:
    """Estimate package-common and raw-outcome/package-policy bridge curves."""
    package_path = package_benchmark_dir(config) / "cache" / "package_full_panel_hs10fixed.parquet"
    common_path = package_benchmark_dir(config) / "common_sample" / "package_common_sample_hs10fixed.parquet"
    raw_path = package_benchmark_dir(config) / "common_sample" / "raw_outcomes_package_policy_hs10fixed.parquet"
    if not package_path.exists() or not common_path.exists() or not raw_path.exists():
        raise FileNotFoundError("Bridge requires corrected package, common, and raw-outcome panels")
    outputs = package_benchmark_dir(config) / "common_sample" / "bridge"
    outputs.mkdir(parents=True, exist_ok=True)
    records: list[pd.DataFrame] = []
    for mode, path in (("package_common_sample_anchor", common_path), ("raw_outcomes_package_policy", raw_path)):
        frame = read_table(path)
        event_frame = _prepare_event_study("imports", frame)
        dynamic_frame = _prepare_dynamic("imports", frame)
        for outcome in OUTCOMES:
            event = _run_event_study_one(config, "imports", outcome, event_frame, mode, _repo_relative(config, path)).frame
            dynamic = _run_dynamic_one(config, "imports", outcome, dynamic_frame, mode, _repo_relative(config, path)).frame
            records.extend([event.rename(columns={"event_time": "horizon"}), dynamic])
            write_parquet(event, outputs / mode / "event" / f"{outcome}.parquet", overwrite=True)
            write_parquet(dynamic, outputs / mode / "dynamic" / f"{outcome}.parquet", overwrite=True)
    package_curves = pd.concat(records[:8], ignore_index=True)
    raw_curves = pd.concat(records[8:], ignore_index=True)
    comparison = package_curves.merge(raw_curves, on=["flow", "spec", "outcome", "event_time", "horizon"], how="outer", suffixes=("_package_common", "_raw_outcome")) if "event_time" in package_curves.columns else package_curves.merge(raw_curves, on=["flow", "spec", "outcome", "horizon"], how="outer", suffixes=("_package_common", "_raw_outcome"))
    write_parquet(comparison, outputs / "package_common_vs_raw_outcome_comparison.parquet", overwrite=True)
    metrics = _compute_bridge_metrics(config, outputs)
    return {"status": "complete", "package_common_path": str(common_path), "raw_outcome_path": str(raw_path), "comparison_path": str(outputs / "package_common_vs_raw_outcome_comparison.parquet"), "metrics_path": str(outputs / "bridge_metrics.parquet"), "gate": metrics["raw_bridge_gate"]}


def _compute_bridge_metrics(config: PipelineConfig, outputs: Path) -> dict[str, Any]:
    import numpy as np
    rows: list[dict[str, Any]] = []
    for spec in ("event", "dynamic"):
        full_path = package_benchmark_dir(config) / f"package_full_{spec}_coefficients.parquet"
        full = read_table(full_path)
        if spec == "event":
            full = full.rename(columns={"event_time": "horizon"})
        for outcome in OUTCOMES:
            package = read_table(outputs / "package_common_sample_anchor" / spec / f"{outcome}.parquet")
            raw = read_table(outputs / "raw_outcomes_package_policy" / spec / f"{outcome}.parquet")
            package = package.rename(columns={"event_time": "horizon"})
            raw = raw.rename(columns={"event_time": "horizon"})
            full_one = full.loc[full["outcome"] == outcome]
            def metrics(left: pd.DataFrame, right: pd.DataFrame, comparison: str) -> dict[str, Any]:
                merged = left.merge(right, on=["flow", "spec", "outcome", "horizon"], suffixes=("_left", "_right"), validate="one_to_one")
                valid = merged["estimate_left"].notna() & merged["estimate_right"].notna()
                x, y = merged.loc[valid, "estimate_left"], merged.loc[valid, "estimate_right"]
                overlap = ((np.minimum(merged["conf_high_left"], merged["conf_high_right"]) - np.maximum(merged["conf_low_left"], merged["conf_low_right"])).clip(lower=0) / (np.maximum(merged["conf_high_left"], merged["conf_high_right"]) - np.minimum(merged["conf_low_left"], merged["conf_low_right"])).replace(0, np.nan)).mean()
                post = merged["horizon"] >= 0
                return {"comparison": comparison, "spec": spec, "outcome": outcome, "n_points": int(valid.sum()), "correlation": float(x.corr(y)), "rmse": float(np.sqrt(np.mean((x-y)**2))), "max_abs_difference": float((x-y).abs().max()), "ci_overlap": float(overlap), "post_treatment_sign_agreement": float((np.sign(merged.loc[post, "estimate_left"]) == np.sign(merged.loc[post, "estimate_right"])).mean())}
            rows.append(metrics(full_one, package, "package_full_vs_package_common"))
            rows.append(metrics(package, raw, "package_common_vs_raw_outcomes_package_policy"))
    frame = pd.DataFrame(rows)
    write_parquet(frame, outputs / "bridge_metrics.parquet", overwrite=True)
    frame.to_csv(outputs / "bridge_metrics.csv", index=False)
    raw = frame.loc[frame["comparison"] == "package_common_vs_raw_outcomes_package_policy"]
    gate = bool((raw["correlation"] >= 0.95).all() and (raw["rmse"] <= 1.25).all() and (raw["max_abs_difference"] <= 2.50).all() and (raw["ci_overlap"] >= 0.80).all() and (raw["post_treatment_sign_agreement"] >= 0.5).all())
    payload = {"version": "v5", "raw_bridge_gate": "passed" if gate else "failed", "thresholds": {"correlation": 0.95, "rmse": 1.25, "max_abs_difference": 2.50, "ci_overlap": 0.80}, "metrics_path": _repo_relative(config, outputs / "bridge_metrics.parquet"), "v5_ready": False if not gate else None}
    write_metadata_json(outputs / "bridge_gate.json", payload)
    return payload


def _fingerprint(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def _repo_relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _package_cache(config: PipelineConfig, overwrite: bool = False) -> tuple[Path, dict[str, Any]]:
    """Build the package-only cache without depending on v4 directories."""
    source = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    out_dir = package_benchmark_dir(config) / "cache"
    cache_path = out_dir / "package_full_panel_hs10fixed.parquet"
    meta_path = out_dir / "package_full_panel_hs10fixed.metadata.json"
    stale_manifest_path = out_dir / "stale_artifacts.json"
    legacy_cache = out_dir / "package_full_panel.parquet"
    if legacy_cache.exists() and not stale_manifest_path.exists():
        write_metadata_json(stale_manifest_path, {
            "version": "v5",
            "status": "stale_not_resumable",
            "reason": "legacy package cache used punctuation stripping that shifted numeric HS10 codes",
            "normalization_example": {"input": "801001090.0", "old": "8010010900", "correct": "0801001090"},
            "artifacts": [
                {"path": _repo_relative(config, legacy_cache), "sha256": _fingerprint(legacy_cache)},
                {"path": _repo_relative(config, out_dir / "common_sample" / "package_common_sample_panel.parquet"), "sha256": _fingerprint(out_dir / "common_sample" / "package_common_sample_panel.parquet")},
            ],
            "downstream_outputs_not_resumable": ["package_full_event_coefficients.parquet", "package_full_dynamic_coefficients.parquet", "package_full_sample_audit.parquet"],
        })
    source_hash = _fingerprint(source)
    if cache_path.exists() and meta_path.exists() and not overwrite:
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("source_fingerprint") == source_hash:
                return cache_path, meta
        except (OSError, json.JSONDecodeError):
            pass
    if not source.exists():
        raise FileNotFoundError(f"Missing package benchmark DTA: {source}")
    out_dir.mkdir(parents=True, exist_ok=True)
    available = set(pd.read_stata(source, convert_categoricals=False, iterator=True, chunksize=1).__next__().columns)
    columns = [column for column in PACKAGE_COLUMNS if column in available]
    writer: pq.ParquetWriter | None = None
    temporary = out_dir / f".{cache_path.name}.{uuid.uuid4().hex}.tmp"
    rows = 0
    try:
        reader = pd.read_stata(source, columns=columns, convert_categoricals=False, iterator=True, chunksize=250_000)
        for chunk in reader:
            year = pd.to_numeric(chunk["year"], errors="coerce")
            month = pd.to_numeric(chunk["month"], errors="coerce")
            chunk = chunk.loc[(year >= 2017) & ((year < 2019) | ((year == 2019) & (month <= 4))) & (pd.to_numeric(chunk["cty_code"], errors="coerce") > 0)].copy()
            if chunk.empty:
                continue
            chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
            chunk["cty_code"] = pd.to_numeric(chunk["cty_code"], errors="coerce").astype("Int64")
            chunk["year"] = year.loc[chunk.index].astype("Int64")
            chunk["month"] = month.loc[chunk.index].astype("Int64")
            for column in ("m_val", "m_q1", "m_p", "m_pduty", "m_stattariff2", "lm_p", "lm_pduty", "lm_q1", "lm_val"):
                if column in chunk.columns:
                    chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(temporary, table.schema, compression="zstd")
            writer.write_table(table)
            rows += len(chunk)
    finally:
        if writer is not None:
            writer.close()
            parquet = pq.ParquetFile(temporary)
            if parquet.metadata.num_rows != rows or str(parquet.metadata.row_group(0).column(0).compression).lower() != "zstd":
                raise RuntimeError("Corrected package cache failed Parquet validation")
            del parquet
            temporary.replace(cache_path)
        elif temporary.exists():
            temporary.unlink()
    if temporary.exists():
        temporary.unlink()
    if rows == 0:
        raise RuntimeError("Package benchmark cache is empty after paper-window filtering")
    metadata = {
        "version": "v5",
        "source_mode": "package_full_benchmark",
        "source_path": _repo_relative(config, source),
        "source_fingerprint": source_hash,
        "cache_path": _repo_relative(config, cache_path),
        "normalization": "shared_normalize_hs_code_v2",
        "rows": int(rows),
        "columns": columns,
        "period_min": PAPER_START_PERIOD,
        "period_max": PAPER_END_PERIOD,
    }
    write_metadata_json(meta_path, metadata)
    return cache_path, metadata


def run_package_benchmark(config: PipelineConfig) -> dict[str, Any]:
    """Run package-only import event and dynamic regressions.

    The package DTA is projected and cached by the existing chunked builder;
    no raw Census keys enter this function.
    """
    out_dir = package_benchmark_dir(config)
    cache_path, cache_meta = _package_cache(config, overwrite=config.overwrite)
    frame = read_table(cache_path)
    # These transformations are deliberately outside the outcome loop.  The
    # previous implementation rebuilt both multi-million-row designs four
    # times, exhausting memory before any checkpoint was written.
    requested_specs = {config.regression_spec} if config.regression_spec in {"event", "dynamic"} else {"event", "dynamic"}
    requested_outcomes = (config.regression_outcome,) if config.regression_outcome in OUTCOMES else OUTCOMES
    event_frame = _prepare_event_study("imports", frame) if "event" in requested_specs else None
    dynamic_frame = _prepare_dynamic("imports", frame) if "dynamic" in requested_specs else None
    code_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    event_sample_hash = hashlib.sha256(pd.util.hash_pandas_object(event_frame[["id", "cty_code", "hs10", "year", "month"]], index=False).values.tobytes()).hexdigest() if event_frame is not None else None
    dynamic_sample_hash = hashlib.sha256(pd.util.hash_pandas_object(dynamic_frame[["id", "cty_code", "hs10", "year", "month"]], index=False).values.tobytes()).hexdigest() if dynamic_frame is not None else None
    event_rows: list[pd.DataFrame] = []
    dynamic_rows: list[pd.DataFrame] = []
    fit_audit: list[dict[str, Any]] = []
    def checkpoint_valid(directory: Path, spec: str, outcome: str) -> bool:
        manifest_path = directory / "manifest.json"
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            return (
                payload.get("version") == "v5"
                and payload.get("fit_id") == f"imports|{spec}|{outcome}"
                and payload.get("source_fingerprint") == _fingerprint(cache_path)
                and payload.get("code_fingerprint") == code_hash
            )
        except (OSError, json.JSONDecodeError):
            return False
    for outcome in requested_outcomes:
        event_checkpoint = out_dir / "checkpoints" / "event" / outcome
        dynamic_checkpoint = out_dir / "checkpoints" / "dynamic" / outcome
        event_checkpoint.mkdir(parents=True, exist_ok=True)
        dynamic_checkpoint.mkdir(parents=True, exist_ok=True)
        event_path = event_checkpoint / "coefficients.parquet"
        dynamic_path = dynamic_checkpoint / "coefficients.parquet"
        if "event" not in requested_specs:
            event_result = pd.DataFrame()
        elif event_path.exists() and checkpoint_valid(event_checkpoint, "event", outcome) and not config.overwrite:
            event_result = read_table(event_path)
        else:
            event_result = _run_event_study_one(config, "imports", outcome, event_frame, "package_full_benchmark", _repo_relative(config, cache_path)).frame
            write_parquet(event_result, event_path, overwrite=True)
        if not event_result.empty:
            write_metadata_json(event_checkpoint / "manifest.json", {
                "version": "v5", "fit_id": f"imports|event|{outcome}",
                "source_mode": "package_full_benchmark", "source_path": _repo_relative(config, cache_path),
                "source_fingerprint": _fingerprint(cache_path), "code_fingerprint": code_hash,
                "specification": "event: id + ct + ht; cluster hs8 + cty_code; baseline -6",
                "outcome": outcome, "observation_count": int(event_result["nobs"].iloc[0]), "sample_hash": event_sample_hash,
            })
            write_parquet(pd.DataFrame([{"fit_id": f"imports|event|{outcome}", "source_mode": "package_full_benchmark", "spec": "event", "outcome": outcome, "nobs": int(event_result["nobs"].iloc[0]), "sample_hash": event_sample_hash}]), event_checkpoint / "sample_audit.parquet", overwrite=True)
        if "dynamic" not in requested_specs:
            dynamic_result = pd.DataFrame()
        elif dynamic_path.exists() and checkpoint_valid(dynamic_checkpoint, "dynamic", outcome) and not config.overwrite:
            dynamic_result = read_table(dynamic_path)
        else:
            dynamic_result = _run_dynamic_one(config, "imports", outcome, dynamic_frame, "package_full_benchmark", _repo_relative(config, cache_path)).frame
            write_parquet(dynamic_result, dynamic_path, overwrite=True)
        if not dynamic_result.empty:
            write_metadata_json(dynamic_checkpoint / "manifest.json", {
                "version": "v5", "fit_id": f"imports|dynamic|{outcome}",
                "source_mode": "package_full_benchmark", "source_path": _repo_relative(config, cache_path),
                "source_fingerprint": _fingerprint(cache_path), "code_fingerprint": code_hash,
                "specification": "dynamic: ht + ct + cs; cluster hs8 + cty_code",
                "outcome": outcome, "observation_count": int(dynamic_result["nobs"].iloc[0]), "sample_hash": dynamic_sample_hash,
            })
            write_parquet(pd.DataFrame([{"fit_id": f"imports|dynamic|{outcome}", "source_mode": "package_full_benchmark", "spec": "dynamic", "outcome": outcome, "nobs": int(dynamic_result["nobs"].iloc[0]), "sample_hash": dynamic_sample_hash}]), dynamic_checkpoint / "sample_audit.parquet", overwrite=True)
        if not event_result.empty:
            event_rows.append(event_result)
            fit_audit.append({"source_mode": "package_full_benchmark", "spec": "event", "outcome": outcome, "rows": int(event_result["nobs"].iloc[0]), "checkpoint": _repo_relative(config, event_path)})
        if not dynamic_result.empty:
            dynamic_rows.append(dynamic_result)
            fit_audit.append({"source_mode": "package_full_benchmark", "spec": "dynamic", "outcome": outcome, "rows": int(dynamic_result["nobs"].iloc[0]), "checkpoint": _repo_relative(config, dynamic_path)})
    event = pd.concat(event_rows, ignore_index=True) if event_rows else pd.DataFrame()
    dynamic = pd.concat(dynamic_rows, ignore_index=True) if dynamic_rows else pd.DataFrame()
    event_path = out_dir / "package_full_event_coefficients.parquet"
    dynamic_path = out_dir / "package_full_dynamic_coefficients.parquet"
    complete = len(fit_audit) == len(requested_specs) * len(requested_outcomes)
    if complete and not event.empty:
        write_parquet(event, event_path, overwrite=True)
    if complete and not dynamic.empty:
        write_parquet(dynamic, dynamic_path, overwrite=True)
    keys = frame[[c for c in ("id", "cty_code", "hs10", "year", "month") if c in frame.columns]].drop_duplicates()
    sample_hash = hashlib.sha256(pd.util.hash_pandas_object(keys, index=False).values.tobytes()).hexdigest()
    sample_audit = pd.DataFrame(fit_audit)
    sample_audit["package_rows"] = int(len(frame))
    sample_audit["sample_hash"] = sample_hash
    sample_audit["start_period"] = PAPER_START_PERIOD
    sample_audit["end_period"] = PAPER_END_PERIOD
    if complete:
        write_parquet(sample_audit, out_dir / "package_full_sample_audit.parquet", overwrite=True)
    manifest = {
        "version": "v5",
        "source_mode": "package_full_benchmark",
        "source_path": _repo_relative(config, config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"),
        "source_fingerprint": _fingerprint(config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"),
        "cache_path": _repo_relative(config, cache_path),
        "cache_fingerprint": _fingerprint(cache_path),
        "code_fingerprint": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "sample_hash": sample_hash,
        "observation_count": int(len(frame)),
        "fixed_effects": {"event": "id + ct + ht", "dynamic": "ht + ct + cs"},
        "clusters": "hs8 + cty_code",
        "event_baseline": -6,
        "outcomes": list(requested_outcomes),
        "specifications_attempted": sorted(requested_specs),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "package_pdf_gate": "pending comparison",
        "status": "complete" if complete and requested_specs == {"event", "dynamic"} and set(requested_outcomes) == set(OUTCOMES) else "partial",
        "completed_fit_count": len(fit_audit),
        "expected_fit_count": len(requested_specs) * len(requested_outcomes),
    }
    if not complete:
        write_metadata_json(out_dir / "package_full_partial_manifest.json", manifest)
        return {"status": "partial", "completed_fit_count": len(fit_audit), "expected_fit_count": len(requested_specs) * len(requested_outcomes), "manifest": str(out_dir / "package_full_partial_manifest.json")}
    write_metadata_json(out_dir / "package_full_manifest.json", manifest)
    (out_dir / "package_benchmark_report.md").write_text(
        "# Package-full benchmark v5\n\n"
        "This benchmark uses only the authors' package estimation data; the raw Census panel is not joined.\n\n"
        f"- observations: {len(frame):,}\n- sample hash: `{sample_hash}`\n- PDF comparison: pending local comparison artifact\n",
        encoding="utf-8",
    )
    return {"event": str(event_path), "dynamic": str(dynamic_path), "manifest": str(out_dir / "package_full_manifest.json"), "rows": int(len(frame)), "status": "complete"}


def run_package_common_sample_benchmark(config: PipelineConfig) -> dict[str, Any]:
    """Materialize the package/raw common sample for bridge estimation.

    This step intentionally materializes the sample and audit first; the
    regressions are run by the Section 301 bridge after the package-full gate
    has passed.  It never labels the common sample as the published benchmark.
    """
    import duckdb

    package_path, _ = _package_cache(config, overwrite=config.overwrite)
    raw_candidates = (
        config.analysis_dir / "us_products_partner_hs10_monthly_regression.parquet",
        config.analysis_dir / "us_products_partner_hs10_monthly.parquet",
    )
    raw_path = next((path for path in raw_candidates if path.exists()), None)
    if raw_path is None:
        raise FileNotFoundError("package_common_sample_anchor requires a local raw trade panel")
    out_dir = package_benchmark_dir(config) / "common_sample"
    out_dir.mkdir(parents=True, exist_ok=True)
    common_path = out_dir / "package_common_sample_hs10fixed.parquet"
    con = duckdb.connect(database=":memory:")
    try:
        query = """
            SELECT p.*
            FROM read_parquet(?) p
            INNER JOIN (
                SELECT DISTINCT cty_code, hs10, year, month
                FROM read_parquet(?)
                WHERE cty_code > 0 AND year >= 2017 AND (year < 2019 OR (year = 2019 AND month <= 4))
            ) r USING (cty_code, hs10, year, month)
        """
        con.execute("COPY (" + query.replace("?", "'{}'", 2).format(str(package_path).replace("'", "''"), str(raw_path).replace("'", "''")) + ") TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(common_path)])
        rows = int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(common_path)]).fetchone()[0])
    except Exception:
        con.close()
        raise
    # Record the staged key-loss decomposition without serializing the full
    # regression key set.  The hashes are over deterministically ordered keys.
    stages = []
    for stage, path in (("package_eligible", package_path), ("raw_eligible", raw_path), ("package_raw_common", common_path)):
        key_query = """SELECT count(*) AS rows,
            count(DISTINCT hs10) AS products,
            count(DISTINCT cty_code) AS countries,
            md5(string_agg(concat_ws('|', cast(cty_code AS varchar), hs10, cast(year AS varchar), cast(month AS varchar)), '|' ORDER BY cty_code, hs10, year, month)) AS key_hash
            FROM read_parquet(?)"""
        rows, products, countries, key_hash = con.execute(key_query, [str(path)]).fetchone()
        stages.append({"stage": stage, "rows": int(rows), "products": int(products), "countries": int(countries), "key_hash": key_hash})
    write_metadata_json(out_dir / "package_common_sample_loss_audit.json", {
        "version": "v5", "source_mode": "package_common_sample_anchor", "stages": stages,
        "normalization": "shared_normalize_hs_code_v2", "status": "sample_only",
    })
    con.close()
    manifest = {
        "version": "v5",
        "source_mode": "package_common_sample_anchor",
        "package_path": _repo_relative(config, package_path),
        "raw_path": _repo_relative(config, raw_path),
        "common_panel_path": _repo_relative(config, common_path),
        "rows": rows,
        "loss_audit_path": _repo_relative(config, out_dir / "package_common_sample_loss_audit.json"),
        "status": "sample_materialized_regressions_pending",
    }
    write_metadata_json(out_dir / "package_common_sample_manifest.json", manifest)
    return manifest
