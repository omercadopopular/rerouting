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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
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
    cache_path = out_dir / "package_full_panel.parquet"
    meta_path = out_dir / "package_full_panel.metadata.json"
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
    rows = 0
    try:
        reader = pd.read_stata(source, columns=columns, convert_categoricals=False, iterator=True, chunksize=250_000)
        for chunk in reader:
            year = pd.to_numeric(chunk["year"], errors="coerce")
            month = pd.to_numeric(chunk["month"], errors="coerce")
            chunk = chunk.loc[(year >= 2017) & ((year < 2019) | ((year == 2019) & (month <= 4))) & (pd.to_numeric(chunk["cty_code"], errors="coerce") > 0)].copy()
            if chunk.empty:
                continue
            chunk["hs10"] = chunk["hs10"].astype("string").str.replace(r"\D", "", regex=True).str.zfill(10).str[-10:]
            chunk["cty_code"] = pd.to_numeric(chunk["cty_code"], errors="coerce").astype("Int64")
            chunk["year"] = year.loc[chunk.index].astype("Int64")
            chunk["month"] = month.loc[chunk.index].astype("Int64")
            for column in ("m_val", "m_q1", "m_p", "m_pduty", "m_stattariff2", "lm_p", "lm_pduty", "lm_q1", "lm_val"):
                if column in chunk.columns:
                    chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(cache_path, table.schema, compression="zstd")
            writer.write_table(table)
            rows += len(chunk)
    finally:
        if writer is not None:
            writer.close()
    if rows == 0:
        raise RuntimeError("Package benchmark cache is empty after paper-window filtering")
    metadata = {
        "version": "v5",
        "source_mode": "package_full_benchmark",
        "source_path": _repo_relative(config, source),
        "source_fingerprint": source_hash,
        "cache_path": _repo_relative(config, cache_path),
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
    event_frame = _prepare_event_study("imports", frame)
    dynamic_frame = _prepare_dynamic("imports", frame)
    requested_specs = {config.regression_spec} if config.regression_spec in {"event", "dynamic"} else {"event", "dynamic"}
    requested_outcomes = (config.regression_outcome,) if config.regression_outcome in OUTCOMES else OUTCOMES
    event_rows: list[pd.DataFrame] = []
    dynamic_rows: list[pd.DataFrame] = []
    fit_audit: list[dict[str, Any]] = []
    for outcome in requested_outcomes:
        event_checkpoint = out_dir / "checkpoints" / "event" / outcome
        dynamic_checkpoint = out_dir / "checkpoints" / "dynamic" / outcome
        event_checkpoint.mkdir(parents=True, exist_ok=True)
        dynamic_checkpoint.mkdir(parents=True, exist_ok=True)
        event_path = event_checkpoint / "coefficients.parquet"
        dynamic_path = dynamic_checkpoint / "coefficients.parquet"
        if "event" not in requested_specs:
            event_result = pd.DataFrame()
        elif event_path.exists() and not config.overwrite:
            event_result = read_table(event_path)
        else:
            event_result = _run_event_study_one(config, "imports", outcome, event_frame, "package_full_benchmark", _repo_relative(config, cache_path)).frame
            write_parquet(event_result, event_path, overwrite=True)
        if not event_result.empty:
            write_metadata_json(event_checkpoint / "manifest.json", {
                "version": "v5", "fit_id": f"imports|event|{outcome}",
                "source_mode": "package_full_benchmark", "source_path": _repo_relative(config, cache_path),
                "source_fingerprint": _fingerprint(cache_path), "code_fingerprint": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "specification": "event: id + ct + ht; cluster hs8 + cty_code; baseline -6",
                "outcome": outcome, "observation_count": int(event_result["nobs"].iloc[0]),
            })
        if "dynamic" not in requested_specs:
            dynamic_result = pd.DataFrame()
        elif dynamic_path.exists() and not config.overwrite:
            dynamic_result = read_table(dynamic_path)
        else:
            dynamic_result = _run_dynamic_one(config, "imports", outcome, dynamic_frame, "package_full_benchmark", _repo_relative(config, cache_path)).frame
            write_parquet(dynamic_result, dynamic_path, overwrite=True)
        if not dynamic_result.empty:
            write_metadata_json(dynamic_checkpoint / "manifest.json", {
                "version": "v5", "fit_id": f"imports|dynamic|{outcome}",
                "source_mode": "package_full_benchmark", "source_path": _repo_relative(config, cache_path),
                "source_fingerprint": _fingerprint(cache_path), "code_fingerprint": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "specification": "dynamic: ht + ct + cs; cluster hs8 + cty_code",
                "outcome": outcome, "observation_count": int(dynamic_result["nobs"].iloc[0]),
            })
        event_rows.append(event_result)
        dynamic_rows.append(dynamic_result)
        fit_audit.extend([
            {"source_mode": "package_full_benchmark", "spec": "event", "outcome": outcome, "rows": int(event_result["nobs"].iloc[0]) if not event_result.empty else 0, "checkpoint": _repo_relative(config, event_path)},
            {"source_mode": "package_full_benchmark", "spec": "dynamic", "outcome": outcome, "rows": int(dynamic_result["nobs"].iloc[0]) if not dynamic_result.empty else 0, "checkpoint": _repo_relative(config, dynamic_path)},
        ])
    event = pd.concat(event_rows, ignore_index=True) if event_rows else pd.DataFrame()
    dynamic = pd.concat(dynamic_rows, ignore_index=True) if dynamic_rows else pd.DataFrame()
    event_path = out_dir / "package_full_event_coefficients.parquet"
    dynamic_path = out_dir / "package_full_dynamic_coefficients.parquet"
    if not event.empty:
        write_parquet(event, event_path, overwrite=True)
    if not dynamic.empty:
        write_parquet(dynamic, dynamic_path, overwrite=True)
    keys = frame[[c for c in ("id", "cty_code", "hs10", "year", "month") if c in frame.columns]].drop_duplicates()
    sample_hash = hashlib.sha256(pd.util.hash_pandas_object(keys, index=False).values.tobytes()).hexdigest()
    sample_audit = pd.DataFrame(fit_audit)
    sample_audit["package_rows"] = int(len(frame))
    sample_audit["sample_hash"] = sample_hash
    sample_audit["start_period"] = PAPER_START_PERIOD
    sample_audit["end_period"] = PAPER_END_PERIOD
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
    }
    write_metadata_json(out_dir / "package_full_manifest.json", manifest)
    (out_dir / "package_benchmark_report.md").write_text(
        "# Package-full benchmark v5\n\n"
        "This benchmark uses only the authors' package estimation data; the raw Census panel is not joined.\n\n"
        f"- observations: {len(frame):,}\n- sample hash: `{sample_hash}`\n- PDF comparison: pending local comparison artifact\n",
        encoding="utf-8",
    )
    return {"event": str(event_path), "dynamic": str(dynamic_path), "manifest": str(out_dir / "package_full_manifest.json"), "rows": int(len(frame))}


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
    common_path = out_dir / "package_common_sample_panel.parquet"
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
    finally:
        con.close()
    manifest = {
        "version": "v5",
        "source_mode": "package_common_sample_anchor",
        "package_path": _repo_relative(config, package_path),
        "raw_path": _repo_relative(config, raw_path),
        "common_panel_path": _repo_relative(config, common_path),
        "rows": rows,
        "status": "sample_materialized_regressions_pending",
    }
    write_metadata_json(out_dir / "package_common_sample_manifest.json", manifest)
    return manifest
