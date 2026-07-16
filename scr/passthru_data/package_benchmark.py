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

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
from .section301_regression_sensitivity_v4 import _build_package_paper_window_cache
from .trade_regression_common import PAPER_END_PERIOD, PAPER_START_PERIOD
from .trade_regressions import _prepare_dynamic, _prepare_event_study, _run_dynamic_one, _run_event_study_one


OUTCOMES = ("val", "q1", "p", "pduty")


def package_benchmark_dir(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _fingerprint(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def run_package_benchmark(config: PipelineConfig) -> dict[str, Any]:
    """Run package-only import event and dynamic regressions.

    The package DTA is projected and cached by the existing chunked builder;
    no raw Census keys enter this function.
    """
    out_dir = package_benchmark_dir(config)
    cache_path, cache_meta = _build_package_paper_window_cache(config, overwrite=config.overwrite)
    frame = read_table(cache_path)
    event_rows: list[pd.DataFrame] = []
    dynamic_rows: list[pd.DataFrame] = []
    for outcome in OUTCOMES:
        event_frame = _prepare_event_study("imports", frame)
        event_rows.append(_run_event_study_one(config, "imports", outcome, event_frame, "package_full_benchmark", str(cache_path)).frame)
        dynamic_frame = _prepare_dynamic("imports", frame)
        dynamic_rows.append(_run_dynamic_one(config, "imports", outcome, dynamic_frame, "package_full_benchmark", str(cache_path)).frame)
    event = pd.concat(event_rows, ignore_index=True)
    dynamic = pd.concat(dynamic_rows, ignore_index=True)
    event_path = out_dir / "package_full_event_coefficients.parquet"
    dynamic_path = out_dir / "package_full_dynamic_coefficients.parquet"
    write_parquet(event, event_path, overwrite=True)
    write_parquet(dynamic, dynamic_path, overwrite=True)
    keys = frame[[c for c in ("id", "cty_code", "hs10", "year", "month") if c in frame.columns]].drop_duplicates()
    sample_hash = hashlib.sha256(pd.util.hash_pandas_object(keys, index=False).values.tobytes()).hexdigest()
    sample_audit = pd.DataFrame([{"source_mode": "package_full_benchmark", "rows": int(len(frame)), "sample_hash": sample_hash, "start_period": PAPER_START_PERIOD, "end_period": PAPER_END_PERIOD}])
    write_parquet(sample_audit, out_dir / "package_full_sample_audit.parquet", overwrite=True)
    manifest = {
        "version": "v5",
        "source_mode": "package_full_benchmark",
        "source_path": str(config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"),
        "source_fingerprint": _fingerprint(config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"),
        "cache_path": str(cache_path),
        "cache_fingerprint": _fingerprint(cache_path),
        "code_fingerprint": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "sample_hash": sample_hash,
        "observation_count": int(len(frame)),
        "fixed_effects": {"event": "id + ct + ht", "dynamic": "ht + ct + cs"},
        "clusters": "hs8 + cty_code",
        "event_baseline": -6,
        "outcomes": list(OUTCOMES),
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
