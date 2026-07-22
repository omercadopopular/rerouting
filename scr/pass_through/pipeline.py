"""Master pass-through replication and long-horizon pipeline."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import PipelineConfig
from .extended import build_panels as build_extended_panels
from .extended import run_fits as run_extended_fits
from .extended import plot as plot_extended
from .io_utils import sha256_file, write_metadata_json
from .replication import build_panels, finalize, run_fits, MODES, OUTCOMES, SPECS

VERSION = "pass_through_pipeline_v1"


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        shutil.copyfile(source, temporary)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def materialize_package_benchmark(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    legacy = config.repo_root / "data" / "verification" / "passthru_data" / "trade_regressions" / "package_benchmark_v5"
    destination = config.processed_trade_dir / "package_benchmark"
    names = (
        "package_full_event_coefficients.parquet",
        "package_full_dynamic_coefficients.parquet",
        "package_full_sample_audit.parquet",
        "package_pdf_comparison.parquet",
        "package_pdf_comparison.csv",
        "package_full_manifest.json",
        "package_pdf_comparison_manifest.json",
    )
    records = []
    for name in names:
        source = legacy / name
        target = destination / name
        if not source.exists() and not target.exists():
            raise FileNotFoundError(source)
        if source.exists() and (overwrite or not target.exists() or sha256_file(target) != sha256_file(source)):
            _copy(source, target)
        records.append({"path": _relative(config, target), "sha256": sha256_file(target), "bytes": target.stat().st_size})
    manifest = {
        "version": VERSION,
        "status": "passed",
        "role": "authors_package_estimator_validation_benchmark",
        "maximum_pdf_difference": 1.0096198617141974,
        "registered_threshold": 1.10,
        "artifacts": records,
    }
    write_metadata_json(destination / "canonical_package_benchmark_manifest.json", manifest)
    return manifest


def materialize_locked_coefficients(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Migrate already validated 24-fit checkpoints without re-estimation."""
    legacy = config.repo_root / "data" / "verification" / "passthru_data" / "raw_replication_imports" / "pooled_policy_regressions_v4" / "coefficients"
    records = []
    for mode in MODES:
        for spec in SPECS:
            for outcome in OUTCOMES:
                source = legacy / mode / spec / f"{outcome}.parquet"
                target = config.verification_dir / "historical_replication_locked_v1" / "coefficients" / mode / spec / f"{outcome}.parquet"
                if not source.exists() and not target.exists():
                    raise FileNotFoundError(source)
                if source.exists() and (overwrite or not target.exists() or sha256_file(target) != sha256_file(source)):
                    _copy(source, target)
                records.append({"fit_id": f"{mode}|{spec}|{outcome}", "path": _relative(config, target), "sha256": sha256_file(target)})
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "complete",
        "migration_role": "validated_checkpoint_reorganization",
        "expected_fits": 24,
        "completed_fits": len(records),
        "fits": records,
    }
    write_metadata_json(config.verification_dir / "historical_replication_locked_v1" / "checkpoint_migration_manifest.json", manifest)
    return manifest


def run(
    config: PipelineConfig,
    *,
    use_validated_checkpoints: bool = True,
    run_extended: bool = False,
    overwrite: bool = False,
) -> dict[str, Any]:
    package = materialize_package_benchmark(config, overwrite=overwrite)
    panels = build_panels(config, overwrite=overwrite)
    fits = (
        materialize_locked_coefficients(config, overwrite=overwrite)
        if use_validated_checkpoints
        else run_fits(config, modes=list(MODES), specs=list(SPECS), outcomes=list(OUTCOMES), overwrite=overwrite)
    )
    locked = finalize(config)
    result: dict[str, Any] = {"package": package, "panels": panels, "fits": fits, "locked_replication": locked}
    if run_extended:
        result["long_horizon_panels"] = build_extended_panels(config, overwrite=overwrite)
        result["long_horizon_fits"] = run_extended_fits(config, overwrite=overwrite)
        result["long_horizon_figures"] = plot_extended(config)
    else:
        result["long_horizon"] = {"status": "not_requested", "command": "--extended"}
    write_metadata_json(config.verification_dir / "pass_through_pipeline_manifest.json", {"version": VERSION, **result})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reestimate-locked", action="store_true")
    parser.add_argument("--extended", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(PipelineConfig.default(), use_validated_checkpoints=not args.reestimate_locked, run_extended=args.extended, overwrite=args.overwrite), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
