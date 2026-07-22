from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scr"))

import pytest

from passthru_data.config import PipelineConfig
from passthru_data.section301_regression_sensitivity_v5 import (
    clear_current_fit,
    expected_artifact_ids,
    expected_fit_ids,
    finalize_v5,
    grid_counts,
    write_current_fit,
    write_failure_manifest,
    run_section301_regression_sensitivity,
)


def _config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        repo_root=tmp_path,
        raw_dir=tmp_path / "raw",
        staging_dir=tmp_path / "staging",
        reference_dir=tmp_path / "reference",
        analysis_dir=tmp_path / "analysis",
        verification_dir=tmp_path / "verification",
        fajgelbaum_root=tmp_path / "fajgelbaum",
        fajgelbaum_analysis_dir=tmp_path / "fajgelbaum" / "analysis",
        manual_input_dir=tmp_path / "manual",
        logs_dir=tmp_path / "logs",
    )


def test_grid_separates_fit_and_materialized_artifact_counts():
    counts = grid_counts()
    assert counts == {"expected_fits": 60, "expected_artifacts": 72}
    assert expected_fit_ids() < expected_artifact_ids()


def test_incomplete_grid_cannot_finalize(tmp_path: Path):
    with pytest.raises(RuntimeError, match="artifact grid incomplete"):
        finalize_v5(_config(tmp_path), [{"artifact_id": "missing", "fit_id": "missing"}])


def test_complete_grid_finalizes_without_policy_release(tmp_path: Path):
    records = [{"artifact_id": artifact_id, "fit_id": artifact_id} for artifact_id in expected_artifact_ids()]
    result = finalize_v5(_config(tmp_path), records)
    assert result["completed_artifacts"] == 72
    out = tmp_path / "verification" / "raw_replication_imports" / "v5"
    assert (out / "section301_sensitivity_coefficients.parquet").exists()
    assert (out / "section301_release_gate.json").exists()
    assert '"ready_for_extension": false' in (out / "section301_release_gate.json").read_text(encoding="utf-8")


def test_current_fit_marker_lifecycle(tmp_path: Path):
    cfg = _config(tmp_path)
    marker = write_current_fit(cfg, fit_id="fit-1", rows=3, formula="y ~ x", fixed_effects="id", clusters="hs8", started_at_utc="now")
    assert marker.exists()
    clear_current_fit(cfg)
    assert not marker.exists()
    failure = write_failure_manifest(cfg, fit_id="fit-2", exc=ValueError("bad fit"))
    assert '"exception_type": "ValueError"' in failure.read_text(encoding="utf-8")


def test_runner_reports_blocked_preflight_without_fabricating_outputs(tmp_path: Path):
    result = run_section301_regression_sensitivity(_config(tmp_path))
    assert result["status"] == "blocked"
    assert result["ready_for_extension"] is False
