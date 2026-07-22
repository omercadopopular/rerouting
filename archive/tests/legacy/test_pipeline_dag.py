from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scr"))

import pytest

from passthru_data.config import PipelineConfig, pipeline_topological_order, selected_steps, validate_only_step_inputs


def _config(tmp_path: Path, only_step: str | None = None) -> PipelineConfig:
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
        only_step=only_step,
    )


def test_pipeline_order_is_acyclic_and_deterministic():
    order = pipeline_topological_order()
    assert len(order) == len(set(order))
    assert order.index("download_trade") < order.index("build_trade_panels")
    assert order.index("build_trade_workhorse_panels") < order.index("run_trade_regressions")


def test_only_step_does_not_expand_prerequisites(tmp_path: Path):
    assert tuple(selected_steps(_config(tmp_path, "run_trade_regressions"))) == ("run_trade_regressions",)


def test_only_step_reports_missing_prerequisite(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="requires missing"):
        validate_only_step_inputs(_config(tmp_path, "run_trade_regressions"), "run_trade_regressions")
