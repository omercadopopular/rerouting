from pathlib import Path
import shutil
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.config import PipelineConfig
from passthru_data.trade_regression_common import filter_to_analysis_window, month_index_from_columns, panel_has_required_columns
from passthru_data.trade_regression_sources import run_trade_regression_source_audit


def _temp_config(tmp_path: Path) -> PipelineConfig:
    data_root = tmp_path / "data"
    verification_dir = data_root / "verification" / "passthru_data"
    cfg = PipelineConfig(
        repo_root=tmp_path,
        raw_dir=data_root / "raw" / "passthru_data",
        staging_dir=data_root / "staging" / "passthru_data",
        reference_dir=data_root / "reference" / "passthru_data",
        analysis_dir=data_root / "analysis" / "passthru_data",
        verification_dir=verification_dir,
        fajgelbaum_root=data_root / "fajgelbaum",
        fajgelbaum_analysis_dir=data_root / "fajgelbaum" / "data" / "analysis",
        manual_input_dir=data_root / "raw" / "passthru_data" / "manual",
        logs_dir=verification_dir / "logs",
    )
    cfg.ensure_directories()
    return cfg


def test_month_index_from_columns_uses_stata_months_when_available() -> None:
    frame = pd.DataFrame({"year": [2018], "month": [2], "mdate": [697]})
    result = month_index_from_columns(frame)
    assert int(result.iloc[0]) == 2018 * 12 + 2 - 1


def test_panel_has_required_columns_flags_minimal_panels() -> None:
    frame = pd.DataFrame({"cty_code": [1], "hs10": ["0101210000"]})
    is_ready, missing = panel_has_required_columns(frame, "imports")
    assert not is_ready
    assert "m_val" in missing
    assert "m_stattariff2" in missing


def test_current_analysis_window_keeps_all_post_2017_months() -> None:
    frame = pd.DataFrame({"year": [2016, 2017, 2019, 2025], "month": [12, 1, 4, 12]})
    result = filter_to_analysis_window(frame, "imports", "current")
    assert result["year"].tolist() == [2017, 2019, 2025]


def test_trade_regression_source_audit_reports_missing_workhorse_columns() -> None:
    temp_dir = ROOT / "_tmp_trade_regression_helpers"
    shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        cfg = _temp_config(temp_dir)
        minimal_panel = pd.DataFrame(
            {
                "cty_code": [1],
                "cty_name": ["CHINA"],
                "hs10": ["0101210000"],
                "hs8": ["01012100"],
                "hs6": ["010121"],
                "hs4": ["0101"],
                "hs2": ["01"],
                "year": [2019],
                "month": [1],
                "mdate": [708],
                "m_val": [1.0],
                "m_q1": [1.0],
            }
        )
        panel_path = cfg.analysis_dir / "m_flow_hs10_fm_new.parquet"
        minimal_panel.to_parquet(panel_path, index=False)
        audit = run_trade_regression_source_audit(cfg)
        assert audit["status"] == "reference_fallback_required"
        assert "regression_ready_local_workhorse_panels" in audit["missing_capabilities"]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
