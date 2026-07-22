from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.config import PipelineConfig
from passthru_data.rtp_long_horizon import build_2018_treatment_crosswalk, build_2025_ieepa_event_panel, validate_public_tariff_ledger


def _config(tmp_path: Path) -> PipelineConfig:
    data_root = tmp_path / "data"
    cfg = PipelineConfig(
        repo_root=tmp_path,
        raw_dir=data_root / "raw" / "passthru_data",
        staging_dir=data_root / "staging" / "passthru_data",
        reference_dir=data_root / "reference" / "passthru_data",
        analysis_dir=data_root / "analysis" / "passthru_data",
        verification_dir=data_root / "verification" / "passthru_data",
        fajgelbaum_root=data_root / "fajgelbaum",
        fajgelbaum_analysis_dir=data_root / "fajgelbaum" / "data" / "analysis",
        manual_input_dir=data_root / "raw" / "passthru_data" / "manual",
        logs_dir=data_root / "verification" / "passthru_data" / "logs",
    )
    cfg.ensure_directories()
    cfg.fajgelbaum_analysis_dir.mkdir(parents=True, exist_ok=True)
    return cfg


def _package_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cty_code": [5700, 5700, 2010],
            "hs10": ["0101210000", "0101210000", "0101210000"],
            "m_ess": [0, 2, 0],
            "m_effective_mdate2": [None, 702, None],
            "naics_str": ["1111", "1111", "1111"],
            "m_china_hit": [0, 1, 0], "m_steel_hit": [0, 0, 0], "m_alum_hit": [0, 0, 0],
            "m_washer_hit": [0, 0, 0], "m_solar_hit": [0, 0, 0],
        }
    )


def test_treatment_crosswalk_uses_first_package_treatment_date(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    _package_rows().to_stata(cfg.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta", write_index=False)
    metadata = build_2018_treatment_crosswalk(cfg)
    crosswalk = pd.read_parquet(metadata["output_path"])
    china = crosswalk.loc[crosswalk["cty_code"] == 5700].iloc[0]
    mexico = crosswalk.loc[crosswalk["cty_code"] == 2010].iloc[0]
    assert china["treated_2018"] == 1
    assert china["event_index"] == 702 + 1960 * 12
    assert mexico["treated_2018"] == 0
    assert mexico["event_index"] == china["event_index"]


def test_tariff_ledger_gate_requires_same_cutoff_and_schema(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    pd.DataFrame({"year": [2025], "month": [12]}).to_parquet(cfg.analysis_dir / "m_flow_hs10_fm_new.parquet", index=False)
    blocked = validate_public_tariff_ledger(cfg)
    assert not blocked["ready"]
    assert "missing_public_tariff_ledger" in blocked["reasons"]

    ledger = pd.DataFrame(
        {
            "cty_code": [5700], "hs10": ["0101210000"], "year": [2025], "month": [12],
            "applicable_total_ad_valorem_duty": [0.2], "is_non_ad_valorem": [False], "is_unresolved": [False],
            "source_url": ["https://example.test"], "policy_panel_version": ["test"],
        }
    )
    ledger.to_parquet(cfg.analysis_dir / "public_tariff_ledger_hs10_monthly.parquet", index=False)
    assert validate_public_tariff_ledger(cfg)["ready"]
    with pytest.raises(RuntimeError, match="is_china_ieepa_treated"):
        build_2025_ieepa_event_panel(cfg)
