from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scr.data_construction.config import PipelineConfig as DataConfig
from scr.data_construction.tariffs import write_source_ledger
from scr.pass_through.estimators import _dynamic_cumulative_terms, _prepare_event_study
from scr.pass_through.replication import FINAL_CHART_SERIES


def _event_fixture() -> pd.DataFrame:
    rows = []
    for month in range(1, 9):
        rows.append(
            {
                "id": 1,
                "cty_code": 5700,
                "hs10": "0101210000",
                "hs8": "01012100",
                "hs6": "010121",
                "year": 2018,
                "month": month,
                "mdate": pd.Timestamp(2018, month, 1),
                "naics_str": "1111",
                "m_val": 1.0,
                "m_q1": 1.0,
                "m_p": 1.0,
                "m_pduty": 1.0,
                "m_stattariff2": 0.1,
                "m_status2": 2,
                "m_effective_mdate2": pd.Timestamp(2018, 2, 1),
                "m_ess": 2,
            }
        )
    return pd.DataFrame(rows)


def test_event_extension_topcodes_at_24_without_changing_baseline() -> None:
    fixture = _event_fixture()
    prepared = _prepare_event_study("imports", fixture, post_horizon=24)
    assert prepared["event_time"].min() == -1
    assert prepared["event_time"].max() == 6
    assert "et_m5" in prepared
    assert "et_p24" in prepared
    assert "et_m6" not in prepared  # -6 remains the omitted category.


def test_dynamic_extension_has_six_leads_and_twenty_four_lags() -> None:
    terms = _dynamic_cumulative_terms(lead_horizon=6, lag_horizon=24)
    assert len(terms) == 31
    assert terms[0] == (-6, ["F6x"])
    assert terms[-1][0] == 24
    assert terms[-1][1][-1] == "L24x"


def test_final_chart_contract_has_exactly_three_truthful_series() -> None:
    assert [row[2] for row in FINAL_CHART_SERIES] == [
        "Original regression",
        "Replication",
        "Alternative timing (independent policy, legal clock)",
    ]


def test_source_ledger_names_all_policy_families(tmp_path: Path) -> None:
    config = DataConfig.default(tmp_path)
    path = write_source_ledger(config)
    payload = json.loads(path.read_text(encoding="utf-8"))
    roles = " ".join(item["role"] for item in payload["sources"])
    assert "MFN" in roles
    assert "Section 201" in roles
    assert "Section 232" in roles
    assert "Section 301" in roles
    assert all(item["url"].startswith("https://") for item in payload["sources"])
