from __future__ import annotations

import pytest

from scr.passthru_data.policy_benchmark_contract import (
    comparison_eligibility,
    load_contract,
)


def test_fig02_published_comparison_requires_nearest_full_month() -> None:
    eligible, reason = comparison_eligibility(
        target="fig_02_m_event",
        reconstructed_calendar="nearest_full_month",
        reconstructed_policy_variables={"m_status2", "m_effective_mdate2", "m_stattariff2"},
    )
    assert eligible is True
    assert reason is None

    eligible, reason = comparison_eligibility(
        target="fig_02_m_event",
        reconstructed_calendar="legal_effective_month",
        reconstructed_policy_variables={"m_status1", "m_effective_mdate1", "m_stattariff1"},
    )
    assert eligible is False
    assert "calendar mismatch" in reason


def test_fig01_can_use_legal_month_when_original_program_does() -> None:
    eligible, reason = comparison_eligibility(
        target="fig_01_rates",
        reconstructed_calendar="legal_effective_month",
        reconstructed_policy_variables={"m_status1", "m_effective_mdate1", "m_stattariff1"},
    )
    assert eligible is True
    assert reason is None


def test_contract_rejects_missing_policy_variable() -> None:
    eligible, reason = comparison_eligibility(
        target="fig_04_dynamic",
        reconstructed_calendar="nearest_full_month_dynamic_design",
        reconstructed_policy_variables={"stat2tf"},
    )
    assert eligible is False
    assert "policy-variable mismatch" in reason


def test_contract_is_committed_and_contains_gate_role() -> None:
    contract = load_contract()
    assert contract["targets"]["fig_02_m_event"]["legal_calendar_is_published_target"] is False
    assert contract["thresholds"]["ci_overlap_role"] == "diagnostic_not_primary_point_estimate_gate"
