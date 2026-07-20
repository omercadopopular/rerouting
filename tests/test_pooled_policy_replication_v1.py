from __future__ import annotations

import pandas as pd

from scr.passthru_data.pooled_policy_replication_v1 import (
    _active_share,
    _family_components,
    _family_source_status,
)
from scr.passthru_data.build_us_products_partner_panel import _rule_family


def test_section201_rule_family_uses_exact_chapter99_codes() -> None:
    assert _rule_family("99034501") == "washer_201"
    assert _rule_family("99034502") == "washer_201"
    assert _rule_family("99034506") == "washer_201"
    assert _rule_family("99034522") == "solar_201"
    assert _rule_family("99034525") == "solar_201"
    assert _rule_family("99034601") == "other"


def test_active_share_uses_inclusive_legal_days() -> None:
    assert abs(_active_share("2018-03-23", "2018-03-31", 2018, 3) - 9 / 31) < 1e-12
    assert _active_share("2018-03-23", "2018-03-31", 2018, 2) == 0.0
    assert _active_share("2018-03-23", "2018-04-02", 2018, 4) == 2 / 30


def test_family_components_sum_overlapping_actions_without_package_fields() -> None:
    actions = pd.DataFrame(
        [
            {
                "action_id": "a",
                "family": "steel_232",
                "partner_name": "CHINA",
                "hs8": "72081000",
                "year": 2018,
                "month": 3,
                "additional_rate": 0.25,
                "day_weighted_additional_rate": 0.0725806452,
                "legal_effective_date": pd.Timestamp("2018-03-23"),
            },
            {
                "action_id": "b",
                "family": "china_301",
                "partner_name": "CHINA",
                "hs8": "72081000",
                "year": 2018,
                "month": 3,
                "additional_rate": 0.10,
                "day_weighted_additional_rate": 0.0290322581,
                "legal_effective_date": pd.Timestamp("2018-03-23"),
            },
        ]
    )
    result = _family_components(actions)
    assert set(result["family"]) == {"steel_232", "china_301"}
    assert abs(result["additional_rate"].sum() - 0.35) < 1e-12
    assert "m_stattariff2" not in result.columns


def test_family_status_does_not_call_exception_only_scope_complete() -> None:
    links = pd.DataFrame(
        [{"rule_code": "99038005", "family": "steel_232"}]
    )
    attrs = pd.DataFrame(
        [
            {"rule_code": "99038001", "increment_rate": 0.25},
            {"rule_code": "99038005", "increment_rate": 0.25},
        ]
    )
    status = _family_source_status(links, attrs)["steel_232"]
    assert status["scope_status"] == "partial_missing_positive_scope"
    assert "99038001" in status["expected_positive_rules_without_scope_links"]


def test_family_status_marks_solar_missing_when_no_independent_source_exists() -> None:
    status = _family_source_status(pd.DataFrame(), pd.DataFrame())["solar_201"]
    assert status["scope_status"] == "partial_missing_positive_scope"


def test_zero_rate_rows_are_not_treatment_actions() -> None:
    # The action-expansion filter is exercised indirectly by the component
    # contract: zero-rate exemption rows cannot create positive components.
    actions = pd.DataFrame(
        [{
            "action_id": "positive",
            "family": "washer_201",
            "partner_name": "CHINA",
            "hs8": "84501100",
            "year": 2018,
            "month": 2,
            "additional_rate": 0.20,
            "day_weighted_additional_rate": 0.20,
            "legal_effective_date": pd.Timestamp("2018-02-07"),
        }]
    )
    assert _family_components(actions)["additional_rate"].tolist() == [0.20]
