from pathlib import Path

import pandas as pd

from scr.passthru_data.pooled_policy_replication_v4 import FAMILIES
from scr.passthru_data.pooled_policy_regression_v4 import _mode_fields


def test_v4_has_all_policy_families():
    assert set(FAMILIES) == {"solar_201", "washer_201", "steel_232", "aluminum_232", "china_301"}


def test_independent_rates_are_not_zero_filled():
    frame = pd.DataFrame({"independent_base_mfn_rate": [None, 0.05]})
    assert frame["independent_base_mfn_rate"].isna().sum() == 1


def test_v4_artifact_names_are_versioned():
    assert "pooled_policy_replication_v4" in str(Path("data/analysis/passthru_data/policy/pooled_policy_replication_v4"))


def test_policy_modes_are_distinct():
    assert {"package_full_policy_anchor", "independent_paper_full_policy", "independent_legal_full_policy"}


def test_paper_clock_uses_partner_specific_v3_tariff_path():
    event_date, tariff, status = _mode_fields("independent_paper_full_policy")
    assert event_date == "paper_event_month"
    assert tariff == "paper_dynamic_total_tariff"
    assert status == "historical_status"


def test_legal_clock_uses_same_bilateral_tariff_path_semantics():
    event_date, tariff, status = _mode_fields("independent_legal_full_policy")
    assert event_date == "legal_event_month"
    assert tariff == "legal_dynamic_total_tariff"
    assert status == "historical_status"
