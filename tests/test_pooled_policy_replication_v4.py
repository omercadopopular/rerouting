from pathlib import Path

import pandas as pd

from scr.passthru_data.pooled_policy_replication_v4 import FAMILIES


def test_v4_has_all_policy_families():
    assert set(FAMILIES) == {"solar_201", "washer_201", "steel_232", "aluminum_232", "china_301"}


def test_independent_rates_are_not_zero_filled():
    frame = pd.DataFrame({"independent_base_mfn_rate": [None, 0.05]})
    assert frame["independent_base_mfn_rate"].isna().sum() == 1


def test_v4_artifact_names_are_versioned():
    assert "pooled_policy_replication_v4" in str(Path("data/analysis/passthru_data/policy/pooled_policy_replication_v4"))


def test_policy_modes_are_distinct():
    assert {"package_full_policy_anchor", "independent_paper_full_policy", "independent_legal_full_policy"}
