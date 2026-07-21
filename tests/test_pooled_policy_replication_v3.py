from __future__ import annotations

import pandas as pd

from scr.passthru_data.pooled_policy_regression_v3 import _metrics
from scr.passthru_data.pooled_policy_replication_v2 import paper_month_from_legal_date


def test_v3_metric_alignment_requires_all_horizons() -> None:
    left = pd.DataFrame({"event_time": range(-6, 7), "estimate": [float(x) for x in range(13)]})
    right = left.copy()
    result = _metrics(left, right, "event_time")
    assert result["n_horizons"] == 13
    assert result["rmse"] == 0.0
    assert result["max_abs_difference"] == 0.0


def test_paper_clock_moves_late_dates_to_next_month() -> None:
    assert paper_month_from_legal_date("2018-03-23") == pd.Timestamp("2018-04-01")
    assert paper_month_from_legal_date("2018-03-15") == pd.Timestamp("2018-03-01")


def test_v3_source_modes_are_not_package_builder_modes() -> None:
    from scr.passthru_data import pooled_policy_replication_v3 as v3

    assert v3.VERSION == "pooled_policy_replication_v3"
    assert "package" not in v3.VERSION


def test_dynamic_policy_scope_is_partner_specific() -> None:
    from pathlib import Path

    source = Path("scr/passthru_data/pooled_policy_replication_v3.py").read_text(encoding="utf-8")
    assert "China only" in source
    assert "bilateral_dayweighted_additional_rate" in source
    assert "within_family_stacking" in source
