from pathlib import Path

import pandas as pd

from scr.passthru_data.bridge_diagnostics import ci_overlap, curve_metrics
from scr.passthru_data.diagnose_pduty_bridge import duty_inclusive_unit_value


def test_ci_overlap_identical_intervals():
    assert ci_overlap(0.0, 1.0, 0.0, 1.0) == 1.0


def test_ci_overlap_partial_touching_and_disjoint():
    assert ci_overlap(0.0, 1.0, 0.5, 1.5) == 1 / 3
    assert ci_overlap(0.0, 1.0, 1.0, 2.0) == 0.0
    assert ci_overlap(0.0, 1.0, 2.0, 3.0) == 0.0


def test_ci_overlap_zero_width_and_baseline_are_excluded():
    assert ci_overlap(1.0, 1.0, 1.0, 1.0) is None
    assert ci_overlap(0.0, 1.0, 0.0, 1.0, baseline=True) is None


def test_curve_metrics_recomputes_after_excluding_normalized_baseline():
    frame = pd.DataFrame(
        {
            "horizon": [-6, -5, -4],
            "estimate_package": [0.0, 1.0, 2.0],
            "estimate_raw": [0.0, 1.0, 3.0],
            "conf_low_package": [0.0, 0.5, 1.5],
            "conf_high_package": [0.0, 1.5, 2.5],
            "conf_low_raw": [0.0, 0.5, 2.5],
            "conf_high_raw": [0.0, 1.5, 3.5],
        }
    )
    registered = curve_metrics(frame, exclude_baseline=False)
    sensitivity = curve_metrics(frame, exclude_baseline=True)
    assert registered["n_points"] == 3
    assert sensitivity["n_points"] == 2
    assert sensitivity["rmse"] > registered["rmse"]


def test_duty_inclusive_price_uses_calculated_duty_without_zero_filling():
    result = duty_inclusive_unit_value(
        pd.Series([100.0, 100.0, 100.0, None]),
        pd.Series([10.0, 0.0, 10.0, 10.0]),
        pd.Series([25.0, 25.0, None, 25.0]),
    )
    assert result.iloc[0] == 12.5
    assert result.iloc[1:].isna().all()
