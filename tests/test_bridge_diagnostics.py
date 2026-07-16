from pathlib import Path

from scr.passthru_data.bridge_diagnostics import ci_overlap


def test_ci_overlap_identical_intervals():
    assert ci_overlap(0.0, 1.0, 0.0, 1.0) == 1.0


def test_ci_overlap_partial_touching_and_disjoint():
    assert ci_overlap(0.0, 1.0, 0.5, 1.5) == 1 / 3
    assert ci_overlap(0.0, 1.0, 1.0, 2.0) == 0.0
    assert ci_overlap(0.0, 1.0, 2.0, 3.0) == 0.0


def test_ci_overlap_zero_width_and_baseline_are_excluded():
    assert ci_overlap(1.0, 1.0, 1.0, 1.0) is None
    assert ci_overlap(0.0, 1.0, 0.0, 1.0, baseline=True) is None
