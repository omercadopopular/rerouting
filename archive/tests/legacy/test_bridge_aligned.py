from pathlib import Path

import pandas as pd

from scr.passthru_data.bridge_diagnostics import ci_overlap
from scr.passthru_data.bridge_aligned import VERSION


def test_aligned_bridge_is_import_only_and_versioned():
    assert VERSION == "bridge_v2_aligned_import"


def test_ci_overlap_baseline_is_not_anordinary_failure():
    assert ci_overlap(0.0, 0.0, 0.0, 0.0, baseline=True) is None


def test_aligned_outcome_masks_can_be_identical():
    frame = pd.DataFrame({"m_val": [1.0, None], "m_q1": [2.0, None]})
    assert frame["m_val"].notna().equals(frame["m_q1"].notna())
