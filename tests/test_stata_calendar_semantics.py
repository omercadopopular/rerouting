import numpy as np
import pandas as pd
import pytest

from scr.passthru_data.trade_regressions import (
    _stata_exact_lookup,
    _stata_first_difference,
    _validate_stata_panel,
)


def _panel(months, values):
    return pd.DataFrame({"id": [1] * len(months), "mdate_index": months, "x": values})


def test_first_difference_requires_exact_previous_month():
    frame = _panel([1, 2, 4], [10.0, 12.0, 18.0])
    frame = _validate_stata_panel(frame)
    result = _stata_first_difference(frame, "x")
    assert np.isnan(result.iloc[0])
    assert result.iloc[1] == 2.0
    assert np.isnan(result.iloc[2])


def test_f1_does_not_cross_gap():
    frame = _validate_stata_panel(_panel([1, 3], [10.0, 30.0]))
    result = _stata_exact_lookup(frame, "x", 1)
    assert result.isna().all()


def test_f2_finds_exact_month_even_when_one_stored_row_away():
    frame = _validate_stata_panel(_panel([1, 3], [10.0, 30.0]))
    result = _stata_exact_lookup(frame, "x", 2)
    assert result.iloc[0] == 30.0
    assert np.isnan(result.iloc[1])


def test_duplicate_id_month_is_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        _validate_stata_panel(_panel([1, 1], [10.0, 11.0]))
