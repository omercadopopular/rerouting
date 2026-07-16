from datetime import date
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.config import inferred_latest_complete_period
from passthru_data.io_utils import iter_months, normalize_hs_code


def test_normalize_hs_code_preserves_leading_zeroes() -> None:
    assert normalize_hs_code("0101210000", 10) == "0101210000"
    assert normalize_hs_code(10121, 6) == "010121"
    assert normalize_hs_code("1", 2) == "01"


def test_normalize_hs_code_handles_stata_numeric_decimal_without_shift() -> None:
    assert normalize_hs_code(801001090.0, 10) == "0801001090"
    assert normalize_hs_code("801001090.0", 10) == "0801001090"


def test_normalize_hs_code_rejects_ambiguous_values() -> None:
    assert normalize_hs_code(None, 10) is None
    assert normalize_hs_code("not-a-code", 10) is None
    assert normalize_hs_code("8.0100109e8", 10) is None
    assert normalize_hs_code("12345678901", 10) is None


def test_iter_months_is_inclusive() -> None:
    assert iter_months("2020-11", "2021-02") == ["2020-11", "2020-12", "2021-01", "2021-02"]


def test_inferred_latest_complete_period_uses_previous_month() -> None:
    assert inferred_latest_complete_period(date(2026, 3, 17)) == "2026-02"
    assert inferred_latest_complete_period(date(2026, 1, 3)) == "2025-12"
