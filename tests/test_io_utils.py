from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.io_utils import iter_months, normalize_hs_code


def test_normalize_hs_code_preserves_leading_zeroes() -> None:
    assert normalize_hs_code("0101210000", 10) == "0101210000"
    assert normalize_hs_code(10121, 6) == "010121"
    assert normalize_hs_code("1", 2) == "01"


def test_iter_months_is_inclusive() -> None:
    assert iter_months("2020-11", "2021-02") == ["2020-11", "2020-12", "2021-01", "2021-02"]
