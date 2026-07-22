from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.download_trade import _standardize_trade_frame


def test_trade_schema_aliases_are_standardized() -> None:
    raw = pd.DataFrame(
        {
            "I_COMMODITY": ["0101210000"],
            "CTY_CODE": ["5700"],
            "CTY_NAME": ["China"],
            "year": [2019],
            "month": [1],
            "GEN_VAL_MO": [100.0],
            "GEN_QY1_MO": [2.0],
        }
    )
    out = _standardize_trade_frame(raw, flow="imports", source_type="test", source_file=Path("dummy.csv"))
    assert out.loc[0, "hs10"] == "0101210000"
    assert out.loc[0, "partner_name"] == "CHINA"
    assert out.loc[0, "period"] == "2019-01"
    assert out.loc[0, "trade_value"] == 100.0
