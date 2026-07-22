import pandas as pd
from pathlib import Path

from scr.passthru_data.download_trade import _standardize_trade_frame


def test_import_standardization_preserves_cif_and_general_value():
    raw = pd.DataFrame(
        {
            "I_COMMODITY": ["0801001090.0"],
            "CTY_CODE": ["5700"],
            "CTY_NAME": ["CHINA"],
            "YEAR": ["2018"],
            "MONTH": ["1"],
            "GEN_VAL_MO": ["16243321"],
            "GEN_CIF_MO": ["17381740"],
            "GEN_QY1_MO": ["6017375"],
            "DUT_VAL_MO": ["17000000"],
            "CAL_DUT_MO": ["678264"],
        }
    )
    result = _standardize_trade_frame(raw, "imports", "test", Path("fixture"))
    assert result.loc[0, "gen_val_mo"] == 16243321
    assert result.loc[0, "gen_cif_mo"] == 17381740
    assert result.loc[0, "gen_qy1_mo"] == 6017375
    assert result.loc[0, "trade_value"] == 16243321
    assert result.loc[0, "quantity"] == 6017375
