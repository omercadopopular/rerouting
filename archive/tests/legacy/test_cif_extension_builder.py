from pathlib import Path

import pandas as pd

from scr.passthru_data import build_trade_extension_v4 as extension
from scr.passthru_data.config import PipelineConfig


def test_archive_native_cif_parser_preserves_import_fields(monkeypatch, tmp_path: Path) -> None:
    chunk = pd.DataFrame(
        {
            "hs10": ["801001090.0", "801001090.0"],
            "cty_code": ["5700", "5700"],
            "year": ["2018", "2018"],
            "month": ["1", "1"],
            "gen_qy1_mo": ["10", "5"],
            "gen_val_mo": ["1000", "500"],
            "gen_cif_mo": ["1100", "550"],
            "dut_val_mo": ["50", "25"],
            "cal_dut_mo": ["11", "5.5"],
        }
    )
    monkeypatch.setattr(extension, "_iter_fixed_width_chunks", lambda *args, **kwargs: iter([chunk]))
    monkeypatch.setattr(extension, "_load_country_lookup", lambda *args, **kwargs: pd.DataFrame({"cty_code": ["5700"], "cty_name": ["TEST"]}))
    archive = tmp_path / "IMDB1801.ZIP"
    archive.write_bytes(b"synthetic")
    frame, audit = extension._parse_archive_cif(PipelineConfig.default(), "imports", "2018-01", archive)
    assert frame.loc[0, "hs10"] == "0801001090"
    assert frame.loc[0, "gen_val_mo"] == 1500
    assert frame.loc[0, "gen_cif_mo"] == 1650
    assert frame.loc[0, "gen_qy1_mo"] == 15
    assert frame.loc[0, "cal_dut_mo"] == 16.5
    assert frame.loc[0, "cif_duty_unit_value"] == 111.1
    assert audit["reconciliation_pass"] is True