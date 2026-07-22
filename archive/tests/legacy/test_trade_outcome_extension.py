from pathlib import Path


def test_outcome_extension_excludes_package_policy_and_preserves_quantity_flags():
    source = Path("scr/passthru_data/build_trade_outcome_extension.py").read_text(encoding="utf-8")
    assert "quantity_missing" in source
    assert "quantity_zero" in source
    assert "cal_dut_mo" in source
    assert "package_policy_used" in source
    assert "policy_columns_excluded" in source
    assert "COMPRESSION ZSTD" in source
