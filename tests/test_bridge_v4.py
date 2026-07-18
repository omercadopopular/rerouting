from pathlib import Path

from scr.passthru_data.bridge_runner import expected_fit_ids
from scr.passthru_data.bridge_runner_v4 import VERSION


def test_v4_uses_new_namespace_and_exact_sixteen_fit_grid():
    assert VERSION == "bridge_v4_realized_calculated_duty"
    assert len(expected_fit_ids()) == 16
    assert all("|" in fit_id for fit_id in expected_fit_ids())


def test_v4_source_builder_keeps_realized_duty_formula_explicit():
    source = Path("scr/passthru_data/bridge_aligned_v4.py").read_text(encoding="utf-8")
    assert "cal_dut_mo" in source
    assert "trade_value::DOUBLE + r.cal_dut_mo" in source
    assert "dut_val_mo_role" in source
    assert "policy_semantics_changed" in source
