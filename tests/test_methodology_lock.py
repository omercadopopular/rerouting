import json
from pathlib import Path


def test_methodology_lock_separates_point_estimate_and_inference_gates():
    path = Path("scr/docs/replication_methodology_lock_v1.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["gates"]["package_import_pdf_gate"] == "passed"
    assert payload["gates"]["raw_outcome_point_estimate_gate"] == "passed"
    assert payload["gates"]["raw_outcome_inference_gate"] == "failed"
    assert payload["gates"]["section301_v5_ready"] is False
    assert "ready_for_extension" not in payload["gates"]
    assert payload["outcomes"]["pduty"] == "(trade_value + cal_dut_mo) / quantity"


def test_policy_decomposition_does_not_promote_release():
    source = Path("scr/passthru_data/decompose_policy_mismatches.py").read_text(encoding="utf-8")
    assert "legal_mapping_changed" in source
    assert '"section301_v5_ready": False' in source
    assert '"ready_for_extension"' not in source
