import json
from pathlib import Path


def test_methodology_lock_separates_historical_and_forward_gates():
    path = Path("scr/docs/replication_methodology_lock_v2.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["gates"]["package_import_pdf_gate"] == "passed"
    assert payload["gates"]["raw_outcome_point_estimate_gate"] == "passed"
    assert payload["gates"]["raw_outcome_inference_diagnostic"] == "failed"
    assert payload["gates"]["paper_compatible_policy_variable_gate"] == "passed"
    assert payload["gates"]["paper_compatible_event_encoding_gate"] == "passed"
    assert payload["gates"]["paper_compatible_policy_curve_gate"] == "passed"
    assert payload["gates"]["historical_pooled_policy_gate"] == "failed"
    assert payload["gates"]["historical_replication_methodology_lock"] == "failed"
    assert payload["gates"]["independent_2018_final_legal_variable_gate"] == "passed"
    assert payload["gates"]["forward_2025_policy_ledger_gate"] == "failed"
    assert payload["gates"]["cpi_real_values_for_historical_replication"] == "not_required_for_replication"
    assert payload["gates"]["section301_v5_ready"] is False
    assert "ready_for_extension" not in payload["gates"]
    assert payload["outcomes"]["value"] == "GEN_CIF_MO / 1,000,000"
    assert payload["outcomes"]["pduty"] == "(GEN_CIF_MO + CAL_DUT_MO) / GEN_QY1_MO"
    assert payload["cpi"]["data_preserved"] is True
    assert payload["policy_evidence"]["paper_compatible_is_independent_legal_evidence"] is False


def test_policy_decomposition_does_not_promote_release():
    source = Path("scr/passthru_data/decompose_policy_mismatches.py").read_text(encoding="utf-8")
    assert "legal_mapping_changed" in source
    assert '"section301_v5_ready": False' in source
    assert '"ready_for_extension"' not in source
