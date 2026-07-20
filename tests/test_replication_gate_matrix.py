import json
from pathlib import Path


def test_gate_matrix_separates_trade_and_policy_readiness():
    path = Path("scr/docs/replication_coverage_matrix.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert "ready_for_extension" not in payload
    assert payload["package_import_pdf_gate"] == "passed"
    assert payload["raw_outcome_point_estimate_gate"] == "passed"
    assert payload["raw_outcome_inference_diagnostic"] == "failed"
    assert payload["raw_trade_archive_ingestion_gate"] == "passed"
    assert payload["raw_trade_staging_reconciliation_gate"] == "passed"
    assert payload["raw_trade_quantity_semantics_gate"] == "pending"
    assert payload["raw_trade_real_value_gate"] == "not_required_for_replication"
    assert payload["cpi_data_preserved_for_future_use"] is True
    assert payload["historical_paper_policy_variable_gate"] == "passed"
    assert payload["historical_paper_policy_curve_gate"] == "passed"
    assert payload["historical_pooled_policy_gate"] == "failed"
    assert payload["historical_replication_methodology_lock"] is False
    assert payload["independent_2018_final_legal_variable_gate"] == "passed"
    assert payload["forward_2025_policy_ledger_gate"] == "failed"
    assert payload["section301_v5_ready"] is False
