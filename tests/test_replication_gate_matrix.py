import json
from pathlib import Path


def test_gate_matrix_separates_trade_and_policy_readiness():
    path = Path("scr/docs/replication_coverage_matrix.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert "ready_for_extension" not in payload
    assert payload["package_import_pdf_gate"] == "passed"
    assert payload["raw_outcome_bridge_gate"] == "failed"
    assert payload["raw_trade_archive_ingestion_gate"] == "passed"
    assert payload["raw_trade_staging_reconciliation_gate"] == "passed"
    assert payload["raw_trade_quantity_semantics_gate"] == "pending"
    assert payload["section301_v5_ready"] is False
