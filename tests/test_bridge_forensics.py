from scr.passthru_data.bridge_forensics import OUTCOMES, VERSION
from scr.passthru_data.audit_bridge_common_loss import VERSION as LOSS_VERSION


def test_bridge_forensics_is_versioned_and_has_all_import_outcomes():
    assert VERSION == "bridge_forensics_v1"
    assert OUTCOMES == ("val", "q1", "p", "pduty")
    assert LOSS_VERSION == "bridge_common_key_loss_audit_v1"


def test_forensics_output_contract_keeps_key_diagnostics_out_of_csv():
    # The module's required key-level artifact is explicitly Parquet.  CSV is
    # reserved for the compact grouped summaries produced beside it.
    assert "aligned_outcome_difference.parquet".endswith(".parquet")
    assert "difference_influence_summary.csv".endswith(".csv")
