from scr.passthru_data.build_trade_extension_v2 import VERSION
from scr.passthru_data.validate_extension_v2 import VERSION as COMPARISON_VERSION
from scr.passthru_data.audit_native_concordance import VERSION as CONCORDANCE_VERSION
from scr.passthru_data.audit_quantity_tokens import VERSION as QUANTITY_TOKEN_VERSION, _classify


def test_archive_native_extension_is_versioned_and_policy_free():
    assert VERSION == "extension_v2_archive_native"
    assert COMPARISON_VERSION == "extension_v2_staging_comparison_v1"
    assert CONCORDANCE_VERSION == "extension_native_concordance_audit_v1"
    assert QUANTITY_TOKEN_VERSION == "extension_quantity_token_audit_v1"


def test_raw_quantity_token_audit_distinguishes_blank_zero_and_positive():
    assert _classify("") == "blank"
    assert _classify("   ") == "blank"
    assert _classify("0") == "explicit_zero"
    assert _classify("000000") == "explicit_zero"
    assert _classify("12.5") == "positive"
    assert _classify("*") == "malformed_or_suppressed"
    assert _classify("-1") == "negative"
