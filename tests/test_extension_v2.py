from scr.passthru_data.build_trade_extension_v2 import VERSION
from scr.passthru_data.validate_extension_v2 import VERSION as COMPARISON_VERSION


def test_archive_native_extension_is_versioned_and_policy_free():
    assert VERSION == "extension_v2_archive_native"
    assert COMPARISON_VERSION == "extension_v2_staging_comparison_v1"
