from scr.passthru_data.build_trade_extension_v2 import VERSION


def test_archive_native_extension_is_versioned_and_policy_free():
    assert VERSION == "extension_v2_archive_native"
