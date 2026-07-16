from pathlib import Path

from scr.passthru_data.validate_trade_extension import VERSION, _partition


def test_extension_validator_uses_versioned_archive_namespace():
    assert VERSION == "extension_archive_validation_v1"


def test_extension_partition_is_monthly_and_separate_from_staging():
    class Dummy:
        analysis_dir = Path("data/analysis/passthru_data")

    path = _partition(Dummy(), "imports", "2025-12")
    assert path.as_posix().endswith("extension_v1/flow=imports/year=2025/month=12/part.parquet")
