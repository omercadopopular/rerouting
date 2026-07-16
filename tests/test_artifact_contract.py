from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scr"))

import pandas as pd

from passthru_data.artifact_contract import (
    canonical_path,
    parquet_compression,
    read_prefer_parquet,
    write_detailed,
)


def test_detailed_artifacts_are_parquet_and_zstd(tmp_path: Path):
    csv_like = tmp_path / "trace.csv"
    out = write_detailed(pd.DataFrame({"key": [1, 2]}), csv_like, "row_trace")
    assert out.suffix == ".parquet"
    assert parquet_compression(out) == "zstd"


def test_reader_prefers_parquet_over_legacy_csv(tmp_path: Path):
    base = tmp_path / "keys.csv"
    base.write_text("key\nlegacy\n", encoding="utf-8")
    write_detailed(pd.DataFrame({"key": ["canonical"]}), base, "regression_keys")
    assert read_prefer_parquet(base)["key"].tolist() == ["canonical"]


def test_small_summary_csv_remains_permitted(tmp_path: Path):
    path = canonical_path(tmp_path / "summary.csv", "aggregate_summary")
    assert path.suffix == ".csv"
