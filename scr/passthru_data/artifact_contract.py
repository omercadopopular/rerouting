"""Canonical storage rules for replication artifacts.

Detailed diagnostics and key-level outputs are always written as compressed
Parquet.  Small aggregate tables may remain CSV, but readers prefer the
Parquet replacement whenever both files are present.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
import hashlib
import json
from datetime import datetime, timezone

import pandas as pd

from .io_utils import read_table, write_metadata_json, write_parquet


DETAILED_CATEGORIES = frozenset({"row_trace", "regression_keys", "detailed_diagnostic"})
SUMMARY_CATEGORIES = frozenset({"aggregate_summary", "human_report", "manifest"})


def canonical_path(path: Path, category: str) -> Path:
    """Return the canonical extension for an artifact category."""
    if category in DETAILED_CATEGORIES:
        return path.with_suffix(".parquet")
    if category == "human_report":
        return path.with_suffix(".md")
    if category == "manifest":
        return path.with_suffix(".json")
    return path


def write_detailed(
    df: pd.DataFrame,
    path: Path,
    category: str = "detailed_diagnostic",
    *,
    key_columns: Iterable[str] | None = None,
    source_fingerprints: dict[str, str] | None = None,
    code_fingerprint: str | None = None,
    specification_fingerprint: str | None = None,
) -> Path:
    if category not in DETAILED_CATEGORIES:
        raise ValueError(f"write_detailed requires a detailed category, got {category!r}")
    output = canonical_path(path, category)
    write_parquet(df, output, overwrite=True)
    schema = [(str(column), str(dtype)) for column, dtype in df.dtypes.items()]
    schema_fingerprint = hashlib.sha256(json.dumps(schema, sort_keys=True).encode()).hexdigest()
    keys = list(key_columns) if key_columns is not None else [column for column in ("id", "cty_code", "hs10", "year", "month", "mdate") if column in df.columns]
    write_metadata_json(output.with_suffix(".metadata.json"), {
        "category": category,
        "canonical_relative_path": output.as_posix(),
        "row_count": int(len(df)),
        "columns": [str(column) for column in df.columns],
        "key_columns": keys,
        "schema_fingerprint": schema_fingerprint,
        "compression": "zstd",
        "source_fingerprints": source_fingerprints or {},
        "code_fingerprint": code_fingerprint,
        "specification_fingerprint": specification_fingerprint,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    return output


def read_prefer_parquet(path: Path, columns: Iterable[str] | None = None) -> pd.DataFrame:
    """Read a canonical Parquet artifact, falling back to legacy CSV only."""
    parquet_path = path if path.suffix.lower() == ".parquet" else path.with_suffix(".parquet")
    if parquet_path.exists():
        kwargs = {"columns": list(columns)} if columns is not None else {}
        return read_table(parquet_path, **kwargs)
    csv_path = path if path.suffix.lower() == ".csv" else path.with_suffix(".csv")
    kwargs = {"usecols": list(columns)} if columns is not None else {}
    return read_table(csv_path, **kwargs)


def parquet_compression(path: Path) -> str | None:
    """Return the codec used by the first row group, for contract tests."""
    import pyarrow.parquet as pq

    metadata = pq.ParquetFile(path).metadata
    if metadata.num_row_groups == 0 or metadata.num_columns == 0:
        return None
    return str(metadata.row_group(0).column(0).compression).lower()
