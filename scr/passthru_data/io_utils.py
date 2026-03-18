"""Shared I/O, normalization, and metadata utilities."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
import json
import re

import pandas as pd

MONTH_PATTERN = re.compile(r"^(?P<year>\d{4})-(?P<month>\d{2})$")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_period(period: str) -> str:
    match = MONTH_PATTERN.match(period.strip())
    if not match:
        raise ValueError(f"Invalid period '{period}'. Expected YYYY-MM.")
    year = int(match.group("year"))
    month = int(match.group("month"))
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in period '{period}'.")
    return f"{year:04d}-{month:02d}"


def iter_months(start_period: str, end_period: str) -> list[str]:
    start = pd.Period(normalize_period(start_period), freq="M")
    end = pd.Period(normalize_period(end_period), freq="M")
    if start > end:
        raise ValueError("start_period must be before or equal to end_period")
    return [str(period) for period in pd.period_range(start, end, freq="M")]


def normalize_hs_code(value: Any, digits: int) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    text = re.sub(r"\D", "", text)
    if not text:
        return None
    return text.zfill(digits)[-digits:]


def normalize_country_code(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text or None


def normalize_country_name(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text.upper() if text else None


def read_table(path: Path, **kwargs: Any) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path, **kwargs)
    if suffix == ".csv":
        return pd.read_csv(path, **kwargs)
    if suffix == ".dta":
        return pd.read_stata(path, convert_categoricals=False, **kwargs)
    if suffix in {".txt", ".tsv"}:
        kwargs.setdefault("sep", "\t")
        return pd.read_csv(path, **kwargs)
    raise ValueError(f"Unsupported table format: {path}")


def write_parquet(df: pd.DataFrame, path: Path, overwrite: bool = True) -> Path:
    ensure_dir(path.parent)
    if path.exists() and not overwrite:
        return path
    df.to_parquet(path, index=False)
    return path


def write_stata_if_enabled(df: pd.DataFrame, path: Path, enabled: bool, overwrite: bool = True) -> Path | None:
    if not enabled:
        return None
    ensure_dir(path.parent)
    if path.exists() and not overwrite:
        return path
    export_df = df.copy()
    for column in export_df.columns:
        if pd.api.types.is_object_dtype(export_df[column]):
            export_df[column] = export_df[column].astype(str).str.slice(0, 244)
    export_df.to_stata(path, write_index=False, version=118)
    return path


def sha256_file(path: Path, block_size: int = 65536) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(block_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, (Path, pd.Timestamp, datetime)):
        return str(value)
    if isinstance(value, pd.Period):
        return str(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def write_metadata_json(path: Path, payload: dict[str, Any]) -> Path:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=_json_default)
    return path


def write_data_dictionary(
    df: pd.DataFrame,
    path: Path,
    descriptions: dict[str, str] | None = None,
    key_columns: Iterable[str] | None = None,
) -> Path:
    key_set = set(key_columns or [])
    records: list[dict[str, Any]] = []
    for column, dtype in df.dtypes.items():
        records.append(
            {
                "column": column,
                "dtype": str(dtype),
                "description": (descriptions or {}).get(column),
                "is_key": column in key_set,
                "non_null": int(df[column].notna().sum()),
                "nunique": int(df[column].nunique(dropna=True)),
            }
        )
    return write_metadata_json(path, {"columns": records})


def add_hierarchy_codes(df: pd.DataFrame, source_column: str = "hs10") -> pd.DataFrame:
    out = df.copy()
    out[source_column] = out[source_column].map(lambda v: normalize_hs_code(v, 10))
    out["hs8"] = out[source_column].str.slice(0, 8)
    out["hs6"] = out[source_column].str.slice(0, 6)
    out["hs4"] = out[source_column].str.slice(0, 4)
    out["hs2"] = out[source_column].str.slice(0, 2)
    return out


def to_stata_compatible_codes(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = out[column].astype("string")
    return out
