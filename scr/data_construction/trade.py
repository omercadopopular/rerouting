"""Archive-native construction of the historical Census import panel.

The builder is deliberately local-only.  It reads the already-downloaded
monthly Census fixed-width ZIP archives, aggregates to partner-HS10-month, and
writes one ZSTD Parquet partition per month.  It never attaches tariff or
treatment variables.
"""

from __future__ import annotations

import json
import zipfile
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from .config import PipelineConfig
from .io_utils import (
    add_hierarchy_codes,
    iter_months,
    normalize_hs_code,
    sha256_file,
    write_metadata_json,
    write_parquet,
)

VERSION = "historical_trade_v1"
DETAIL_MEMBER = "IMP_DETL.TXT"
COUNTRY_MEMBER = "COUNTRY.TXT"
DETAIL_COLSPECS = [
    (0, 10),
    (10, 14),
    (22, 26),
    (26, 28),
    (148, 163),
    (178, 193),
    (208, 223),
    (88, 103),
    (103, 118),
]
DETAIL_NAMES = [
    "hs10",
    "cty_code",
    "year",
    "month",
    "gen_qy1_mo",
    "gen_val_mo",
    "gen_cif_mo",
    "dut_val_mo",
    "cal_dut_mo",
]
COUNTRY_COLSPECS = [(0, 4), (11, 61)]


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _member(archive: zipfile.ZipFile, expected: str) -> str:
    for candidate in archive.namelist():
        if candidate.lower() == expected.lower():
            return candidate
    raise KeyError(f"{expected} not found in {archive.filename}")


def _chunks(path: Path, chunksize: int = 250_000) -> Iterator[pd.DataFrame]:
    archive = zipfile.ZipFile(path)
    handle = archive.open(_member(archive, DETAIL_MEMBER))
    wrapper = TextIOWrapper(handle, encoding="latin1", errors="ignore")
    try:
        yield from pd.read_fwf(
            wrapper,
            colspecs=DETAIL_COLSPECS,
            names=DETAIL_NAMES,
            dtype="string",
            chunksize=chunksize,
        )
    finally:
        wrapper.close()
        archive.close()


def _country_lookup(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        with archive.open(_member(archive, COUNTRY_MEMBER)) as handle:
            wrapper = TextIOWrapper(handle, encoding="latin1", errors="ignore")
            frame = pd.read_fwf(
                wrapper,
                colspecs=COUNTRY_COLSPECS,
                names=["cty_code", "cty_name"],
                dtype="string",
            )
    frame["cty_code"] = frame["cty_code"].str.strip().str.zfill(4)
    frame["cty_name"] = frame["cty_name"].str.strip().str.upper()
    return frame.drop_duplicates("cty_code")


def archive_path(config: PipelineConfig, period: str) -> Path:
    return config.raw_dir / "trade" / "imports" / f"IMDB{period[2:4]}{period[5:7]}.ZIP"


def build_month(config: PipelineConfig, period: str, *, overwrite: bool = False) -> dict[str, Any]:
    year, month = int(period[:4]), int(period[5:7])
    source = archive_path(config, period)
    if not source.exists():
        raise FileNotFoundError(source)
    destination = (
        config.processed_trade_dir
        / "intermediate"
        / "monthly_imports"
        / f"year={year:04d}"
        / f"month={month:02d}"
        / "part.parquet"
    )
    audit_path = destination.with_suffix(".json")
    source_hash = sha256_file(source)
    if destination.exists() and audit_path.exists() and not overwrite:
        prior = json.loads(audit_path.read_text(encoding="utf-8"))
        if prior.get("source_sha256") == source_hash and prior.get("version") == VERSION:
            return prior

    pieces: list[pd.DataFrame] = []
    source_rows = 0
    discarded = 0
    numeric = ["gen_qy1_mo", "gen_val_mo", "gen_cif_mo", "dut_val_mo", "cal_dut_mo"]
    for chunk in _chunks(source):
        source_rows += len(chunk)
        chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
        chunk["cty_code"] = chunk["cty_code"].str.strip().str.zfill(4)
        chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
        chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce")
        for column in numeric:
            chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
        before = len(chunk)
        chunk = chunk.loc[(chunk["year"] == year) & (chunk["month"] == month)].copy()
        discarded += before - len(chunk)
        chunk = chunk.dropna(subset=["hs10", "cty_code"])
        if not chunk.empty:
            pieces.append(chunk)
    if not pieces:
        raise RuntimeError(f"{source} produced no {period} detail rows")

    raw = pd.concat(pieces, ignore_index=True)
    grouped = raw.groupby(["cty_code", "hs10", "year", "month"], as_index=False)[numeric].sum(min_count=1)
    grouped = grouped.merge(_country_lookup(source), on="cty_code", how="left")
    grouped = grouped.rename(columns={"cty_code": "partner_code", "cty_name": "partner_name"})
    grouped["period"] = period
    grouped["quantity_missing"] = grouped["gen_qy1_mo"].isna()
    grouped["quantity_zero"] = grouped["gen_qy1_mo"].eq(0)
    grouped["m_val"] = grouped["gen_cif_mo"] / 1_000_000.0
    grouped["m_q1"] = grouped["gen_qy1_mo"] / 1_000_000.0
    grouped["m_p"] = grouped["gen_cif_mo"] / grouped["gen_qy1_mo"].where(grouped["gen_qy1_mo"] > 0)
    grouped["m_pduty"] = (grouped["gen_cif_mo"] + grouped["cal_dut_mo"]) / grouped["gen_qy1_mo"].where(grouped["gen_qy1_mo"] > 0)
    grouped = add_hierarchy_codes(grouped, "hs10")
    grouped["source_archive"] = _relative(config, source)
    grouped["source_sha256"] = source_hash
    grouped["source_member"] = DETAIL_MEMBER
    grouped["parser_version"] = VERSION
    grouped = grouped.sort_values(["partner_code", "hs10"]).reset_index(drop=True)
    write_parquet(grouped, destination, overwrite=True)

    source_cif = float(raw["gen_cif_mo"].sum(min_count=1))
    output_cif = float(grouped["gen_cif_mo"].sum(min_count=1))
    tolerance = max(1.0, 1e-8 * abs(source_cif))
    audit = {
        "version": VERSION,
        "period": period,
        "source_archive": _relative(config, source),
        "source_sha256": source_hash,
        "partition": _relative(config, destination),
        "partition_sha256": sha256_file(destination),
        "source_rows": source_rows,
        "discarded_period_rows": discarded,
        "output_rows": len(grouped),
        "duplicate_keys": int(grouped.duplicated(["partner_code", "hs10", "year", "month"]).sum()),
        "source_cif_total": source_cif,
        "output_cif_total": output_cif,
        "reconciliation_difference": output_cif - source_cif,
        "reconciliation_tolerance": tolerance,
        "reconciliation_pass": abs(output_cif - source_cif) <= tolerance,
        "quantity_missing_rows": int(grouped["quantity_missing"].sum()),
        "quantity_zero_rows": int(grouped["quantity_zero"].sum()),
        "calculated_duty_nonmissing_rows": int(grouped["cal_dut_mo"].notna().sum()),
    }
    write_metadata_json(audit_path, audit)
    return audit


def build_trade(
    config: PipelineConfig,
    *,
    start_period: str = "2017-01",
    end_period: str = "2020-10",
    overwrite: bool = False,
) -> dict[str, Any]:
    config.ensure_directories()
    audits = [build_month(config, period, overwrite=overwrite) for period in iter_months(start_period, end_period)]
    audit_frame = pd.DataFrame(audits).sort_values("period")
    verification = config.processed_trade_dir / "verification"
    write_parquet(audit_frame, verification / "monthly_reconciliation.parquet", overwrite=True)
    audit_frame[["period", "output_rows", "reconciliation_difference", "reconciliation_pass"]].to_csv(
        verification / "monthly_reconciliation.csv", index=False
    )
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_period": start_period,
        "end_period": end_period,
        "months": len(audits),
        "rows": int(audit_frame["output_rows"].sum()),
        "duplicate_keys": int(audit_frame["duplicate_keys"].sum()),
        "reconciliation_failures": int((~audit_frame["reconciliation_pass"].astype(bool)).sum()),
        "policy_columns_present": False,
        "status": "passed" if audit_frame["reconciliation_pass"].all() and not audit_frame["duplicate_keys"].any() else "failed",
    }
    write_metadata_json(config.processed_trade_dir / "trade_build_manifest.json", manifest)
    return manifest
