"""Construct the Fajgelbaum--Khandelwal (2026) import panel.

The March-2026 paper measures realized tariffs from Census imports for
consumption.  The raw detail file contains one row for each rate provision,
district, sub-country, origin, HS10, and month.  This module preserves the
rate-provision information, then aggregates it to the paper's
origin--HS10--month variety.

It deliberately does not attach a February-event treatment.  Event timing is
inferred later from the observed applied tariff.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Iterator

import numpy as np
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

VERSION = "fajgelbaum_khandelwal_2025_trade_v1"
DETAIL_MEMBER = "IMP_DETL.TXT"
COUNTRY_MEMBER = "COUNTRY.TXT"
CENSUS_IMPORT_PAGE = "https://www.census.gov/foreign-trade/data/"
EPISODES = {
    "trade_war_2018": ("2017-01", "2019-12"),
    "tariffs_2025": ("2024-01", "2025-12"),
}
EVENT_HORIZON_EXTENSION = {
    # The published 2018--19 event study ends in 2019-12. Extending treatment
    # cohorts from that episode to h=24 requires outcome observations through
    # 2021-12. These partitions live in a separate namespace so that the
    # frozen paper-window source fingerprint does not change.
    "trade_war_2018": ("2020-01", "2021-12"),
}

# Zero-based, half-open offsets from Documentation/IMP_DETL.lay.  The prior
# extension omitted rate_prov and used general-import fields with consumption
# duties.  Keep these positions explicit and covered by tests.
DETAIL_COLSPECS = [
    (0, 10),    # commodity
    (10, 14),   # country
    (14, 16),   # subcountry
    (16, 18),   # district of entry
    (18, 20),   # district of unlading
    (20, 22),   # rate provision
    (22, 26),   # year
    (26, 28),   # month
    (43, 58),   # consumption quantity 1
    (58, 73),   # consumption quantity 2
    (73, 88),   # consumption customs value
    (88, 103),  # consumption dutiable value
    (103, 118), # calculated duty
    (118, 133), # consumption charges
    (133, 148), # consumption CIF
    (148, 163), # general quantity 1 (diagnostic)
    (163, 178), # general quantity 2 (diagnostic)
    (178, 193), # general customs value (diagnostic)
    (193, 208), # general charges (diagnostic)
    (208, 223), # general CIF (diagnostic)
]
DETAIL_NAMES = [
    "hs10",
    "partner_code",
    "partner_subcode",
    "district_entry",
    "district_unlading",
    "rate_prov",
    "year",
    "month",
    "con_qy1_mo",
    "con_qy2_mo",
    "con_val_mo",
    "dut_val_mo",
    "cal_dut_mo",
    "con_cha_mo",
    "con_cif_mo",
    "gen_qy1_mo",
    "gen_qy2_mo",
    "gen_val_mo",
    "gen_cha_mo",
    "gen_cif_mo",
]
NUMERIC_FIELDS = DETAIL_NAMES[8:]
RATE_KEY = ["partner_code", "hs10", "rate_prov", "year", "month"]
VARIETY_KEY = ["partner_code", "hs10", "year", "month"]
COUNTRY_COLSPECS = [(0, 4), (11, 61)]


def repo_relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _hash_records(records: list[dict[str, Any]], fields: tuple[str, ...]) -> str:
    payload = [{field: record.get(field) for field in fields} for record in records]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _archive_path(config: PipelineConfig, period: str) -> Path:
    return config.raw_dir / "trade" / "imports" / f"IMDB{period[2:4]}{period[5:7]}.ZIP"


def _member(archive: zipfile.ZipFile, expected: str) -> str:
    for candidate in archive.namelist():
        if candidate.lower() == expected.lower():
            return candidate
    raise KeyError(f"{expected} not found in {archive.filename}")


def _chunks(path: Path, chunksize: int = 250_000) -> Iterator[pd.DataFrame]:
    archive = zipfile.ZipFile(path)
    wrapper = TextIOWrapper(
        archive.open(_member(archive, DETAIL_MEMBER)),
        encoding="latin1",
        errors="strict",
    )
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


def _countries(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        with archive.open(_member(archive, COUNTRY_MEMBER)) as handle:
            frame = pd.read_fwf(
                TextIOWrapper(handle, encoding="latin1", errors="strict"),
                colspecs=COUNTRY_COLSPECS,
                names=["partner_code", "partner_name"],
                dtype="string",
            )
    frame["partner_code"] = frame["partner_code"].str.strip().str.zfill(4)
    frame["partner_name"] = frame["partner_name"].str.strip().str.upper()
    return frame.drop_duplicates("partner_code")


def _prepare_chunk(chunk: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
    chunk["partner_code"] = chunk["partner_code"].str.strip().str.zfill(4)
    chunk["rate_prov"] = chunk["rate_prov"].str.strip().str.zfill(2)
    chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
    chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce")
    for column in NUMERIC_FIELDS:
        chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
    return chunk.loc[
        (chunk["year"] == year)
        & (chunk["month"] == month)
        & chunk["hs10"].notna()
        & chunk["partner_code"].notna()
        & chunk["rate_prov"].notna()
    ]


def _aggregate_rate_provisions(pieces: list[pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(pieces, ignore_index=True)
    return (
        combined.groupby(RATE_KEY, as_index=False, dropna=False)[NUMERIC_FIELDS]
        .sum(min_count=1)
        .sort_values(RATE_KEY, kind="mergesort")
        .reset_index(drop=True)
    )


def _variety_panel(rate_frame: pd.DataFrame, countries: pd.DataFrame, period: str) -> pd.DataFrame:
    sums = (
        rate_frame.groupby(VARIETY_KEY, as_index=False, dropna=False)[NUMERIC_FIELDS]
        .sum(min_count=1)
    )
    flags = (
        rate_frame.assign(
            duty_reported=rate_frame["cal_dut_mo"].notna(),
            duty_missing=rate_frame["cal_dut_mo"].isna(),
            provision79=rate_frame["rate_prov"].eq("79"),
            provision79_value=np.where(
                rate_frame["rate_prov"].eq("79"),
                rate_frame["con_val_mo"].fillna(0),
                0,
            ),
        )
        .groupby(VARIETY_KEY, as_index=False)
        .agg(
            rate_provision_count=("rate_prov", "nunique"),
            duty_reported_rate_rows=("duty_reported", "sum"),
            duty_missing_rate_rows=("duty_missing", "sum"),
            rate_provision79_present=("provision79", "max"),
            rate_provision79_value=("provision79_value", "sum"),
        )
    )
    out = sums.merge(flags, on=VARIETY_KEY, how="left")
    out = out.merge(countries, on="partner_code", how="left")
    out["period"] = period
    out["flow"] = "imports"
    out["quantity_missing"] = out["con_qy1_mo"].isna()
    out["quantity_zero"] = out["con_qy1_mo"].eq(0)
    out["quantity_positive"] = out["con_qy1_mo"].gt(0)
    value = out["con_val_mo"].where(out["con_val_mo"] > 0)
    quantity = out["con_qy1_mo"].where(out["con_qy1_mo"] > 0)
    recorded_duty = out["cal_dut_mo"]
    out["applied_tariff"] = recorded_duty / value
    out["log_one_plus_applied_tariff"] = np.log1p(out["applied_tariff"])
    out["import_value"] = out["con_val_mo"]
    out["quantity"] = out["con_qy1_mo"]
    out["before_tariff_unit_value"] = value / quantity
    out["duty_inclusive_unit_value"] = (value + recorded_duty) / quantity
    out["rate_provision79_value_share"] = out["rate_provision79_value"] / value
    out["duty_measure_incomplete"] = out["rate_provision79_present"] | out["duty_missing_rate_rows"].gt(0)
    # Compatibility aliases are local to the new workhorse.  They now have
    # the paper's consumption definitions, not the old CIF definitions.
    out["m_val"] = out["import_value"] / 1_000_000.0
    out["m_q1"] = out["quantity"] / 1_000_000.0
    out["m_p"] = out["before_tariff_unit_value"]
    out["m_pduty"] = out["duty_inclusive_unit_value"]
    out = add_hierarchy_codes(out, "hs10")
    return out.sort_values(VARIETY_KEY, kind="mergesort").reset_index(drop=True)


def build_trade_month(
    config: PipelineConfig,
    period: str,
    *,
    overwrite: bool = False,
    chunksize: int = 250_000,
    output_namespace: str = "fk2025",
) -> dict[str, Any]:
    source = _archive_path(config, period)
    if not source.exists():
        raise FileNotFoundError(source)
    year, month = int(period[:4]), int(period[5:7])
    root = config.processed_trade_dir / output_namespace
    rate_path = root / "rate_provision" / f"year={year}" / f"month={month:02d}" / "part.parquet"
    variety_path = root / "variety_month" / f"year={year}" / f"month={month:02d}" / "part.parquet"
    audit_path = root / "audits" / f"imports_{period}.json"
    source_hash = sha256_file(source)
    if rate_path.exists() and variety_path.exists() and audit_path.exists() and not overwrite:
        prior = json.loads(audit_path.read_text(encoding="utf-8"))
        if prior.get("source_sha256") == source_hash and prior.get("version") == VERSION:
            return {**prior, "build_action": "reused_valid_partition"}

    rate_pieces: list[pd.DataFrame] = []
    source_rows = retained_rows = 0
    source_totals = {column: 0.0 for column in ("con_val_mo", "con_qy1_mo", "cal_dut_mo")}
    source_nonmissing = {column: 0 for column in source_totals}
    for chunk in _chunks(source, chunksize=chunksize):
        source_rows += len(chunk)
        prepared = _prepare_chunk(chunk, year, month)
        retained_rows += len(prepared)
        for column in source_totals:
            source_totals[column] += float(prepared[column].sum(skipna=True))
            source_nonmissing[column] += int(prepared[column].notna().sum())
        if not prepared.empty:
            rate_pieces.append(
                prepared.groupby(RATE_KEY, as_index=False, dropna=False)[NUMERIC_FIELDS]
                .sum(min_count=1)
            )
    if not rate_pieces:
        raise RuntimeError(f"{source} produced no valid rows for {period}")

    rate_frame = _aggregate_rate_provisions(rate_pieces)
    variety = _variety_panel(rate_frame, _countries(source), period)
    for frame in (rate_frame, variety):
        frame["source_archive"] = repo_relative(config, source)
        frame["source_member"] = DETAIL_MEMBER
        frame["source_sha256"] = source_hash
        frame["parser_version"] = VERSION
    write_parquet(rate_frame, rate_path, overwrite=True)
    write_parquet(variety, variety_path, overwrite=True)

    output_totals = {
        column: float(variety[column].sum(skipna=True))
        for column in source_totals
    }
    differences = {
        column: output_totals[column] - source_totals[column]
        for column in source_totals
    }
    tolerances = {
        column: max(1.0, 1e-8 * abs(source_totals[column]))
        for column in source_totals
    }
    audit = {
        "version": VERSION,
        "output_namespace": output_namespace,
        "period": period,
        "source_archive": repo_relative(config, source),
        "source_member": DETAIL_MEMBER,
        "source_sha256": source_hash,
        "rate_provision_partition": repo_relative(config, rate_path),
        "rate_provision_partition_sha256": sha256_file(rate_path),
        "variety_partition": repo_relative(config, variety_path),
        "variety_partition_sha256": sha256_file(variety_path),
        "source_rows": source_rows,
        "retained_source_rows": retained_rows,
        "rate_provision_rows": len(rate_frame),
        "output_rows": len(variety),
        "duplicate_rate_keys": int(rate_frame.duplicated(RATE_KEY).sum()),
        "duplicate_variety_keys": int(variety.duplicated(VARIETY_KEY).sum()),
        "source_totals": source_totals,
        "source_nonmissing": source_nonmissing,
        "output_totals": output_totals,
        "reconciliation_differences": differences,
        "reconciliation_tolerances": tolerances,
        "reconciliation_pass": all(
            abs(differences[column]) <= tolerances[column] for column in differences
        ),
        "rate_provisions": sorted(rate_frame["rate_prov"].dropna().unique().tolist()),
        "rate_provision79_value": float(variety["rate_provision79_value"].sum()),
        "quantity_missing_rows": int(variety["quantity_missing"].sum()),
        "quantity_zero_rows": int(variety["quantity_zero"].sum()),
        "quantity_positive_rows": int(variety["quantity_positive"].sum()),
        "build_action": "built",
    }
    write_metadata_json(audit_path, audit)
    return audit


def build_event_horizon_extension(
    config: PipelineConfig,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Build only the extra outcome months needed by the 2018 h=24 exercise.

    The locked paper-window partitions under ``fk2025`` are never rewritten.
    """
    namespace = "fk2025_event_horizon_extension"
    periods = list(
        iter_months(*EVENT_HORIZON_EXTENSION["trade_war_2018"])
    )
    audits = [
        build_trade_month(
            config,
            period,
            overwrite=overwrite,
            output_namespace=namespace,
        )
        for period in periods
    ]
    frame = pd.DataFrame(audits).sort_values("period")
    root = config.processed_trade_dir / namespace
    write_parquet(
        frame,
        root / "monthly_reconciliation.parquet",
        overwrite=True,
    )
    frame[
        [
            "period",
            "output_rows",
            "reconciliation_pass",
            "quantity_missing_rows",
            "quantity_zero_rows",
            "rate_provision79_value",
        ]
    ].to_csv(root / "monthly_reconciliation.csv", index=False)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": (
            "passed"
            if bool(frame["reconciliation_pass"].all())
            else "failed"
        ),
        "role": "additional_outcome_months_for_2018_h24_event_extension",
        "output_namespace": namespace,
        "months": len(frame),
        "start_period": min(periods),
        "end_period": max(periods),
        "rows": int(frame["output_rows"].sum()),
        "rate_provision_rows": int(frame["rate_provision_rows"].sum()),
        "duplicate_rate_keys": int(frame["duplicate_rate_keys"].sum()),
        "duplicate_variety_keys": int(frame["duplicate_variety_keys"].sum()),
        "reconciliation_failures": int(
            (~frame["reconciliation_pass"]).sum()
        ),
        "source_set_fingerprint": _hash_records(
            audits,
            ("period", "source_sha256"),
        ),
        "partition_set_fingerprint": _hash_records(
            audits,
            (
                "period",
                "rate_provision_partition_sha256",
                "variety_partition_sha256",
            ),
        ),
        "policy_columns_present": False,
    }
    write_metadata_json(root / "trade_manifest.json", manifest)
    return manifest


def local_trade_inventory(config: PipelineConfig) -> dict[str, Any]:
    records = []
    for episode, (start, end) in EPISODES.items():
        for period in iter_months(start, end):
            path = _archive_path(config, period)
            records.append({
                "episode": episode,
                "period": period,
                "path": repo_relative(config, path),
                "exists": path.exists(),
                "bytes": path.stat().st_size if path.exists() else None,
            })
    missing = [row["period"] for row in records if not row["exists"]]
    return {
        "version": VERSION,
        "official_page": CENSUS_IMPORT_PAGE,
        "required_months": len(records),
        "missing_periods": missing,
        "records": records,
        "status": "passed" if not missing else "blocked_missing_archives",
    }


def build_trade_extension(
    config: PipelineConfig,
    *,
    overwrite: bool = False,
    episodes: tuple[str, ...] = tuple(EPISODES),
) -> dict[str, Any]:
    inventory = local_trade_inventory(config)
    root = config.processed_trade_dir / "fk2025"
    write_metadata_json(root / "input_inventory.json", inventory)
    if inventory["missing_periods"]:
        return inventory
    periods: list[str] = []
    for episode in episodes:
        start, end = EPISODES[episode]
        periods.extend(iter_months(start, end))
    periods = sorted(set(periods))
    audits = [build_trade_month(config, period, overwrite=overwrite) for period in periods]
    frame = pd.DataFrame(audits).sort_values("period")
    write_parquet(frame, root / "monthly_reconciliation.parquet", overwrite=True)
    compact = pd.DataFrame({
        "period": frame["period"],
        "output_rows": frame["output_rows"],
        "reconciliation_pass": frame["reconciliation_pass"],
        "quantity_missing_rows": frame["quantity_missing_rows"],
        "quantity_zero_rows": frame["quantity_zero_rows"],
        "rate_provision79_value": frame["rate_provision79_value"],
    })
    compact.to_csv(root / "monthly_reconciliation.csv", index=False)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if frame["reconciliation_pass"].all() else "failed",
        "episodes": list(episodes),
        "months": len(frame),
        "start_period": min(periods),
        "end_period": max(periods),
        "rows": int(frame["output_rows"].sum()),
        "rate_provision_rows": int(frame["rate_provision_rows"].sum()),
        "duplicate_rate_keys": int(frame["duplicate_rate_keys"].sum()),
        "duplicate_variety_keys": int(frame["duplicate_variety_keys"].sum()),
        "reconciliation_failures": int((~frame["reconciliation_pass"]).sum()),
        "source_set_fingerprint": _hash_records(audits, ("period", "source_sha256")),
        "partition_set_fingerprint": _hash_records(
            audits, ("period", "rate_provision_partition_sha256", "variety_partition_sha256")
        ),
        "primary_value": "imports_for_consumption_customs_value",
        "primary_tariff": "recorded_calculated_duty_divided_by_consumption_customs_value",
        "policy_columns_present": False,
    }
    write_metadata_json(root / "trade_manifest.json", manifest)
    return manifest


def run(
    config: PipelineConfig,
    *,
    build_trade: bool = True,
    overwrite: bool = False,
    episodes: tuple[str, ...] = tuple(EPISODES),
) -> dict[str, Any]:
    config.ensure_directories()
    trade = (
        build_trade_extension(config, overwrite=overwrite, episodes=episodes)
        if build_trade
        else local_trade_inventory(config)
    )
    policy: dict[str, Any] = {"status": "not_requested"}
    if build_trade and "tariffs_2025" in episodes and trade.get("status") == "passed":
        from .policy_extension_2025 import build_observable_policy_panel

        policy = build_observable_policy_panel(config)
    return {"version": VERSION, "trade": trade, "statutory_policy": policy}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--build-event-horizon-extension", action="store_true")
    parser.add_argument("--episode", choices=(*EPISODES, "all"), default="all")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    episodes = tuple(EPISODES) if args.episode == "all" else (args.episode,)
    config = PipelineConfig.default()
    if args.build_event_horizon_extension:
        result = build_event_horizon_extension(
            config,
            overwrite=args.overwrite,
        )
    else:
        result = run(
            config,
            build_trade=not args.inventory_only,
            overwrite=args.overwrite,
            episodes=episodes,
        )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
