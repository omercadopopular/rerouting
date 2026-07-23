"""Construct the February-2025 trade and tariff extension.

The module deliberately separates two claims:

* Census archive ingestion is releaseable once every monthly partition
  reconciles to its source archive.
* Policy treatment is releaseable only when every included action has a
  reviewed product/partner scope, rate, dates, exclusions, and stacking rule.

No regression runner is allowed to treat a partial source inventory as a
validated statutory tariff panel.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import re
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Iterable, Iterator

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

VERSION = "extension_2025_v1"
EVENT_PERIOD = "2025-02"
TRADE_START = "2024-08"
TARGET_POST_HORIZON = 24
SHORT_POST_HORIZON = 6
CHINA_HK_CODES = ("5700", "5820")

CENSUS_IMPORT_PAGE = "https://www.census.gov/foreign-trade/data/IMDB.html"
CENSUS_EXPORT_PAGE = "https://www.census.gov/foreign-trade/data/EXDB.html"

DETAIL_MEMBER = "IMP_DETL.TXT"
COUNTRY_MEMBER = "COUNTRY.TXT"
DETAIL_COLSPECS = [
    (0, 10), (10, 14), (22, 26), (26, 28), (148, 163),
    (178, 193), (208, 223), (88, 103), (103, 118),
]
DETAIL_NAMES = [
    "hs10", "partner_code", "year", "month", "gen_qy1_mo",
    "gen_val_mo", "gen_cif_mo", "dut_val_mo", "cal_dut_mo",
]
COUNTRY_COLSPECS = [(0, 4), (11, 61)]


@dataclass(frozen=True)
class PolicySource:
    action_id: str
    legal_authority: str
    policy_family: str
    partner_scope: str
    hts_heading: str
    legal_effective_date: str
    legal_end_date: str | None
    additional_rate: float | None
    rate_semantics: str
    stacking_rule: str
    scope_status: str
    official_url: str
    source_note: str


# This is a source ledger, not a product-level treatment ledger.  Actions with
# incomplete monthly-HS10 scope remain explicit blockers below.
POLICY_SOURCES: tuple[PolicySource, ...] = (
    PolicySource(
        "china_ieepa_fentanyl_10_feb",
        "IEEPA; Executive Order 14195",
        "IEEPA_CHINA_FENTANYL",
        "China and Hong Kong",
        "9903.01.20",
        "2025-02-04",
        "2025-03-03",
        0.10,
        "total_action_component_rate",
        "additive_to_all_other_duties",
        "broad_scope_reviewed_entry_exceptions_unobservable",
        "https://www.federalregister.gov/documents/full_text/html/2025/02/07/2025-02408.html",
        "All PRC articles; in-transit and Chapter-98 exceptions require entry data.",
    ),
    PolicySource(
        "china_ieepa_fentanyl_20_mar",
        "IEEPA; amendment to Executive Order 14195",
        "IEEPA_CHINA_FENTANYL",
        "China and Hong Kong",
        "9903.01.24",
        "2025-03-04",
        "2025-11-09",
        0.20,
        "total_action_component_rate_replacing_prior_10_percent",
        "replaces_prior_fentanyl_component; additive_to_other_duties",
        "broad_scope_reviewed_entry_exceptions_unobservable",
        "https://www.federalregister.gov/d/2025-03677",
        "Cumulative fentanyl component is 20 percent from March 4.",
    ),
    PolicySource(
        "china_ieepa_fentanyl_10_nov",
        "IEEPA; United States--PRC arrangement",
        "IEEPA_CHINA_FENTANYL",
        "China and Hong Kong",
        "9903.01.24",
        "2025-11-10",
        None,
        0.10,
        "total_action_component_rate_replacing_prior_20_percent",
        "replaces_prior_fentanyl_component; additive_to_other_duties",
        "broad_scope_reviewed_entry_exceptions_unobservable",
        "https://www.whitehouse.gov/fact-sheets/2025/11/fact-sheet-president-donald-j-trump-strikes-deal-on-economic-and-trade-relations-with-china/",
        "Ten percentage-point reduction effective November 10, 2025.",
    ),
    PolicySource(
        "reciprocal_baseline_apr",
        "IEEPA; Executive Order 14257",
        "IEEPA_RECIPROCAL",
        "All partners subject to enumerated exceptions",
        "9903.01.25",
        "2025-04-05",
        None,
        0.10,
        "total_reciprocal_component_rate_not_increment_from_prior_month",
        "not_stacked_on_232_or_annex_ii_exclusions",
        "blocked_product_exclusion_annex_not_materialized",
        "https://www.federalregister.gov/documents/full_text/html/2025/04/07/2025-06063.html",
        "Annex II and later partner-specific modifications must be materialized.",
    ),
    PolicySource(
        "china_reciprocal_escalation_apr",
        "IEEPA; Executive Orders 14259 and 14266",
        "IEEPA_RECIPROCAL",
        "China including Hong Kong and Macau",
        "9903.01.63",
        "2025-04-10",
        "2025-05-13",
        1.25,
        "total_reciprocal_component_rate_not_increment_from_prior_month",
        "replaces_reciprocal_component_only",
        "blocked_short_lived_entry_and_annex_exceptions",
        "https://www.federalregister.gov/documents/full_text/html/2025/04/15/2025-06462.html",
        "The reciprocal component was 125 percent on April 10 before suspension.",
    ),
    PolicySource(
        "china_reciprocal_10_may",
        "IEEPA; Executive Order 14298",
        "IEEPA_RECIPROCAL",
        "China including Hong Kong and Macau",
        "9903.01.25",
        "2025-05-14",
        None,
        0.10,
        "total_reciprocal_component_rate_replacing_suspended_125_percent",
        "replaces_reciprocal_component_only",
        "blocked_annex_exceptions_not_materialized",
        "https://www.federalregister.gov/documents/full_text/html/2025/05/21/2025-09297.html",
        "Heightened China reciprocal component suspended; 10 percent retained.",
    ),
    PolicySource(
        "steel_aluminum_232_mar",
        "Section 232; Proclamations 10895 and 10896",
        "SECTION_232_METALS",
        "All partners, subject to product and content rules",
        "9903.80/9903.85",
        "2025-03-12",
        None,
        0.25,
        "total_section_232_component_rate; incremental_change_depends_on_inherited_scope",
        "sector_component; reciprocal_tariff_excluded",
        "blocked_content_based_and_derivative_scope",
        "https://www.federalregister.gov/documents/full_text/html/2025/02/18/2025-02833.html",
        "Some derivative duties apply only to declared metal content.",
    ),
    PolicySource(
        "steel_aluminum_232_jun",
        "Section 232; Proclamation 10947",
        "SECTION_232_METALS",
        "All partners, subject to product and content rules",
        "9903.80/9903.85",
        "2025-06-04",
        None,
        0.50,
        "total_section_232_component_rate_replacing_25_percent",
        "replaces_25_percent_sector_component",
        "blocked_content_based_and_derivative_scope",
        "https://www.federalregister.gov/documents/full_text/html/2025/06/09/2025-10524.html",
        "Rate increased from 25 to 50 percent.",
    ),
    PolicySource(
        "automobiles_232_apr",
        "Section 232; Proclamation 10908",
        "SECTION_232_AUTOS",
        "All partners, subject to USMCA and content rules",
        "9903.94/9903.95",
        "2025-04-03",
        None,
        0.25,
        "total_section_232_component_rate",
        "sector_component; reciprocal_tariff_excluded",
        "blocked_non_us_content_and_usmca_entry_rules",
        "https://www.federalregister.gov/documents/full_text/html/2025/04/03/2025-05930.html",
        "Importer/model-specific non-US content is unavailable at monthly HS10.",
    ),
)


def repo_relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _fingerprint_records(records: Iterable[dict[str, Any]], fields: tuple[str, ...]) -> str:
    payload = [{field: record.get(field) for field in fields} for record in records]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def paper_event_period(effective_date: str | date | pd.Timestamp) -> str:
    stamp = pd.Timestamp(effective_date)
    assigned = stamp if stamp.day <= 15 else stamp + pd.offsets.MonthBegin(1)
    return str(assigned.to_period("M"))


def supported_post_horizon(latest_period: str, event_period: str = EVENT_PERIOD, target: int = TARGET_POST_HORIZON) -> int:
    latest = pd.Period(latest_period, freq="M")
    event = pd.Period(event_period, freq="M")
    available = (latest.year - event.year) * 12 + latest.month - event.month
    return max(-1, min(int(target), available))


def day_weighted_rate(intervals: Iterable[tuple[str, str | None, float]], period: str) -> float:
    """Average a component using the locked paper-arithmetic convention.

    The registered historical reconstruction counts ``D-d`` initial-month
    days for an action effective on day ``d`` of a ``D``-day month.  The
    effective day itself is therefore excluded; later full months retain all
    calendar days.  This intentionally matches the locked historical code.
    """
    month = pd.Period(period, freq="M")
    start = month.start_time.normalize()
    end = month.end_time.normalize()
    total = 0.0
    for effective, terminal, rate in intervals:
        action_start = pd.Timestamp(effective).normalize()
        action_end = pd.Timestamp(terminal).normalize() if terminal else pd.Timestamp.max.normalize()
        overlap_start = max(start, action_start)
        overlap_end = min(end, action_end)
        if overlap_start <= overlap_end:
            active_days = (overlap_end - overlap_start).days + 1
            if action_start.to_period("M") == month and overlap_start == action_start:
                active_days -= 1
            total += float(rate) * max(active_days, 0)
    return total / calendar.monthrange(month.year, month.month)[1]


def _archive_path(config: PipelineConfig, period: str) -> Path:
    return config.raw_dir / "trade" / "imports" / f"IMDB{period[2:4]}{period[5:7]}.ZIP"


def local_trade_inventory(config: PipelineConfig) -> dict[str, Any]:
    directory = config.raw_dir / "trade" / "imports"
    rows = []
    for path in sorted(directory.glob("IMDB????.ZIP")):
        match = re.fullmatch(r"IMDB(\d{2})(\d{2})\.ZIP", path.name, flags=re.I)
        if not match:
            continue
        year, month = 2000 + int(match.group(1)), int(match.group(2))
        if 1 <= month <= 12:
            rows.append({"period": f"{year:04d}-{month:02d}", "path": repo_relative(config, path), "bytes": path.stat().st_size})
    periods = sorted(row["period"] for row in rows)
    latest = max(periods) if periods else None
    required = iter_months(TRADE_START, latest) if latest else []
    missing = sorted(set(required).difference(periods))
    return {
        "official_page": CENSUS_IMPORT_PAGE,
        "archives": rows,
        "latest_local_period": latest,
        "required_start": TRADE_START,
        "missing_required_periods": missing,
        "short_horizon_complete": bool(latest and supported_post_horizon(latest, target=SHORT_POST_HORIZON) >= SHORT_POST_HORIZON and not missing),
        "maximum_supported_post_horizon": supported_post_horizon(latest) if latest else -1,
        "target_post_horizon": TARGET_POST_HORIZON,
    }


def _member(archive: zipfile.ZipFile, expected: str) -> str:
    for candidate in archive.namelist():
        if candidate.lower() == expected.lower():
            return candidate
    raise KeyError(f"{expected} not found in {archive.filename}")


def _chunks(path: Path, chunksize: int = 250_000) -> Iterator[pd.DataFrame]:
    archive = zipfile.ZipFile(path)
    wrapper = TextIOWrapper(archive.open(_member(archive, DETAIL_MEMBER)), encoding="latin1", errors="ignore")
    try:
        yield from pd.read_fwf(wrapper, colspecs=DETAIL_COLSPECS, names=DETAIL_NAMES, dtype="string", chunksize=chunksize)
    finally:
        wrapper.close()
        archive.close()


def _countries(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        with archive.open(_member(archive, COUNTRY_MEMBER)) as handle:
            frame = pd.read_fwf(TextIOWrapper(handle, encoding="latin1", errors="ignore"), colspecs=COUNTRY_COLSPECS, names=["partner_code", "partner_name"], dtype="string")
    frame["partner_code"] = frame["partner_code"].str.strip().str.zfill(4)
    frame["partner_name"] = frame["partner_name"].str.strip().str.upper()
    return frame.drop_duplicates("partner_code")


def build_trade_month(config: PipelineConfig, period: str, *, overwrite: bool = False) -> dict[str, Any]:
    source = _archive_path(config, period)
    if not source.exists():
        raise FileNotFoundError(source)
    year, month = int(period[:4]), int(period[5:7])
    destination = config.processed_trade_dir / "extension_2025" / f"year={year:04d}" / f"month={month:02d}" / "part.parquet"
    audit_path = config.processed_trade_dir / "extension_2025" / "audits" / f"imports_{period}.json"
    source_hash = sha256_file(source)
    if destination.exists() and audit_path.exists() and not overwrite:
        prior = json.loads(audit_path.read_text(encoding="utf-8"))
        if prior.get("source_sha256") == source_hash and prior.get("version") == VERSION:
            return {**prior, "build_action": "reused_valid_partition"}

    numeric = ["gen_qy1_mo", "gen_val_mo", "gen_cif_mo", "dut_val_mo", "cal_dut_mo"]
    pieces: list[pd.DataFrame] = []
    source_rows = discarded = 0
    for chunk in _chunks(source):
        source_rows += len(chunk)
        chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
        chunk["partner_code"] = chunk["partner_code"].str.strip().str.zfill(4)
        chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
        chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce")
        for column in numeric:
            chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
        before = len(chunk)
        chunk = chunk.loc[(chunk["year"] == year) & (chunk["month"] == month)].dropna(subset=["partner_code", "hs10"])
        discarded += before - len(chunk)
        if not chunk.empty:
            pieces.append(chunk)
    if not pieces:
        raise RuntimeError(f"{source} produced no rows for {period}")
    raw = pd.concat(pieces, ignore_index=True)
    grouped = raw.groupby(["partner_code", "hs10", "year", "month"], as_index=False)[numeric].sum(min_count=1)
    grouped = grouped.merge(_countries(source), on="partner_code", how="left")
    grouped["period"] = period
    grouped["flow"] = "imports"
    grouped["quantity_missing"] = grouped["gen_qy1_mo"].isna()
    grouped["quantity_zero"] = grouped["gen_qy1_mo"].eq(0)
    grouped["m_val"] = grouped["gen_cif_mo"] / 1_000_000.0
    grouped["m_q1"] = grouped["gen_qy1_mo"] / 1_000_000.0
    denominator = grouped["gen_qy1_mo"].where(grouped["gen_qy1_mo"] > 0)
    grouped["m_p"] = grouped["gen_cif_mo"] / denominator
    grouped["m_pduty"] = (grouped["gen_cif_mo"] + grouped["cal_dut_mo"]) / denominator
    grouped = add_hierarchy_codes(grouped, "hs10")
    grouped["source_archive"] = repo_relative(config, source)
    grouped["source_member"] = DETAIL_MEMBER
    grouped["source_sha256"] = source_hash
    grouped["parser_version"] = VERSION
    grouped = grouped.sort_values(["partner_code", "hs10"]).reset_index(drop=True)
    write_parquet(grouped, destination, overwrite=True)
    source_total = float(raw["gen_cif_mo"].sum(min_count=1))
    output_total = float(grouped["gen_cif_mo"].sum(min_count=1))
    tolerance = max(1.0, 1e-8 * abs(source_total))
    audit = {
        "version": VERSION,
        "period": period,
        "source_archive": repo_relative(config, source),
        "source_sha256": source_hash,
        "source_member": DETAIL_MEMBER,
        "partition": repo_relative(config, destination),
        "partition_sha256": sha256_file(destination),
        "source_rows": source_rows,
        "discarded_period_rows": discarded,
        "output_rows": len(grouped),
        "duplicate_keys": int(grouped.duplicated(["partner_code", "hs10", "year", "month"]).sum()),
        "source_cif_total": source_total,
        "output_cif_total": output_total,
        "reconciliation_difference": output_total - source_total,
        "reconciliation_tolerance": tolerance,
        "reconciliation_pass": abs(output_total - source_total) <= tolerance,
        "quantity_missing_rows": int(grouped["quantity_missing"].sum()),
        "quantity_zero_rows": int(grouped["quantity_zero"].sum()),
        "calculated_duty_nonmissing_rows": int(grouped["cal_dut_mo"].notna().sum()),
        "policy_columns_present": False,
        "build_action": "built",
    }
    write_metadata_json(audit_path, audit)
    return audit


def build_trade_extension(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    inventory = local_trade_inventory(config)
    latest = inventory["latest_local_period"]
    if not latest:
        raise FileNotFoundError(config.raw_dir / "trade" / "imports")
    periods = iter_months(TRADE_START, latest)
    missing = inventory["missing_required_periods"]
    if missing:
        result = {"version": VERSION, "status": "blocked_missing_archives", "missing": missing, **inventory}
        write_metadata_json(config.processed_trade_dir / "extension_2025" / "trade_extension_manifest.json", result)
        return result
    audits = [build_trade_month(config, period, overwrite=overwrite) for period in periods]
    audit = pd.DataFrame(audits).sort_values("period")
    root = config.processed_trade_dir / "extension_2025"
    write_parquet(audit, root / "monthly_reconciliation.parquet", overwrite=True)
    audit[["period", "output_rows", "reconciliation_difference", "reconciliation_pass"]].to_csv(root / "monthly_reconciliation.csv", index=False)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if audit["reconciliation_pass"].all() and not audit["duplicate_keys"].any() else "failed",
        "source_mode": "official_census_archive_native",
        "start_period": TRADE_START,
        "end_period": latest,
        "months": len(audit),
        "rows": int(audit["output_rows"].sum()),
        "duplicate_keys": int(audit["duplicate_keys"].sum()),
        "reconciliation_failures": int((~audit["reconciliation_pass"].astype(bool)).sum()),
        "built_months_this_invocation": int(audit["build_action"].eq("built").sum()),
        "reused_months_this_invocation": int(audit["build_action"].eq("reused_valid_partition").sum()),
        "source_set_fingerprint": _fingerprint_records(audits, ("period", "source_sha256")),
        "partition_set_fingerprint": _fingerprint_records(audits, ("period", "partition_sha256")),
        "short_horizon_complete": inventory["short_horizon_complete"],
        "maximum_supported_post_horizon": inventory["maximum_supported_post_horizon"],
        "target_post_horizon": TARGET_POST_HORIZON,
        "policy_columns_present": False,
    }
    write_metadata_json(root / "trade_extension_manifest.json", manifest)
    return manifest


def build_policy_source_inventory(config: PipelineConfig) -> dict[str, Any]:
    root = config.processed_tariff_dir / "extension_2025"
    source_frame = pd.DataFrame(asdict(source) for source in POLICY_SOURCES)
    source_frame["paper_event_period"] = source_frame["legal_effective_date"].map(paper_event_period)
    write_parquet(source_frame, root / "policy_source_ledger.parquet", overwrite=True)
    source_frame[["action_id", "policy_family", "partner_scope", "legal_effective_date", "additional_rate", "rate_semantics", "scope_status"]].to_csv(root / "policy_source_ledger_summary.csv", index=False)

    local_paths = [
        config.raw_dir / "policy" / "annual" / "tariff_data_2025.zip",
        config.raw_dir / "policy" / "annual" / "tariff_data_2026.zip",
        config.raw_dir / "policy" / "current" / "export" / "2026HTSRev5_chapter_99.csv",
    ]
    local_paths.extend(sorted((config.raw_dir / "policy" / "archive" / "pdf").glob("2025HTS*.pdf")))
    local = []
    for path in local_paths:
        local.append({
            "path": repo_relative(config, path),
            "exists": path.exists(),
            "bytes": path.stat().st_size if path.exists() else None,
            "sha256": sha256_file(path) if path.exists() else None,
        })
    local_frame = pd.DataFrame(local)
    write_parquet(local_frame, root / "policy_local_source_inventory.parquet", overwrite=True)
    blockers = sorted({source.scope_status for source in POLICY_SOURCES if source.scope_status.startswith("blocked")})
    unmaterialized = [
        "versioned product-level Annex-II reciprocal exclusions",
        "reviewed Section-232 derivative/content scope",
        "entry-level USMCA and non-US auto content",
        "complete partner-rate modification ledger through 2025-12",
        "independently reconstructed inherited 2024-12-31 MFN/201/232/301 baseline on the 2025 HS10 universe",
    ]
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "blocked_unmaterialized_product_ledger" if blockers or unmaterialized else "passed",
        "official_sources_only": True,
        "ledger_path": repo_relative(config, root / "policy_source_ledger.parquet"),
        "actions": len(source_frame),
        "scope_blockers": blockers,
        "unmaterialized_components": unmaterialized,
        "known_missing_official_sources": [],
        "official_source_completeness_review_pending": True,
        "local_sources": local,
        "local_2025_hts_release_count": int(sum(Path(item["path"]).name.startswith("2025HTS") for item in local)),
        "local_source_set_fingerprint": _fingerprint_records(local, ("path", "sha256")),
        "december_2024_baseline_required": True,
        "january_2025_changes_role": "pre_transition_carry_in_not_new_administration_treatment",
        "unresolved_values_filled_with_zero": False,
        "policy_gate": "failed",
        "event_estimation_authorized": False,
    }
    write_metadata_json(root / "policy_extension_manifest.json", manifest)
    write_metadata_json(root / "policy_missing_sources.json", {
        "version": VERSION,
        "known_missing_official_sources": [],
        "source_completeness_review_pending": True,
        "unmaterialized_components": unmaterialized,
        "status": "blocked_materialization_not_proven_missing_source",
    })
    return manifest


def run(config: PipelineConfig, *, build_trade: bool = True, overwrite: bool = False) -> dict[str, Any]:
    config.ensure_directories()
    inventory = local_trade_inventory(config)
    write_metadata_json(config.processed_trade_dir / "extension_2025" / "input_inventory.json", inventory)
    policy = build_policy_source_inventory(config)
    trade = build_trade_extension(config, overwrite=overwrite) if build_trade else {"status": "not_requested", **inventory}
    return {"version": VERSION, "trade": trade, "policy": policy}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    result = run(PipelineConfig.default(), build_trade=not args.inventory_only, overwrite=args.overwrite)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
