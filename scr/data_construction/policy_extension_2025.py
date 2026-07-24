"""Construct the statutory instrument for the Fajgelbaum--Khandelwal design.

The primary panel is deliberately limited to duties whose applicability is a
deterministic function of partner, ordinary HTS code, and calendar date.  A
monthly country--HS10 record cannot reveal quota tier, metal/US content,
USMCA certification, in-transit status, or importer attestations.  Products
potentially governed by those rules are therefore flagged and excluded from
the primary estimation sample; unresolved components are never set to zero.

Official 2025 HTS revision PDFs are parsed locally with the ``pdftotext``
binary shipped with Git for Windows.  No network access or package policy
variable enters this construction.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json, write_parquet
from .partner_reciprocal_2025 import (
    AUGUST_SCHEDULE_START,
    CHINA_RECIPROCAL_CODES,
    EU_PARTNER_CODES,
    GENERAL_EXEMPT_CODES,
    GENERAL_START,
    NON_FLAT_PARTNER_CODES,
    parse_april_country_rates,
    parsed_release_schedule,
    resolve_country_text,
)


VERSION = "fajgelbaum_khandelwal_2025_statutory_v3"
CHINA_HK_CODES = ("5700", "5820")
POLICY_START = "2024-01"
POLICY_END = "2025-12"

NOTE31_RATES = {
    "b": ("2024-09-27", 0.25),
    "c": ("2024-09-27", 0.50),
    "d": ("2024-09-27", 1.00),
    "e": ("2025-01-01", 0.25),
    "f": ("2025-01-01", 0.50),
    "j": ("2025-01-01", 0.25),
}

FENTANYL_INTERVALS = (
    ("2025-02-04", "2025-03-03", 0.10),
    ("2025-03-04", "2025-11-09", 0.20),
    ("2025-11-10", None, 0.10),
)

RECIPROCAL_CHINA_INTERVALS = (
    ("2025-04-05", "2025-04-08", 0.10),
    ("2025-04-09", "2025-04-09", 0.84),
    ("2025-04-10", "2025-05-13", 1.25),
    ("2025-05-14", None, 0.10),
)


def _repo_relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _hash_records(records: Iterable[dict[str, Any]]) -> str:
    payload = json.dumps(list(records), sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def _pdftotext() -> Path:
    located = shutil.which("pdftotext")
    candidates = [
        Path(located) if located else None,
        Path(r"C:\Program Files\Git\mingw64\bin\pdftotext.exe"),
        Path(r"C:\Program Files\Git\usr\bin\pdftotext.exe"),
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    raise FileNotFoundError("pdftotext is required to parse the local official HTS PDFs")


def _pdf_text(path: Path, first_page: int, last_page: int) -> str:
    result = subprocess.run(
        [str(_pdftotext()), "-f", str(first_page), "-l", str(last_page), "-layout", str(path), "-"],
        check=True,
        capture_output=True,
    )
    return result.stdout.decode("utf-8", errors="replace").replace("\r", "")


def _codes(text: str) -> tuple[str, ...]:
    values: set[str] = set()
    for token in re.findall(r"(?<!\d)(\d{4}(?:\.\d{2}){0,3})(?!\d)", text):
        digits = token.replace(".", "")
        if len(digits) in (4, 6, 8, 10) and not digits.startswith(("98", "99")):
            values.add(digits)
    return tuple(sorted(values))


def _section(text: str, start_pattern: str, end_pattern: str) -> str:
    start = re.search(start_pattern, text, flags=re.I | re.S)
    if not start:
        return ""
    end = re.search(end_pattern, text[start.end():], flags=re.I | re.S)
    terminal = start.end() + end.start() if end else len(text)
    return text[start.end():terminal]


def parse_reciprocal_exclusions(pdf_path: Path) -> tuple[str, ...]:
    """Return the product prefixes in note 2(v)(iii)(a) of a revision."""
    text = _pdf_text(pdf_path, 3350, 3650)
    body = _section(
        text,
        r"(?:\(iii\)|\(a\))\s+As\s+provided\s+(?:for\s+)?in\s+heading\s+9903\.01\.32.*?HTSUS:",
        r"\n\s*(?:\(b\)|\(iv\))\s+As\s+provided",
    )
    return _codes(body)


def parse_note31_rules(pdf_path: Path) -> dict[str, tuple[str, ...]]:
    """Parse the 2024 review and January-2025 Section 301 product lists."""
    text = _pdf_text(pdf_path, 3780, 3820)
    result: dict[str, tuple[str, ...]] = {}
    letters = ("b", "c", "d", "e", "f", "j")
    next_letter = {"b": "c", "c": "d", "d": "e", "e": "f", "f": "g", "j": "k"}
    for letter in letters:
        heading = {"b": "01", "c": "02", "d": "03", "e": "04", "f": "05", "j": "11"}[letter]
        later = next_letter[letter]
        body = _section(
            text,
            rf"\({letter}\)\s+Heading\s+9903\.91\.{heading}.*?:",
            rf"\n\s*\({later}\)\s+",
        )
        result[letter] = _codes(body)
    if not all(result.values()):
        missing = [letter for letter, values in result.items() if not values]
        raise ValueError(f"Could not parse Section 301 note 31 subdivisions: {missing}")
    return result


def parse_complex_scope_prefixes(pdf_path: Path) -> dict[str, tuple[str, ...]]:
    """Return conservative product unions for entry-dependent 232/auto rules."""
    notes = _pdf_text(pdf_path, 3520, 3810)
    metal16 = _section(notes, r"\n\s*16\.\s", r"\n\s*17\.\s")
    metal19 = _section(notes, r"\n\s*19\.\s", r"\n\s*20\.\s")
    auto = _pdf_text(pdf_path, 3810, 3845)
    auto33 = _section(auto, r"\n\s*33\.\s", r"\n\s*34\.\s")
    parsed = {
        "section232_note16": _codes(metal16),
        "section232_note19": _codes(metal19),
        "section232_auto_note33": _codes(auto33),
    }
    if not parsed["section232_auto_note33"]:
        raise ValueError("Could not parse automobile scope from U.S. note 33")
    return parsed


def _release_rows(config: PipelineConfig) -> list[dict[str, Any]]:
    details = config.raw_dir / "policy" / "archive" / "catalog" / "details"
    pdf_dir = config.raw_dir / "policy" / "archive" / "pdf"
    rows: list[dict[str, Any]] = []
    for metadata_path in sorted(details.glob("2025HTS*.json")):
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))["reldetail"]
        pdf = pdf_dir / f"{payload['name']}.pdf"
        target = pd.Timestamp(payload.get("target") or payload["releaseStartDate"])
        rows.append({
            "release_name": payload["name"],
            "effective_date": target.date().isoformat(),
            "release_start_date": pd.Timestamp(payload["releaseStartDate"]).date().isoformat(),
            "release_end_date": pd.Timestamp(payload["releaseEndDate"]).date().isoformat(),
            "pdf_path": _repo_relative(config, pdf),
            "pdf_sha256": sha256_file(pdf),
            "pdf": pdf,
        })
    return sorted(rows, key=lambda row: (row["effective_date"], row["release_name"]))


def build_revision_rules(config: PipelineConfig) -> dict[str, Any]:
    root = config.processed_tariff_dir / "fk2025"
    releases = _release_rows(config)
    reciprocal_path = root / "reciprocal_exclusion_revision_ledger.parquet"
    note31_path = root / "section301_2024_2025_modifications.parquet"
    complex_path = root / "excluded_complex_scope.parquet"
    # Reuse extracted rules only when their embedded source hashes match the
    # current local PDFs.  This avoids reparsing roughly 600 MB on every run
    # without allowing a stale extraction to survive a source revision.
    expected_hash = {row["release_name"]: row["pdf_sha256"] for row in releases}
    frame: pd.DataFrame | None = None
    if reciprocal_path.exists():
        candidate = pd.read_parquet(reciprocal_path)
        source_pairs = candidate[["release_name", "source_sha256"]].drop_duplicates() if {"release_name", "source_sha256"}.issubset(candidate.columns) else pd.DataFrame()
        sources_match = not source_pairs.empty and all(expected_hash.get(row.release_name) == row.source_sha256 for row in source_pairs.itertuples(index=False))
        if candidate["hts_prefix"].nunique() >= 100 and sources_match:
            frame = candidate
    if frame is None:
        rows: list[dict[str, Any]] = []
        for release in releases:
            codes = parse_reciprocal_exclusions(release["pdf"])
            for code in codes:
                rows.append({
                    "release_name": release["release_name"],
                    "effective_date": release["effective_date"],
                    "release_start_date": release["release_start_date"],
                    "release_end_date": release["release_end_date"],
                    "rule_kind": "reciprocal_annex_ii_exclusion",
                    "hts_prefix": code,
                    "source_path": release["pdf_path"],
                    "source_sha256": release["pdf_sha256"],
                })
        frame = pd.DataFrame(rows)
        if frame.empty or frame.loc[frame["effective_date"] >= "2025-04-05", "hts_prefix"].nunique() < 100:
            raise ValueError("Official reciprocal exclusion extraction did not produce a plausible product list")
        write_parquet(frame, reciprocal_path, overwrite=True)

    final_release = next(row for row in releases if row["release_name"] == "2025HTSRev32")
    final_pdf = final_release["pdf"]
    final_hash = final_release["pdf_sha256"]
    note31_frame: pd.DataFrame | None = pd.read_parquet(note31_path) if note31_path.exists() else None
    if note31_frame is None or note31_frame.empty or "source_sha256" not in note31_frame or set(note31_frame["source_sha256"].dropna()) != {final_hash}:
        note31 = parse_note31_rules(final_pdf)
        note31_rows = []
        for subdivision, codes in note31.items():
            effective, rate = NOTE31_RATES[subdivision]
            for code in codes:
                note31_rows.append({"subdivision": subdivision, "hts_prefix": code, "effective_date": effective, "section301_rate": rate, "source_path": final_release["pdf_path"], "source_sha256": final_hash})
        note31_frame = pd.DataFrame(note31_rows).drop_duplicates()
        write_parquet(note31_frame, note31_path, overwrite=True)

    complex_frame: pd.DataFrame | None = pd.read_parquet(complex_path) if complex_path.exists() else None
    if complex_frame is None or complex_frame.empty or "source_sha256" not in complex_frame or set(complex_frame["source_sha256"].dropna()) != {final_hash}:
        complex_rows = []
        for kind, codes in parse_complex_scope_prefixes(final_pdf).items():
            complex_rows.extend({"scope_kind": kind, "hts_prefix": code, "decision": "excluded_unobservable_entry_component", "source_path": final_release["pdf_path"], "source_sha256": final_hash} for code in codes)
        complex_frame = pd.DataFrame(complex_rows).drop_duplicates()
        write_parquet(complex_frame, complex_path, overwrite=True)
    return {
        "releases": releases,
        "reciprocal_rules": frame,
        "note31": note31_frame,
        "complex": complex_frame,
    }


def build_partner_reciprocal_ledger(
    config: PipelineConfig,
    releases: list[dict[str, Any]],
    partners: pd.DataFrame,
) -> pd.DataFrame:
    """Parse and cache partner-specific flat reciprocal schedules by revision."""

    root = config.processed_tariff_dir / "fk2025"
    destination = root / "partner_reciprocal_revision_ledger.parquet"
    expected = {
        release["release_name"]: release["pdf_sha256"]
        for release in releases
        if release["effective_date"] >= "2025-04-05"
    }
    if destination.exists():
        candidate = pd.read_parquet(destination)
        required = {
            "release_name",
            "source_sha256",
            "partner_code",
            "additional_rate",
        }
        observed = (
            dict(
                candidate[["release_name", "source_sha256"]]
                .drop_duplicates()
                .itertuples(index=False, name=None)
            )
            if required.issubset(candidate.columns)
            else {}
        )
        if observed == expected:
            return candidate

    rows: list[dict[str, Any]] = []
    for release in releases:
        if release["effective_date"] < "2025-04-05":
            continue
        schedule = parsed_release_schedule(release["pdf"], partners)
        rows.append(
            {
                "partner_code": "__DEFAULT__",
                "heading": "9903.01.25",
                "country_text": "ALL_OTHER_PARTNERS",
                "additional_rate": 0.10,
                "release_name": release["release_name"],
                "effective_date": release["effective_date"],
                "source_path": release["pdf_path"],
                "source_sha256": release["pdf_sha256"],
                "schedule_kind": "flat_additive_reciprocal_default",
            }
        )
        for record in schedule.to_dict("records"):
            rows.append(
                {
                    **record,
                    "release_name": release["release_name"],
                    "effective_date": release["effective_date"],
                    "source_path": release["pdf_path"],
                    "source_sha256": release["pdf_sha256"],
                    "schedule_kind": "flat_additive_reciprocal",
                }
            )
    ledger = pd.DataFrame(rows).drop_duplicates()
    if ledger.empty:
        raise ValueError("Partner reciprocal parser returned no official schedules")
    observed_releases = set(ledger["release_name"])
    if observed_releases != set(expected):
        missing = sorted(set(expected).difference(observed_releases))
        raise ValueError(f"Partner reciprocal ledger is missing releases: {missing}")
    write_parquet(ledger, destination, overwrite=True)
    return ledger


def build_partner_month_rates(
    rules: dict[str, Any],
    partners: pd.DataFrame,
    ledger: pd.DataFrame,
    months: list[str],
) -> pd.DataFrame:
    """Construct exact-day partner reciprocal rates for the flat-rate sample."""

    releases = rules["releases"]
    release_schedule: dict[str, dict[str, float]] = {}
    for release_name, group in ledger.groupby("release_name", sort=False):
        release_schedule[str(release_name)] = {
            str(record.partner_code).zfill(4): float(record.additional_rate)
            for record in group.itertuples(index=False)
        }

    final_release = max(releases, key=lambda row: row["effective_date"])
    april = parse_april_country_rates(final_release["pdf"])
    april_schedule: dict[str, float] = {}
    for record in april.itertuples(index=False):
        for code in resolve_country_text(str(record.country_text), partners):
            april_schedule[code] = float(record.additional_rate)

    partner_codes = sorted(
        partners["partner_code"].astype("string").str.zfill(4).dropna().unique()
    )
    non_flat = set(NON_FLAT_PARTNER_CODES) | set(EU_PARTNER_CODES)
    rows: list[dict[str, Any]] = []
    for period in months:
        month = pd.Period(period, freq="M")
        days = pd.date_range(month.start_time, month.end_time, freq="D")
        for code in partner_codes:
            daily_rates: list[float] = []
            active_names: list[str] = []
            observable = code not in non_flat and code not in GENERAL_EXEMPT_CODES
            for day in days:
                eligible = [
                    release
                    for release in releases
                    if release["effective_date"] <= day.date().isoformat()
                ]
                active = eligible[-1]["release_name"] if eligible else ""
                active_names.append(active)
                if day < GENERAL_START:
                    rate = 0.0
                elif code in GENERAL_EXEMPT_CODES:
                    rate = 0.0
                elif code in CHINA_RECIPROCAL_CODES:
                    rate = _rate_on_date(RECIPROCAL_CHINA_INTERVALS, day)
                elif day == pd.Timestamp("2025-04-09"):
                    rate = april_schedule.get(code, 0.10)
                elif day >= AUGUST_SCHEDULE_START:
                    rate = release_schedule.get(active, {}).get(code, 0.10)
                else:
                    rate = 0.10
                daily_rates.append(float(rate))
            rows.append(
                {
                    "partner_code": code,
                    "year": month.year,
                    "month": month.month,
                    "partner_reciprocal_rate": sum(daily_rates) / len(daily_rates),
                    "partner_schedule_observable": observable,
                    "partner_schedule_reason": (
                        "simple_flat_additive"
                        if observable
                        else "excluded_partner_specific_nonflat_or_certification_rule"
                    ),
                    "partner_active_release": active_names[-1],
                }
            )
    return pd.DataFrame(rows)


def _load_mfn(config: PipelineConfig, year: int) -> pd.DataFrame:
    source = config.raw_dir / "policy" / "annual" / f"tariff_data_{year}.zip"
    with zipfile.ZipFile(source) as archive:
        member = next(name for name in archive.namelist() if name.lower().endswith(".txt"))
        with archive.open(member) as handle:
            frame = pd.read_csv(handle, dtype={"hts8": "string"}, encoding="latin1", low_memory=False)
    frame["hs8"] = frame["hts8"].str.replace(r"\D", "", regex=True).str.zfill(8)
    frame["mfn_ad_val_rate"] = pd.to_numeric(frame["mfn_ad_val_rate"], errors="coerce")
    frame["mfn_rate_type_code"] = frame["mfn_rate_type_code"].astype("string")
    frame["mfn_simple_advalorem"] = frame["mfn_rate_type_code"].isin(["0", "7"])
    return frame[["hs8", "mfn_ad_val_rate", "mfn_rate_type_code", "mfn_simple_advalorem"]].drop_duplicates("hs8")


def _load_inherited(config: PipelineConfig) -> pd.DataFrame:
    source = config.processed_tariff_dir / "final" / "historical_tariffs.parquet"
    con = duckdb.connect()
    try:
        return con.execute(
            """
            SELECT hs8,
                   max(coalesce(solar_201_additional_rate,0)+coalesce(washer_201_additional_rate,0)
                       +coalesce(steel_232_additional_rate,0)+coalesce(aluminum_232_additional_rate,0)) AS inherited_201_232_rate,
                   max(coalesce(china_301_additional_rate,0)) AS inherited_301_rate
            FROM read_parquet(?)
            WHERE year=2019 AND month=12 AND cast(cty_code AS VARCHAR) IN ('5700','5820')
            GROUP BY hs8
            """,
            [str(source)],
        ).fetchdf()
    finally:
        con.close()


def _weighted(intervals: Iterable[tuple[str, str | None, float]], period: str) -> float:
    month = pd.Period(period, freq="M")
    first, last = month.start_time.normalize(), month.end_time.normalize()
    days = calendar.monthrange(month.year, month.month)[1]
    total = 0.0
    for begin, terminal, rate in intervals:
        begin_date = pd.Timestamp(begin).normalize()
        end_date = pd.Timestamp(terminal).normalize() if terminal else pd.Timestamp.max.normalize()
        left, right = max(first, begin_date), min(last, end_date)
        if left <= right:
            total += ((right - left).days + 1) * float(rate)
    return total / days


def _rate_on_date(intervals: Iterable[tuple[str, str | None, float]], date: pd.Timestamp) -> float:
    value = 0.0
    normalized = pd.Timestamp(date).normalize()
    for begin, terminal, rate in intervals:
        left = pd.Timestamp(begin).normalize()
        right = pd.Timestamp(terminal).normalize() if terminal else pd.Timestamp.max.normalize()
        if left <= normalized <= right:
            value += float(rate)
    return value


def _matches(code: str, prefixes: Iterable[str]) -> bool:
    return any(code.startswith(prefix) for prefix in prefixes)


def _atomic_copy(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".parquet", dir=path.parent)
    os.close(fd)
    temporary = Path(name)
    temporary.unlink(missing_ok=True)
    escaped = str(temporary).replace("'", "''")
    try:
        con.execute(f"COPY ({query}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        check = con.execute("SELECT count(*) FROM read_parquet(?)", [str(temporary)]).fetchone()[0]
        if check <= 0:
            raise ValueError("Refusing to publish an empty tariff artifact")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def build_observable_policy_panel(config: PipelineConfig) -> dict[str, Any]:
    root = config.processed_tariff_dir / "fk2025"
    trade_glob = str(config.processed_trade_dir / "fk2025" / "variety_month" / "year=*" / "month=*" / "part.parquet").replace("\\", "/")
    rules = build_revision_rules(config)
    mfn_2024 = _load_mfn(config, 2024).rename(columns={
        "mfn_ad_val_rate": "mfn_ad_val_rate_2024",
        "mfn_rate_type_code": "mfn_rate_type_code_2024",
        "mfn_simple_advalorem": "mfn_simple_advalorem_2024",
    })
    mfn_2025 = _load_mfn(config, 2025).rename(columns={
        "mfn_ad_val_rate": "mfn_ad_val_rate_2025",
        "mfn_rate_type_code": "mfn_rate_type_code_2025",
        "mfn_simple_advalorem": "mfn_simple_advalorem_2025",
    })
    inherited = _load_inherited(config)

    con = duckdb.connect()
    try:
        products = con.execute(
            f"SELECT DISTINCT hs10, substring(hs10,1,8) hs8 FROM read_parquet('{trade_glob}', hive_partitioning=false)"
        ).fetchdf()
        partners = con.execute(
            f"""
            SELECT DISTINCT lpad(cast(partner_code AS VARCHAR),4,'0') AS partner_code,
                            partner_name
            FROM read_parquet('{trade_glob}', hive_partitioning=false)
            ORDER BY partner_code
            """
        ).fetchdf()
        months = [str(value) for value in pd.period_range(POLICY_START, POLICY_END, freq="M")]
        partner_ledger = build_partner_reciprocal_ledger(
            config, rules["releases"], partners
        )
        partner_month = build_partner_month_rates(
            rules, partners, partner_ledger, months
        )
        con.register("partner_rate_lookup", partner_month)

        # Product-specific Section 301 modifications are total component rates,
        # not increments.  Longest-prefix matching preserves 10-digit rules.
        note31_records = rules["note31"].to_dict("records")
        complex_prefixes = tuple(rules["complex"]["hts_prefix"].drop_duplicates())
        product_rows = []
        for record in products.itertuples(index=False):
            candidates = [row for row in note31_records if str(record.hs10).startswith(row["hts_prefix"])]
            pre_rate = max((row["section301_rate"] for row in candidates if row["effective_date"] <= "2024-12-31"), default=0.0)
            jan_rate = max((row["section301_rate"] for row in candidates if row["effective_date"] <= "2025-01-01"), default=pre_rate)
            product_rows.append({
                "hs10": record.hs10,
                "hs8": record.hs8,
                "note31_rate_2024": pre_rate,
                "note31_rate_2025": jan_rate,
                "complex_scope_excluded": _matches(str(record.hs10), complex_prefixes),
            })
        product = pd.DataFrame(product_rows).merge(mfn_2024, on="hs8", how="left").merge(mfn_2025, on="hs8", how="left").merge(inherited, on="hs8", how="left")
        # Freeze the ordinary-duty baseline at the end-2024 schedule, matching
        # the historical paper's exclusion of unrelated treaty/schedule
        # changes.  The 2025 annual file is used only for codes absent in 2024.
        product["mfn_ad_val_rate"] = product["mfn_ad_val_rate_2024"].combine_first(product["mfn_ad_val_rate_2025"])
        product["mfn_rate_type_code"] = product["mfn_rate_type_code_2024"].combine_first(product["mfn_rate_type_code_2025"])
        product["mfn_simple_advalorem"] = product["mfn_simple_advalorem_2024"].combine_first(product["mfn_simple_advalorem_2025"])
        product["mfn_baseline_source"] = "missing_both_2024_2025"
        product.loc[product["mfn_ad_val_rate_2025"].notna(), "mfn_baseline_source"] = "2025_new_code_fallback"
        product.loc[product["mfn_ad_val_rate_2024"].notna(), "mfn_baseline_source"] = "2024_frozen_baseline"
        product[["inherited_201_232_rate", "inherited_301_rate"]] = product[["inherited_201_232_rate", "inherited_301_rate"]].fillna(0.0)
        con.register("product_lookup", product)

        release_rules = rules["reciprocal_rules"]
        release_dates = rules["releases"]
        release_exclusions: dict[str, dict[str, bool]] = {}
        for release in release_dates:
            prefixes = tuple(release_rules.loc[release_rules["release_name"] == release["release_name"], "hts_prefix"])
            release_exclusions[release["release_name"]] = {
                str(record.hs10): _matches(str(record.hs10), prefixes)
                for record in products.itertuples(index=False)
            }

        reciprocal_rows = []
        for period in months:
            month_period = pd.Period(period, freq="M")
            days = pd.date_range(month_period.start_time, month_period.end_time, freq="D")
            daily: list[tuple[str | None, float]] = []
            for day in days:
                eligible_releases = [row for row in release_dates if row["effective_date"] <= day.date().isoformat()]
                active_release = eligible_releases[-1]["release_name"] if eligible_releases else None
                daily.append((active_release, _rate_on_date(RECIPROCAL_CHINA_INTERVALS, day)))
            month_end_release = daily[-1][0]
            month_end = next((row for row in reversed(release_dates) if row["release_name"] == month_end_release), release_dates[0])
            for record in products.itertuples(index=False):
                code = str(record.hs10)
                weighted_rate = sum(
                    rate for release_name, rate in daily
                    if rate and release_name is not None and not release_exclusions[release_name][code]
                ) / len(days)
                reciprocal_rows.append({
                    "hs10": record.hs10,
                    "year": int(period[:4]),
                    "month": int(period[5:]),
                    "annex_ii_excluded": release_exclusions.get(month_end_release, {}).get(code, False),
                    "china_reciprocal_rate": weighted_rate,
                    "hts_release_name": month_end["release_name"],
                    "hts_release_sha256": month_end["pdf_sha256"],
                })
        reciprocal = pd.DataFrame(reciprocal_rows)
        con.register("reciprocal_lookup", reciprocal)

        action = pd.DataFrame({
            "period": months,
            "fentanyl_rate": [_weighted(FENTANYL_INTERVALS, period) for period in months],
        })
        action["year"] = action["period"].str[:4].astype(int)
        action["month"] = action["period"].str[5:].astype(int)
        con.register("action_lookup", action)

        policy_query = f"""
        WITH components AS (
        SELECT cast(t.partner_code AS VARCHAR) AS partner_code, t.hs10, t.year, t.month,
               p.mfn_ad_val_rate AS base_mfn_rate,
               p.mfn_rate_type_code,
               p.mfn_baseline_source,
               CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820')
                    THEN p.inherited_201_232_rate ELSE 0.0 END AS inherited_201_232_rate,
               CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820')
                    THEN CASE
                         WHEN t.year=2024 AND t.month=9 THEN
                              (26*p.inherited_301_rate + 4*greatest(p.inherited_301_rate,p.note31_rate_2024))/30.0
                         WHEN make_date(cast(t.year AS BIGINT),cast(t.month AS BIGINT),1) >= DATE '2025-01-01'
                              THEN greatest(p.inherited_301_rate,p.note31_rate_2024,p.note31_rate_2025)
                         WHEN make_date(cast(t.year AS BIGINT),cast(t.month AS BIGINT),1) >= DATE '2024-10-01'
                              THEN greatest(p.inherited_301_rate,p.note31_rate_2024)
                         ELSE p.inherited_301_rate END
                    ELSE 0.0 END AS inherited_301_rate,
               CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820') THEN a.fentanyl_rate ELSE 0.0 END AS china_fentanyl_rate,
               CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820')
                    THEN r.china_reciprocal_rate ELSE 0.0 END AS china_reciprocal_rate,
               CASE WHEN r.annex_ii_excluded
                    THEN 0.0 ELSE pr.partner_reciprocal_rate END AS partner_reciprocal_rate,
               pr.partner_schedule_observable,
               pr.partner_schedule_reason,
               pr.partner_active_release,
               r.annex_ii_excluded, p.complex_scope_excluded,
               TRUE AS event_scope_eligible,
               (coalesce(p.mfn_simple_advalorem,FALSE)
                    AND pr.partner_schedule_observable
                    AND NOT p.complex_scope_excluded
                    AND coalesce(p.inherited_201_232_rate,0)=0) AS dynamic_scope_eligible,
               (coalesce(p.mfn_simple_advalorem,FALSE)
                    AND pr.partner_schedule_observable
                    AND NOT p.complex_scope_excluded
                    AND coalesce(p.inherited_201_232_rate,0)=0) AS primary_scope_eligible,
               CASE WHEN p.mfn_simple_advalorem IS NULL THEN 'excluded_missing_2025_mfn_mapping'
                    WHEN NOT p.mfn_simple_advalorem THEN 'excluded_non_advalorem_mfn'
                    WHEN p.complex_scope_excluded THEN 'excluded_content_certification_or_entry_dependent_scope'
                    WHEN p.inherited_201_232_rate>0 THEN 'excluded_unverified_2025_201_232_continuation'
                    WHEN NOT pr.partner_schedule_observable THEN pr.partner_schedule_reason
                    ELSE NULL END AS exclusion_reason,
               r.hts_release_name, r.hts_release_sha256,
               CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820') THEN '2025-02' ELSE NULL END AS first_increase_period,
               CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820')
                    THEN a.fentanyl_rate ELSE 0.0 END
                 + CASE WHEN r.annex_ii_excluded
                        THEN 0.0 ELSE pr.partner_reciprocal_rate END
                 AS new_admin_treatment_intensity,
               p.mfn_ad_val_rate
                 + CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820') THEN p.inherited_201_232_rate ELSE 0.0 END
                 + CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820')
                        THEN CASE
                             WHEN t.year=2024 AND t.month=9 THEN (26*p.inherited_301_rate + 4*greatest(p.inherited_301_rate,p.note31_rate_2024))/30.0
                             WHEN make_date(cast(t.year AS BIGINT),cast(t.month AS BIGINT),1) >= DATE '2025-01-01' THEN greatest(p.inherited_301_rate,p.note31_rate_2024,p.note31_rate_2025)
                             WHEN make_date(cast(t.year AS BIGINT),cast(t.month AS BIGINT),1) >= DATE '2024-10-01' THEN greatest(p.inherited_301_rate,p.note31_rate_2024)
                             ELSE p.inherited_301_rate END
                        ELSE 0.0 END
                 + CASE WHEN cast(t.partner_code AS VARCHAR) IN ('5700','5820') THEN a.fentanyl_rate ELSE 0.0 END
                 + CASE WHEN r.annex_ii_excluded
                        THEN 0.0 ELSE pr.partner_reciprocal_rate END
                  AS statutory_paper_coverage_rate,
               '{VERSION}' AS policy_version,
               'official_independent_components_with_paper_coverage_instrument' AS policy_source_mode
        FROM read_parquet('{trade_glob}', hive_partitioning=false) t
        JOIN product_lookup p USING (hs10)
        JOIN reciprocal_lookup r USING (hs10,year,month)
        JOIN action_lookup a USING (year,month)
        JOIN partner_rate_lookup pr
          ON lpad(cast(t.partner_code AS VARCHAR),4,'0')=pr.partner_code
         AND t.year=pr.year AND t.month=pr.month
        )
        SELECT *,
               statutory_paper_coverage_rate AS statutory_total_rate,
               CASE WHEN dynamic_scope_eligible
                    THEN statutory_paper_coverage_rate END
                    AS statutory_deterministic_rate
        FROM components
        ORDER BY partner_code, hs10, year, month
        """
        policy_path = root / "final_tariff_panel.parquet"
        _atomic_copy(con, policy_query, policy_path)

        event_query = f"""
        SELECT t.*, p.* EXCLUDE (partner_code,hs10,year,month)
        FROM read_parquet('{trade_glob}', hive_partitioning=false) t
        JOIN read_parquet('{str(policy_path).replace('\\', '/')}') p USING (partner_code,hs10,year,month)
        ORDER BY partner_code, hs10, year, month
        """
        event_path = config.processed_trade_dir / "fk2025" / "workhorse_2025.parquet"
        _atomic_copy(con, event_query, event_path)

        monthly_validation_path = root / "policy_monthly_validation.parquet"
        monthly_validation_query = f"""
        SELECT year, month, count(*) AS rows,
               count_if(event_scope_eligible) AS event_eligible_rows,
               count_if(dynamic_scope_eligible) AS dynamic_eligible_rows,
               sum(con_val_mo) AS nominal_consumption_value,
               sum(con_val_mo) FILTER (WHERE dynamic_scope_eligible) AS dynamic_eligible_consumption_value,
               round(sum(con_val_mo * china_fentanyl_rate) FILTER (WHERE partner_code IN ('5700','5820'))
                 / nullif(sum(con_val_mo) FILTER (WHERE partner_code IN ('5700','5820')),0),12) AS china_value_weighted_fentanyl_rate,
               round(sum(con_val_mo * china_reciprocal_rate) FILTER (WHERE partner_code IN ('5700','5820'))
                 / nullif(sum(con_val_mo) FILTER (WHERE partner_code IN ('5700','5820')),0),12) AS china_value_weighted_reciprocal_rate,
               round(sum(con_val_mo * partner_reciprocal_rate) FILTER (WHERE dynamic_scope_eligible)
                 / nullif(sum(con_val_mo) FILTER (WHERE dynamic_scope_eligible),0),12) AS all_partner_value_weighted_reciprocal_rate,
               round(sum(con_val_mo * statutory_total_rate) FILTER (WHERE partner_code IN ('5700','5820') AND dynamic_scope_eligible)
                 / nullif(sum(con_val_mo) FILTER (WHERE partner_code IN ('5700','5820') AND dynamic_scope_eligible),0),12) AS china_value_weighted_total_rate,
               count_if(dynamic_scope_eligible AND partner_reciprocal_rate<>0) AS treated_partner_product_rows,
               count_if(statutory_paper_coverage_rate IS NULL)
                   AS unresolved_paper_coverage_rate_rows,
               count_if(statutory_deterministic_rate IS NULL)
                   AS unresolved_deterministic_rate_rows
        FROM read_parquet('{str(event_path).replace('\\', '/')}')
        GROUP BY year, month ORDER BY year, month
        """
        _atomic_copy(con, monthly_validation_query, monthly_validation_path)

        exclusion_summary = con.execute(
            f"""
            SELECT coalesce(exclusion_reason,'dynamic_eligible') AS scope_status,
                   count(*) AS rows, count(DISTINCT hs10) AS products,
                   sum(con_val_mo) AS nominal_consumption_value,
                   sum(con_val_mo)/sum(sum(con_val_mo)) OVER () AS nominal_value_share
            FROM read_parquet('{str(event_path).replace('\\', '/')}')
            GROUP BY 1 ORDER BY rows DESC
            """
        ).fetchdf()
        exclusion_summary_path = root / "policy_exclusion_summary.csv"
        exclusion_summary.to_csv(exclusion_summary_path, index=False)

        summary = con.execute(
            """
            SELECT count(*) AS row_count,
                   sum(event_scope_eligible::INT) AS event_eligible_rows,
                   sum(dynamic_scope_eligible::INT) AS dynamic_eligible_rows,
                   sum((NOT dynamic_scope_eligible)::INT) AS dynamic_excluded_rows,
                   sum((base_mfn_rate IS NULL)::INT) AS missing_mfn_mapping_rows,
                   sum((complex_scope_excluded)::INT) AS complex_excluded_rows,
                   sum((NOT partner_schedule_observable)::INT) AS nonflat_partner_schedule_rows,
                   sum((NOT coalesce(annex_ii_excluded,false) AND partner_code IN ('5700','5820'))::INT) AS china_reciprocal_covered_rows,
                   count(DISTINCT partner_code) FILTER (WHERE dynamic_scope_eligible) AS dynamic_eligible_partners,
                   count(DISTINCT hs10) AS products,
                   count(DISTINCT partner_code) AS partners,
                   min(make_date(year,month,1)) AS start_date,
                   max(make_date(year,month,1)) AS end_date
            FROM read_parquet(?)
            """,
            [str(policy_path)],
        ).fetchdf().iloc[0].to_dict()
    finally:
        con.close()

    source_hashes = {
        "historical_tariffs": sha256_file(config.processed_tariff_dir / "final" / "historical_tariffs.parquet"),
        "annual_2024": sha256_file(config.raw_dir / "policy" / "annual" / "tariff_data_2024.zip"),
        "annual_2025": sha256_file(config.raw_dir / "policy" / "annual" / "tariff_data_2025.zip"),
        "hts_revision_set": _hash_records([{"name": row["release_name"], "sha256": row["pdf_sha256"]} for row in rules["releases"]]),
    }
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed_with_documented_statutory_ambiguities",
        "policy_gate": "constructed_for_iv_not_an_empirical_result_gate",
        "statutory_construction_gate": "passed_with_documented_ambiguities",
        "event_estimation_authorized": True,
        "dynamic_estimation_authorized": True,
        "primary_convention": "applied tariffs define event treatment; the paper-coverage statutory schedule is the Table-4 instrument and retains explicit entry-dependent ambiguity flags",
        "instrument_conventions": {
            "paper_coverage": "retain all rows with a mapped ordinary rate; omit unobservable content-dependent duty components while applying the published partner/product schedules available from official sources",
            "deterministic": "restrict to simple ad-valorem ordinary rates, observable flat partner schedules, no complex content/certification scope, and no unverified inherited 201/232 component",
        },
        "unresolved_values_filled_with_zero": False,
        "tariff_panel": _repo_relative(config, policy_path),
        "event_panel": _repo_relative(config, event_path),
        "tariff_panel_sha256": sha256_file(policy_path),
        "event_panel_sha256": sha256_file(event_path),
        "monthly_validation": _repo_relative(config, monthly_validation_path),
        "monthly_validation_sha256": sha256_file(monthly_validation_path),
        "exclusion_summary": _repo_relative(config, exclusion_summary_path),
        "source_hashes": source_hashes,
        "rule_artifact_hashes": {
            "reciprocal_exclusions": sha256_file(root / "reciprocal_exclusion_revision_ledger.parquet"),
            "section301_modifications": sha256_file(root / "section301_2024_2025_modifications.parquet"),
            "complex_scope_exclusions": sha256_file(root / "excluded_complex_scope.parquet"),
            "partner_reciprocal_schedules": sha256_file(root / "partner_reciprocal_revision_ledger.parquet"),
        },
        "summary": summary,
        "included_components": ["frozen_2024_MFN_ad_valorem_with_2025_new_code_fallback", "inherited_independent_Section301", "2024_and_January_2025_Section301_modifications", "China_IEEPA_fentanyl", "all_partner_simple_additive_IEEPA_reciprocal_with_versioned_AnnexII"],
        "unobserved_or_omitted_components": [
            "quota_or_TRQ_tier",
            "metal_or_US_content_dependent_duty",
            "USMCA_or_importer_certification",
            "in_transit_or_entry_status_exception",
            "specific_or_compound_MFN",
            "unverified_2025_Section201_or_232_continuation",
            "partner_total_duty_floor_or_separate_bilateral_action",
        ],
        "strict_deterministic_scope_exclusions": [
            "non_simple_MFN",
            "complex_content_or_certification_scope",
            "unverified_inherited_Section201_or_232_component",
            "unmapped_ordinary_rate",
        ],
        "inherited_scope_note": (
            "The independent 2019 Section-301 scope is carried by HS8 and "
            "updated by official note-31 modifications. The broad "
            "paper-coverage instrument retains the corresponding trade rows "
            "and applies every observable published partner/product schedule; "
            "unobservable entry-level content, certification, quota-tier, or "
            "in-transit components are omitted and flagged. The strict "
            "deterministic sensitivity additionally excludes rows with an "
            "unverified inherited Section-201/232 continuation or complex "
            "scope. No authors-package tariff value fills either instrument."
        ),
    }
    write_metadata_json(root / "policy_extension_manifest.json", manifest)
    write_metadata_json(
        root / "policy_missing_sources.json",
        {
            "version": VERSION,
            "status": "none_for_observable_primary_scope",
            "missing_sources": [],
            "excluded_unobservable_components": manifest[
                "unobserved_or_omitted_components"
            ],
        },
    )
    pd.DataFrame([summary]).to_csv(root / "policy_scope_summary.csv", index=False)
    return manifest


if __name__ == "__main__":
    print(json.dumps(build_observable_policy_panel(PipelineConfig.default()), indent=2, default=str))
