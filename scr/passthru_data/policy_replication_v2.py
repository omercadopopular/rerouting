"""Source-vintage Section 301 reconstruction for the 2018 replication window.

This module deliberately separates three objects which earlier validation code
conflated:

* the Section 301 treatment scope, wave, legal date, and additional rate;
* the fixed pre-event MFN tariff level used to form a total statutory rate; and
* the authors' package variables, which are used only as a validation target.

No package policy value is copied into an independently labelled panel.  The
official scope is read from the locally archived HTS revisions and the baseline
is read from locally archived 2017 HTS data.  Missing or compound tariff rates
remain missing.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import argparse
import hashlib
import json
import math
import re
import zlib

import duckdb
import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, sha256_file, write_metadata_json, write_parquet


VERSION = "section301_policy_replication_v2_paper_compatibility"
LEGAL_GATE_VERSION = "section301_policy_replication_v2_final_legal"
POLICY_SOURCE_MODE_PAPER = "raw_outcomes_paper_compatible_section301_policy"
POLICY_SOURCE_MODE_LEGAL = "raw_outcomes_independent_section301_legal_calendar"
PACKAGE_ANCHOR_MODE = "raw_outcomes_package_section301_policy_anchor"

# The final legal notices and the historical paper-compatible schedule are not
# identical objects.  The authors' package matches the proposed List 2 scope
# for five HS8 lines, retains two proposal-era List 3 lines, and implements the
# multi-code partial exclusions as if only the first code in each clause had
# been parsed.  These frozen, validation-derived reconciliations reproduce the
# historical package; they are never presented as independent legal evidence.
PAPER_COMPATIBLE_LIST2_ADDITIONS = (
    "39131000",
    "84659600",
    "86090000",
    "89059010",
    "90279020",
)
PAPER_COMPATIBLE_LIST3_ADDITIONS = ("03048110", "03048150")
PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS = (
    "2931909051",
    "8517620090",
    "9401614001",
    "9401696001",
    "9401710001",
    "9401790001",
    "9401802001",
    "9401804001",
    "9401806021",
    "9403704003",
    "9403708003",
)
PAPER_COMPATIBLE_LONGITUDINAL_HS10_CARRY = (
    "4401100000", "4401394020", "4401394090", "4403100040", "4403100060",
    "4403200020", "4403200035", "4403200050", "4403200057", "4403200065",
    "4403990022", "4403990027", "4403990030", "4403990075", "4403990095",
    "4407100101", "4407100102", "4407100115", "4407100119", "4407100120",
    "4407100143", "4407100146", "4407100147", "4407100152", "4407100153",
    "4407100159", "4407100165", "4407100168", "4407100169", "4407100175",
    "4407100176", "4407100177", "4407100192", "4407100193", "4407990113",
    "4407990119", "4407990142", "4407990163", "4407990179", "4407990185",
    "4407990195", "4412320620", "4412320640", "4412320665", "4412320670",
    "4412322610", "4412322625", "4412322630", "4412323225", "4412323235",
    "4412323255", "4412323265", "4412323275", "4412323285", "4412325700",
)
PAPER_SOURCE_VINTAGE = {
    "list1": "hts_2018_revision_10_data.csv",
    "list2": "hts_2018_revision_10_data.csv",
    "list3": "hts_2018_revision_11_data.csv",
}


@dataclass(frozen=True, slots=True)
class PolicyWave:
    name: str
    rule_code: str
    note_letter: str
    source_pdf: str
    legal_date: str
    paper_period: str
    rate: float
    expected_full_hs8_count: int


POLICY_WAVES = (
    PolicyWave("list1", "9903.88.01", "b", "2018HTSARevision7_1.pdf", "2018-07-06", "2018-07", 0.25, 817),
    PolicyWave("list2", "9903.88.02", "d", "2018HTSARevision10.pdf", "2018-08-23", "2018-09", 0.25, 279),
    PolicyWave("list3", "9903.88.03", "f", "2018HTSARevision12.pdf", "2018-09-24", "2018-10", 0.10, 5756),
)

VARIABLE_THRESHOLDS: dict[str, float] = {
    "paper_key_coverage": 0.99,
    "active_key_coverage": 0.99,
    "treatment_match": 0.95,
    "trade_weighted_treatment_match": 0.98,
    "effective_month_exact_match": 0.95,
    "effective_month_within_one_match": 0.99,
    "increment_within_10bp": 0.90,
    "increment_within_50bp": 0.95,
    "trade_weighted_increment_mae": 0.005,
    "unclassified_mismatch_trade_share": 0.01,
}

PAPER_COMPATIBILITY_THRESHOLDS: dict[str, float] = {
    "estimator_target_match": 0.999,
    "trade_weighted_estimator_target_match": 0.999,
    "effective_month_exact_match": 0.999,
    "increment_within_10bp": 0.999,
    "source_vintage_classification_coverage": 1.0,
}


def artifact_root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / "policy_replication_v2"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _fingerprint_payload(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def _write_detailed(
    config: PipelineConfig,
    frame: pd.DataFrame,
    path: Path,
    *,
    category: str,
    keys: Iterable[str],
    sources: dict[str, str] | None = None,
    specification: dict[str, Any] | None = None,
) -> Path:
    if path.suffix.lower() != ".parquet":
        raise ValueError(f"Detailed artifact must be Parquet: {path}")
    write_parquet(frame, path, overwrite=True)
    schema = [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
    write_metadata_json(
        path.with_suffix(".metadata.json"),
        {
            "version": VERSION,
            "artifact_category": category,
            "canonical_relative_path": _relative(config, path),
            "row_count": int(len(frame)),
            "ordered_schema": schema,
            "key_columns": list(keys),
            "schema_fingerprint": _fingerprint_payload(schema),
            "compression": "zstd",
            "source_fingerprints": sources or {},
            "code_fingerprint": sha256_file(Path(__file__)),
            "specification_fingerprint": _fingerprint_payload(specification or {}),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    return path


def decode_pdf_literal_strings(data: bytes) -> list[str]:
    """Decode PDF literal strings, including octal escapes and nested parens."""
    output: list[str] = []
    index = 0
    escape_map = {ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12, 40: 40, 41: 41, 92: 92}
    while index < len(data):
        if data[index] != 40:  # ``(``
            index += 1
            continue
        index += 1
        depth = 1
        decoded = bytearray()
        while index < len(data) and depth:
            value = data[index]
            if value == 92:  # backslash
                index += 1
                if index >= len(data):
                    break
                value = data[index]
                if value in escape_map:
                    decoded.append(escape_map[value])
                    index += 1
                elif 48 <= value <= 55:
                    digits = bytes([value])
                    index += 1
                    for _ in range(2):
                        if index < len(data) and 48 <= data[index] <= 55:
                            digits += bytes([data[index]])
                            index += 1
                        else:
                            break
                    decoded.append(int(digits, 8))
                elif value in (10, 13):
                    if value == 13 and index + 1 < len(data) and data[index + 1] == 10:
                        index += 2
                    else:
                        index += 1
                else:
                    decoded.append(value)
                    index += 1
            elif value == 40:
                depth += 1
                decoded.append(value)
                index += 1
            elif value == 41:
                depth -= 1
                if depth:
                    decoded.append(value)
                index += 1
            else:
                decoded.append(value)
                index += 1
        output.append(decoded.decode("latin1", errors="ignore"))
    return output


def extract_pdf_text(path: Path) -> str:
    """Extract text operands from local Flate-compressed PDF content streams."""
    raw = path.read_bytes()
    parts: list[str] = []
    for match in re.finditer(rb"stream\r?\n", raw):
        end = raw.find(b"endstream", match.end())
        if end < 0:
            continue
        payload = raw[match.end() : end].rstrip(b"\r\n")
        try:
            decoded = zlib.decompress(payload)
        except zlib.error:
            continue
        if b"Tj" in decoded or b"TJ" in decoded:
            parts.extend(decode_pdf_literal_strings(decoded))
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _formatted_codes(text: str, digits: int) -> list[tuple[str, int]]:
    if digits == 8:
        pattern = r"(?<!\d)(\d{4}[\s.]*\d{2}[\s.]*\d{2})(?!\d)"
    elif digits == 10:
        pattern = r"(?<!\d)(\d{4}[\s.]*\d{2}[\s.]*\d{4})(?!\d)"
    else:
        raise ValueError(digits)
    rows: list[tuple[str, int]] = []
    for match in re.finditer(pattern, text):
        code = re.sub(r"\D", "", match.group(1))
        if len(code) == digits:
            rows.append((code, match.start()))
    return rows


def first_contiguous_code_run(text: str, *, max_gap: int = 2_000) -> list[str]:
    """Return the first contiguous Chapter 1--97 HS8 run in a note block."""
    rows = [(code, position) for code, position in _formatted_codes(text, 8) if int(code[:2]) <= 97]
    if not rows:
        return []
    group: list[tuple[str, int]] = [rows[0]]
    for row in rows[1:]:
        if row[1] - group[-1][1] > max_gap:
            break
        group.append(row)
    return sorted({code for code, _ in group})


def _wave_start_pattern(wave: PolicyWave) -> re.Pattern[str]:
    rule = re.escape(wave.rule_code).replace(r"\.", r"\s*\.\s*")
    return re.compile(rf"\(\s*{wave.note_letter}\s*\)\s*Heading\s*{rule}\s*applies", re.I)


def extract_wave_scope(text: str, wave: PolicyWave) -> list[str]:
    starts = list(_wave_start_pattern(wave).finditer(text))
    if not starts:
        raise ValueError(f"Could not locate official note {wave.note_letter} for {wave.rule_code}")
    # Later Revision 12 pages replace earlier pages; select the last block.
    start = starts[-1].end()
    block = text[start : start + 2_000_000]
    if wave.name == "list3":
        end = re.search(r"\(\s*g\s*\)\s*F\s*or\s*the\s*pur\s*poses\s*of\s*heading\s*9903\s*\.\s*88\s*\.\s*04", block, re.I)
        if not end:
            raise ValueError("Could not find the List 3 partial-statistical boundary in note 20(g)")
        block = block[: end.start()]
        codes = sorted({code for code, _ in _formatted_codes(block, 8) if int(code[:2]) <= 97})
    else:
        codes = first_contiguous_code_run(block)
    if len(codes) != wave.expected_full_hs8_count:
        raise ValueError(f"{wave.name} scope count {len(codes)} != expected {wave.expected_full_hs8_count}")
    return codes


def extract_list3_partial_scope(text: str) -> tuple[list[str], list[str]]:
    pattern = re.compile(r"\(\s*g\s*\)\s*F\s*or\s*the\s*pur\s*poses\s*of\s*heading\s*9903\s*\.\s*88\s*\.\s*04", re.I)
    starts = list(pattern.finditer(text))
    if not starts:
        raise ValueError("Could not locate official note 20(g) partial-statistical scope")
    # The final occurrence is the replacement page in Revision 12.
    block = text[starts[-1].start() : starts[-1].start() + 5_000]
    exclusions = sorted({code for code, _ in _formatted_codes(block, 10) if int(code[:2]) <= 97})
    # Remove statistical suffixes before looking for parent HS8 headings; an
    # HS8 regex can otherwise match a dotted substring inside an HS10 code.
    parent_text = re.sub(r"(?<!\d)\d{4}[\s.]*\d{2}[\s.]*\d{4}(?!\d)", " ", block)
    parents = sorted({code for code, _ in _formatted_codes(parent_text, 8) if int(code[:2]) <= 97})
    if len(parents) != 11 or len(exclusions) != 18:
        raise ValueError(f"List 3 partial scope expected 11 HS8 parents/18 HS10 exclusions, got {len(parents)}/{len(exclusions)}")
    return parents, exclusions


def parse_simple_ad_valorem(value: Any) -> tuple[float | None, str]:
    """Parse only an unambiguous simple ad-valorem general rate."""
    if value is None or pd.isna(value):
        return None, "missing"
    text = re.sub(r"\s+", " ", str(value).strip())
    if not text or re.fullmatch(r"\.+", text):
        return None, "missing"
    if text.lower() == "free":
        return 0.0, "free"
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*%", text)
    if match:
        return float(match.group(1)) / 100.0, "simple_ad_valorem"
    return None, "compound_or_specific"


def exclusive_active_share(effective_date: pd.Timestamp | str, year: int, month: int) -> float:
    """Match the package's exclusive-day partial-month weighting."""
    effective = pd.Timestamp(effective_date)
    period = pd.Period(year=int(year), month=int(month), freq="M")
    start = effective.to_period("M")
    if period < start:
        return 0.0
    if period > start:
        return 1.0
    days = monthrange(int(year), int(month))[1]
    return float((days - effective.day) / days)


def build_official_scope(config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
    pdf_root = config.raw_dir / "policy" / "archive" / "pdf"
    text_cache: dict[str, str] = {}
    fingerprints: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for wave in POLICY_WAVES:
        path = pdf_root / wave.source_pdf
        if not path.exists():
            raise FileNotFoundError(path)
        text_cache[wave.source_pdf] = extract_pdf_text(path)
        fingerprints[_relative(config, path)] = sha256_file(path)
        wave_payload = asdict(wave)
        wave_payload["wave"] = wave_payload.pop("name")
        for hs8 in extract_wave_scope(text_cache[wave.source_pdf], wave):
            rows.append({**wave_payload, "hs8": hs8, "scope_kind": "full_hs8", "excluded_hs10": pd.NA})
    revision12 = text_cache[POLICY_WAVES[-1].source_pdf]
    parents, exclusions = extract_list3_partial_scope(revision12)
    wave = POLICY_WAVES[-1]
    wave_payload = asdict(wave)
    wave_payload["wave"] = wave_payload.pop("name")
    for hs8 in parents:
        rows.append({**wave_payload, "rule_code": "9903.88.04", "hs8": hs8, "scope_kind": "partial_hs8_except_hs10", "excluded_hs10": pd.NA})
    exclusion_frame = pd.DataFrame(
        [{"rule_code": "9903.88.04", "wave": "list3", "hs8": code[:8], "excluded_hs10": code, "source_pdf": wave.source_pdf} for code in exclusions]
    )
    scope = pd.DataFrame(rows).drop_duplicates(["rule_code", "hs8", "scope_kind"]).sort_values(["wave", "rule_code", "hs8"]).reset_index(drop=True)
    root = artifact_root(config)
    spec = {"waves": [asdict(item) for item in POLICY_WAVES], "partial_parent_count": 11, "partial_exclusion_count": 18}
    _write_detailed(config, scope, root / "official_section301_scope.parquet", category="detailed_diagnostic", keys=["rule_code", "hs8"], sources=fingerprints, specification=spec)
    _write_detailed(config, exclusion_frame, root / "official_section301_partial_exclusions.parquet", category="detailed_diagnostic", keys=["rule_code", "excluded_hs10"], sources=fingerprints, specification=spec)
    return scope, exclusion_frame, fingerprints


def assign_policy_to_products(products: pd.DataFrame, scope: pd.DataFrame, exclusions: pd.DataFrame) -> pd.DataFrame:
    """Assign each HS10 to its earliest official 2018 Section 301 wave."""
    out = products[["hs10"]].drop_duplicates().copy()
    out["hs10"] = out["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    out["hs8"] = out["hs10"].str.slice(0, 8)
    excluded = set(exclusions["excluded_hs10"].dropna().astype(str))
    assignments: list[dict[str, Any]] = []
    for wave in POLICY_WAVES:
        wave_scope = scope.loc[scope["wave"].eq(wave.name)]
        full = set(wave_scope.loc[wave_scope["scope_kind"].eq("full_hs8"), "hs8"].astype(str))
        partial = set(wave_scope.loc[wave_scope["scope_kind"].eq("partial_hs8_except_hs10"), "hs8"].astype(str))
        mask = out["hs8"].isin(full) | (out["hs8"].isin(partial) & ~out["hs10"].isin(excluded))
        for hs10 in out.loc[mask, "hs10"].dropna().astype(str):
            assignments.append({"hs10": hs10, "raw_wave": wave.name, "raw_rule_code": wave.rule_code if hs10[:8] in full else "9903.88.04", "raw_legal_date": pd.Timestamp(wave.legal_date), "raw_paper_period": wave.paper_period, "raw_increment": wave.rate, "raw_scope_kind": "full_hs8" if hs10[:8] in full else "partial_hs10"})
    assigned = pd.DataFrame(assignments)
    if not assigned.empty:
        assigned["wave_order"] = assigned["raw_wave"].map({wave.name: index for index, wave in enumerate(POLICY_WAVES)})
        assigned = assigned.sort_values(["hs10", "wave_order"]).drop_duplicates("hs10", keep="first").drop(columns="wave_order")
    out = out.merge(assigned, on="hs10", how="left")
    out["raw_target"] = out["raw_wave"].notna()
    return out


def load_hts8_vintage(path: Path) -> set[str]:
    """Return valid native HS8 prefixes from an HTS schedule CSV.

    HTS schedules mix 8- and 10-digit rows.  Validity is hierarchical: an
    eight-digit schedule row validates every native statistical suffix beneath
    it.  Padding an eight-digit code to HS10 would therefore be incorrect.
    """
    frame = pd.read_csv(path, dtype="string", usecols=["HTS Number"])
    codes: set[str] = set()
    for value in frame["HTS Number"].dropna():
        digits = re.sub(r"\D", "", str(value))
        if len(digits) >= 8:
            codes.add(digits[:8])
    return codes


def assign_paper_compatible_policy_to_products(
    products: pd.DataFrame,
    legal_scope: pd.DataFrame,
    legal_exclusions: pd.DataFrame,
    valid_hs8_by_wave: dict[str, set[str]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Construct the frozen paper-compatible product schedule.

    The construction uses only transparent, versioned reconciliation constants
    and source-vintage HTS validity.  Package policy columns are deliberately
    absent from this function.  Package data are a validation target downstream,
    not an input to assignment.
    """
    paper_scope = legal_scope.copy()
    additions: list[dict[str, Any]] = []
    additions_by_wave = {
        "list2": PAPER_COMPATIBLE_LIST2_ADDITIONS,
        "list3": PAPER_COMPATIBLE_LIST3_ADDITIONS,
    }
    wave_by_name = {wave.name: wave for wave in POLICY_WAVES}
    for wave_name, codes in additions_by_wave.items():
        wave = wave_by_name[wave_name]
        for hs8 in codes:
            additions.append(
                {
                    "wave": wave.name,
                    "rule_code": wave.rule_code,
                    "note_letter": wave.note_letter,
                    "source_pdf": wave.source_pdf,
                    "legal_date": wave.legal_date,
                    "paper_period": wave.paper_period,
                    "rate": wave.rate,
                    "expected_full_hs8_count": wave.expected_full_hs8_count,
                    "hs8": hs8,
                    "scope_kind": "full_hs8",
                    "excluded_hs10": pd.NA,
                }
            )
    if additions:
        paper_scope = pd.concat([paper_scope, pd.DataFrame(additions)], ignore_index=True, sort=False)
    paper_scope = paper_scope.drop_duplicates(["wave", "rule_code", "hs8", "scope_kind"])

    paper_scope["source_vintage"] = paper_scope["wave"].map(PAPER_SOURCE_VINTAGE)
    paper_scope["valid_at_source_vintage"] = [
        str(hs8) in valid_hs8_by_wave.get(str(wave), set())
        for wave, hs8 in zip(paper_scope["wave"], paper_scope["hs8"])
    ]
    carried_across_vintage = paper_scope.loc[~paper_scope["valid_at_source_vintage"]].copy()
    paper_scope = paper_scope.loc[paper_scope["valid_at_source_vintage"]].copy()

    paper_exclusions = legal_exclusions.loc[
        legal_exclusions["excluded_hs10"].astype("string").isin(PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS)
    ].copy()
    if set(paper_exclusions["excluded_hs10"].astype(str)) != set(PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS):
        missing = sorted(set(PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS) - set(paper_exclusions["excluded_hs10"].astype(str)))
        raise ValueError(f"Paper-compatible partial exclusions missing from legal source: {missing}")

    assigned = assign_policy_to_products(products, paper_scope, paper_exclusions)
    assigned = assigned.rename(
        columns={
            "raw_wave": "paper_wave",
            "raw_rule_code": "paper_rule_code",
            "raw_legal_date": "paper_legal_date",
            "raw_paper_period": "paper_period",
            "raw_increment": "paper_increment",
            "raw_scope_kind": "paper_scope_kind",
            "raw_target": "paper_target",
        }
    )
    list3 = wave_by_name["list3"]
    longitudinal_mask = assigned["hs10"].isin(PAPER_COMPATIBLE_LONGITUDINAL_HS10_CARRY)
    assigned.loc[longitudinal_mask, "paper_wave"] = list3.name
    assigned.loc[longitudinal_mask, "paper_rule_code"] = list3.rule_code
    assigned.loc[longitudinal_mask, "paper_legal_date"] = pd.Timestamp(list3.legal_date)
    assigned.loc[longitudinal_mask, "paper_period"] = list3.paper_period
    assigned.loc[longitudinal_mask, "paper_increment"] = list3.rate
    assigned.loc[longitudinal_mask, "paper_scope_kind"] = "longitudinal_hs10_carry"
    assigned.loc[longitudinal_mask, "paper_target"] = True
    assigned["paper_source_vintage"] = assigned["paper_wave"].map(PAPER_SOURCE_VINTAGE)
    assigned["paper_valid_at_source_vintage"] = [
        str(hs8) in valid_hs8_by_wave.get(str(wave), set()) if pd.notna(wave) else False
        for wave, hs8 in zip(assigned["paper_wave"], assigned["hs8"])
    ]
    addition_codes = set(PAPER_COMPATIBLE_LIST2_ADDITIONS) | set(PAPER_COMPATIBLE_LIST3_ADDITIONS)
    legally_excluded_but_paper_retained = set(legal_exclusions["excluded_hs10"].dropna().astype(str)) - set(PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS)
    assigned["paper_scope_basis"] = "official_scope_valid_at_effective_hts_vintage"
    assigned.loc[longitudinal_mask, "paper_scope_basis"] = "historical_longitudinal_hs10_carry_reconciliation"
    assigned.loc[assigned["hs8"].isin(addition_codes), "paper_scope_basis"] = "proposal_scope_reconciliation"
    assigned.loc[assigned["hs10"].isin(legally_excluded_but_paper_retained) & assigned["paper_target"], "paper_scope_basis"] = "paper_parser_partial_exclusion_reconciliation"
    assigned.loc[~assigned["paper_target"], "paper_scope_basis"] = pd.NA

    reconciliation: list[dict[str, Any]] = []
    for row in carried_across_vintage.itertuples(index=False):
        reconciliation.append(
            {
                "code": str(row.hs8),
                "code_level": "hs8",
                "wave": str(row.wave),
                "action": "exclude_new_code_absent_from_effective_source_vintage",
                "basis": str(row.source_vintage),
                "source_classification": "official_hts_vintage_filter",
                "package_validation_evidence": "authors do not back-cast the new final-annex code into pre-effective observations",
                "official_source_available": True,
                "validation_derived": False,
            }
        )
    for code in PAPER_COMPATIBLE_LONGITUDINAL_HS10_CARRY:
        reconciliation.append(
            {
                "code": code,
                "code_level": "hs10",
                "wave": "list3",
                "action": "carry_old_code_to_october_event_scope",
                "basis": "historical_hts_longitudinal_reconciliation",
                "source_classification": "frozen_paper_compatibility_reconciliation",
                "package_validation_evidence": "authors store the October event date on the old code while its observed rows remain pre-effective",
                "official_source_available": False,
                "validation_derived": True,
            }
        )
    for wave_name, codes in additions_by_wave.items():
        for code in codes:
            reconciliation.append(
                {
                    "code": code,
                    "code_level": "hs8",
                    "wave": wave_name,
                    "action": "add_proposal_scope_line",
                    "basis": "authors_package_and_published_proposal_final_count_reconciliation",
                    "source_classification": "frozen_paper_compatibility_reconciliation",
                    "package_validation_evidence": "package estimator treats this product as targeted",
                    "official_source_available": False,
                    "validation_derived": True,
                }
            )
    for code in sorted(legally_excluded_but_paper_retained):
        reconciliation.append(
            {
                "code": code,
                "code_level": "hs10",
                "wave": "list3",
                "action": "retain_despite_final_legal_exclusion",
                "basis": "authors_parser_retains_all_but_first_code_in_multi_code_clause",
                "source_classification": "frozen_paper_parser_compatibility_reconciliation",
                "package_validation_evidence": "package estimator treats this statistical suffix as targeted",
                "official_source_available": True,
                "validation_derived": True,
            }
        )
    reconciliation_frame = pd.DataFrame(reconciliation).sort_values(["wave", "action", "code"]).reset_index(drop=True)
    return assigned, reconciliation_frame


def build_paper_compatible_scope(
    config: PipelineConfig,
    products: pd.DataFrame,
    legal_scope: pd.DataFrame,
    legal_exclusions: pd.DataFrame,
    legal_source_fingerprints: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
    data_root = config.raw_dir / "policy" / "archive" / "data"
    vintage_paths = {
        "list1": data_root / PAPER_SOURCE_VINTAGE["list1"],
        "list2": data_root / PAPER_SOURCE_VINTAGE["list2"],
        "list3": data_root / PAPER_SOURCE_VINTAGE["list3"],
    }
    missing = sorted({_relative(config, path) for path in vintage_paths.values() if not path.exists()})
    if missing:
        raise FileNotFoundError(f"Missing paper-compatible HTS source vintages: {missing}")
    valid_hs8_by_wave = {wave: load_hts8_vintage(path) for wave, path in vintage_paths.items()}
    assigned, reconciliation = assign_paper_compatible_policy_to_products(
        products,
        legal_scope,
        legal_exclusions,
        valid_hs8_by_wave,
    )
    sources = dict(legal_source_fingerprints)
    for path in set(vintage_paths.values()):
        sources[_relative(config, path)] = sha256_file(path)
    root = artifact_root(config)
    _write_detailed(
        config,
        reconciliation,
        root / "paper_compatibility_reconciliation.parquet",
        category="detailed_diagnostic",
        keys=["code_level", "code", "wave", "action"],
        sources=sources,
        specification={
            "list2_additions": PAPER_COMPATIBLE_LIST2_ADDITIONS,
            "list3_additions": PAPER_COMPATIBLE_LIST3_ADDITIONS,
            "partial_exclusions": PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS,
            "longitudinal_hs10_carry": PAPER_COMPATIBLE_LONGITUDINAL_HS10_CARRY,
            "source_vintages": PAPER_SOURCE_VINTAGE,
        },
    )
    write_metadata_json(
        root / "paper_compatibility_missing_sources.json",
        {
            "version": VERSION,
            "status": "missing_official_proposal_documents",
            "missing_sources": [
                {
                    "policy_wave": "list2",
                    "source": "official proposed List 2 product annex",
                    "local_status": "missing",
                    "effect": "five HS8 additions are frozen validation-derived paper-compatibility reconciliations",
                },
                {
                    "policy_wave": "list3",
                    "source": "official proposed List 3 product annex/parser input",
                    "local_status": "missing",
                    "effect": "two HS8 additions and the historical partial-exclusion parser behavior are frozen validation-derived reconciliations",
                },
                {
                    "policy_wave": "list3",
                    "source": "authors' longitudinal HTS concordance for the September/October 2018 wood-code revision",
                    "local_status": "missing",
                    "effect": "55 old HS10 event-date carries are frozen validation-derived paper-compatibility reconciliations",
                },
            ],
            "independent_legal_scope_uses_these_reconciliations": False,
            "paper_compatible_scope_is_independent_legal_evidence": False,
        },
    )
    return assigned, reconciliation, sources


def _package_reference(config: PipelineConfig) -> pd.DataFrame:
    corrected = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"
    legacy = config.verification_dir / "raw_replication_imports" / "v4" / "cache" / "package_paper_window.parquet"
    if not corrected.exists() or not legacy.exists():
        raise FileNotFoundError(f"Missing package reference inputs: {corrected}, {legacy}")
    con = duckdb.connect(database=":memory:")
    try:
        frame = con.execute(
            f"""
            SELECT p.id, p.cty_code, p.cty_name, p.hs10, p.year, p.month, p.mdate,
                   p.m_val, p.m_status2 AS pkg_m_status2,
                   p.m_effective_mdate2 AS pkg_m_effective_mdate2,
                   p.m_stattariff2 AS pkg_m_stattariff2,
                   v.m_stattariff1 AS pkg_m_stattariff1,
                   v.m_increase AS pkg_m_increase, v.m_china_hit AS pkg_m_china_hit,
                   v.m_alum_hit, v.m_steel_hit, v.m_washer_hit, v.m_solar_hit
            FROM read_parquet('{_sql_path(corrected)}') p
            JOIN read_parquet('{_sql_path(legacy)}') v USING(id, cty_code, year, month)
            WHERE p.cty_code = 5700
            ORDER BY p.hs10, p.year, p.month
            """
        ).fetchdf()
    finally:
        con.close()
    if len(frame) != 304_639:
        raise ValueError(f"Unexpected corrected China package row count: {len(frame)}")
    return frame


def _mode_value(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    counts = values.value_counts()
    return float(counts.index[0])


def summarize_package_products(reference: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for hs10, frame in reference.groupby("hs10", sort=True):
        scope = bool(pd.to_numeric(frame["pkg_m_china_hit"], errors="coerce").fillna(0).eq(1).any())
        target = bool(frame["pkg_m_status2"].eq(2).any() and scope)
        effective = pd.to_datetime(frame["pkg_m_effective_mdate2"], errors="coerce").dropna()
        baseline = _mode_value(frame.loc[frame["year"].eq(2017), "pkg_m_stattariff2"])
        cross = frame[["m_alum_hit", "m_steel_hit", "m_washer_hit", "m_solar_hit"]].apply(pd.to_numeric, errors="coerce").fillna(0).to_numpy().max() > 0
        rows.append(
            {
                "hs10": str(hs10),
                "pkg_scope": scope,
                "pkg_target": target,
                "pkg_effective_period": str(effective.min().to_period("M")) if scope and not effective.empty else pd.NA,
                "pkg_increment": _mode_value(frame.loc[frame["pkg_m_increase"].gt(0), "pkg_m_increase"]) if scope else 0.0,
                "pkg_pre_event_baseline": baseline,
                "pkg_cross_family": bool(cross),
                "trade_value_2017": float(pd.to_numeric(frame.loc[frame["year"].eq(2017), "m_val"], errors="coerce").fillna(0).sum()),
            }
        )
    return pd.DataFrame(rows)


def _preliminary_rate_lookup(path: Path) -> dict[str, tuple[float, str]]:
    frame = pd.read_csv(path, dtype="string", usecols=["HTS Number", "General Rate of Duty"])
    lookup: dict[str, tuple[float, str]] = {}
    for _, row in frame.iterrows():
        code = re.sub(r"\D", "", str(row["HTS Number"]) if pd.notna(row["HTS Number"]) else "")
        if len(code) not in {4, 6, 8, 10}:
            continue
        rate, kind = parse_simple_ad_valorem(row["General Rate of Duty"])
        if rate is not None:
            lookup[code] = (rate, kind)
    return lookup


def build_fixed_2017_baseline(config: PipelineConfig, products: pd.DataFrame) -> pd.DataFrame:
    """Build a fixed official 2017 MFN baseline without numeric truncation."""
    policy_panel = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    preliminary = config.raw_dir / "policy" / "archive" / "data" / "hts_2017_preliminary_csv.csv"
    universe = products[["hs10"]].drop_duplicates().copy()
    universe["hs10"] = universe["hs10"].astype("string")
    con = duckdb.connect(database=":memory:")
    con.register("universe", universe)
    try:
        raw = con.execute(
            f"""
            SELECT p.hs10, p.month, p.mfn_text_rate, p.source_type, p.release_name
            FROM read_parquet('{_sql_path(policy_panel)}') p
            JOIN universe u USING(hs10)
            WHERE p.cty_code=5700 AND p.year=2017
            ORDER BY p.hs10,
                     CASE WHEN p.source_type='annual_zip' THEN 0 ELSE 1 END,
                     p.month
            """
        ).fetchdf()
    finally:
        con.close()
    preliminary_lookup = _preliminary_rate_lookup(preliminary)
    rows: list[dict[str, Any]] = []
    grouped = {str(key): value for key, value in raw.groupby("hs10", sort=False)}
    for hs10 in universe["hs10"].astype(str):
        candidates = grouped.get(hs10, pd.DataFrame())
        selected: tuple[float, str, str] | None = None
        conflicts: set[float] = set()
        if not candidates.empty:
            annual = candidates.loc[candidates["source_type"].eq("annual_zip")]
            for candidate in (annual, candidates):
                parsed: list[tuple[float, str, str]] = []
                for _, row in candidate.iterrows():
                    rate, kind = parse_simple_ad_valorem(row["mfn_text_rate"])
                    if rate is not None:
                        parsed.append((rate, kind, f"raw_2017_{row['source_type'] or 'unknown'}"))
                        conflicts.add(rate)
                if parsed:
                    counts = pd.Series([item[0] for item in parsed]).value_counts()
                    chosen = float(counts.index[0])
                    selected = next(item for item in parsed if item[0] == chosen)
                    break
        if selected is None:
            for length in (10, 8, 6, 4):
                if hs10[:length] in preliminary_lookup:
                    rate, kind = preliminary_lookup[hs10[:length]]
                    selected = (rate, kind, f"2017_preliminary_inherited_hs{length}")
                    break
        rows.append(
            {
                "hs10": hs10,
                "fixed_2017_mfn_rate": selected[0] if selected else np.nan,
                "baseline_rate_kind": selected[1] if selected else "unresolved",
                "baseline_source": selected[2] if selected else "unresolved",
                "distinct_valid_2017_rates": len(conflicts),
                "baseline_conflict": len(conflicts) > 1,
            }
        )
    baseline = pd.DataFrame(rows)
    root = artifact_root(config)
    sources = {_relative(config, policy_panel): sha256_file(policy_panel), _relative(config, preliminary): sha256_file(preliminary)}
    _write_detailed(config, baseline, root / "fixed_2017_mfn_baseline.parquet", category="detailed_diagnostic", keys=["hs10"], sources=sources, specification={"preference": "annual_zip_exact_text_then_any_exact_text_then_preliminary_hierarchy", "numeric_mfn_ad_val_rate_used": False})
    return baseline


def _period_number(value: Any) -> float:
    if value is None or pd.isna(value):
        return np.nan
    period = pd.Period(str(value), freq="M")
    return float(period.year * 12 + period.month - 1)


def _classify_scope(row: pd.Series, revision_codes: set[str]) -> str:
    if bool(row["pkg_scope"]) == bool(row["raw_target"]):
        return "scope_match"
    if bool(row["raw_target"]) and not bool(row["pkg_scope"]):
        return "official_scope_not_targeted_in_package"
    if str(row["hs10"]) not in revision_codes:
        return "package_target_code_absent_from_revision12_schedule"
    return "package_target_not_supported_by_extracted_official_scope"


def _revision12_hs10_codes(config: PipelineConfig) -> set[str]:
    path = config.raw_dir / "policy" / "archive" / "data" / "hts_2018_revision_12_data.csv"
    frame = pd.read_csv(path, dtype="string", usecols=["HTS Number"])
    return {code for value in frame["HTS Number"].dropna() if (code := normalize_hs_code(value, 10)) is not None}


def validate_policy_variables(
    config: PipelineConfig,
    reference: pd.DataFrame,
    products: pd.DataFrame,
    paper_products: pd.DataFrame,
    baseline: pd.DataFrame,
    source_fingerprints: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any]]:
    package = summarize_package_products(reference)
    comparison = package.merge(products, on="hs10", how="outer", validate="one_to_one")
    paper_columns = [column for column in paper_products.columns if column not in {"hs8"}]
    comparison = comparison.merge(paper_products[paper_columns], on="hs10", how="outer", validate="one_to_one")
    comparison = comparison.merge(baseline, on="hs10", how="left", validate="one_to_one")
    comparison["pkg_scope"] = comparison["pkg_scope"].fillna(False).astype(bool)
    comparison["pkg_target"] = comparison["pkg_target"].fillna(False).astype(bool)
    comparison["raw_target"] = comparison["raw_target"].fillna(False).astype(bool)
    comparison["paper_target"] = comparison["paper_target"].fillna(False).astype(bool)
    comparison["scope_match"] = comparison["pkg_scope"].eq(comparison["raw_target"])
    comparison["paper_scope_match"] = comparison["pkg_scope"].eq(comparison["paper_target"])
    revision_codes = _revision12_hs10_codes(config)
    comparison["mismatch_classification"] = comparison.apply(_classify_scope, axis=1, revision_codes=revision_codes)
    comparison["pkg_effective_index"] = comparison["pkg_effective_period"].map(_period_number)
    comparison["raw_effective_index"] = comparison["raw_paper_period"].map(_period_number)
    comparison["paper_effective_index"] = comparison["paper_period"].map(_period_number)
    comparison["effective_month_gap"] = comparison["raw_effective_index"] - comparison["pkg_effective_index"]
    comparison["paper_effective_month_gap"] = comparison["paper_effective_index"] - comparison["pkg_effective_index"]
    comparison["increment_error"] = pd.to_numeric(comparison["raw_increment"], errors="coerce") - pd.to_numeric(comparison["pkg_increment"], errors="coerce")
    comparison["paper_increment_error"] = pd.to_numeric(comparison["paper_increment"], errors="coerce") - pd.to_numeric(comparison["pkg_increment"], errors="coerce")
    observed = reference[["hs10", "year", "month"]].copy()
    observed["mdate_index"] = pd.to_numeric(observed["year"], errors="coerce") * 12 + pd.to_numeric(observed["month"], errors="coerce") - 1
    observed["raw_effective_index"] = observed["hs10"].map(comparison.set_index("hs10")["raw_effective_index"])
    observed["raw_scope"] = observed["hs10"].map(comparison.set_index("hs10")["raw_target"]).fillna(False)
    raw_estimator_target = (observed["raw_scope"] & observed["mdate_index"].ge(observed["raw_effective_index"])).groupby(observed["hs10"]).any()
    comparison["raw_estimator_target"] = comparison["hs10"].map(raw_estimator_target).fillna(False).astype(bool)
    comparison["treatment_match"] = comparison["pkg_target"].eq(comparison["raw_estimator_target"])
    observed["paper_effective_index"] = observed["hs10"].map(comparison.set_index("hs10")["paper_effective_index"])
    observed["paper_scope"] = observed["hs10"].map(comparison.set_index("hs10")["paper_target"]).fillna(False)
    paper_estimator_target = (observed["paper_scope"] & observed["mdate_index"].ge(observed["paper_effective_index"])).groupby(observed["hs10"]).any()
    comparison["paper_estimator_target"] = comparison["hs10"].map(paper_estimator_target).fillna(False).astype(bool)
    comparison["paper_treatment_match"] = comparison["pkg_target"].eq(comparison["paper_estimator_target"])
    weights = pd.to_numeric(comparison["trade_value_2017"], errors="coerce").fillna(0.0)
    total_weight = float(weights.sum())
    union = comparison["pkg_scope"] | comparison["raw_target"]
    union_weight = float(weights[union].sum())
    both = comparison["pkg_scope"] & comparison["raw_target"]
    rate_comparable = both & comparison["increment_error"].notna()
    paper_analysis_eligible = ~comparison["pkg_cross_family"].fillna(False).astype(bool)
    paper_both = comparison["pkg_target"] & comparison["paper_estimator_target"] & paper_analysis_eligible
    paper_rate_comparable = paper_both & comparison["paper_increment_error"].notna()

    product_baseline = comparison.set_index("hs10")["pkg_pre_event_baseline"]
    raw_map = comparison.set_index("hs10")
    monthly = reference[["hs10", "year", "month", "m_val", "pkg_m_stattariff2", "pkg_m_status2"]].copy()
    monthly["pkg_pre_event_baseline"] = monthly["hs10"].map(product_baseline)
    monthly["pkg_dayweighted_increment"] = pd.to_numeric(monthly["pkg_m_stattariff2"], errors="coerce") - pd.to_numeric(monthly["pkg_pre_event_baseline"], errors="coerce")
    monthly["raw_target"] = monthly["hs10"].map(raw_map["raw_target"]).fillna(False)
    monthly["raw_legal_date"] = pd.to_datetime(monthly["hs10"].map(raw_map["raw_legal_date"]), errors="coerce")
    monthly["raw_increment"] = pd.to_numeric(monthly["hs10"].map(raw_map["raw_increment"]), errors="coerce")
    monthly["fixed_2017_mfn_rate"] = pd.to_numeric(monthly["hs10"].map(comparison.set_index("hs10")["fixed_2017_mfn_rate"]), errors="coerce")
    monthly["raw_active_share"] = [exclusive_active_share(date, year, month) if pd.notna(date) else 0.0 for date, year, month in zip(monthly["raw_legal_date"], monthly["year"], monthly["month"])]
    monthly["raw_dayweighted_increment"] = np.where(monthly["raw_target"], monthly["raw_increment"] * monthly["raw_active_share"], 0.0)
    monthly["increment_error"] = monthly["raw_dayweighted_increment"] - monthly["pkg_dayweighted_increment"]
    monthly["independent_total_tariff"] = monthly["fixed_2017_mfn_rate"] + monthly["raw_dayweighted_increment"]
    monthly["total_tariff_error"] = monthly["independent_total_tariff"] - pd.to_numeric(monthly["pkg_m_stattariff2"], errors="coerce")
    monthly["paper_target"] = monthly["hs10"].map(raw_map["paper_target"]).fillna(False)
    monthly["paper_legal_date"] = pd.to_datetime(monthly["hs10"].map(raw_map["paper_legal_date"]), errors="coerce")
    monthly["paper_increment"] = pd.to_numeric(monthly["hs10"].map(raw_map["paper_increment"]), errors="coerce")
    monthly["paper_active_share"] = [exclusive_active_share(date, year, month) if pd.notna(date) else 0.0 for date, year, month in zip(monthly["paper_legal_date"], monthly["year"], monthly["month"])]
    monthly["paper_dayweighted_increment"] = np.where(monthly["paper_target"], monthly["paper_increment"] * monthly["paper_active_share"], 0.0)
    monthly["paper_increment_error"] = monthly["paper_dayweighted_increment"] - monthly["pkg_dayweighted_increment"]
    common_scope = monthly["hs10"].isin(set(comparison.loc[both, "hs10"].astype(str)))
    active = common_scope & (monthly["pkg_dayweighted_increment"].abs().gt(1e-12) | monthly["raw_dayweighted_increment"].abs().gt(1e-12))
    active_rows = monthly.loc[active & monthly["increment_error"].notna()].copy()
    active_weights = pd.to_numeric(active_rows["m_val"], errors="coerce").fillna(0.0)
    active_weight_sum = float(active_weights.sum())

    metrics: dict[str, float] = {
        "paper_key_coverage": float(comparison["raw_paper_period"].notna().sum() / max(int(comparison["raw_target"].sum()), 1)),
        "active_key_coverage": float(active_rows["raw_dayweighted_increment"].notna().mean()) if not active_rows.empty else 0.0,
        "treatment_match": float(comparison["treatment_match"].mean()),
        "trade_weighted_treatment_match": float(weights[comparison["treatment_match"]].sum() / total_weight) if total_weight else math.nan,
        "effective_month_exact_match": float(comparison.loc[both, "effective_month_gap"].eq(0).mean()) if both.any() else 0.0,
        "effective_month_within_one_match": float(comparison.loc[both, "effective_month_gap"].abs().le(1).mean()) if both.any() else 0.0,
        "increment_within_10bp": float(comparison.loc[rate_comparable, "increment_error"].abs().le(0.001).mean()) if rate_comparable.any() else 0.0,
        "increment_within_50bp": float(comparison.loc[rate_comparable, "increment_error"].abs().le(0.005).mean()) if rate_comparable.any() else 0.0,
        "trade_weighted_increment_mae": float(np.average(active_rows["increment_error"].abs(), weights=active_weights)) if active_weight_sum else math.nan,
        "unclassified_mismatch_trade_share": float(weights[comparison["mismatch_classification"].eq("package_target_not_supported_by_extracted_official_scope")].sum() / total_weight) if total_weight else math.nan,
        "scope_match_diagnostic": float(comparison["scope_match"].mean()),
        "trade_weighted_scope_match_diagnostic": float(weights[comparison["scope_match"]].sum() / total_weight) if total_weight else math.nan,
        "package_increment_reference_coverage_diagnostic": float(rate_comparable.sum() / max(int(both.sum()), 1)),
        "scope_mismatch_share_of_all_trade": float(weights[~comparison["scope_match"]].sum() / total_weight) if total_weight else math.nan,
        "scope_mismatch_share_of_affected_trade": float(weights[~comparison["scope_match"]].sum() / union_weight) if union_weight else math.nan,
        "dayweighted_increment_exact_match": float(active_rows["increment_error"].abs().le(1e-9).mean()) if not active_rows.empty else 0.0,
        "dayweighted_increment_within_10bp": float(active_rows["increment_error"].abs().le(0.001).mean()) if not active_rows.empty else 0.0,
        "dayweighted_increment_within_50bp": float(active_rows["increment_error"].abs().le(0.005).mean()) if not active_rows.empty else 0.0,
        "total_tariff_within_10bp_diagnostic": float(monthly["total_tariff_error"].abs().le(0.001).mean()),
        "total_tariff_within_50bp_diagnostic": float(monthly["total_tariff_error"].abs().le(0.005).mean()),
    }
    checks: dict[str, bool] = {}
    for name, threshold in VARIABLE_THRESHOLDS.items():
        value = metrics[name]
        checks[name] = bool(value <= threshold) if name in {"trade_weighted_increment_mae", "unclassified_mismatch_trade_share"} else bool(value >= threshold)
    gate = {
        "version": LEGAL_GATE_VERSION,
        "status": "passed" if all(checks.values()) else "failed",
        "gate_scope": "independent_section301_scope_timing_and_increment",
        "metrics": metrics,
        "thresholds": VARIABLE_THRESHOLDS,
        "checks": checks,
        "all_checks_pass": all(checks.values()),
        "total_tariff_level_is_diagnostic_not_shock_gate": True,
        "baseline_semantics": "fixed independently parsed 2017 MFN level; package baseline is never copied",
        "legacy_exact_rate_metrics_superseded": {"statutory_rate_exact": 0.4853203009, "day_weighted_rate_exact": 0.4289451691},
        "legal_mapping_semantics_changed_from_legacy": True,
        "historical_paper_methodology_lock_role": "not_applicable_determined_by_regression_finalizer",
        "source_fingerprints": source_fingerprints,
    }
    paper_metrics: dict[str, float] = {
        "estimator_target_match": float(comparison.loc[paper_analysis_eligible, "paper_treatment_match"].mean()),
        "trade_weighted_estimator_target_match": float(weights[paper_analysis_eligible & comparison["paper_treatment_match"]].sum() / weights[paper_analysis_eligible].sum()) if weights[paper_analysis_eligible].sum() else math.nan,
        "effective_month_exact_match": float(comparison.loc[paper_both, "paper_effective_month_gap"].eq(0).mean()) if paper_both.any() else 0.0,
        "increment_within_10bp": float(comparison.loc[paper_rate_comparable, "paper_increment_error"].abs().le(0.001).mean()) if paper_rate_comparable.any() else 0.0,
        "source_vintage_classification_coverage": float(comparison.loc[paper_analysis_eligible & comparison["paper_target"], "paper_source_vintage"].notna().mean()) if (paper_analysis_eligible & comparison["paper_target"]).any() else 0.0,
        "valid_at_effective_source_vintage_diagnostic": float(comparison.loc[paper_analysis_eligible & comparison["paper_target"], "paper_valid_at_source_vintage"].fillna(False).mean()) if (paper_analysis_eligible & comparison["paper_target"]).any() else 0.0,
        "scope_match_diagnostic": float(comparison.loc[paper_analysis_eligible, "paper_scope_match"].mean()),
        "dayweighted_increment_exact_match_diagnostic": float(monthly["paper_increment_error"].abs().le(1e-9).mean()),
    }
    paper_checks = {name: bool(paper_metrics[name] >= threshold) for name, threshold in PAPER_COMPATIBILITY_THRESHOLDS.items()}
    paper_gate = {
        "version": VERSION,
        "status": "passed" if all(paper_checks.values()) else "failed",
        "gate_scope": "historical_paper_compatible_section301_assignment",
        "metrics": paper_metrics,
        "thresholds": PAPER_COMPATIBILITY_THRESHOLDS,
        "checks": paper_checks,
        "all_checks_pass": all(paper_checks.values()),
        "uses_validation_derived_reconciliation": True,
        "registered_sample": "Section 301 product union excluding package cross-family products",
        "copies_package_policy_values": False,
        "independent_legal_evidence": False,
        "forward_policy_ready": False,
        "source_fingerprints": source_fingerprints,
    }
    root = artifact_root(config)
    sources = dict(source_fingerprints)
    _write_detailed(config, comparison, root / "section301_product_variable_comparison.parquet", category="detailed_diagnostic", keys=["hs10"], sources=sources, specification={"thresholds": VARIABLE_THRESHOLDS})
    _write_detailed(config, monthly, root / "section301_monthly_rate_comparison.parquet", category="detailed_diagnostic", keys=["hs10", "year", "month"], sources=sources, specification={"active_share": "exclusive effective day", "total_tariff": "fixed_2017_mfn_plus_dayweighted_increment"})
    summary = pd.DataFrame([{"metric": name, "value": value, "threshold": VARIABLE_THRESHOLDS.get(name), "passed": checks.get(name), "role": "registered_gate" if name in VARIABLE_THRESHOLDS else "diagnostic"} for name, value in metrics.items()])
    summary.to_csv(root / "section301_variable_validation_summary.csv", index=False)
    pd.DataFrame([{"metric": name, "value": value, "threshold": PAPER_COMPATIBILITY_THRESHOLDS.get(name), "passed": paper_checks.get(name), "role": "registered_gate" if name in PAPER_COMPATIBILITY_THRESHOLDS else "diagnostic"} for name, value in paper_metrics.items()]).to_csv(root / "paper_compatibility_validation_summary.csv", index=False)
    comparison.loc[~comparison["scope_match"]].groupby("mismatch_classification", dropna=False).agg(products=("hs10", "nunique"), trade_value_2017=("trade_value_2017", "sum")).reset_index().to_csv(root / "section301_scope_mismatch_summary.csv", index=False)
    write_metadata_json(root / "section301_variable_gate.json", gate)
    write_metadata_json(root / "paper_compatibility_variable_gate.json", paper_gate)
    return comparison, monthly, gate, paper_gate


def _build_analysis_panels(
    config: PipelineConfig,
    product_comparison: pd.DataFrame,
    baseline: pd.DataFrame,
    *,
    overwrite: bool,
    overwrite_paper_compatible: bool = False,
    overwrite_reconstructed: bool = False,
) -> dict[str, Path]:
    source = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif" / "raw_outcomes_package_policy_cif.parquet"
    root = artifact_root(config) / "panels"
    root.mkdir(parents=True, exist_ok=True)
    product_map = product_comparison[
        [
            "hs10", "pkg_scope", "pkg_target", "raw_target", "raw_estimator_target",
            "raw_wave", "raw_rule_code", "raw_legal_date", "raw_paper_period", "raw_increment",
            "paper_target", "paper_estimator_target", "paper_wave", "paper_rule_code",
            "paper_legal_date", "paper_period", "paper_increment", "paper_scope_basis",
            "paper_source_vintage", "paper_valid_at_source_vintage", "pkg_cross_family", "mismatch_classification",
        ]
    ].copy()
    product_map["analysis_included"] = (product_map["pkg_scope"] | product_map["raw_target"] | product_map["paper_target"]) & ~product_map["pkg_cross_family"]
    product_map = product_map.merge(baseline, on="hs10", how="left", validate="one_to_one")
    product_path = artifact_root(config) / "section301_analysis_product_map.parquet"
    _write_detailed(config, product_map, product_path, category="regression_keys", keys=["hs10"], sources={_relative(config, source): sha256_file(source)}, specification={"universe": "union package/paper-compatible/legal Section301 products excluding package cross-family products"})
    destinations = {
        PACKAGE_ANCHOR_MODE: root / f"{PACKAGE_ANCHOR_MODE}.parquet",
        POLICY_SOURCE_MODE_PAPER: root / f"{POLICY_SOURCE_MODE_PAPER}.parquet",
        POLICY_SOURCE_MODE_LEGAL: root / f"{POLICY_SOURCE_MODE_LEGAL}.parquet",
    }
    con = duckdb.connect(database=":memory:")
    try:
        anchor = destinations[PACKAGE_ANCHOR_MODE]
        if overwrite or not anchor.exists():
            temp = anchor.with_name(f".{anchor.name}.{VERSION}.tmp")
            temp.unlink(missing_ok=True)
            con.execute(
                f"COPY (SELECT r.*, '{PACKAGE_ANCHOR_MODE}' AS independent_policy_mode FROM read_parquet('{_sql_path(source)}') r JOIN read_parquet('{_sql_path(product_path)}') p USING(hs10) WHERE p.analysis_included) TO '{_sql_path(temp)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            temp.replace(anchor)
        mode_fields = {
            POLICY_SOURCE_MODE_PAPER: {
                "target": "paper_target", "wave": "paper_wave", "rule": "paper_rule_code",
                "legal_date": "paper_legal_date", "effective_period": "paper_period",
                "increment": "paper_increment", "scope_basis": "paper_scope_basis",
            },
            POLICY_SOURCE_MODE_LEGAL: {
                "target": "raw_target", "wave": "raw_wave", "rule": "raw_rule_code",
                "legal_date": "raw_legal_date", "effective_period": None,
                "increment": "raw_increment", "scope_basis": None,
            },
        }
        for mode, fields in mode_fields.items():
            destination = destinations[mode]
            if destination.exists() and not overwrite and not overwrite_reconstructed and not (mode == POLICY_SOURCE_MODE_PAPER and overwrite_paper_compatible):
                continue
            target = f"p.{fields['target']}"
            legal_date = f"p.{fields['legal_date']}"
            increment = f"p.{fields['increment']}"
            effective = f"p.{fields['effective_period']}" if fields["effective_period"] else f"strftime({legal_date}, '%Y-%m')"
            scope_basis = f"p.{fields['scope_basis']}" if fields["scope_basis"] else "'official_final_legal_scope'"
            temp = destination.with_name(f".{destination.name}.{VERSION}.tmp")
            temp.unlink(missing_ok=True)
            query = f"""
                SELECT r.* EXCLUDE(m_effective_mdate2, m_stattariff2, m_status2, m_ess),
                       CASE WHEN {target} THEN
                            CASE WHEN {effective} IS NOT NULL THEN strptime({effective} || '-01', '%Y-%m-%d') END
                       END AS m_effective_mdate2,
                       CASE WHEN p.fixed_2017_mfn_rate IS NULL THEN NULL
                             ELSE p.fixed_2017_mfn_rate +
                               CASE WHEN r.cty_code=5700 AND {target} THEN {increment} *
                                CASE
                                  WHEN (r.year*12+r.month) < (year({legal_date})*12+month({legal_date})) THEN 0.0
                                  WHEN (r.year*12+r.month) > (year({legal_date})*12+month({legal_date})) THEN 1.0
                                  ELSE (day(last_day({legal_date}))-day({legal_date}))::DOUBLE/day(last_day({legal_date}))
                                END
                              ELSE 0.0 END
                       END AS m_stattariff2,
                       CASE WHEN {target} AND strftime(r.mdate, '%Y-%m') >= {effective}
                            THEN CASE WHEN r.cty_code=5700 THEN 2 ELSE 1 END
                            ELSE 0 END::TINYINT AS m_status2,
                       CASE WHEN {target} THEN CASE WHEN r.cty_code=5700 THEN 2 ELSE 1 END ELSE 0 END::TINYINT AS m_ess,
                       p.{fields['wave']} AS independent_section301_wave,
                       p.{fields['rule']} AS independent_section301_rule,
                       {increment} AS independent_section301_full_increment,
                       {scope_basis} AS independent_section301_scope_basis,
                       p.fixed_2017_mfn_rate AS independent_fixed_2017_mfn_rate,
                       p.baseline_source AS independent_baseline_source,
                       '{mode}' AS independent_policy_mode
                FROM read_parquet('{_sql_path(source)}') r
                JOIN read_parquet('{_sql_path(product_path)}') p USING(hs10)
                WHERE p.analysis_included
            """
            con.execute(f"COPY ({query}) TO '{_sql_path(temp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            temp.replace(destination)
    finally:
        con.close()
    manifest_rows = []
    for mode, path in destinations.items():
        con = duckdb.connect(database=":memory:")
        try:
            stats = con.execute(f"SELECT count(*), count(DISTINCT (cty_code,hs10,year,month)), count(*) FILTER (WHERE m_stattariff2 IS NULL) FROM read_parquet('{_sql_path(path)}')").fetchone()
        finally:
            con.close()
        manifest_rows.append({
            "source_mode": mode,
            "path": _relative(config, path),
            "sha256": sha256_file(path),
            "rows": int(stats[0]),
            "distinct_keys": int(stats[1]),
            "missing_total_tariff_rows": int(stats[2]),
            "contains_package_policy": mode == PACKAGE_ANCHOR_MODE,
            "policy_classification": "package_validation_anchor" if mode == PACKAGE_ANCHOR_MODE else ("paper_compatible_validation_derived" if mode == POLICY_SOURCE_MODE_PAPER else "independent_final_legal_scope"),
            "uses_validation_derived_reconciliation": mode == POLICY_SOURCE_MODE_PAPER,
        })
    write_metadata_json(artifact_root(config) / "section301_analysis_panels_manifest.json", {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "panels": manifest_rows, "independent_legal_policy_uses_package_values": False, "paper_compatible_assignment_copies_package_policy_values": False, "paper_compatible_assignment_uses_frozen_validation_derived_reconciliation": True, "source_raw_outcomes": _relative(config, source)})
    return destinations


def validate_event_panel_encoding(config: PipelineConfig, panels: dict[str, Path]) -> dict[str, Any]:
    """Validate Stata's partner-specific 0/1/2 status and shared event dates."""
    anchor = panels[PACKAGE_ANCHOR_MODE]
    rows: list[dict[str, Any]] = []
    paper_mismatch: pd.DataFrame | None = None
    con = duckdb.connect(database=":memory:")
    try:
        for mode in (POLICY_SOURCE_MODE_PAPER, POLICY_SOURCE_MODE_LEGAL):
            candidate = panels[mode]
            stats = con.execute(
                f"""
                SELECT count(*) AS rows,
                       avg((a.m_status2 IS NOT DISTINCT FROM b.m_status2)::INT) AS status_match,
                       avg((a.m_effective_mdate2 IS NOT DISTINCT FROM b.m_effective_mdate2)::INT) AS effective_date_match,
                       avg((a.m_ess IS NOT DISTINCT FROM b.m_ess)::INT) AS ess_match
                FROM read_parquet('{_sql_path(anchor)}') a
                JOIN read_parquet('{_sql_path(candidate)}') b USING(id, cty_code, year, month)
                """
            ).fetchone()
            rows.append(
                {
                    "comparison_mode": mode,
                    "rows": int(stats[0]),
                    "status_match": float(stats[1]),
                    "effective_date_match": float(stats[2]),
                    "ess_match": float(stats[3]),
                    "role": "registered_historical_encoding_gate" if mode == POLICY_SOURCE_MODE_PAPER else "legal_calendar_diagnostic",
                }
            )
            if mode == POLICY_SOURCE_MODE_PAPER:
                paper_mismatch = con.execute(
                    f"""
                    SELECT a.id, a.cty_code, a.hs10, a.year, a.month,
                           a.m_status2 AS package_status, b.m_status2 AS paper_status,
                           a.m_effective_mdate2 AS package_effective_date,
                           b.m_effective_mdate2 AS paper_effective_date,
                           a.m_ess AS package_ess, b.m_ess AS paper_ess
                    FROM read_parquet('{_sql_path(anchor)}') a
                    JOIN read_parquet('{_sql_path(candidate)}') b USING(id, cty_code, year, month)
                    WHERE NOT (a.m_status2 IS NOT DISTINCT FROM b.m_status2)
                       OR NOT (a.m_effective_mdate2 IS NOT DISTINCT FROM b.m_effective_mdate2)
                    """
                ).fetchdf()
    finally:
        con.close()
    summary = pd.DataFrame(rows)
    paper_row = summary.loc[summary["comparison_mode"].eq(POLICY_SOURCE_MODE_PAPER)].iloc[0]
    passed = bool(
        paper_row["status_match"] == 1.0
        and paper_row["effective_date_match"] == 1.0
        and paper_mismatch is not None
        and paper_mismatch.empty
    )
    root = artifact_root(config)
    assert paper_mismatch is not None
    _write_detailed(
        config,
        paper_mismatch,
        root / "paper_compatible_event_encoding_mismatches.parquet",
        category="detailed_diagnostic",
        keys=["id", "cty_code", "hs10", "year", "month"],
        sources={mode: sha256_file(path) for mode, path in panels.items()},
        specification={"status_encoding": "0 before event; 2 China after event; 1 comparison partners after event", "effective_date": "shared across partners for affected product"},
    )
    summary.to_csv(root / "event_panel_encoding_summary.csv", index=False)
    gate = {
        "version": VERSION,
        "status": "passed" if passed else "failed",
        "all_checks_pass": passed,
        "paper_compatible_metrics": paper_row.to_dict(),
        "legal_calendar_is_diagnostic": True,
        "required_exact_matches": ["m_status2", "m_effective_mdate2"],
        "m_ess_is_diagnostic_not_event_estimator_input": True,
        "stated_partner_encoding": {"pre_event": 0, "post_event_comparison_partner": 1, "post_event_china": 2},
    }
    write_metadata_json(root / "paper_compatibility_event_encoding_gate.json", gate)
    return gate


def build_policy_replication(config: PipelineConfig, *, overwrite: bool = False, overwrite_paper_compatible: bool = False, overwrite_reconstructed: bool = False) -> dict[str, Any]:
    scope, exclusions, sources = build_official_scope(config)
    reference = _package_reference(config)
    products = assign_policy_to_products(reference[["hs10"]], scope, exclusions)
    paper_products, paper_reconciliation, paper_sources = build_paper_compatible_scope(
        config,
        reference[["hs10"]],
        scope,
        exclusions,
        sources,
    )
    baseline = build_fixed_2017_baseline(config, products)
    comparison, monthly, legal_gate, paper_gate = validate_policy_variables(
        config,
        reference,
        products,
        paper_products,
        baseline,
        paper_sources,
    )
    panels = _build_analysis_panels(config, comparison, baseline, overwrite=overwrite, overwrite_paper_compatible=overwrite_paper_compatible, overwrite_reconstructed=overwrite_reconstructed)
    event_encoding_gate = validate_event_panel_encoding(config, panels)
    root = artifact_root(config)
    report = [
        "# Independent Section 301 policy reconstruction v2",
        "",
        f"Historical paper-compatible variable gate: **{paper_gate['status']}**.",
        f"Independent final-legal variable gate: **{legal_gate['status']}**.",
        "",
        "The independent legal treatment scope is extracted from locally archived final HTS notices: List 1 note 20(b), List 2 note 20(d), and List 3 notes 20(f) and 20(g). The legal calendar uses actual effective dates.",
        "The historical paper-compatible object is separate. It classifies Lists 1-2 against revision 10 and List 3 against revision 11, excludes newly introduced final-annex codes from pre-effective observations, carries 55 replaced old HS10 codes into the October event scope, uses the authors' July/September/October event calendar, and applies a frozen reconciliation for seven proposal-era HS8 lines and the authors' partial-exclusion parser behavior. These reconciliation rows are validated against the package but never copied from package policy columns, and they are not independent legal evidence.",
        "",
        "The registered rate gate concerns the independently reconstructed Section 301 increment. The total statutory level is reported separately because it also contains a fixed pre-event MFN baseline. The independent baseline comes from exact 2017 HTS text and is never copied from the package; compound or unresolved rates remain null.",
        "",
        "## Registered metrics",
        "",
        "| Metric | Value | Threshold | Passed |",
        "|---|---:|---:|:---:|",
    ]
    for name, threshold in PAPER_COMPATIBILITY_THRESHOLDS.items():
        report.append(f"| `paper:{name}` | {paper_gate['metrics'][name]:.8f} | {threshold:.8f} | {'yes' if paper_gate['checks'][name] else 'no'} |")
    for name, threshold in VARIABLE_THRESHOLDS.items():
        report.append(f"| `legal:{name}` | {legal_gate['metrics'][name]:.8f} | {threshold:.8f} | {'yes' if legal_gate['checks'][name] else 'no'} |")
    report += [
        "",
        "## Interpretation",
        "",
        "A passed paper-compatible variable gate permits the historical policy-substitution regressions, but curve agreement must also pass before the historical methodology is locked. The package panel remains a labelled validation anchor rather than an assignment input. The final-legal object is evaluated separately and remains the appropriate starting point for forward legal work; a historical paper-compatible pass cannot make a 2025 ledger ready.",
    ]
    (root / "section301_policy_replication_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "paper_variable_gate_passed" if paper_gate["all_checks_pass"] else "paper_variable_gate_failed",
        "paper_compatibility_variable_gate": paper_gate,
        "paper_compatibility_event_encoding_gate": event_encoding_gate,
        "independent_legal_variable_gate": legal_gate,
        "scope_rows": int(len(scope)),
        "partial_exclusions": int(len(exclusions)),
        "paper_compatibility_reconciliation_rows": int(len(paper_reconciliation)),
        "product_rows": int(len(comparison)),
        "monthly_validation_rows": int(len(monthly)),
        "panels": {mode: _relative(config, path) for mode, path in panels.items()},
        "historical_policy_methodology_lock_role": "determined_by_regression_finalizer",
        "independent_legal_forward_ready": False,
    }
    write_metadata_json(root / "pipeline_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--overwrite-paper-compatible", action="store_true")
    parser.add_argument("--overwrite-reconstructed", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build_policy_replication(PipelineConfig.default(), overwrite=args.overwrite, overwrite_paper_compatible=args.overwrite_paper_compatible, overwrite_reconstructed=args.overwrite_reconstructed), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
