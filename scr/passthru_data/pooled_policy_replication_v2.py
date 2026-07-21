"""Source-auditable pooled tariff reconstruction, version 2.

Version 1 mixed legal schedules with the paper's treatment variables.  It also
treated context links as product scope and summed mutually exclusive Chapter
99 alternatives.  This module is deliberately fail-closed: unresolved scope,
quota assignment, or source vintage produces a blocked manifest rather than a
plausible-looking independent panel.

The package is used only as a validation target.  No package policy column is
read while constructing the independent legal ledger.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import argparse
import hashlib
import json
import re

import pandas as pd

from .config import PipelineConfig
from . import pooled_policy_replication_v1 as v1
from .io_utils import normalize_hs_code, sha256_file, write_metadata_json, write_parquet
from .policy_benchmark_contract import load_contract


VERSION = "pooled_policy_replication_v2"
FAMILIES = v1.FAMILIES
LEGAL_SOURCE_MODE = "independent_local_official_sources"
PAPER_SOURCE_MODE = "paper_compatible_from_independent_legal_ledger"

# These are historical initial shocks, not a substitute for the legal
# contemporaneous schedule.  They are used only by the paper-compatible
# transformation and are intentionally kept separate from LEGAL_RATE_SCHEDULE.
PAPER_INITIAL_SHOCKS = {
    "99034501": 0.20,
    "99034502": 0.50,
    "99034506": 0.50,
    "99034522": 0.30,
    "99034525": 0.30,
    "99038001": 0.25,
    "99038501": 0.10,
}

# Source-supported historical safeguard steps.  The schedule is deliberately
# bounded to the paper window; no later rate is extrapolated through a stale
# Chapter-99 table or filled with zero.
LEGAL_RATE_SCHEDULE = {
    "99034501": (("2018-02-07", 0.20), ("2019-02-07", 0.18)),
    "99034502": (("2018-02-07", 0.50), ("2019-02-07", 0.45)),
    "99034506": (("2018-02-07", 0.50), ("2019-02-07", 0.45)),
    "99034522": (("2018-02-07", 0.30), ("2019-02-07", 0.25)),
    "99034525": (("2018-02-07", 0.30), ("2019-02-07", 0.25)),
    "99038001": (("2018-03-23", 0.25),),
    "99038501": (("2018-03-23", 0.10),),
}
LEGAL_SCHEDULE_END = pd.Timestamp("2019-04-30")


@dataclass(frozen=True)
class PolicyObject:
    """Names the object represented by a policy field."""

    name: str
    source: str
    calendar: str
    role: str
    package_reference_allowed: bool = False


LEGAL_OBJECT = PolicyObject(
    "independent_legal", "local_official_sources", "legal_effective_date", "legal_schedule"
)
PAPER_OBJECT = PolicyObject(
    "paper_compatible_from_legal", "independent_legal_ledger", "paper_month", "paper_transformation"
)
PACKAGE_OBJECT = PolicyObject(
    "package_reference", "fajgelbaum_replication_package", "m_status/mdate", "validation_target", True
)

PACKAGE_POLICY_COLUMNS = (
    "m_status1",
    "m_status2",
    "m_hit",
    "m_effective_date",
    "m_effective_mdate1",
    "m_effective_mdate2",
    "m_increase",
    "m_stattariff1",
    "m_stattariff2",
    "m_china_hit",
    "m_steel_hit",
    "m_alum_hit",
    "m_washer_hit",
    "m_solar_hit",
)

TARGET_CROSSWALK = {
    "fig_01_rates": {
        "calendar": "legal",
        "status": "m_status1",
        "effective_month": "m_effective_mdate1",
        "rate": "m_stattariff1",
    },
    "fig_02_m_event": {
        "calendar": "nearest_full_month",
        "status": "m_status2",
        "effective_month": "m_effective_mdate2",
        "treatment": "m_status2 == 2",
    },
    "tab_02_sector_sumstats": {
        "calendar": "paper_table",
        "treatment": "m_hit",
        "shock": "m_increase",
        "family_hits": ["m_china_hit", "m_steel_hit", "m_alum_hit", "m_washer_hit", "m_solar_hit"],
    },
    "fig_04_dynamic": {
        "calendar": "read_tab_04_sigma_omega_specification",
        "rate": "m_stattariff2_or_registered_dynamic_target",
    },
}


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def analysis_root(config: PipelineConfig) -> Path:
    path = config.analysis_dir / "policy" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def specification_fingerprint() -> str:
    payload = {
        "version": VERSION,
        "objects": [asdict(LEGAL_OBJECT), asdict(PAPER_OBJECT), asdict(PACKAGE_OBJECT)],
        "calendars": ["legal_effective_date", "paper_month", "m_status1/m_effective_mdate1", "m_status2/m_effective_mdate2"],
        "roles": sorted(ROLE_BY_RULE.items()),
        "stacking": "typed_role_selection_then_additive_across_families",
        "unresolved_policy": "null_and_blocked_not_zero",
        "paper_initial_shocks": sorted(PAPER_INITIAL_SHOCKS.items()),
        "legal_rate_schedule": sorted((key, value) for key, value in LEGAL_RATE_SCHEDULE.items()),
        "nearest_full_month_rule": "effective_day_greater_than_half_month_moves_to_next_month",
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def paper_month_from_legal_date(value: Any) -> pd.Timestamp | pd.NaT:
    """Apply the paper's nearest-full-month convention to an effective date."""

    date = pd.to_datetime(value, errors="coerce")
    if pd.isna(date):
        return pd.NaT
    month_end = date + pd.offsets.MonthEnd(0)
    if int(date.day) > int(month_end.day) / 2:
        return (date + pd.offsets.MonthBegin(1)).normalize()
    return date.normalize().replace(day=1)


def paper_initial_shock(rule_code: Any) -> float | None:
    return PAPER_INITIAL_SHOCKS.get(normalize_hs_code(rule_code, 8) or "")


def legal_rate_for_date(rule_code: Any, value: Any) -> float | None:
    """Return a source-bounded legal rate, or null outside the audited range."""

    code = normalize_hs_code(rule_code, 8) or ""
    date = pd.to_datetime(value, errors="coerce")
    if pd.isna(date) or date > LEGAL_SCHEDULE_END or code not in LEGAL_RATE_SCHEDULE:
        return None
    chosen = None
    for start, rate in LEGAL_RATE_SCHEDULE[code]:
        if date >= pd.Timestamp(start):
            chosen = rate
    return chosen


def scientific_code_fingerprint() -> str:
    """Hash the committed implementation, independent of orchestration paths."""
    path = Path(__file__)
    return sha256_file(path) if path.exists() else "missing"


# These codes are known from the local HTS descriptions.  Quota alternatives
# are explicitly non-universal; the final rate cannot be assigned without
# entry-level quota allocation.
ROLE_BY_RULE = {
    "99034501": "quota_or_trq_alternative",
    "99034502": "quota_or_trq_alternative",
    "99034506": "quota_or_trq_alternative",
    "99034522": "quota_or_trq_alternative",
    "99034525": "universal_additional_duty",
    "99038001": "universal_additional_duty",
    "99038002": "replacement_country_rate",
    "99038061": "conditional_entry_exception",
    "99038501": "universal_additional_duty",
    "99038505": "quota_or_trq_alternative",
    "99038506": "quota_or_trq_alternative",
    "99038809": "transitional_entry_rule",
    "99038815": "transitional_entry_rule",
    "99038816": "transitional_entry_rule",
}


def rule_role(rule_code: Any, description: Any = None) -> str:
    code = normalize_hs_code(rule_code, 8) or str(rule_code or "")
    if code in ROLE_BY_RULE:
        return ROLE_BY_RULE[code]
    text = str(description or "").upper()
    if "QUALIFYING CONTRACT" in text or "EXCLUDED FROM" in text:
        return "conditional_entry_exception"
    if "QUOTA" in text or "QUANTIT" in text:
        return "quota_or_trq_alternative"
    if "ENTERED FOR CONSUMPTION" in text or "EXPORTED TO THE UNITED STATES BEFORE" in text:
        return "transitional_entry_rule"
    return "universal_additional_duty"


def source_confidence(value: Any) -> str:
    text = str(value or "").lower()
    if "official_scope" in text:
        return "explicit_note_enumeration"
    if "same_row" in text or "explicit" in text:
        return "structural_same_row"
    if "heading_expansion" in text or "parts_subheading" in text:
        return "heading_expansion"
    if "note" in text:
        return "explicit_note_enumeration"
    if not text or text == "nan" or "cross" in text or "nearby" in text:
        return "unresolved"
    return "unresolved"


def structural_washer_links(source_path: Path) -> pd.DataFrame:
    """Read Note 17 washer scope from a local structured HTS table.

    This helper is deliberately source-only and returns no rows when the
    required structural identifiers are absent.  It never consults package
    family flags and never invents a fallback scope.
    """

    columns = ["HTS Number", "Description"]
    if not source_path.exists():
        return pd.DataFrame(columns=["hs8", "rule_code", "family", "scope_source"])
    try:
        source = pd.read_csv(source_path, usecols=columns, dtype="string")
    except Exception:
        return pd.DataFrame(columns=["hs8", "rule_code", "family", "scope_source"])
    identifiers = " ".join(
        value
        for column in columns
        for value in source[column].dropna().astype(str).tolist()
    )
    required = {"8450.11.00", "8450.20.00", "9903.45.01", "9903.45.02", "9903.45.06"}
    if not required.issubset(set(re.findall(r"\d{4}\.\d{2}\.\d{2}", identifiers))):
        return pd.DataFrame(columns=["hs8", "rule_code", "family", "scope_source"])
    rows = [
        {
            "hs8": hs8,
            "rule_code": rule,
            "family": "washer_201",
            "release_name": source_path.stem,
            "release_start_date": pd.Timestamp("2018-02-07"),
            "release_end_date": pd.Timestamp("2019-12-31"),
            "scope_source": "local_hts_note_17_structural_scope",
        }
        for rule, hs8_values in {
            "99034501": ("84501100", "84502000"),
            "99034502": ("84501100", "84502000"),
            "99034506": ("84509020", "84509060"),
        }.items()
        for hs8 in hs8_values
    ]
    return pd.DataFrame(rows)


def source_qualified_links(config: PipelineConfig) -> pd.DataFrame:
    """Keep only links whose provenance is an explicit structural scope.

    The v1 cache contains broad PDF-context links.  They remain available for
    diagnostics, but cannot enter v2 production scope.
    """
    links = v1._all_links(config)
    # Section 301 already has a separately audited source-only scope ledger.
    # Reuse that ledger as an independent family component instead of feeding
    # its broad PDF-context links through the generic pooled parser.
    section301_scope = config.verification_dir / "raw_replication_imports" / "policy_replication_v2" / "official_section301_scope.parquet"
    if section301_scope.exists():
        try:
            section = pd.read_parquet(section301_scope, columns=["rule_code", "hs8", "source_pdf", "legal_date", "paper_period"])
            section = section.rename(columns={"source_pdf": "release_name", "legal_date": "release_start_date", "paper_period": "release_end_date"})
            section["rule_code"] = section["rule_code"].map(lambda x: normalize_hs_code(x, 8))
            section["hs8"] = section["hs8"].map(lambda x: normalize_hs_code(x, 8))
            section["family"] = "china_301"
            section["scope_source"] = "section301_policy_replication_v2_official_scope"
            links = pd.concat([links, section], ignore_index=True, sort=False)
        except Exception:
            pass
    # Note 18's structural definition explicitly refers to subheading
    # 8541.40.60.  The broad PDF-context cache incorrectly associated the
    # solar rules with 8541.90.00 and 8507.20.80.  Add only the explicit
    # source-defined heading, retaining quota/exclusion status separately.
    solar_pdf = config.raw_dir / "policy" / "archive" / "pdf" / "2018HTSARevision12.pdf"
    if solar_pdf.exists():
        try:
            text = v1.extract_pdf_text(solar_pdf)
            normalized = text.lower().replace(" ", "")
            if "9903.45.25" in normalized and "8541.40.60" in normalized:
                solar_rows = pd.DataFrame([
                    {
                        "release_name": solar_pdf.stem,
                        "release_start_date": pd.Timestamp("2018-01-01"),
                        "release_end_date": pd.Timestamp("2019-12-31"),
                        "hs8": "85414060",
                        "rule_code": rule,
                        "family": "solar_201",
                        "scope_source": "local_hts_note_18_structural_85414060",
                    }
                    for rule in ("99034522", "99034525")
                ])
                links = pd.concat([links, solar_rows], ignore_index=True, sort=False)
        except Exception:
            pass
    # Note 17(c) and 17(f) identify finished residential machines and parts
    # separately.  The earlier parser retained only the parts line, which
    # understated washer scope.  Use the local Revision 12 structured HTS
    # table as the source and record the structural basis explicitly.
    washer_source = config.raw_dir / "policy" / "archive" / "data" / "hts_2018_revision_12_data.csv"
    washer_rows = structural_washer_links(washer_source)
    if not washer_rows.empty:
        links = pd.concat([links, washer_rows], ignore_index=True, sort=False)
    if links.empty:
        return pd.DataFrame(columns=["hs8", "rule_code", "family", "scope_confidence"])
    out = links.copy()
    out["scope_confidence"] = out.get("scope_source", "").map(source_confidence)
    out["rule_code"] = out["rule_code"].map(lambda x: normalize_hs_code(x, 8))
    out["hs8"] = out["hs8"].map(lambda x: normalize_hs_code(x, 8))
    for column in ("release_start_date", "release_end_date"):
        if column in out:
            out[column] = pd.to_datetime(out[column], errors="coerce")
    for column in ("release_name", "scope_source", "family", "legal_role"):
        if column in out:
            out[column] = out[column].astype("string")
    out = out[out["scope_confidence"].isin({"structural_same_row", "heading_expansion", "explicit_note_enumeration"})]
    out = out[out["rule_code"].notna() & out["hs8"].notna()]
    out["legal_role"] = out["rule_code"].map(rule_role)
    # A quota alternative is retained as a conditional source record but is not
    # promoted to a universal product treatment by the builder.
    return out.drop_duplicates(["rule_code", "hs8", "scope_confidence"]).reset_index(drop=True)


def family_source_status(links: pd.DataFrame, attrs: pd.DataFrame) -> dict[str, dict[str, Any]]:
    attrs = attrs.copy()
    if not attrs.empty:
        attrs["rule_code"] = attrs["rule_code"].map(lambda x: normalize_hs_code(x, 8))
    result: dict[str, dict[str, Any]] = {}
    for family in FAMILIES:
        family_attrs = attrs[attrs["rule_code"].isin(set(v1.EXPECTED_POSITIVE_RULE_PREFIXES[family]))] if not attrs.empty else attrs
        # China uses a prefix, unlike the exact 201/232 code families.
        if family == "china_301" and not attrs.empty:
            family_attrs = attrs[attrs["rule_code"].astype(str).str.startswith("990388")]
        positive = sorted(set(family_attrs.loc[pd.to_numeric(family_attrs.get("increment_rate"), errors="coerce").gt(0), "rule_code"])) if not family_attrs.empty else []
        family_links = links[links["family"] == family] if not links.empty else links
        linked = sorted(set(family_links["rule_code"])) if not family_links.empty else []
        universal = sorted(code for code in positive if rule_role(code) == "universal_additional_duty")
        missing = sorted(set(universal) - set(linked))
        quota = sorted(code for code in positive if rule_role(code) == "quota_or_trq_alternative")
        result[family] = {
            "positive_rules": positive,
            "linked_rules": linked,
            "universal_positive_rules": universal,
            "quota_or_trq_rules": quota,
            "missing_universal_scope": missing,
            "scope_status": "complete" if family_links is not None and not missing and (universal or quota) else "blocked_missing_scope",
            "quota_status": "blocked_without_entry_allocation" if quota else "not_applicable",
        }
    return result


def select_stack_action(actions: pd.DataFrame) -> pd.DataFrame:
    """Apply legal role precedence within a family/action group.

    This function is intentionally deterministic and never sums quota tiers.
    Additive pooling occurs only after this family-level selection and only
    across distinct policy families.
    """
    if actions.empty:
        return actions.copy()
    frame = actions.copy()
    frame["legal_role"] = frame["rule_code"].map(rule_role)
    key = [c for c in ["partner_name", "hs8", "year", "month", "family"] if c in frame.columns]
    if not key:
        return frame
    chosen: list[pd.DataFrame] = []
    for _, group in frame.groupby(key, dropna=False, sort=False):
        roles = set(group["legal_role"])
        if "replacement_country_rate" in roles:
            group = group[group["legal_role"].eq("replacement_country_rate")]
        elif "universal_additional_duty" in roles:
            group = group[group["legal_role"].eq("universal_additional_duty")]
        elif "quota_or_trq_alternative" in roles:
            # Monthly trade data do not reveal entry-level quota allocation.
            # Preserve the unresolved record but do not assign its rate.
            group = group.iloc[0:0]
        else:
            group = group.iloc[0:0]
        if not group.empty:
            chosen.append(group)
    return pd.concat(chosen, ignore_index=True) if chosen else frame.iloc[0:0].copy()


def stale_manifest(config: PipelineConfig) -> dict[str, Any]:
    files = [
        "data/analysis/passthru_data/policy/pooled_policy_replication_v1/family_policy_schedule.parquet",
        "data/analysis/passthru_data/policy/pooled_policy_replication_v1/legal_action_ledger.parquet",
        "data/analysis/passthru_data/policy/pooled_policy_replication_v1/independent_final_legal_pooled_policy.parquet",
    ]
    records = []
    for rel in files:
        path = config.repo_root / rel
        records.append({
            "path": rel,
            "exists": path.exists(),
            "sha256": sha256_file(path) if path.exists() else None,
            "decision": "historical_diagnostic_not_resumable",
            "reason": "v1 fingerprint did not cover scientific implementation, source scope, or stacking semantics",
        })
    return {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_mode": LEGAL_SOURCE_MODE,
        "records": records,
        "resume_allowed": False,
        "replacement_namespace": VERSION,
    }


def rule_inventory(attrs: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    """Return a compact source/rule inventory for the next review pass."""
    if attrs.empty:
        return pd.DataFrame(columns=["rule_code", "family", "legal_role", "scope_rows", "decision"])
    frame = attrs.copy()
    frame["rule_code"] = frame["rule_code"].map(lambda x: normalize_hs_code(x, 8))
    rows: list[dict[str, Any]] = []
    for code, group in frame.groupby("rule_code", dropna=True):
        if not code:
            continue
        family = v1._rule_family(str(code))
        role = rule_role(code, group.get("description", pd.Series(dtype="string")).dropna().iloc[0] if "description" in group and group["description"].notna().any() else "")
        scope_rows = int((links[links["rule_code"].eq(code)]).shape[0]) if not links.empty else 0
        if role == "quota_or_trq_alternative":
            decision = "conditional_unresolved_entry_allocation"
        elif scope_rows == 0 and role == "universal_additional_duty":
            decision = "blocked_missing_structural_scope"
        else:
            decision = "eligible_for_family_review"
        rows.append({
            "rule_code": str(code),
            "family": family,
            "legal_role": role,
            "min_source_year": int(pd.to_numeric(group["year"], errors="coerce").min()) if "year" in group else None,
            "max_source_year": int(pd.to_numeric(group["year"], errors="coerce").max()) if "year" in group else None,
            "distinct_rates": int(group["increment_rate"].nunique(dropna=True)) if "increment_rate" in group else 0,
            "min_rate": float(pd.to_numeric(group["increment_rate"], errors="coerce").min()) if "increment_rate" in group else None,
            "max_rate": float(pd.to_numeric(group["increment_rate"], errors="coerce").max()) if "increment_rate" in group else None,
            "scope_rows": scope_rows,
            "decision": decision,
        })
    return pd.DataFrame(rows).sort_values(["family", "rule_code"]).reset_index(drop=True)


def build_preflight(config: PipelineConfig) -> dict[str, Any]:
    out = root(config)
    write_metadata_json(out / "pooled_policy_v1_stale_manifest.json", stale_manifest(config))
    write_metadata_json(out / "policy_target_crosswalk.json", {
        "version": VERSION,
        "package_columns": list(PACKAGE_POLICY_COLUMNS),
        "targets": TARGET_CROSSWALK,
        "package_is_validation_only": True,
    })
    write_metadata_json(out / "historical_policy_benchmark_contract.json", load_contract())
    attrs = v1._load_tradewar_rule_attributes(config)
    links = source_qualified_links(config)
    statuses = family_source_status(links, attrs)
    write_parquet(links, out / "source_qualified_scope_links.parquet", overwrite=True)
    write_parquet(rule_inventory(attrs, links), out / "rule_inventory.parquet", overwrite=True)
    write_metadata_json(out / "family_source_status.json", {
        "version": VERSION,
        "families": statuses,
        "source_mode": LEGAL_SOURCE_MODE,
        "package_policy_used_by_builder": False,
        "code_fingerprint": scientific_code_fingerprint(),
        "specification_fingerprint": specification_fingerprint(),
    })
    blocked = [f for f, s in statuses.items() if s["scope_status"] != "complete" or s["quota_status"].startswith("blocked")]
    result = {
        "version": VERSION,
        "status": "blocked_missing_or_conditional_scope" if blocked else "preflight_passed",
        "blocked_families": blocked,
        "source_qualified_link_rows": int(len(links)),
        "source_mode": LEGAL_SOURCE_MODE,
        "paper_mode": PAPER_SOURCE_MODE,
        "package_policy_used_by_builder": False,
        "code_fingerprint": scientific_code_fingerprint(),
        "specification_fingerprint": specification_fingerprint(),
        "legal_calendar": "independent_legal_effective_date",
        "paper_calendars": ["m_status1/m_effective_mdate1", "m_status2/m_effective_mdate2"],
        "paper_initial_shocks": PAPER_INITIAL_SHOCKS,
        "legal_rate_schedule_end": LEGAL_SCHEDULE_END.strftime("%Y-%m-%d"),
        "published_comparison_rule": "only do-file-consistent calendar and policy variables may enter the paper gate",
        "ready_for_policy_regression": not blocked,
        "ready_for_2025_event": False,
    }
    write_metadata_json(out / "pooled_policy_v2_preflight.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-closed pooled policy v2 source preflight")
    parser.add_argument("--preflight-only", action="store_true", default=True)
    args = parser.parse_args()
    result = build_preflight(PipelineConfig.default())
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["status"] == "preflight_passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
