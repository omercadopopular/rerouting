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
import os
import tempfile

import pandas as pd

from .config import PipelineConfig
from . import policy_sources as v1
from .io_utils import normalize_hs_code, sha256_file, write_metadata_json, write_parquet
from .policy_benchmark_contract import load_contract


VERSION = "pooled_policy_replication_v2"
FAMILIES = v1.FAMILIES
LEGAL_SOURCE_MODE = "independent_local_official_sources"
PAPER_SOURCE_MODE = "paper_compatible_from_independent_legal_ledger"
# The paper's appendix explicitly omits the small set of varieties whose
# additional tariff applies only after a quota threshold.  This is a
# paper-compatible exclusion, not a legal assertion that the quota tier is
# irrelevant.  The legal object remains unresolved without entry allocation.
PAPER_QUOTA_DECISION = "omit_threshold_only_quota_increment_per_appendix_footnote_1"

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
            "paper_compatible_quota_rules": quota,
            "paper_compatible_quota_decision": PAPER_QUOTA_DECISION if quota else "not_applicable",
            "paper_scope_status": "complete" if family_links is not None and not missing and (universal or quota) else "blocked_missing_scope",
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
    legal_blocked = [f for f, s in statuses.items() if s["scope_status"] != "complete" or s["quota_status"].startswith("blocked")]
    paper_blocked = [f for f, s in statuses.items() if s.get("paper_scope_status") != "complete"]
    if paper_blocked:
        status = "blocked_paper_scope"
    elif legal_blocked:
        status = "paper_eligible_legal_quota_blocked"
    else:
        status = "preflight_passed"
    result = {
        "version": VERSION,
        "status": status,
        "blocked_families": legal_blocked,
        "paper_blocked_families": paper_blocked,
        "source_qualified_link_rows": int(len(links)),
        "source_mode": LEGAL_SOURCE_MODE,
        "paper_mode": PAPER_SOURCE_MODE,
        "package_policy_used_by_builder": False,
        "code_fingerprint": scientific_code_fingerprint(),
        "specification_fingerprint": specification_fingerprint(),
        "legal_calendar": "independent_legal_effective_date",
        "paper_calendars": ["m_status1/m_effective_mdate1", "m_status2/m_effective_mdate2"],
        "paper_initial_shocks": PAPER_INITIAL_SHOCKS,
        "paper_quota_decision": PAPER_QUOTA_DECISION,
        "legal_rate_schedule_end": LEGAL_SCHEDULE_END.strftime("%Y-%m-%d"),
        "published_comparison_rule": "only do-file-consistent calendar and policy variables may enter the paper gate",
        "ready_for_policy_regression": not paper_blocked,
        "ready_for_paper_policy_regression": not paper_blocked,
        "ready_for_independent_legal_policy_regression": not legal_blocked,
        "independent_legal_policy_gate": not legal_blocked,
        "paper_compatible_policy_gate": not paper_blocked,
        "ready_for_2025_event": False,
    }
    write_metadata_json(out / "pooled_policy_v2_preflight.json", result)
    return result


def _exclusive_active_days(effective_date: Any, year: int, month: int) -> tuple[int, int]:
    """Return active days and month length using the registered convention."""
    date = pd.to_datetime(effective_date, errors="coerce")
    month_start = pd.Timestamp(year=int(year), month=int(month), day=1)
    month_end = month_start + pd.offsets.MonthEnd(1)
    if pd.isna(date) or date.normalize() > month_end:
        return 0, int(month_end.day)
    if date.normalize() < month_start:
        return int(month_end.day), int(month_end.day)
    return max(0, int(month_end.day) - int(date.day)), int(month_end.day)


def _build_v2_actions(config: PipelineConfig, *, paper_compatible: bool) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Expand only source-qualified links into a paper or legal action ledger."""
    attrs = v1._load_tradewar_rule_attributes(config)
    links = source_qualified_links(config)
    actions = v1._expand_actions(config, links, attrs)
    if actions.empty:
        return actions, {"source_link_rows": int(len(links)), "action_rows": 0}
    actions = actions.copy()
    actions["rule_code"] = actions["rule_code"].map(lambda value: normalize_hs_code(value, 8))
    actions["legal_role"] = actions["rule_code"].map(rule_role)
    actions["legal_effective_date"] = pd.to_datetime(actions["legal_effective_date"], errors="coerce")
    quota_action_count = int(actions["legal_role"].eq("quota_or_trq_alternative").sum())
    if paper_compatible:
        # The published m_* treatment variables are product-level shocks: the
        # package assigns a treated product's increment to every bilateral
        # observation in the event-study sample, rather than restricting the
        # treatment column to the partner that enacted the measure.  Preserve
        # the independent source scope in the legal object, but expand the
        # paper object across the observed partner universe before estimation.
        templates = actions.drop(columns=["partner_name"]).drop_duplicates(
            ["rule_code", "hs8", "year", "month", "family", "legal_effective_date", "legal_end_date", "additional_rate"]
        )
        # A sentinel partner keeps the ledger compact; the panel writer
        # expands it by joining on product and month for every partner.
        actions = templates.assign(partner_name="__ALL_PARTNERS__")
        actions["action_id"] = actions.apply(
            lambda row: f"{row['rule_code']}|{row['hs8']}|{row.get('release_name','')}|ALL|{int(row['year']):04d}-{int(row['month']):02d}",
            axis=1,
        )
        # The appendix excludes only the small threshold-only variety set; it
        # does not remove the principal safeguard schedules.  Monthly HS10
        # data cannot allocate an entry to an in-/over-quota tier, so the paper
        # object uses the source-listed principal rate and records that this is
        # a deterministic paper convention.  The legal object remains null/
        # blocked for unresolved entry-level quota allocation.
    # Recompute active days after the v1 inclusive-day expansion.  The package
    # example's arithmetic excludes the effective date itself.
    day_values = [
        _exclusive_active_days(date, year, month)
        for date, year, month in zip(actions["legal_effective_date"], actions["year"], actions["month"])
    ]
    actions["active_days"] = [value[0] for value in day_values]
    actions["days_in_month"] = [value[1] for value in day_values]
    actions = actions.loc[actions["active_days"].gt(0)].copy()
    actions["active_share"] = actions["active_days"] / actions["days_in_month"]
    actions["day_weighted_additional_rate"] = actions["additional_rate"] * actions["active_share"]
    actions["paper_effective_month"] = actions["legal_effective_date"].map(paper_month_from_legal_date).dt.strftime("%Y-%m")
    actions["paper_quota_decision"] = PAPER_QUOTA_DECISION if paper_compatible else "quota_rate_unresolved_without_entry_allocation"
    actions = actions.drop_duplicates("action_id").reset_index(drop=True)
    return actions, {
        "source_link_rows": int(len(links)),
        "action_rows": int(len(actions)),
        "quota_actions_omitted": 0 if paper_compatible else quota_action_count,
        "quota_actions_retained_paper_convention": quota_action_count if paper_compatible else 0,
        "paper_compatible": paper_compatible,
    }


def _write_v2_panel(config: PipelineConfig, actions: pd.DataFrame, *, paper_compatible: bool, overwrite: bool) -> Path:
    """Materialize an independently sourced partner-HS10-month policy panel."""
    panel_name = "paper_compatible_full_trade_policy_panel.parquet" if paper_compatible else "independent_legal_full_trade_policy_panel.parquet"
    destination = analysis_root(config) / panel_name
    components_path = analysis_root(config) / ("paper_compatible_family_policy_schedule.parquet" if paper_compatible else "legal_family_policy_schedule.parquet")
    if destination.exists() and not overwrite:
        return destination
    if actions.empty:
        raise RuntimeError("Cannot materialize pooled policy panel from an empty action ledger")
    # Paper-compatible policy is product-level and is joined to every partner;
    # the legal object remains partner-specific.
    keys = (["hs8", "year", "month", "family"] if paper_compatible
            else ["partner_name", "hs8", "year", "month", "family"])
    components = actions.groupby(keys, as_index=False).agg(
        action_count=("action_id", "nunique"),
        additional_rate=("additional_rate", "sum"),
        day_weighted_additional_rate=("day_weighted_additional_rate", "sum"),
        legal_effective_date=("legal_effective_date", "min"),
        paper_effective_month=("paper_effective_month", "min"),
        source_action_ids=("action_id", lambda values: "|".join(sorted(set(map(str, values))))),
    )
    components["policy_mode"] = PAPER_SOURCE_MODE if paper_compatible else LEGAL_SOURCE_MODE
    write_parquet(components, components_path, overwrite=True)
    raw_panel = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    baseline = config.verification_dir / "raw_replication_imports" / "policy_replication_v2" / "fixed_2017_mfn_baseline.parquet"
    if not raw_panel.exists() or not baseline.exists():
        raise FileNotFoundError(f"Missing independent raw panel or baseline: {raw_panel} / {baseline}")
    import duckdb
    component_sql = str(components_path).replace("'", "''")
    raw_sql = str(raw_panel).replace("'", "''")
    baseline_sql = str(baseline).replace("'", "''")
    family_rollup = ", ".join(
        f"max(CASE WHEN family = '{family}' THEN additional_rate END) AS {family}_additional_rate, "
        f"max(CASE WHEN family = '{family}' THEN day_weighted_additional_rate END) AS {family}_day_weighted_additional_rate, "
        f"min(CASE WHEN family = '{family}' THEN legal_effective_date END) AS {family}_legal_effective_date, "
        f"min(CASE WHEN family = '{family}' THEN paper_effective_month END) AS {family}_paper_effective_month, "
        f"max(CASE WHEN family = '{family}' THEN source_action_ids END) AS {family}_source_action_ids"
        for family in FAMILIES
    )
    family_select = ", ".join(
        f"a.{family}_additional_rate, a.{family}_day_weighted_additional_rate, a.{family}_legal_effective_date, a.{family}_paper_effective_month, "
        f"CASE WHEN a.{family}_additional_rate IS NULL THEN 0 ELSE 1 END AS {family}_hit"
        for family in FAMILIES
    )
    add_sum = " + ".join(f"coalesce(a.{family}_additional_rate, 0)" for family in FAMILIES)
    weighted_sum = " + ".join(f"coalesce(a.{family}_day_weighted_additional_rate, 0)" for family in FAMILIES)
    legal_dates = ", ".join(f"a.{family}_legal_effective_date" for family in FAMILIES)
    paper_dates = ", ".join(f"a.{family}_paper_effective_month" for family in FAMILIES)
    con = duckdb.connect()
    temporary = destination.with_name(f".{destination.name}.{VERSION}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        query = f"""
        WITH c AS (
          SELECT {"NULL::VARCHAR AS partner_name," if paper_compatible else "partner_name,"} hs8, year, month, family,
                 sum(additional_rate) AS additional_rate,
                 sum(day_weighted_additional_rate) AS day_weighted_additional_rate,
                 min(legal_effective_date) AS legal_effective_date,
                 min(paper_effective_month) AS paper_effective_month,
                 max(source_action_ids) AS source_action_ids
          FROM read_parquet('{component_sql}')
          GROUP BY ALL
        ), a AS (
          SELECT {"hs8, year, month," if paper_compatible else "partner_name, hs8, year, month,"} {family_rollup}
          FROM c
          GROUP BY {"hs8, year, month" if paper_compatible else "partner_name, hs8, year, month"}
        ), p AS (
          SELECT cty_code, upper(cty_name) AS cty_name, hs10, hs8, year, month
          FROM read_parquet('{raw_sql}')
          WHERE year BETWEEN {int(config.start_period[:4])} AND {int(config.end_period[:4])}
        )
        SELECT p.cty_code, p.cty_name, p.hs10, p.hs8, p.year, p.month,
               b.fixed_2017_mfn_rate AS independent_base_mfn_rate,
               b.baseline_rate_kind, b.baseline_source,
               {family_select},
               ({add_sum}) AS independent_additional_rate,
               ({weighted_sum}) AS independent_day_weighted_additional_rate,
               CASE WHEN ({add_sum}) > 0 THEN TRUE ELSE FALSE END AS independent_treated,
               CASE WHEN ({add_sum}) > 0 AND p.cty_code = 5700 THEN 2 WHEN ({add_sum}) > 0 THEN 1 ELSE 0 END::TINYINT AS independent_paper_status,
               CASE WHEN ({add_sum}) > 0 AND p.cty_code = 5700 THEN 2 WHEN ({add_sum}) > 0 THEN 1 ELSE 0 END::TINYINT AS independent_legal_status,
               CASE WHEN b.fixed_2017_mfn_rate IS NULL THEN NULL ELSE b.fixed_2017_mfn_rate + ({add_sum}) END AS independent_paper_total_tariff,
               CASE WHEN b.fixed_2017_mfn_rate IS NULL THEN NULL ELSE b.fixed_2017_mfn_rate + ({weighted_sum}) END AS independent_paper_dayweighted_total_tariff,
               CASE WHEN b.fixed_2017_mfn_rate IS NULL THEN NULL ELSE b.fixed_2017_mfn_rate + ({add_sum}) END AS independent_legal_total_tariff,
               CASE WHEN b.fixed_2017_mfn_rate IS NULL THEN NULL ELSE b.fixed_2017_mfn_rate + ({weighted_sum}) END AS independent_legal_dayweighted_total_tariff,
               strftime(least({legal_dates}), '%Y-%m') AS independent_legal_effective_month,
               coalesce({paper_dates}) AS independent_paper_effective_month,
               '{PAPER_SOURCE_MODE if paper_compatible else LEGAL_SOURCE_MODE}' AS policy_source_mode,
               '{PAPER_QUOTA_DECISION if paper_compatible else 'quota_rate_unresolved_without_entry_allocation'}' AS quota_decision
        FROM p
        LEFT JOIN a ON {"p.hs8=a.hs8 AND p.year=a.year AND p.month=a.month" if paper_compatible else "p.cty_name=a.partner_name AND p.hs8=a.hs8 AND p.year=a.year AND p.month=a.month"}
        LEFT JOIN read_parquet('{baseline_sql}') b USING (hs10)
        """
        con.execute(f"COPY ({query}) TO '{str(temporary).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(temporary)
        if pf.metadata.num_rows <= 0 or pf.metadata.num_row_groups <= 0:
            raise RuntimeError("Pooled v2 panel validation failed: empty Parquet")
        compression = {pf.metadata.row_group(0).column(i).compression for i in range(pf.metadata.row_group(0).num_columns)}
        if compression != {"ZSTD"}:
            raise RuntimeError(f"Pooled v2 panel expected ZSTD, got {compression}")
        del pf
        temporary.replace(destination)
    finally:
        con.close()
        temporary.unlink(missing_ok=True)
    write_metadata_json(destination.with_suffix(".json"), {
        "version": VERSION,
        "artifact_category": "detailed_diagnostic",
        "canonical_relative_path": relative(config, destination),
        "row_count": int(pd.read_parquet(destination, columns=["hs10"]).shape[0]),
        "key_columns": ["cty_code", "hs10", "year", "month"],
        "source_paths": [relative(config, raw_panel), relative(config, baseline), relative(config, components_path)],
        "source_hashes": [sha256_file(raw_panel), sha256_file(baseline), sha256_file(components_path)],
        "compression": "ZSTD",
        "paper_compatible": paper_compatible,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    return destination


def build_pooled_v2_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Build paper-compatible and legal independent pooled panels."""
    preflight = build_preflight(config)
    if not preflight.get("paper_compatible_policy_gate"):
        raise RuntimeError("Paper-compatible pooled panel blocked by source preflight")
    paper_actions, paper_meta = _build_v2_actions(config, paper_compatible=True)
    legal_actions, legal_meta = _build_v2_actions(config, paper_compatible=False)
    paper_ledger = analysis_root(config) / "paper_compatible_action_ledger.parquet"
    legal_ledger = analysis_root(config) / "legal_action_ledger_v2.parquet"
    write_parquet(paper_actions, paper_ledger, overwrite=True)
    write_parquet(legal_actions, legal_ledger, overwrite=True)
    paper_panel = _write_v2_panel(config, paper_actions, paper_compatible=True, overwrite=overwrite)
    legal_panel = _write_v2_panel(config, legal_actions, paper_compatible=False, overwrite=overwrite)
    manifest = {
        "version": VERSION,
        "status": "built_paper_and_legal_panels",
        "paper_action_ledger": relative(config, paper_ledger),
        "legal_action_ledger": relative(config, legal_ledger),
        "paper_panel": relative(config, paper_panel),
        "legal_panel": relative(config, legal_panel),
        "paper_metadata": paper_meta,
        "legal_metadata": legal_meta,
        "paper_quota_decision": PAPER_QUOTA_DECISION,
        "independent_legal_gate": False,
        "paper_compatible_regression_gate": "pending_validation",
        "package_policy_used_by_builder": False,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "code_fingerprint": scientific_code_fingerprint(),
        "specification_fingerprint": specification_fingerprint(),
    }
    write_metadata_json(root(config) / "pooled_policy_v2_panel_manifest.json", manifest)
    return manifest


def validate_pooled_v2_panels(config: PipelineConfig) -> dict[str, Any]:
    """Compare the independent paper panel with the package projection."""
    paper_panel = analysis_root(config) / "paper_compatible_full_trade_policy_panel.parquet"
    legal_panel = analysis_root(config) / "independent_legal_full_trade_policy_panel.parquet"
    package_projection = root(config) / "package_policy_projection_v2.parquet"
    if not package_projection.exists():
        legacy = config.verification_dir / "raw_replication_imports" / "pooled_policy_replication_v1" / "package_policy_projection_v2.parquet"
        if legacy.exists():
            package_projection = legacy
    if not paper_panel.exists() or not legal_panel.exists() or not package_projection.exists():
        result = {"status": "blocked_missing_artifact", "paper_panel": relative(config, paper_panel), "legal_panel": relative(config, legal_panel), "package_projection": relative(config, package_projection)}
        write_metadata_json(root(config) / "pooled_policy_v2_variable_gate.json", result)
        return result
    import duckdb
    comparison_path = root(config) / "pooled_policy_v2_validation_comparison.parquet"
    summary_path = root(config) / "pooled_policy_v2_validation_summary.csv"
    con = duckdb.connect()
    try:
        p = str(paper_panel).replace("'", "''")
        l = str(legal_panel).replace("'", "''")
        r = str(package_projection).replace("'", "''")
        query = f"""
        WITH paper AS (SELECT * FROM read_parquet('{p}')),
             legal AS (SELECT * FROM read_parquet('{l}')),
             pkg AS (SELECT * FROM read_parquet('{r}') WHERE year BETWEEN 2017 AND 2019)
        SELECT p.cty_code,p.cty_name,p.hs10,p.hs8,p.year,p.month,
               p.independent_base_mfn_rate,p.independent_additional_rate,p.independent_day_weighted_additional_rate,
               p.solar_201_additional_rate,p.washer_201_additional_rate,p.steel_232_additional_rate,p.aluminum_232_additional_rate,p.china_301_additional_rate,
               p.independent_paper_total_tariff,p.independent_paper_dayweighted_total_tariff,
               p.independent_paper_status,p.independent_paper_effective_month,
               l.independent_legal_effective_month,
               pkg.m_increase,pkg.m_stattariff1,pkg.m_stattariff2,pkg.m_effective_mdate1,pkg.m_effective_mdate2,
               pkg.m_china_hit,pkg.m_steel_hit,pkg.m_alum_hit,pkg.m_washer_hit,pkg.m_solar_hit,pkg.m_val,
               -- m_*_hit are product-level ever-treated flags in the package;
               -- the monthly treatment comparison must use the package's
               -- monthly additional-rate field, m_increase.
               CASE WHEN pkg.m_increase IS NULL OR p.independent_treated IS NULL THEN NULL
                    WHEN (cast(pkg.m_increase AS DOUBLE) > 0) = p.independent_treated THEN 1 ELSE 0 END AS treatment_match,
               CASE WHEN pkg.m_increase IS NULL OR p.independent_additional_rate IS NULL THEN NULL ELSE abs(cast(pkg.m_increase AS DOUBLE)-p.independent_additional_rate) END AS increment_abs_diff,
               CASE WHEN pkg.m_increase IS NULL OR p.independent_day_weighted_additional_rate IS NULL THEN NULL ELSE abs(cast(pkg.m_increase AS DOUBLE)-p.independent_day_weighted_additional_rate) END AS dayweighted_increment_abs_diff,
               CASE WHEN pkg.m_effective_mdate2 IS NULL OR p.independent_paper_effective_month IS NULL THEN NULL WHEN strftime(pkg.m_effective_mdate2,'%Y-%m')=p.independent_paper_effective_month THEN 1 ELSE 0 END AS paper_month_exact,
               CASE WHEN pkg.m_effective_mdate1 IS NULL OR l.independent_legal_effective_month IS NULL THEN NULL WHEN strftime(pkg.m_effective_mdate1,'%Y-%m')=l.independent_legal_effective_month THEN 1 ELSE 0 END AS legal_month_exact,
               CASE WHEN pkg.m_stattariff2 IS NULL OR p.independent_paper_dayweighted_total_tariff IS NULL THEN NULL ELSE abs(cast(pkg.m_stattariff2 AS DOUBLE)-p.independent_paper_dayweighted_total_tariff) END AS total_dayweighted_abs_diff,
               CASE WHEN pkg.m_stattariff1 IS NULL OR p.independent_paper_total_tariff IS NULL THEN NULL ELSE abs(cast(pkg.m_stattariff1 AS DOUBLE)-p.independent_paper_total_tariff) END AS total_abs_diff
        FROM paper p
        JOIN legal l USING (cty_code,hs10,year,month)
        LEFT JOIN pkg USING (cty_code,hs10,year,month)
        WHERE p.year BETWEEN 2017 AND 2019
        """
        temp = comparison_path.with_name(f".{comparison_path.name}.{VERSION}.tmp")
        temp.unlink(missing_ok=True)
        con.execute(f"COPY ({query}) TO '{str(temp).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        temp.replace(comparison_path)
        aggregate = con.execute(f"""
          SELECT count(*) AS nrows,
                 count(*) FILTER (WHERE treatment_match IS NOT NULL) AS package_rows,
                 avg(treatment_match) FILTER (WHERE treatment_match IS NOT NULL) AS treatment_match,
                 sum(CASE WHEN m_val>0 AND treatment_match IS NOT NULL THEN m_val*treatment_match ELSE 0 END)/nullif(sum(CASE WHEN m_val>0 AND treatment_match IS NOT NULL THEN m_val ELSE 0 END),0) AS trade_weighted_treatment_match,
                 avg(increment_abs_diff) AS increment_mae,
                 avg(dayweighted_increment_abs_diff) AS dayweighted_increment_mae,
                 avg(CASE WHEN increment_abs_diff<=0.001 THEN 1.0 ELSE 0.0 END) FILTER (WHERE increment_abs_diff IS NOT NULL) AS increment_within_10bp,
                 avg(CASE WHEN increment_abs_diff<=0.005 THEN 1.0 ELSE 0.0 END) FILTER (WHERE increment_abs_diff IS NOT NULL) AS increment_within_50bp,
                 avg(paper_month_exact) FILTER (WHERE paper_month_exact IS NOT NULL) AS paper_month_exact,
                 avg(legal_month_exact) FILTER (WHERE legal_month_exact IS NOT NULL) AS legal_month_exact,
                 avg(total_abs_diff) AS total_tariff_mae,
                 avg(total_dayweighted_abs_diff) AS total_dayweighted_tariff_mae,
                 avg(CASE WHEN total_abs_diff<=0.01 THEN 1.0 ELSE 0.0 END) FILTER (WHERE total_abs_diff IS NOT NULL) AS total_within_1pp
          FROM read_parquet('{str(comparison_path).replace(chr(39), chr(39)*2)}')
        """).fetchdf().iloc[0].to_dict()
        weighted = con.execute(f"""
          SELECT
            sum(CASE WHEN m_val IS NOT NULL AND increment_abs_diff IS NOT NULL THEN abs(cast(m_val AS DOUBLE))*increment_abs_diff ELSE 0 END)
              / nullif(sum(CASE WHEN m_val IS NOT NULL AND increment_abs_diff IS NOT NULL THEN abs(cast(m_val AS DOUBLE)) ELSE 0 END),0) AS trade_weighted_increment_mae,
            sum(CASE WHEN m_val IS NOT NULL AND treatment_match IS NOT NULL AND treatment_match=1 THEN abs(cast(m_val AS DOUBLE)) ELSE 0 END)
              / nullif(sum(CASE WHEN m_val IS NOT NULL AND treatment_match IS NOT NULL THEN abs(cast(m_val AS DOUBLE)) ELSE 0 END),0) AS trade_weighted_treatment_match
          FROM read_parquet('{str(comparison_path).replace(chr(39), chr(39)*2)}')
        """).fetchdf().iloc[0].to_dict()
        aggregate.update({key: (float(value) if pd.notna(value) else None) for key, value in weighted.items()})
        rows = []
        for family, column in {"solar_201":"m_solar_hit","washer_201":"m_washer_hit","steel_232":"m_steel_hit","aluminum_232":"m_alum_hit","china_301":"m_china_hit"}.items():
            rate_column = f"{family}_additional_rate"
            rows.append(con.execute(f"""
              SELECT '{family}' AS family,
                     count(*) FILTER (WHERE {rate_column} IS NOT NULL) AS independent_scope_rows,
                     avg(CASE WHEN {rate_column} IS NOT NULL AND cast({column} AS DOUBLE)=1 THEN 1.0 ELSE 0.0 END) FILTER (WHERE {rate_column} IS NOT NULL) AS package_family_hit_match,
                     avg(abs(cast({rate_column} AS DOUBLE)-cast(m_increase AS DOUBLE))) FILTER (WHERE {rate_column} IS NOT NULL AND m_increase IS NOT NULL) AS family_rate_mae
              FROM read_parquet('{str(comparison_path).replace(chr(39), chr(39)*2)}')
            """).fetchdf().iloc[0].to_dict())
        family_summary = pd.DataFrame(rows)
        write_parquet(family_summary, root(config) / "pooled_policy_v2_family_validation.parquet", overwrite=True)
        family_summary.to_csv(root(config) / "pooled_policy_v2_family_validation_summary.csv", index=False)
        pd.DataFrame([aggregate]).to_csv(summary_path, index=False)
    finally:
        con.close()
    paper_thresholds = {
        "treatment_match": float(v1.PAPER_THRESHOLDS["treatment_match"]),
        "trade_weighted_treatment_match": float(v1.PAPER_THRESHOLDS["trade_weighted_treatment_match"]),
        "paper_month_exact": float(v1.PAPER_THRESHOLDS["effective_month_exact_match"]),
        "increment_within_10bp": float(v1.PAPER_THRESHOLDS["increment_within_10bp"]),
        "increment_within_50bp": float(v1.PAPER_THRESHOLDS["increment_within_50bp"]),
        "trade_weighted_increment_mae": float(v1.PAPER_THRESHOLDS["trade_weighted_increment_mae"]),
    }
    paper_checks = {
        "treatment_match": float(aggregate.get("treatment_match") or 0) >= paper_thresholds["treatment_match"],
        "trade_weighted_treatment_match": float(aggregate.get("trade_weighted_treatment_match") or 0) >= paper_thresholds["trade_weighted_treatment_match"],
        "paper_month_exact": float(aggregate.get("paper_month_exact") or 0) >= paper_thresholds["paper_month_exact"],
        "increment_within_10bp": float(aggregate.get("increment_within_10bp") or 0) >= paper_thresholds["increment_within_10bp"],
        "increment_within_50bp": float(aggregate.get("increment_within_50bp") or 0) >= paper_thresholds["increment_within_50bp"],
        "trade_weighted_increment_mae": float(aggregate.get("trade_weighted_increment_mae") or 1) <= paper_thresholds["trade_weighted_increment_mae"],
    }
    gates = {
        "status": "diagnostic_complete",
        "paper_panel": relative(config, paper_panel),
        "legal_panel": relative(config, legal_panel),
        "package_projection": relative(config, package_projection),
        "aggregate": aggregate,
        "registered_curve_gate": load_contract().get("thresholds", {}),
        "paper_compatible_policy_gate": bool(all(paper_checks.values())),
        "independent_legal_policy_gate": False,
        "paper_compatible_policy_thresholds": paper_thresholds,
        "paper_compatible_policy_checks": paper_checks,
        "reason": "Variable and regression gates are evaluated separately; this artifact is the variable-level comparison.",
        "package_policy_used_by_builder": False,
    }
    write_metadata_json(root(config) / "pooled_policy_v2_variable_gate.json", gates)
    (root(config) / "pooled_policy_v2_validation_report.md").write_text(
        "# Pooled policy v2 validation\n\n"
        "This report compares independently constructed policy components with the authors' package projection. The package is validation-only.\n\n"
        + pd.DataFrame([aggregate]).to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    return gates


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-closed pooled policy v2 source preflight")
    parser.add_argument("--preflight-only", action="store_true", default=True)
    parser.add_argument("--build-panels", action="store_true")
    parser.add_argument("--validate-panels", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    if args.build_panels:
        result = build_pooled_v2_panels(PipelineConfig.default(), overwrite=args.overwrite)
        return 0
    if args.validate_panels:
        result = validate_pooled_v2_panels(PipelineConfig.default())
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("status") == "diagnostic_complete" else 2
    result = build_preflight(PipelineConfig.default())
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("paper_compatible_policy_gate") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
