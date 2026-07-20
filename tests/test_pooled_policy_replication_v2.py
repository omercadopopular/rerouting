from __future__ import annotations

import pandas as pd

from scr.passthru_data.pooled_policy_replication_v2 import (
    LEGAL_OBJECT,
    PAPER_OBJECT,
    PACKAGE_OBJECT,
    family_source_status,
    rule_role,
    select_stack_action,
    source_confidence,
    specification_fingerprint,
)


def test_policy_objects_are_separate() -> None:
    assert LEGAL_OBJECT.name != PAPER_OBJECT.name
    assert PAPER_OBJECT.source == "independent_legal_ledger"
    assert PACKAGE_OBJECT.package_reference_allowed is True
    assert LEGAL_OBJECT.package_reference_allowed is False


def test_quota_and_replacement_roles_are_not_universal() -> None:
    assert rule_role("99034501") == "quota_or_trq_alternative"
    assert rule_role("99034522") == "quota_or_trq_alternative"
    assert rule_role("99038505") == "quota_or_trq_alternative"
    assert rule_role("99038002") == "replacement_country_rate"
    assert rule_role("99038001") == "universal_additional_duty"


def test_source_confidence_rejects_context_links() -> None:
    assert source_confidence("local_hts_note_16_heading_expansion") == "heading_expansion"
    assert source_confidence("explicit_note_enumeration") == "structural_same_row"
    assert source_confidence("nearby_pdf_context") == "unresolved"
    assert source_confidence("nan") == "unresolved"


def test_family_selection_does_not_sum_quota_tiers() -> None:
    actions = pd.DataFrame(
        [
            {
                "action_id": "quota-1",
                "rule_code": "99034501",
                "family": "washer_201",
                "partner_name": "CHINA",
                "hs8": "84502000",
                "year": 2018,
                "month": 2,
                "additional_rate": 0.20,
            },
            {
                "action_id": "quota-2",
                "rule_code": "99034502",
                "family": "washer_201",
                "partner_name": "CHINA",
                "hs8": "84502000",
                "year": 2018,
                "month": 2,
                "additional_rate": 0.50,
            },
        ]
    )
    assert select_stack_action(actions).empty


def test_replacement_rate_wins_over_general_rate() -> None:
    actions = pd.DataFrame(
        [
            {
                "action_id": "general",
                "rule_code": "99038001",
                "family": "steel_232",
                "partner_name": "TURKEY",
                "hs8": "72081000",
                "year": 2018,
                "month": 8,
                "additional_rate": 0.25,
            },
            {
                "action_id": "turkey",
                "rule_code": "99038002",
                "family": "steel_232",
                "partner_name": "TURKEY",
                "hs8": "72081000",
                "year": 2018,
                "month": 8,
                "additional_rate": 0.50,
            },
        ]
    )
    selected = select_stack_action(actions)
    assert selected["rule_code"].tolist() == ["99038002"]


def test_family_status_reports_conditional_quota_as_blocked() -> None:
    attrs = pd.DataFrame(
        [
            {"rule_code": "99034501", "increment_rate": 0.20},
            {"rule_code": "99034525", "increment_rate": 0.25},
        ]
    )
    links = pd.DataFrame(
        [
            {"rule_code": "99034525", "family": "solar_201", "hs8": "85414060"},
        ]
    )
    status = family_source_status(links, attrs)
    assert status["washer_201"]["quota_status"] == "blocked_without_entry_allocation"


def test_specification_fingerprint_is_stable() -> None:
    assert specification_fingerprint() == specification_fingerprint()
    assert len(specification_fingerprint()) == 64
