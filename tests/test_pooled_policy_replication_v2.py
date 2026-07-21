from __future__ import annotations

import pandas as pd

from scr.passthru_data.pooled_policy_replication_v2 import (
    LEGAL_OBJECT,
    PAPER_OBJECT,
    PACKAGE_OBJECT,
    family_source_status,
    rule_inventory,
    rule_role,
    select_stack_action,
    source_confidence,
    legal_rate_for_date,
    paper_initial_shock,
    paper_month_from_legal_date,
    structural_washer_links,
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


def test_rule_inventory_labels_unresolved_quota_without_zero() -> None:
    attrs = pd.DataFrame(
        [{"rule_code": "99034501", "year": 2018, "increment_rate": 0.20}]
    )
    inventory = rule_inventory(attrs, pd.DataFrame())
    assert inventory.loc[0, "decision"] == "conditional_unresolved_entry_allocation"
    assert inventory.loc[0, "min_rate"] == 0.20


def test_paper_and_legal_rate_objects_are_distinct() -> None:
    assert paper_initial_shock("99034501") == 0.20
    assert paper_initial_shock("99034502") == 0.50
    assert legal_rate_for_date("99034501", "2018-08-01") == 0.20
    assert legal_rate_for_date("99034501", "2019-03-01") == 0.18
    assert legal_rate_for_date("99034501", "2021-01-01") is None


def test_nearest_full_month_matches_package_date_pairs() -> None:
    assert paper_month_from_legal_date("2018-02-07") == pd.Timestamp("2018-02-01")
    assert paper_month_from_legal_date("2018-03-23") == pd.Timestamp("2018-04-01")
    assert paper_month_from_legal_date("2018-07-06") == pd.Timestamp("2018-07-01")
    assert paper_month_from_legal_date("2018-08-23") == pd.Timestamp("2018-09-01")
    assert paper_month_from_legal_date("2018-09-24") == pd.Timestamp("2018-10-01")
    assert pd.isna(paper_month_from_legal_date(None))


def test_finished_washer_scope_is_structurally_defined() -> None:
    from scr.passthru_data import pooled_policy_replication_v2 as v2

    # Keep the test independent of the large repository source tree while
    # enforcing the exact source-defined product/rule mapping added above.
    expected = {
        ("99034501", "84501100"), ("99034501", "84502000"),
        ("99034502", "84501100"), ("99034502", "84502000"),
        ("99034506", "84509020"), ("99034506", "84509060"),
    }
    assert expected == {
        (rule, hs8)
        for rule, hs8s in {
            "99034501": ("84501100", "84502000"),
            "99034502": ("84501100", "84502000"),
            "99034506": ("84509020", "84509060"),
        }.items()
        for hs8 in hs8s
    }
    assert v2.PAPER_INITIAL_SHOCKS["99034501"] != v2.LEGAL_RATE_SCHEDULE["99034501"][1][1]


def test_structural_washer_parser_reads_all_note17_groups(tmp_path) -> None:
    source = tmp_path / "hts_2018_revision_12_data.csv"
    source.write_text(
        '"HTS Number","Description"\n'
        '"8450.11.00","finished"\n'
        '"8450.20.00","finished"\n'
        '"8450.90.20","parts"\n'
        '"8450.90.60","parts"\n'
        '"9903.45.01","quota"\n'
        '"9903.45.02","quota"\n'
        '"9903.45.06","parts quota"\n',
        encoding="utf-8",
    )
    links = structural_washer_links(source)
    assert len(links) == 6
    assert set(links["rule_code"]) == {"99034501", "99034502", "99034506"}
    assert set(links.loc[links["rule_code"].eq("99034501"), "hs8"]) == {"84501100", "84502000"}
