from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from scr.passthru_data.policy_regression_v2 import (
    POLICY_SOURCE_MODE_LEGAL,
    POLICY_SOURCE_MODE_PAPER,
    _curve_metrics,
    clone_source_fit_id,
    expected_estimator_fit_ids,
    expected_fit_ids,
    registered_paper_curve_gate,
)

from scr.passthru_data.policy_replication_v2 import (
    PACKAGE_ANCHOR_MODE,
    PolicyWave,
    PAPER_COMPATIBLE_LIST2_ADDITIONS,
    PAPER_COMPATIBLE_LIST3_ADDITIONS,
    PAPER_COMPATIBLE_LONGITUDINAL_HS10_CARRY,
    PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS,
    assign_policy_to_products,
    assign_paper_compatible_policy_to_products,
    decode_pdf_literal_strings,
    exclusive_active_share,
    extract_list3_partial_scope,
    extract_wave_scope,
    first_contiguous_code_run,
    load_hts8_vintage,
    parse_simple_ad_valorem,
    validate_event_panel_encoding,
)
from scr.passthru_data.config import PipelineConfig


def test_pdf_literal_decoder_handles_escapes_octal_and_nested_parentheses() -> None:
    content = rb"(plain) Tj [(a\(b\)) (octal\040space) (line\nnext)] TJ"
    assert decode_pdf_literal_strings(content) == ["plain", "a(b)", "octal space", "line\nnext"]


def test_first_contiguous_code_run_stops_before_unrelated_schedule() -> None:
    text = "intro 2845.90.00 4011.30.00 page header 4012.13.00 " + ("x" * 2_100) + " 0101.21.00"
    assert first_contiguous_code_run(text) == ["28459000", "40113000", "40121300"]


def test_wave_scope_uses_exact_note_heading_and_latest_replacement() -> None:
    wave = PolicyWave("test", "9903.88.03", "f", "local.pdf", "2018-09-24", "2018-10", 0.10, 2)
    old = "(f) Heading 9903.88.03 applies 1111.11.11 2222.22.22 (g) F or the pur poses of heading 9903.88.04"
    replacement = "(f) Heading 9903.88.03 applies 3333.33.33 4444.44.44 (g) F or the pur poses of heading 9903.88.04"
    assert extract_wave_scope(old + " " + replacement, wave) == ["33333333", "44444444"]


def test_partial_scope_extracts_parents_and_exclusions() -> None:
    parents = [f"{index:04d}.00.00" for index in range(1000, 1011)]
    exclusions = [f"{1000 + (index % 11):04d}.00.{index:04d}" for index in range(18)]
    text = "(g) F or the pur poses of heading 9903.88.04 " + " ".join(parents + exclusions)
    parsed_parents, parsed_exclusions = extract_list3_partial_scope(text)
    assert len(parsed_parents) == 11
    assert len(parsed_exclusions) == 18


def test_simple_ad_valorem_parser_never_coerces_unresolved_rates_to_zero() -> None:
    assert parse_simple_ad_valorem("Free") == (0.0, "free")
    assert parse_simple_ad_valorem("6.5%") == (0.065, "simple_ad_valorem")
    assert parse_simple_ad_valorem("................................") == (None, "missing")
    assert parse_simple_ad_valorem("1.5¢/kg + 2.3%") == (None, "compound_or_specific")
    assert parse_simple_ad_valorem(None) == (None, "missing")


def test_exclusive_partial_month_shares_match_package_convention() -> None:
    assert math.isclose(exclusive_active_share("2018-07-06", 2018, 7), 25 / 31)
    assert math.isclose(exclusive_active_share("2018-08-23", 2018, 8), 8 / 31)
    assert math.isclose(exclusive_active_share("2018-09-24", 2018, 9), 6 / 30)
    assert exclusive_active_share("2018-09-24", 2018, 8) == 0.0
    assert exclusive_active_share("2018-09-24", 2018, 10) == 1.0


def test_partial_scope_exclusions_are_applied_at_hs10() -> None:
    scope = pd.DataFrame(
        [
            {
                "wave": "list3",
                "rule_code": "9903.88.04",
                "hs8": "94017100",
                "scope_kind": "partial_hs8_except_hs10",
            }
        ]
    )
    exclusions = pd.DataFrame([{"excluded_hs10": "9401710005"}])
    products = pd.DataFrame({"hs10": ["9401710005", "9401710099", None]})
    assigned = assign_policy_to_products(products, scope, exclusions)
    assert not bool(assigned.loc[assigned["hs10"].eq("9401710005"), "raw_target"].item())
    assert bool(assigned.loc[assigned["hs10"].eq("9401710099"), "raw_target"].item())
    assert not bool(assigned.loc[assigned["hs10"].isna(), "raw_target"].item())


def test_policy_regression_grid_distinguishes_fits_from_verified_clones() -> None:
    assert len(expected_fit_ids()) == 24
    assert len(expected_estimator_fit_ids()) == 24
    assert clone_source_fit_id(f"{POLICY_SOURCE_MODE_LEGAL}|dynamic|pduty") is None
    assert clone_source_fit_id(f"{POLICY_SOURCE_MODE_LEGAL}|event|pduty") is None


def test_curve_gate_metrics_use_point_estimates_and_treat_ci_as_diagnostic() -> None:
    horizon = list(range(-6, 7))
    left = pd.DataFrame(
        {
            "horizon": horizon,
            "estimate": [0.0] + [float(value) for value in range(1, 13)],
            "conf_low": [0.0] + [float(value) - 1 for value in range(1, 13)],
            "conf_high": [0.0] + [float(value) + 1 for value in range(1, 13)],
        }
    )
    right = left.copy()
    right.loc[right["horizon"].ne(-6), ["conf_low", "conf_high"]] += 0.5
    metrics = _curve_metrics(left, right, "horizon")
    assert metrics["aligned_horizons"] == 13
    assert metrics["correlation"] == 1.0
    assert metrics["rmse"] == 0.0
    assert metrics["ci_overlap_diagnostic"] < 1.0


def test_hts_vintage_validity_is_hierarchical_at_hs8(tmp_path: Path) -> None:
    path = tmp_path / "vintage.csv"
    path.write_text('"HTS Number"\n"0801.00.10"\n"8609.00.00.00"\n"0101"\n', encoding="utf-8")
    assert load_hts8_vintage(path) == {"08010010", "86090000"}


def test_paper_compatible_scope_classifies_vintages_and_preserves_event_date_carry() -> None:
    legal_rows = [
        {"wave": "list3", "rule_code": "9903.88.03", "hs8": "44189999", "scope_kind": "full_hs8"},
        {"wave": "list3", "rule_code": "9903.88.04", "hs8": "94017100", "scope_kind": "partial_hs8_except_hs10"},
    ]
    legal_scope = pd.DataFrame(legal_rows)
    paper_retained = {
        "9401710005", "9401710006", "9401710007", "9401790002",
        "9401790003", "9401790004", "9401806023",
    }
    all_exclusions = set(PAPER_COMPATIBLE_PARTIAL_EXCLUSIONS) | paper_retained
    legal_exclusions = pd.DataFrame(
        [{"rule_code": "9903.88.04", "wave": "list3", "hs8": code[:8], "excluded_hs10": code} for code in sorted(all_exclusions)]
    )
    products = pd.DataFrame(
        {
            "hs10": [
                "8609000000",  # proposal-era List 2 line
                "0304811000",  # proposal-era List 3 line
                "9401710001",  # first code in clause: excluded by paper parser
                "9401710005",  # later code in clause: retained by paper parser
                "4418999900",  # new final-annex code absent from revision 11
                "4401100000",  # old longitudinal code carried to the October event
            ]
        }
    )
    valid = {
        "list1": set(),
        "list2": set(PAPER_COMPATIBLE_LIST2_ADDITIONS),
        "list3": set(PAPER_COMPATIBLE_LIST3_ADDITIONS) | {"94017100"},
    }
    assigned, reconciliation = assign_paper_compatible_policy_to_products(products, legal_scope, legal_exclusions, valid)
    lookup = assigned.set_index("hs10")
    assert bool(lookup.loc["8609000000", "paper_target"])
    assert lookup.loc["8609000000", "paper_wave"] == "list2"
    assert bool(lookup.loc["0304811000", "paper_target"])
    assert lookup.loc["0304811000", "paper_wave"] == "list3"
    assert not bool(lookup.loc["9401710001", "paper_target"])
    assert bool(lookup.loc["9401710005", "paper_target"])
    assert not bool(lookup.loc["4418999900", "paper_target"])
    assert bool(lookup.loc["4401100000", "paper_target"])
    assert lookup.loc["4401100000", "paper_scope_basis"] == "historical_longitudinal_hs10_carry_reconciliation"
    assert reconciliation.loc[reconciliation["code"].eq("44189999"), "action"].item() == "exclude_new_code_absent_from_effective_source_vintage"
    assert reconciliation.loc[reconciliation["code"].eq(PAPER_COMPATIBLE_LONGITUDINAL_HS10_CARRY[0]), "action"].item() == "carry_old_code_to_october_event_scope"
    assert reconciliation.loc[reconciliation["code"].eq("86090000"), "validation_derived"].item()
    assert reconciliation.loc[reconciliation["code"].eq("9401710005"), "validation_derived"].item()


def test_legal_calendar_diagnostics_do_not_enter_registered_paper_gate() -> None:
    rows = []
    for specification in ("event", "dynamic"):
        for outcome in ("val", "q1", "p", "pduty"):
            rows.append({"comparison_mode": POLICY_SOURCE_MODE_PAPER, "specification": specification, "outcome": outcome, "registered_gate_member": True, "point_estimate_thresholds_passed": True})
            rows.append({"comparison_mode": POLICY_SOURCE_MODE_LEGAL, "specification": specification, "outcome": outcome, "registered_gate_member": False, "point_estimate_thresholds_passed": False})
    assert registered_paper_curve_gate(pd.DataFrame(rows))


def test_calendar_eligible_field_cannot_promote_legal_curve() -> None:
    rows = []
    for specification in ("event", "dynamic"):
        for outcome in ("val", "q1", "p", "pduty"):
            rows.append({
                "comparison_role": "registered_historical_replication_gate",
                "registered_gate_member": True,
                "published_comparison_eligible": True,
                "point_estimate_thresholds_passed": True,
            })
            rows.append({
                "comparison_role": "legal_calendar_diagnostic",
                "registered_gate_member": True,
                "published_comparison_eligible": False,
                "point_estimate_thresholds_passed": True,
            })
    assert registered_paper_curve_gate(pd.DataFrame(rows))


def test_final_legal_gate_does_not_claim_historical_lock_role() -> None:
    source = Path("scr/passthru_data/policy_replication_v2.py").read_text(encoding="utf-8")
    assert 'LEGAL_GATE_VERSION = "section301_policy_replication_v2_final_legal"' in source
    assert '"historical_paper_methodology_lock_role": "not_applicable_determined_by_regression_finalizer"' in source
    assert '"historical_policy_methodology_lock_role": "determined_by_regression_finalizer"' in source
    assert '"historical_policy_methodology_locked": False' not in source


def test_event_encoding_gate_requires_shared_dates_and_partner_status_codes(tmp_path: Path) -> None:
    config = PipelineConfig.default(tmp_path)
    root = tmp_path / "panels"
    root.mkdir(parents=True)
    anchor = pd.DataFrame(
        {
            "id": [1, 2, 1, 2],
            "cty_code": [5700, 1220, 5700, 1220],
            "hs10": ["0101000000"] * 4,
            "year": [2018] * 4,
            "month": [9, 9, 10, 10],
            "m_status2": [0, 0, 2, 1],
            "m_effective_mdate2": pd.to_datetime(["2018-10-01"] * 4),
            "m_ess": [2, 1, 2, 1],
        }
    )
    paths = {
        PACKAGE_ANCHOR_MODE: root / "anchor.parquet",
        POLICY_SOURCE_MODE_PAPER: root / "paper.parquet",
        POLICY_SOURCE_MODE_LEGAL: root / "legal.parquet",
    }
    anchor.to_parquet(paths[PACKAGE_ANCHOR_MODE], index=False)
    anchor.to_parquet(paths[POLICY_SOURCE_MODE_PAPER], index=False)
    legal = anchor.copy()
    legal.loc[legal["cty_code"].eq(1220), "m_status2"] = 0
    legal.to_parquet(paths[POLICY_SOURCE_MODE_LEGAL], index=False)
    gate = validate_event_panel_encoding(config, paths)
    assert gate["all_checks_pass"] is True
    assert gate["stated_partner_encoding"] == {"pre_event": 0, "post_event_comparison_partner": 1, "post_event_china": 2}
