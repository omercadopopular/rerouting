from pathlib import Path
import os
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.raw_replication_validation import (
    build_china_301_source_audit,
    build_china_301_key_trace_from_artifacts,
    build_china_301_rate_trace_from_artifacts,
    build_china_301_rate_timing_trace_from_artifacts,
    build_china_301_rate_provenance_from_artifacts,
    build_china_301_rate_mismatch_decomposition_from_artifacts,
    build_china_301_benchmark_definition_trace_from_artifacts,
    build_china_301_statutory_component_trace_from_artifacts,
    build_china_301_trace_from_artifacts,
    build_china_301_rule_assignment_trace_from_artifacts,
    build_22042150_panel_trace_from_artifacts,
    build_china_301_variable_semantics_from_artifacts,
    build_china_301_universe_trace_from_artifacts,
    build_raw_replication_artifact_freshness,
    build_raw_source_health_report,
    compare_raw_reconstruction,
    _classify_china_301_key_stage,
    _classify_china_301_rate_stage,
    _classify_china_301_benchmark_definition_stage,
    _classify_china_301_statutory_component_stage,
    _classify_china_301_rate_timing_stage,
    _classify_china_301_rule_assignment_stage,
    run_raw_replication_validation_china_semantics_corrected,
    _inspect_source_candidate,
    summarize_discrepancies,
    run_raw_replication_validation_china_current,
)
from passthru_data.raw_replication_validation import build_china_301_wave_link_audit_from_artifacts
from passthru_data.build_us_products_partner_panel import _load_raw_tradewar_overlay, _load_tradewar_pdf_csv_link_provenance, _load_tradewar_pdf_csv_links, _load_tradewar_rule_attributes


def _config(tmp_path: Path):
    from passthru_data.config import PipelineConfig

    data_root = tmp_path / "data"
    cfg = PipelineConfig(
        repo_root=tmp_path,
        raw_dir=data_root / "raw" / "passthru_data",
        staging_dir=data_root / "staging" / "passthru_data",
        reference_dir=data_root / "reference" / "passthru_data",
        analysis_dir=data_root / "analysis" / "passthru_data",
        verification_dir=data_root / "verification" / "passthru_data",
        fajgelbaum_root=data_root / "fajgelbaum",
        fajgelbaum_analysis_dir=data_root / "fajgelbaum" / "data" / "analysis",
        manual_input_dir=data_root / "raw" / "passthru_data" / "manual",
        logs_dir=data_root / "verification" / "passthru_data" / "logs",
        analysis_window="current",
    )
    cfg.ensure_directories()
    return cfg


def _reference() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cty_code": [5700], "hs10": ["0101210000"], "year": [2018], "month": [7],
            "m_val": [2.0], "m_q1": [3.0], "m_stattariff1": [0.27], "m_stattariff2": [0.23],
            "m_hit": [1], "m_ess": [2], "m_status2": [1], "m_china_hit": [1],
            "m_steel_hit": [0], "m_alum_hit": [0], "m_washer_hit": [0], "m_solar_hit": [0],
            "m_effective_mdate2": ["2018-07-06"],
        }
    )


def _raw(increment: float | None = 0.25) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cty_code": [5700], "hs10": ["0101210000"], "year": [2018], "month": [7],
            "m_val": [2_000_000.0], "m_q1": [3_000_000.0], "m_statutory_tariff1": [0.27],
            "m_statutory_tariff2": [0.23], "m_policy_source": ["trade_war_raw_overlay"],
            "mfn_text_rate": ["2%"], "tw_increment_rate_raw": [increment],
            "tw_rule_code_raw": ["99038801"], "tw_scope_source_raw": ["machine"],
        }
    )


def test_raw_reconstruction_match_accounts_for_package_million_scale() -> None:
    cells, metrics = compare_raw_reconstruction(_reference(), _raw())
    assert cells.loc[0, "discrepancy_type"] == "match"
    assert metrics.loc[metrics["metric"] == "statutory_rate_match_rows", "value"].item() == 1


def test_raw_reconstruction_classifies_missing_policy_scope() -> None:
    cells, _ = compare_raw_reconstruction(_reference(), _raw(increment=None))
    assert cells.loc[0, "discrepancy_type"] == "missing_raw_policy_scope"


def test_event_status_alone_does_not_create_active_tariff_gap() -> None:
    reference = _reference()
    reference["year"] = 2017
    reference["month"] = 12
    reference["m_status2"] = 0
    raw = _raw(increment=None)
    raw["year"] = 2017
    raw["month"] = 12
    cells, _ = compare_raw_reconstruction(reference, raw)
    assert cells.loc[0, "ref_event_status"] == 2
    assert not cells.loc[0, "ref_active"]
    assert cells.loc[0, "discrepancy_type"] == "match"


def test_family_summary_uses_active_tariff_status() -> None:
    cells, _ = compare_raw_reconstruction(_reference(), _raw(increment=None))
    summaries = summarize_discrepancies(cells)
    by_family = summaries["by_family"]
    assert by_family.loc[by_family["family"].eq("m_china_hit"), "rows"].item() == 1


def test_china_301_summary_uses_effective_month_proxy() -> None:
    reference = _reference().copy()
    reference["month"] = 9
    reference["m_effective_mdate2"] = ["2018-09-24"]
    raw = _raw(increment=None)
    raw["month"] = 9
    cells, _ = compare_raw_reconstruction(reference, raw)
    summaries = summarize_discrepancies(cells)
    china = summaries["china_301_top_hs8_month_wave"]
    assert china.loc[china["hs8"].eq("01012100"), "rows"].item() == 1
    assert china.loc[china["hs8"].eq("01012100"), "ref_effective_period"].item() == "2018-09"


def test_china_301_source_audit_reports_source_layer_counts() -> None:
    reference = _reference().copy()
    reference["month"] = 10
    reference["m_effective_mdate2"] = ["2018-10-01"]
    raw = _raw(increment=None)
    raw["month"] = 10
    cells, _ = compare_raw_reconstruction(reference, raw)
    machine_links = pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]})
    pdf_links = pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]})
    rule_attrs = pd.DataFrame({"rule_code": ["99038801"], "year": [2018], "month": [10], "increment_rate": [0.25]})
    overlay = pd.DataFrame(
        {
            "cty_name": ["CHINA"],
            "hs8": ["01012100"],
            "year": [2018],
            "month": [10],
            "tw_rule_code_raw": ["99038801"],
            "tw_increment_rate_raw": [0.25],
            "tw_scope_source_raw": ["machine_or_pdf"],
        }
    )
    panel = pd.DataFrame(
        {
            "cty_code": [5700],
            "hs10": ["0101210000"],
            "year": [2018],
            "month": [10],
            "tw_increment_rate_raw": [0.25],
            "tw_rule_code_raw": ["99038801"],
            "tw_scope_source_raw": ["machine_or_pdf"],
        }
    )
    audit, errors = build_china_301_source_audit(
        cells,
        machine_links=machine_links,
        pdf_links=pdf_links,
        rule_attrs=rule_attrs,
        overlay=overlay,
        panel=panel,
    )
    assert errors["machine_links"] is None
    assert audit.loc[audit["hs8"].eq("01012100"), "machine_links_rows"].item() == 1
    assert audit.loc[audit["hs8"].eq("01012100"), "overlay_increment_rows"].item() == 1
    assert audit.loc[audit["hs8"].eq("01012100"), "panel_increment_rows"].item() == 1
    assert audit.loc[audit["hs8"].eq("01012100"), "diagnosed_stage"].item() == "present_with_increment"


@pytest.mark.parametrize(
    "machine_links,pdf_links,rule_attrs,overlay,panel,source_health_summary,expected_stage",
    [
        (
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame(columns=["rule_code", "year", "month", "increment_rate"]),
            pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw"]),
            pd.DataFrame(columns=["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"]),
            None,
            "absent_from_raw_links",
        ),
        (
            pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}),
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame(columns=["rule_code", "year", "month", "increment_rate"]),
            pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw"]),
            pd.DataFrame(columns=["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"]),
            None,
            "raw_links_missing_rule_attrs",
        ),
        (
            pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}),
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame({"rule_code": ["99038801"], "year": [2018], "month": [10], "increment_rate": [0.25]}),
            pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw"]),
            pd.DataFrame(columns=["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"]),
            None,
            "lost_before_overlay",
        ),
        (
            pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}),
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame({"rule_code": ["99038801"], "year": [2018], "month": [10], "increment_rate": [0.25]}),
            pd.DataFrame({"cty_name": ["CHINA"], "hs8": ["01012100"], "year": [2018], "month": [10], "tw_increment_rate_raw": [0.25], "tw_rule_code_raw": ["99038801"]}),
            pd.DataFrame(columns=["cty_code", "hs10", "year", "month", "tw_increment_rate_raw"]),
            None,
            "lost_after_overlay",
        ),
        (
            pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}),
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame({"rule_code": ["99038801"], "year": [2018], "month": [10], "increment_rate": [0.25]}),
            pd.DataFrame({"cty_name": ["CHINA"], "hs8": ["01012100"], "year": [2018], "month": [10], "tw_increment_rate_raw": [0.25], "tw_rule_code_raw": ["99038801"]}),
            pd.DataFrame({"cty_code": [5700], "hs10": ["0101210000"], "year": [2018], "month": [10], "tw_increment_rate_raw": [0.25]}),
            None,
            "present_with_increment",
        ),
        (
            pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}),
            pd.DataFrame(columns=["hs8", "rule_code", "release_name"]),
            pd.DataFrame({"rule_code": ["99038801"], "year": [2018], "month": [10], "increment_rate": [0.25]}),
            pd.DataFrame({"cty_name": ["CHINA"], "hs8": ["01012100"], "year": [2018], "month": [10], "tw_increment_rate_raw": [0.25], "tw_rule_code_raw": ["99038801"]}),
            pd.DataFrame({"cty_code": [5700], "hs10": ["0101210000"], "year": [2018], "month": [10], "tw_increment_rate_raw": [0.25]}),
            {"blocked_by_source_availability": True, "blocking_artifacts": ["tradewar_rule_attributes"]},
            "source_unavailable",
        ),
    ],
)
def test_china_301_source_audit_classifies_trace_stages(
    machine_links: pd.DataFrame,
    pdf_links: pd.DataFrame,
    rule_attrs: pd.DataFrame,
    overlay: pd.DataFrame,
    panel: pd.DataFrame,
    source_health_summary,
    expected_stage: str,
) -> None:
    reference = _reference().copy()
    reference["month"] = 10
    reference["m_effective_mdate2"] = ["2018-10-01"]
    raw = _raw(increment=None)
    raw["month"] = 10
    cells, _ = compare_raw_reconstruction(reference, raw)
    audit, errors = build_china_301_source_audit(
        cells,
        machine_links=machine_links,
        pdf_links=pdf_links,
        rule_attrs=rule_attrs,
        overlay=overlay,
        panel=panel,
        source_health_summary=source_health_summary,
    )
    assert audit.loc[audit["hs8"].eq("01012100"), "diagnosed_stage"].item() == expected_stage
    assert errors["panel"] in {None, "empty"}


def test_raw_source_health_report_flags_blocked_artifacts_and_fallbacks(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.reference_dir.mkdir(parents=True, exist_ok=True)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "release_name": ["2018HTS"],
            "year": [2018],
            "release_start_date": ["2018-01-01"],
            "release_end_date": ["2018-12-31"],
        }
    ).to_parquet(cfg.reference_dir / "policy_release_catalog.parquet", index=False)
    (cfg.reference_dir / "policy_release_catalog.csv").touch()
    pd.DataFrame(
        {
            "year": [2018],
            "file_name": ["rev.csv"],
            "archive_release_name": ["2018HTS"],
            "file_ext": ["csv"],
        }
    ).to_parquet(cfg.reference_dir / "policy_archive_revision_index.parquet", index=False)
    (cfg.reference_dir / "policy_archive_revision_index.csv").touch()
    pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}).to_parquet(
        cfg.reference_dir / "tradewar_machine_links.parquet",
        index=False,
    )
    pd.DataFrame({"hs8": ["01012100"], "rule_code": ["99038801"], "release_name": ["2018HTS"]}).to_parquet(
        cfg.reference_dir / "tradewar_pdf_links.parquet",
        index=False,
    )
    (cfg.reference_dir / "tradewar_rule_attributes.parquet").touch()
    pd.DataFrame(
        {
            "cty_name": ["CHINA"],
            "hs8": ["01012100"],
            "year": [2018],
            "month": [7],
            "tw_increment_rate_raw": [0.25],
            "tw_rule_code_raw": ["99038801"],
        }
    ).to_parquet(cfg.analysis_dir / "tradewar_overlay_raw.parquet", index=False)

    report, summary, tables, errors = build_raw_source_health_report(cfg)

    assert "policy_release_catalog" in set(report["artifact"])
    release_summary = summary["artifacts"]["policy_release_catalog"]
    index_summary = summary["artifacts"]["policy_archive_revision_index"]
    assert release_summary["selected_candidate"] == "parquet"
    assert index_summary["selected_candidate"] == "parquet"
    rule_rows = report.loc[report["artifact"].eq("tradewar_rule_attributes")]
    assert rule_rows["zero_byte"].all()
    assert summary["blocked_by_source_availability"] is True
    assert "tradewar_rule_attributes" in summary["blocking_artifacts"]
    assert tables["machine_links"] is not None
    assert tables["rule_attrs"] is None
    assert "tradewar_rule_attributes" in errors


def test_source_health_probe_marks_cloud_provider_failure_as_placeholder(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "policy_release_catalog.csv"
    source.write_text("release_name,year\n2018HTS,2018\n", encoding="utf-8")

    def _raise(*_args, **_kwargs):
        raise OSError("The cloud file provider is not running")

    monkeypatch.setattr("passthru_data.raw_replication_validation.read_table", _raise)
    row = _inspect_source_candidate(source, "policy_release_catalog", "csv", ["release_name", "year"])

    assert row["readable"] is False
    assert row["appears_placeholder"] is True
    assert row["zero_byte"] is False


def test_china_301_trace_builder_from_artifacts_classifies_all_stages(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.reference_dir.mkdir(parents=True, exist_ok=True)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "release_name": ["2018HTS"],
            "year": [2018],
            "release_start_date": ["2018-01-01"],
            "release_end_date": ["2018-12-31"],
        }
    ).to_csv(cfg.reference_dir / "policy_release_catalog.csv", index=False)
    pd.DataFrame(
        {
            "year": [2018],
            "file_name": ["rev.csv"],
            "archive_release_name": ["2018HTS"],
            "file_ext": ["csv"],
        }
    ).to_csv(cfg.reference_dir / "policy_archive_revision_index.csv", index=False)
    pd.DataFrame(
        {
            "hs8": ["0101.21.00", "01012101", "01012102", "01012103"],
            "rule_code": ["9903.88.01", "99038802", "99038803", "99038804"],
            "release_name": ["2018HTS", "2018HTS", "2018HTS", "2018HTS"],
        }
    ).to_parquet(cfg.reference_dir / "tradewar_machine_links.parquet", index=False)
    pd.DataFrame(
        {
            "hs8": ["02000000"],
            "rule_code": ["99038899"],
            "release_name": ["2018HTS"],
        }
    ).to_parquet(cfg.reference_dir / "tradewar_pdf_links.parquet", index=False)
    pd.DataFrame(
        {
            "rule_code": ["9903.88.01", "99038802", "99038803"],
            "year": [2018, 2018, 2018],
            "month": [10, 10, 10],
            "increment_rate": [0.25, 0.25, 0.25],
            "description": ["List 1", "List 2", "List 3"],
        }
    ).to_parquet(cfg.reference_dir / "tradewar_rule_attributes.parquet", index=False)
    pd.DataFrame(
        {
            "cty_name": ["CHINA", "CHINA", "CHINA"],
            "hs8": ["01012100", "01012101", "02000000"],
            "year": [2018, 2018, 2018],
            "month": [10, 10, 10],
            "tw_rule_code_raw": ["9903.88.01", "99038802", "99038899"],
            "tw_increment_rate_raw": [0.25, 0.25, 0.25],
            "tw_scope_source_raw": ["machine_or_pdf", "machine_or_pdf", "machine_or_pdf"],
        }
    ).to_parquet(cfg.analysis_dir / "tradewar_overlay_raw.parquet", index=False)

    discrepancies = pd.DataFrame(
        [
            *[
                {
                    "hs10": "0101.21.00.00",
                    "year": 2018,
                    "month": 10,
                    "ref_m_effective_mdate2": "2018-10-01",
                    "ref_m_china_hit": 1,
                    "discrepancy_type": "missing_raw_policy_scope",
                }
                for _ in range(5)
            ],
            *[
                {
                    "hs10": "0101210100",
                    "year": 2018,
                    "month": 10,
                    "ref_m_effective_mdate2": "2018-10-01",
                    "ref_m_china_hit": 1,
                    "discrepancy_type": "missing_raw_policy_scope",
                }
                for _ in range(4)
            ],
            *[
                {
                    "hs10": "0101210200",
                    "year": 2018,
                    "month": 10,
                    "ref_m_effective_mdate2": "2018-10-01",
                    "ref_m_china_hit": 1,
                    "discrepancy_type": "missing_raw_policy_scope",
                }
                for _ in range(3)
            ],
            *[
                {
                    "hs10": "0101210300",
                    "year": 2018,
                    "month": 10,
                    "ref_m_effective_mdate2": "2018-10-01",
                    "ref_m_china_hit": 1,
                    "discrepancy_type": "missing_raw_policy_scope",
                }
                for _ in range(2)
            ],
            {
                "hs10": "0101210400",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "discrepancy_type": "missing_raw_policy_scope",
            },
        ]
    ).assign(
        cty_code=5700,
        ref_m_status2=1,
        ref_m_stattariff1=0.27,
        ref_m_stattariff2=0.27,
    )
    discrepancies.loc[discrepancies["hs10"].eq("0101210600"), "ref_m_stattariff1"] = 0.29
    discrepancies.loc[discrepancies["hs10"].eq("0101210700"), "ref_m_stattariff2"] = 0.29
    discrepancies.to_parquet(trace_dir / "raw_replication_discrepancies.parquet", index=False)
    pd.DataFrame(
        {
            "cty_code": [5700, 2010],
            "hs10": ["0101210000", "0101210000"],
            "year": [2018, 2018],
            "month": [10, 10],
            "tw_increment_rate_raw": [0.25, 0.25],
            "tw_rule_code_raw": ["99038801", "99038801"],
            "tw_scope_source_raw": ["machine_or_pdf", "machine_or_pdf"],
        }
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)

    result = build_china_301_trace_from_artifacts(cfg)
    trace = pd.read_csv(
        trace_dir / "raw_replication_china_301_trace.csv",
        dtype={"hs8": "string", "ref_effective_period": "string", "diagnosed_stage": "string"},
    )

    assert result["source_unavailable"] is False
    assert result["rows"] == 5
    assert list(trace.columns) == [
        "hs8",
        "year",
        "month",
        "ref_effective_period",
        "missing_scope_rows",
        "machine_link_rows",
        "pdf_link_rows",
        "raw_link_rows",
        "rule_attr_rows",
        "overlay_rows",
        "overlay_increment_rows",
        "panel_rows",
        "panel_increment_rows",
        "diagnosed_stage",
    ]
    assert trace.loc[trace["hs8"].eq("01012100"), "diagnosed_stage"].item() == "present_with_increment"
    assert trace.loc[trace["hs8"].eq("01012101"), "diagnosed_stage"].item() == "lost_after_overlay"
    assert trace.loc[trace["hs8"].eq("01012102"), "diagnosed_stage"].item() == "lost_before_overlay"
    assert trace.loc[trace["hs8"].eq("01012103"), "diagnosed_stage"].item() == "raw_links_missing_rule_attrs"
    assert trace.loc[trace["hs8"].eq("01012104"), "diagnosed_stage"].item() == "absent_from_raw_links"
    assert trace.loc[trace["hs8"].eq("01012100"), "panel_rows"].item() == 1
    assert trace.loc[trace["hs8"].eq("01012100"), "panel_increment_rows"].item() == 1
    assert trace.loc[trace["hs8"].eq("01012101"), "panel_rows"].item() == 0
    assert trace.loc[trace["hs8"].eq("01012100"), "machine_link_rows"].item() == 1
    assert trace.loc[trace["hs8"].eq("01012100"), "raw_link_rows"].item() == 1
    assert trace.loc[trace["hs8"].eq("01012103"), "rule_attr_rows"].item() == 0


def test_artifact_freshness_report_flags_outputs_as_stale_when_inputs_are_newer(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    output_dir = cfg.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)

    overlay_path = cfg.analysis_dir / "tradewar_overlay_raw.parquet"
    panel_path = cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    pd.DataFrame({"cty_name": ["CHINA"], "hs8": ["01012100"], "year": [2018], "month": [7], "tw_increment_rate_raw": [0.25]}).to_parquet(overlay_path, index=False)
    pd.DataFrame({"cty_code": [5700], "hs10": ["0101210000"], "year": [2018], "month": [7], "tw_increment_rate_raw": [0.25]}).to_parquet(panel_path, index=False)

    gate_path = output_dir / "raw_replication_release_gate.json"
    gate_path.write_text("{}", encoding="utf-8")

    old = pd.Timestamp("2020-01-01", tz="UTC").timestamp()
    new = pd.Timestamp("2020-02-01", tz="UTC").timestamp()
    os.utime(gate_path, (old, old))
    os.utime(overlay_path, (new, new))
    os.utime(panel_path, (new, new))

    freshness = build_raw_replication_artifact_freshness(cfg)
    gate_row = freshness.loc[freshness["artifact"].eq("raw_replication_release_gate.json")].iloc[0]
    assert bool(gate_row["stale_relative_to_inputs"]) is True
    assert bool(gate_row["readable"]) is True
    assert gate_row["row_count"] == 1


def test_partial_china_validation_never_sets_release_gate_true(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.fajgelbaum_analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "cty_code": [5700, 5700, 2010],
            "cty_name": ["CHINA", "CHINA", "MEXICO"],
            "hs10": ["0101210000", "0101210100", "0101210000"],
            "year": [2018, 2018, 2018],
            "month": [7, 8, 7],
            "m_val": [100.0, 80.0, 50.0],
            "m_q1": [10.0, 8.0, 5.0],
            "m_stattariff1": [0.27, 0.27, 0.20],
            "m_stattariff2": [0.27, 0.27, 0.20],
            "m_hit": [1, 1, 1],
            "m_ess": [2, 2, 2],
            "m_status2": [1, 0, 1],
            "m_china_hit": [1, 1, 1],
            "m_steel_hit": [0, 0, 0],
            "m_alum_hit": [0, 0, 0],
            "m_washer_hit": [0, 0, 0],
            "m_solar_hit": [0, 0, 0],
        }
    ).to_stata(cfg.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta", write_index=False)
    pd.DataFrame(
        {
            "cty_code": [5700, 5700, 5700],
            "cty_name": ["CHINA", "CHINA", "CHINA"],
            "hs10": ["0101210000", "0101210100", "0101210200"],
            "year": [2018, 2018, 2018],
            "month": [7, 8, 9],
            "m_val": [1000000.0, 800000.0, 750000.0],
            "m_q1": [100.0, 80.0, 75.0],
            "m_statutory_tariff1": [0.27, 0.27, 0.27],
            "m_statutory_tariff2": [0.27, 0.27, 0.27],
            "m_policy_source": ["trade_war_raw_overlay", "trade_war_raw_overlay", "trade_war_raw_overlay"],
            "mfn_text_rate": ["2%", "2%", "2%"],
            "tw_increment_rate_raw": [0.25, pd.NA, 0.25],
            "tw_rule_code_raw": ["99038801", pd.NA, "99038801"],
            "tw_scope_source_raw": ["machine", pd.NA, "machine"],
        }
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)

    result = run_raw_replication_validation_china_current(cfg)
    gate = pd.read_json(result["gate_path"], typ="series")
    assert gate["ready_for_extension"] is False
    assert gate["reason"] == "china_301_current_partial_validation_only"
    assert gate["paper_key_coverage_rate"] == 1.0
    assert gate["tariff_active_treatment_match_rate"] == 1.0
    universe = pd.read_csv(result["validation_universe_path"])
    assert universe.loc[universe["universe"].eq("all_reference_rows"), "rows"].item() == 3
    assert universe.loc[universe["universe"].eq("china_hit_partner_current_validation_rows"), "rows"].item() == 2
    assert universe.loc[universe["universe"].eq("active_china_hit_nonchina_partner_reference_rows"), "rows"].item() == 1
    assert not (cfg.verification_dir / "raw_replication_imports" / "raw_replication_release_gate.json").exists()

    denominators = pd.read_csv(result["metric_denominators_path"])
    assert denominators.loc[denominators["metric"].eq("benchmark_active_rows"), "value"].item() == 1
    assert denominators.loc[denominators["metric"].eq("exact_key_matched_rows"), "value"].item() == 2
    assert denominators.loc[denominators["metric"].eq("either_active_matched_rows"), "value"].item() == 1
    raw_only = pd.read_csv(result["raw_only_keys_path"])
    assert raw_only.shape[0] == 1


def test_china_301_variable_semantics_table_records_target_and_status(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    result = build_china_301_variable_semantics_from_artifacts(cfg)
    table = pd.read_csv(result["trace_path"])
    assert result["rows"] >= 6
    assert set(["m_target", "m_status2", "m_china_hit", "m_stattariff1", "m_stattariff2"]).issubset(set(table["variable"].astype(str)))
    assert table.loc[table["variable"].eq("m_status2"), "benchmark_condition"].item().startswith("m_status2 > 0")


def test_china_301_universe_trace_separates_targeted_china_and_nonchina_rows(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.fajgelbaum_analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "year": 2018,
                "month": 10,
                "ref_m_val": 1.0,
                "ref_m_q1": 1.0,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "ref_m_hit": 1,
                "ref_m_ess": 2,
                "ref_m_status2": 1,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_steel_hit": 0,
                "ref_m_alum_hit": 0,
                "ref_m_washer_hit": 0,
                "ref_m_solar_hit": 0,
                "raw_m_val": 1.0,
                "raw_m_q1": 1.0,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_m_policy_source": "trade_war_raw_overlay",
                "raw_mfn_text_rate": "2%",
                "raw_tw_increment_rate_raw": 0.25,
                "raw_tw_rule_code_raw": "99038801",
                "raw_tw_scope_source_raw": "machine",
                "_merge": "both",
                "ref_active": True,
                "raw_active": True,
                "discrepancy_type": "match",
            },
            {
                "cty_code": 2010,
                "hs10": "0101210100",
                "year": 2018,
                "month": 10,
                "ref_m_val": 1.0,
                "ref_m_q1": 1.0,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "ref_m_hit": 1,
                "ref_m_ess": 2,
                "ref_m_status2": 1,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_steel_hit": 0,
                "ref_m_alum_hit": 0,
                "ref_m_washer_hit": 0,
                "ref_m_solar_hit": 0,
                "raw_m_val": pd.NA,
                "raw_m_q1": pd.NA,
                "raw_m_statutory_tariff1": pd.NA,
                "raw_m_statutory_tariff2": pd.NA,
                "raw_m_policy_source": pd.NA,
                "raw_mfn_text_rate": pd.NA,
                "raw_tw_increment_rate_raw": pd.NA,
                "raw_tw_rule_code_raw": pd.NA,
                "raw_tw_scope_source_raw": pd.NA,
                "_merge": "left_only",
                "ref_active": True,
                "raw_active": False,
                "discrepancy_type": "missing_raw_policy_scope",
            },
        ]
    ).to_parquet(trace_dir / "raw_replication_discrepancies.parquet", index=False)
    pd.DataFrame(
        {
            "cty_name": ["CHINA", "CHINA"],
            "hs8": ["01012100", "01012101"],
            "year": [2018, 2018],
            "month": [10, 10],
            "tw_increment_rate_raw": [0.25, 0.25],
            "tw_rule_code_raw": ["99038801", "99038801"],
            "tw_scope_source_raw": ["machine_or_pdf", "machine_or_pdf"],
        }
    ).to_parquet(cfg.analysis_dir / "tradewar_overlay_raw.parquet", index=False)
    pd.DataFrame(
        {
            "cty_code": [5700, 5700],
            "cty_name": ["CHINA", "CHINA"],
            "hs10": ["0101210000", "0101210100"],
            "year": [2018, 2018],
            "month": [10, 10],
            "tw_increment_rate_raw": [0.25, pd.NA],
            "tw_rule_code_raw": ["99038801", pd.NA],
            "tw_scope_source_raw": ["machine_or_pdf", pd.NA],
        }
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)

    result = build_china_301_universe_trace_from_artifacts(cfg)
    trace = pd.read_csv(trace_dir / "raw_replication_china_301_universe_trace.csv")
    assert result["rows"] == 2
    assert set(trace["diagnosed_stage"].astype(str)) >= {"china_active_applied", "non_china_product_scope_only"}
    assert trace.loc[trace["cty_code"].eq(5700), "corrected_missing_raw_scope"].any() is False or bool(trace.loc[trace["cty_code"].eq(5700), "corrected_missing_raw_scope"].any()) is False
    assert trace.loc[trace["cty_code"].eq(2010), "diagnosed_stage"].item() == "non_china_product_scope_only"


def test_semantics_corrected_validation_filters_to_targeted_china_universe(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.fajgelbaum_analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "cty_code": [5700, 5700, 2010],
            "cty_name": ["CHINA", "CHINA", "MEXICO"],
            "hs10": ["0101210000", "0101210100", "0101210000"],
            "year": [2018, 2018, 2018],
            "month": [10, 10, 10],
            "m_val": [100.0, 80.0, 50.0],
            "m_q1": [10.0, 8.0, 5.0],
            "m_stattariff1": [0.27, 0.27, 0.20],
            "m_stattariff2": [0.27, 0.27, 0.20],
            "m_hit": [1, 1, 1],
            "m_ess": [2, 2, 2],
            "m_status2": [1, 0, 1],
            "m_china_hit": [1, 1, 1],
            "m_steel_hit": [0, 0, 0],
            "m_alum_hit": [0, 0, 0],
            "m_washer_hit": [0, 0, 0],
            "m_solar_hit": [0, 0, 0],
            "m_effective_mdate2": ["2018-10-01", "2018-10-01", "2018-10-01"],
        }
    ).to_stata(cfg.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta", write_index=False)
    pd.DataFrame(
        {
            "cty_code": [5700, 5700, 5700],
            "cty_name": ["CHINA", "CHINA", "CHINA"],
            "hs10": ["0101210000", "0101210100", "0101210200"],
            "year": [2018, 2018, 2018],
            "month": [10, 10, 10],
            "m_val": [1000000.0, 800000.0, 750000.0],
            "m_q1": [100.0, 80.0, 75.0],
            "m_stattariff1": [0.27, 0.27, 0.27],
            "m_stattariff2": [0.27, 0.27, 0.27],
            "m_policy_source": ["trade_war_raw_overlay", "trade_war_raw_overlay", "trade_war_raw_overlay"],
            "mfn_text_rate": ["2%", "2%", "2%"],
            "tw_increment_rate_raw": [0.25, pd.NA, 0.25],
            "tw_rule_code_raw": ["99038801", pd.NA, "99038801"],
            "tw_scope_source_raw": ["machine", pd.NA, "machine"],
        }
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)
    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "year": 2018,
                "month": 10,
                "ref_m_val": 100.0,
                "ref_m_q1": 10.0,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "ref_m_hit": 1,
                "ref_m_ess": 2,
                "ref_m_status2": 1,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_steel_hit": 0,
                "ref_m_alum_hit": 0,
                "ref_m_washer_hit": 0,
                "ref_m_solar_hit": 0,
                "raw_m_val": 1000000.0,
                "raw_m_q1": 100.0,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_m_policy_source": "trade_war_raw_overlay",
                "raw_mfn_text_rate": "2%",
                "raw_tw_increment_rate_raw": 0.25,
                "raw_tw_rule_code_raw": "99038801",
                "raw_tw_scope_source_raw": "machine",
                "_merge": "both",
                "ref_active": True,
                "raw_active": True,
                "discrepancy_type": "match",
            }
        ]
    ).to_parquet(trace_dir / "raw_replication_discrepancies.parquet", index=False)

    result = run_raw_replication_validation_china_semantics_corrected(cfg)
    gate = pd.read_json(result["gate_path"], typ="series")
    assert gate["ready_for_extension"] is False
    assert gate["reason"] == "china_301_semantics_corrected_validation_only"
    assert gate["paper_key_coverage_rate"] <= 1.0
    assert not (trace_dir / "raw_replication_release_gate.json").exists()
    semantics = pd.read_csv(result["variable_semantics_path"])
    assert "m_target" in set(semantics["variable"].astype(str))
    assert Path(result["rate_trace_path"]).name == "raw_replication_china_301_rate_trace_china_301_semantics_corrected.csv"
    assert Path(result["rate_timing_trace_path"]).name == "raw_replication_china_301_rate_timing_trace_china_301_semantics_corrected.csv"
    assert Path(result["rate_provenance_path"]).name == "raw_replication_china_301_rate_provenance_china_301_semantics_corrected.csv"
    assert Path(result["rate_mismatch_decomposition_path"]).name == "raw_replication_china_301_rate_mismatch_decomposition_china_301_semantics_corrected.csv"
    assert Path(result["statutory_component_trace_path"]).name == "raw_replication_china_301_statutory_component_trace_china_301_semantics_corrected.csv"
    assert Path(result["benchmark_definition_trace_path"]).name == "raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv"
    assert Path(result["rate_trace_path"]).exists()
    assert Path(result["rate_timing_trace_path"]).exists()
    assert Path(result["benchmark_definition_trace_path"]).exists()
    freshness = pd.read_csv(result["artifact_freshness_path"])
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_rate_trace_china_301_semantics_corrected.csv"), "exists"].item() is True
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv"), "exists"].item() is True


def test_semantics_corrected_freshness_only_tracks_semantics_corrected_outputs(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    output_dir = cfg.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)

    overlay_path = cfg.analysis_dir / "tradewar_overlay_raw.parquet"
    panel_path = cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    pd.DataFrame({"cty_name": ["CHINA"], "hs8": ["01012100"], "year": [2018], "month": [7]}).to_parquet(overlay_path, index=False)
    pd.DataFrame({"cty_code": [5700], "hs10": ["0101210000"], "year": [2018], "month": [7]}).to_parquet(panel_path, index=False)

    baseline_gate = output_dir / "raw_replication_release_gate.json"
    baseline_gate.write_text("{}", encoding="utf-8")
    corrected_gate = output_dir / "raw_replication_release_gate_china_301_semantics_corrected.json"
    corrected_gate.write_text("{}", encoding="utf-8")

    old = pd.Timestamp("2020-01-01", tz="UTC").timestamp()
    new = pd.Timestamp("2020-02-01", tz="UTC").timestamp()
    os.utime(baseline_gate, (old, old))
    os.utime(corrected_gate, (new, new))
    os.utime(overlay_path, (new, new))
    os.utime(panel_path, (new, new))

    freshness = build_raw_replication_artifact_freshness(cfg, artifact_suffix="_china_301_semantics_corrected")

    assert "raw_replication_release_gate.json" not in set(freshness["artifact"])
    corrected_row = freshness.loc[freshness["artifact"].eq("raw_replication_release_gate_china_301_semantics_corrected.json")].iloc[0]
    assert bool(corrected_row["stale_relative_to_inputs"]) is False


def test_22042150_panel_trace_reports_missing_raw_trade_key(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "hs10": "2204215005",
                "year": 2019,
                "month": 1,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "discrepancy_type": "missing_raw_key",
            }
        ]
    ).to_parquet(trace_dir / "raw_replication_discrepancies.parquet", index=False)
    pd.DataFrame(
        {
            "cty_name": ["CHINA"],
            "hs8": ["22042150"],
            "year": [2019],
            "month": [1],
            "tw_increment_rate_raw": [0.10],
            "tw_rule_code_raw": ["99038803"],
            "tw_scope_source_raw": ["machine_or_pdf|deterministic_grouping"],
        }
    ).to_parquet(cfg.analysis_dir / "tradewar_overlay_raw.parquet", index=False)
    pd.DataFrame(
        {
            "cty_code": [5700],
            "cty_name": ["CHINA"],
            "hs10": ["2204215099"],
            "year": [2019],
            "month": [1],
            "tw_increment_rate_raw": [pd.NA],
            "tw_rule_code_raw": [pd.NA],
            "tw_scope_source_raw": [pd.NA],
        }
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)

    result = build_22042150_panel_trace_from_artifacts(cfg)
    trace = pd.read_csv(trace_dir / "raw_replication_22042150_panel_trace.csv", dtype={"hs10": "string", "hs8": "string", "diagnosed_stage": "string"})
    assert result["rows"] == 1
    assert trace.loc[0, "diagnosed_stage"] == "no_raw_trade_key"
    assert trace.loc[0, "raw_trade_key_present"] is False or bool(trace.loc[0, "raw_trade_key_present"]) is False
    assert trace.loc[0, "overlay_hs8_present"] is True or bool(trace.loc[0, "overlay_hs8_present"]) is True


def test_china_301_key_trace_classifies_exact_key_failure_modes(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_policy_scope",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_policy_scope",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210200",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_policy_scope",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210300",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_policy_scope",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210400",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_policy_scope",
            },
            {
                "cty_code": 5700,
                "hs10": "0200000000",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_key",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210600",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.29,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "statutory_rate_mismatch",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210700",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.29,
                "discrepancy_type": "day_weighted_rate_mismatch",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210400",
                "year": 2018,
                "month": 10,
                "ref_m_effective_mdate2": "2018-10-01",
                "ref_m_china_hit": 1,
                "ref_m_status2": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "discrepancy_type": "missing_raw_policy_scope",
            },
        ]
    ).to_parquet(trace_dir / "raw_replication_discrepancies.parquet", index=False)

    pd.DataFrame(
        {
            "cty_name": ["CHINA"],
            "hs8": ["01012100"],
            "year": [2018],
            "month": [10],
            "tw_increment_rate_raw": [0.25],
            "tw_rule_code_raw": ["99038801"],
            "tw_scope_source_raw": ["machine_or_pdf"],
        }
    ).to_parquet(cfg.analysis_dir / "tradewar_overlay_raw.parquet", index=False)
    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210100",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": pd.NA,
                "tw_rule_code_raw": pd.NA,
                "tw_scope_source_raw": pd.NA,
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210200",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
                {
                    "cty_code": 5700,
                    "cty_name": "CHINA",
                    "hs10": "0101210300",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210300",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210400",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210600",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210700",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "9999999900",
                "year": 2018,
                "month": 10,
                "tw_increment_rate_raw": 0.25,
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
        ]
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)

    result = build_china_301_key_trace_from_artifacts(cfg)
    trace = pd.read_csv(
        trace_dir / "raw_replication_china_301_key_trace.csv",
        dtype={"cty_code": "Int64", "hs10": "string", "hs8": "string", "diagnosed_stage": "string"},
    )

    assert result["rows"] == 8
    assert trace.loc[trace["hs10"].eq("0101210000"), "diagnosed_stage"].item() == "hs8_overlay_present_exact_hs10_absent"
    assert trace.loc[trace["hs10"].eq("0101210100"), "diagnosed_stage"].item() == "raw_key_present_no_increment"
    assert trace.loc[trace["hs10"].eq("0101210200"), "diagnosed_stage"].item() == "panel_increment_present_but_validation_mismatch"
    assert trace.loc[trace["hs10"].eq("0101210300"), "diagnosed_stage"].item() == "duplicate_raw_key"
    assert trace.loc[trace["hs10"].eq("0101210400"), "diagnosed_stage"].item() == "duplicate_reference_key"
    assert trace.loc[trace["hs10"].eq("0200000000"), "diagnosed_stage"].item() == "raw_key_absent"
    assert trace.loc[trace["hs10"].eq("0101210600"), "diagnosed_stage"].item() == "statutory_rate_mismatch"
    assert trace.loc[trace["hs10"].eq("0101210700"), "diagnosed_stage"].item() == "day_weighted_rate_mismatch"
    leak = _classify_china_301_key_stage(
        pd.Series(
            {
                "cty_code": 2010,
                "ref_active": False,
                "ref_m_china_hit": 0,
                "duplicate_reference_key_rows": 1,
                "duplicate_raw_key_rows": 1,
                "raw_key_present": True,
                "overlay_hs8_month_present": True,
            }
        )
    )
    assert leak == "non_china_or_inactive_reference_leak"


def test_tradewar_pdf_csv_link_provenance_prefers_same_row_rules_over_context_only(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    extract_dir = cfg.staging_dir / "policy" / "pdf_extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "pdf_file": "2019HTSAREV10.pdf",
                "page": 100,
                "hs_code": "8481.80.90",
                "hs_digits": "84818090",
                "description_blob": "No. Other. 1/ See subheading 9903.88.01. Rates of Duty.",
                "rate_token_1": "",
                "rate_token_2": "",
                "context_excerpt": "neighbor text mentions 9903.88.04 only",
                "code_level": 10,
            },
            {
                "pdf_file": "2019HTSAREV10.pdf",
                "page": 101,
                "hs_code": "8481.80.95",
                "hs_digits": "84818095",
                "description_blob": "No explicit rule on this row.",
                "rate_token_1": "",
                "rate_token_2": "",
                "context_excerpt": "1/ See subheading 9903.88.04. Rates of Duty.",
                "code_level": 10,
            },
        ]
    ).to_csv(extract_dir / "2019HTSAREV10_extracted_rows.csv", index=False)

    monkeypatch.setattr(
        "passthru_data.build_us_products_partner_panel._load_tradewar_release_catalog",
        lambda _cfg: pd.DataFrame(
            {
                "release_name": ["2019HTSAREV10"],
                "release_start_date": [pd.Timestamp("2019-01-01")],
                "release_end_date": [pd.Timestamp("2019-12-31")],
                "year": [2019],
            }
        ),
    )

    provenance = _load_tradewar_pdf_csv_link_provenance(cfg)
    links = _load_tradewar_pdf_csv_links(cfg)

    assert set(links["rule_code"].astype(str)) == {"99038801", "99038804"}
    same_row = provenance.loc[provenance["hs8"].eq("84818090")].iloc[0]
    context_only = provenance.loc[provenance["hs8"].eq("84818095")].iloc[0]
    assert same_row["rule_code"] == "99038801"
    assert same_row["extraction_method"] == "product_line_same_row_text"
    assert bool(same_row["rule_found_in_same_row"]) is True
    assert bool(same_row["rule_found_only_in_context"]) is False
    assert "9903.88.04" not in str(same_row["matched_rule_text"])
    assert context_only["rule_code"] == "99038804"
    assert context_only["extraction_method"] == "product_line_context_excerpt"
    assert bool(context_only["rule_found_in_same_row"]) is False
    assert bool(context_only["rule_found_only_in_context"]) is True


def test_china_301_wave_audit_classifies_provenance_patterns(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    output_dir = cfg.verification_dir / "raw_replication_imports"
    output_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "hs8": ["84818090", "84818090", "85365090", "85365090", "85044095", "84821050", "94032000"],
            "missing_scope_rows": [50, 50, 40, 40, 30, 20, 10],
        }
    ).to_csv(output_dir / "raw_replication_china_301_top_hs8_month_wave.csv", index=False)

    provenance = pd.DataFrame(
        [
            {
                "release_name": "2019A",
                "release_start_date": pd.Timestamp("2019-01-01"),
                "release_end_date": pd.Timestamp("2019-12-31"),
                "source_file": "2019A_extracted_rows.csv",
                "source_page": 1,
                "source_row": 1,
                "hs8": "84818090",
                "rule_code": "99038801",
                "extraction_method": "product_line_same_row_text",
                "rule_found_in_same_row": True,
                "rule_found_only_in_context": False,
                "matched_rule_text": "9903.88.01",
            },
            {
                "release_name": "2019A",
                "release_start_date": pd.Timestamp("2019-01-01"),
                "release_end_date": pd.Timestamp("2019-12-31"),
                "source_file": "2019A_extracted_rows.csv",
                "source_page": 1,
                "source_row": 2,
                "hs8": "84818090",
                "rule_code": "99038802",
                "extraction_method": "product_line_same_row_text",
                "rule_found_in_same_row": True,
                "rule_found_only_in_context": False,
                "matched_rule_text": "9903.88.02",
            },
            {
                "release_name": "2018A",
                "release_start_date": pd.Timestamp("2018-01-01"),
                "release_end_date": pd.Timestamp("2018-12-31"),
                "source_file": "2018A_extracted_rows.csv",
                "source_page": 2,
                "source_row": 1,
                "hs8": "85365090",
                "rule_code": "99038803",
                "extraction_method": "product_line_same_row_text",
                "rule_found_in_same_row": True,
                "rule_found_only_in_context": False,
                "matched_rule_text": "9903.88.03",
            },
            {
                "release_name": "2019A",
                "release_start_date": pd.Timestamp("2019-01-01"),
                "release_end_date": pd.Timestamp("2019-12-31"),
                "source_file": "2019A_extracted_rows.csv",
                "source_page": 2,
                "source_row": 2,
                "hs8": "85365090",
                "rule_code": "99038804",
                "extraction_method": "product_line_same_row_text",
                "rule_found_in_same_row": True,
                "rule_found_only_in_context": False,
                "matched_rule_text": "9903.88.04",
            },
            {
                "release_name": "2019A",
                "release_start_date": pd.Timestamp("2019-01-01"),
                "release_end_date": pd.Timestamp("2019-12-31"),
                "source_file": "2019A_extracted_rows.csv",
                "source_page": 3,
                "source_row": 1,
                "hs8": "85044095",
                "rule_code": "99038804",
                "extraction_method": "product_line_context_excerpt",
                "rule_found_in_same_row": False,
                "rule_found_only_in_context": True,
                "matched_rule_text": "9903.88.04",
            },
            {
                "release_name": "2019A",
                "release_start_date": pd.Timestamp("2019-01-01"),
                "release_end_date": pd.Timestamp("2019-12-31"),
                "source_file": "2019A_extracted_rows.csv",
                "source_page": 4,
                "source_row": 1,
                "hs8": "84821050",
                "rule_code": "99038801",
                "extraction_method": "chapter99_enumeration_link",
                "rule_found_in_same_row": True,
                "rule_found_only_in_context": False,
                "matched_rule_text": "8481.80.90;8482.10.50",
            },
            {
                "release_name": "2019A",
                "release_start_date": pd.Timestamp("2019-01-01"),
                "release_end_date": pd.Timestamp("2019-12-31"),
                "source_file": "2019A_extracted_rows.csv",
                "source_page": 5,
                "source_row": 1,
                "hs8": "94032000",
                "rule_code": "99038801",
                "extraction_method": "product_line_same_row_text",
                "rule_found_in_same_row": True,
                "rule_found_only_in_context": False,
                "matched_rule_text": "9903.88.01",
            },
        ]
    )
    monkeypatch.setattr(
        "passthru_data.raw_replication_validation._load_tradewar_pdf_csv_link_provenance",
        lambda _cfg: provenance,
    )

    result = build_china_301_wave_link_audit_from_artifacts(cfg)
    audit = pd.read_csv(output_dir / "raw_replication_china_301_wave_link_audit.csv", dtype={"hs8": "string", "rule_code": "string"})
    conflicts = pd.read_csv(output_dir / "raw_replication_china_301_wave_conflicts.csv", dtype={"hs8": "string", "diagnosed_stage": "string"})
    materiality = pd.read_csv(output_dir / "raw_replication_china_301_wave_materiality.csv", dtype={"diagnosed_stage": "string"})

    assert result["rows"] == len(audit)
    assert set(conflicts["diagnosed_stage"].astype(str)) == {
        "single_core_rule",
        "same_release_multiple_action_rules",
        "rule03_rule04_temporal_pair",
        "product_context_only_link",
        "chapter99_enumeration_link",
    }
    assert conflicts.loc[conflicts["hs8"].eq("85044095"), "diagnosed_stage"].item() == "product_context_only_link"
    assert conflicts.loc[conflicts["hs8"].eq("84818090"), "diagnosed_stage"].item() == "same_release_multiple_action_rules"
    assert conflicts.loc[conflicts["hs8"].eq("85365090"), "diagnosed_stage"].item() == "rule03_rule04_temporal_pair"
    assert conflicts.loc[conflicts["hs8"].eq("84821050"), "diagnosed_stage"].item() == "chapter99_enumeration_link"
    assert conflicts.loc[conflicts["hs8"].eq("94032000"), "diagnosed_stage"].item() == "single_core_rule"
    assert "focus_missing_scope_rows" in audit.columns
    assert materiality["rows"].sum() == len(conflicts)


def test_china_301_rate_trace_classifies_exact_key_failure_modes(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "year": 2018,
                "month": 10,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "ref_m_effective_mdate2": "2018-10-01",
                "discrepancy_type": "statutory_rate_mismatch",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "year": 2018,
                "month": 10,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.29,
                "ref_m_effective_mdate2": "2018-10-01",
                "discrepancy_type": "day_weighted_rate_mismatch",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210200",
                "year": 2018,
                "month": 10,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.29,
                "ref_m_stattariff2": 0.29,
                "ref_m_effective_mdate2": "2018-10-01",
                "discrepancy_type": "missing_raw_policy_scope",
            },
                {
                    "cty_code": 5700,
                    "hs10": "0101210300",
                    "year": 2018,
                    "month": 10,
                    "ref_active": True,
                    "ref_m_status2": 1,
                    "ref_m_china_hit": 1,
                    "ref_m_stattariff1": 0.27,
                    "ref_m_stattariff2": 0.27,
                    "ref_m_effective_mdate2": "2018-10-01",
                    "discrepancy_type": "missing_raw_key",
                },
                {
                    "cty_code": 5700,
                    "hs10": "0101210400",
                    "year": 2018,
                    "month": 10,
                    "ref_active": True,
                    "ref_m_status2": 1,
                    "ref_m_china_hit": 1,
                    "ref_m_stattariff1": 0.27,
                    "ref_m_stattariff2": 0.27,
                    "ref_m_effective_mdate2": "2018-10-01",
                    "discrepancy_type": "missing_raw_policy_scope",
                },
            ]
        ).to_parquet(trace_dir / "raw_replication_discrepancies.parquet", index=False)

    pd.DataFrame(
        {
            "cty_name": ["CHINA"],
            "hs8": ["01012100"],
            "year": [2018],
            "month": [10],
            "tw_increment_rate_raw": [0.25],
            "tw_rule_code_raw": ["99038801"],
            "tw_scope_source_raw": ["machine_or_pdf"],
        }
    ).to_parquet(cfg.analysis_dir / "tradewar_overlay_raw.parquet", index=False)
    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210000",
                "year": 2018,
                "month": 10,
                "mfn_ad_val_rate": 0.02,
                "base_pref_rate_raw": 0.02,
                "base_statutory_rate_raw": 0.02,
                "tw_increment_rate_raw": 0.25,
                "tw_active_share_raw": 1.0,
                "m_statutory_tariff1": 0.27,
                "m_statutory_tariff2": 0.27,
                "m_policy_source": "trade_war_raw_overlay",
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210100",
                "year": 2018,
                "month": 10,
                "mfn_ad_val_rate": 0.02,
                "base_pref_rate_raw": 0.02,
                "base_statutory_rate_raw": 0.02,
                "tw_increment_rate_raw": 0.25,
                "tw_active_share_raw": 0.5,
                "m_statutory_tariff1": 0.27,
                "m_statutory_tariff2": 0.29,
                "m_policy_source": "trade_war_raw_overlay",
                "tw_rule_code_raw": "99038801",
                "tw_scope_source_raw": "machine_or_pdf",
            },
            {
                "cty_code": 5700,
                "cty_name": "CHINA",
                "hs10": "0101210300",
                "year": 2018,
                "month": 10,
                "mfn_ad_val_rate": 0.02,
                "base_pref_rate_raw": 0.02,
                "base_statutory_rate_raw": 0.02,
                "tw_increment_rate_raw": pd.NA,
                "tw_active_share_raw": pd.NA,
                "m_statutory_tariff1": 0.02,
                "m_statutory_tariff2": 0.02,
                    "m_policy_source": "mfn_schedule_only",
                    "tw_rule_code_raw": pd.NA,
                    "tw_scope_source_raw": pd.NA,
                },
                {
                    "cty_code": 5700,
                    "cty_name": "CHINA",
                    "hs10": "0101210200",
                    "year": 2018,
                    "month": 10,
                    "mfn_ad_val_rate": 0.02,
                    "base_pref_rate_raw": 0.02,
                    "base_statutory_rate_raw": 0.02,
                    "tw_increment_rate_raw": pd.NA,
                    "tw_active_share_raw": pd.NA,
                    "m_statutory_tariff1": 0.02,
                    "m_statutory_tariff2": 0.02,
                    "m_policy_source": "mfn_schedule_only",
                    "tw_rule_code_raw": pd.NA,
                    "tw_scope_source_raw": pd.NA,
                },
            ]
        ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)

    result = build_china_301_rate_trace_from_artifacts(cfg)
    trace = pd.read_csv(
        trace_dir / "raw_replication_china_301_rate_trace.csv",
        dtype={"cty_code": "Int64", "hs10": "string", "hs8": "string", "diagnosed_stage": "string"},
    )

    assert result["rows"] == 5
    assert trace.loc[trace["hs10"].eq("0101210000"), "diagnosed_stage"].item() == "statutory_rate_mismatch"
    assert trace.loc[trace["hs10"].eq("0101210100"), "diagnosed_stage"].item() == "day_weighted_rate_mismatch"
    assert trace.loc[trace["hs10"].eq("0101210200"), "diagnosed_stage"].item() == "raw_key_present_no_increment"
    assert trace.loc[trace["hs10"].eq("0101210300"), "diagnosed_stage"].item() == "raw_key_present_no_increment"
    assert trace.loc[trace["hs10"].eq("0101210400"), "diagnosed_stage"].item() == "raw_key_absent"
    assert "raw_formula_statutory_rate" in trace.columns
    assert "ref_month_active_share" in trace.columns


def test_china_301_rate_timing_trace_is_built_and_tracked(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "hs8": "01012100",
                "year": 2018,
                "month": 7,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.27,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "raw_tw_active_share_raw": 1.0,
                "ref_effective_period": "2018-07",
                "raw_panel_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "raw_panel_policy_source": "trade_war_raw_overlay",
                "discrepancy_type": "day_weighted_rate_mismatch",
                "raw_formula_statutory_gap": 0.0,
                "raw_formula_day_weighted_gap": 0.0,
                "ref_rate_gap_statutory": 0.0,
                "ref_rate_gap_day_weighted": 0.0,
                "benchmark_implied_increment": 0.25,
                "benchmark_implied_active_share": 1.0,
                "raw_key_present": True,
                "raw_panel_hs10_present": True,
                "raw_panel_hs8_month_present": True,
                "overlay_hs8_month_present": True,
                "diagnosed_stage": "benchmark_uses_full_month",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "hs8": "01012101",
                "year": 2018,
                "month": 7,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.23064516129032258,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "raw_tw_active_share_raw": 0.8387096774193549,
                "ref_effective_period": "2018-07",
                "raw_panel_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "raw_panel_policy_source": "trade_war_raw_overlay",
                "discrepancy_type": "day_weighted_rate_mismatch",
                "raw_formula_statutory_gap": 0.0,
                "raw_formula_day_weighted_gap": 0.0,
                "ref_rate_gap_statutory": 0.0,
                "ref_rate_gap_day_weighted": 0.0,
                "benchmark_implied_increment": 0.25,
                "benchmark_implied_active_share": 0.8387096774193549,
                "raw_key_present": True,
                "raw_panel_hs10_present": True,
                "raw_panel_hs8_month_present": True,
                "overlay_hs8_month_present": True,
                "diagnosed_stage": "benchmark_uses_legal_effective_day",
            },
        ]
    ).to_csv(trace_dir / "raw_replication_china_301_rate_trace.csv", index=False)

    result = build_china_301_rate_timing_trace_from_artifacts(cfg)
    timing = pd.read_csv(
        trace_dir / "raw_replication_china_301_rate_timing_trace.csv",
        dtype={"cty_code": "Int64", "hs10": "string", "hs8": "string", "diagnosed_stage": "string"},
    )
    freshness = build_raw_replication_artifact_freshness(cfg)

    assert result["rows"] == 2
    assert set(timing["diagnosed_stage"]) == {"benchmark_uses_full_month", "benchmark_uses_legal_effective_day"}
    assert (trace_dir / "raw_replication_china_301_rate_timing_by_month.csv").exists()
    assert (trace_dir / "raw_replication_china_301_rate_timing_by_rule.csv").exists()
    assert (trace_dir / "raw_replication_china_301_rate_timing_by_stage.csv").exists()
    assert (trace_dir / "raw_replication_china_301_rate_timing_quantiles.csv").exists()
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_rate_timing_trace.csv"), "exists"].item() is True
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_rate_timing_by_month.csv"), "exists"].item() is True


def test_china_301_rate_provenance_and_mismatch_decomposition_are_built(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "hs8": "01012100",
                "year": 2018,
                "month": 7,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.275,
                "ref_m_stattariff2": 0.275,
                "ref_effective_period": "2018-07",
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "raw_tw_active_share_raw": 1.0,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_formula_statutory_gap": 0.0,
                "raw_formula_day_weighted_gap": 0.0,
                "ref_rate_gap_statutory": 0.005,
                "ref_rate_gap_day_weighted": 0.005,
                "benchmark_implied_increment": 0.255,
                "benchmark_implied_active_share": 1.0,
                "raw_panel_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "raw_panel_policy_source": "machine_or_pdf",
                "discrepancy_type": "statutory_rate_mismatch",
                "diagnosed_stage": "statutory_rate_mismatch",
                "raw_key_present": True,
                "raw_panel_hs10_present": True,
                "raw_panel_hs8_month_present": True,
                "overlay_hs8_month_present": True,
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "hs8": "01012101",
                "year": 2018,
                "month": 7,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.27,
                "ref_m_stattariff2": 0.23064516129032258,
                "ref_effective_period": "2018-07",
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "raw_tw_active_share_raw": 0.8387096774193549,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_formula_statutory_gap": 0.0,
                "raw_formula_day_weighted_gap": 0.0,
                "ref_rate_gap_statutory": 0.0,
                "ref_rate_gap_day_weighted": 0.0,
                "benchmark_implied_increment": 0.25,
                "benchmark_implied_active_share": 0.8387096774193549,
                "raw_panel_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "raw_panel_policy_source": "machine_or_pdf",
                "discrepancy_type": "day_weighted_rate_mismatch",
                "diagnosed_stage": "day_weighted_rate_mismatch",
                "raw_key_present": True,
                "raw_panel_hs10_present": True,
                "raw_panel_hs8_month_present": True,
                "overlay_hs8_month_present": True,
            },
        ]
    ).to_csv(trace_dir / "raw_replication_china_301_rate_trace.csv", index=False)

    pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "year": 2018,
                "month": 7,
                "discrepancy_type": "statutory_rate_mismatch",
                "diagnosed_stage": "benchmark_source_precision_diff",
                "closest_candidate_timing": "full_month",
                "closest_candidate_abs_gap": 0.0,
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "year": 2018,
                "month": 7,
                "discrepancy_type": "day_weighted_rate_mismatch",
                "diagnosed_stage": "benchmark_uses_legal_effective_day",
                "closest_candidate_timing": "legal_effective_date",
                "closest_candidate_abs_gap": 0.0,
            },
        ]
    ).to_csv(trace_dir / "raw_replication_china_301_rate_timing_trace.csv", index=False)

    provenance_result = build_china_301_rate_provenance_from_artifacts(cfg)
    decomposition_result = build_china_301_rate_mismatch_decomposition_from_artifacts(cfg)
    provenance = pd.read_csv(trace_dir / "raw_replication_china_301_rate_provenance.csv")
    decomposition = pd.read_csv(trace_dir / "raw_replication_china_301_rate_mismatch_decomposition.csv")
    freshness = build_raw_replication_artifact_freshness(cfg)

    assert provenance_result["rows"] == 2
    assert decomposition_result["rows"] == 2
    assert set(provenance["rate_provenance_stage"]) == {
        "benchmark_source_precision_diff",
        "benchmark_timing_mismatch",
    }
    assert "median_ref_rate_gap_statutory_pp" in decomposition.columns
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_rate_provenance.csv"), "exists"].item() is True
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_rate_mismatch_decomposition.csv"), "exists"].item() is True


@pytest.mark.parametrize(
    "row,expected",
    [
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 0.0,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "exact_raw_components_match",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": 0.275,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 0.5,
                "raw_vs_overlay_increment_gap_pp": 0.0,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "base_rate_precision_difference",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.30,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 0.0,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 5.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "benchmark_increment_definition_difference",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.20,
                "rule_attribute_increment": 0.20,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 5.0,
                "raw_vs_rule_attribute_increment_gap_pp": 5.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 5.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "panel_overlay_increment_mismatch",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.20,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 5.0,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "overlay_rule_attribute_mismatch",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": pd.NA,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 0.0,
                "raw_vs_rule_attribute_increment_gap_pp": pd.NA,
                "benchmark_implied_vs_rule_attribute_gap_pp": pd.NA,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 0,
            },
            "missing_rule_attribute",
        ),
        (
            {
                "raw_total_statutory_rate": 9999.0,
                "raw_base_statutory_rate_raw": 9999.0,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 0.0,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "non_ad_valorem_or_sentinel",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": pd.NA,
                "benchmark_implied_increment": pd.NA,
                "raw_vs_reference_gap_pp": pd.NA,
                "raw_vs_overlay_increment_gap_pp": pd.NA,
                "raw_vs_rule_attribute_increment_gap_pp": pd.NA,
                "benchmark_implied_vs_rule_attribute_gap_pp": pd.NA,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "duplicate_rule_attribute_rows": 1,
            },
            "requires_full_model_review",
        ),
        (
            {
                "raw_total_statutory_rate": 0.27,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "overlay_increment": 0.25,
                "rule_attribute_increment": 0.25,
                "ref_statutory_rate": 0.37,
                "benchmark_implied_increment": 0.25,
                "raw_vs_reference_gap_pp": 10.0,
                "raw_vs_overlay_increment_gap_pp": 0.0,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038802",
                "duplicate_rule_attribute_rows": 1,
            },
            "rule_code_missing_or_ambiguous",
        ),
    ],
)
def test_china_301_statutory_component_stage_classifier_covers_component_cases(row: dict[str, object], expected: str) -> None:
    assert _classify_china_301_statutory_component_stage(pd.Series(row)) == expected


def test_china_301_statutory_component_trace_is_built(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.reference_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        {
            "cty_code": 5700,
            "hs10": "0101210000",
            "hs8": "01012100",
            "year": 2018,
            "month": 7,
            "ref_active": True,
            "ref_m_status2": 1,
            "ref_m_china_hit": 1,
            "ref_m_stattariff1": 0.37,
            "ref_m_stattariff2": 0.37,
            "ref_effective_period": "2018-07",
            "raw_key_present": True,
            "raw_panel_hs10_present": True,
            "raw_panel_hs8_month_present": True,
            "raw_panel_increment": 0.25,
            "raw_panel_rule_code": "99038801",
            "raw_panel_policy_source": "machine_or_pdf",
            "raw_rule_code": "99038801",
            "raw_policy_source": "machine_or_pdf",
            "raw_mfn_ad_val_rate": 0.02,
            "raw_base_pref_rate_raw": 0.02,
            "raw_base_statutory_rate_raw": 0.02,
            "raw_tw_active_share_raw": 1.0,
            "raw_m_statutory_tariff1": 0.27,
            "raw_m_statutory_tariff2": 0.27,
            "raw_formula_statutory_rate": 0.27,
            "raw_formula_day_weighted_rate": 0.27,
            "raw_formula_statutory_gap": 0.0,
            "raw_formula_day_weighted_gap": 0.0,
            "ref_rate_gap_statutory": 0.10,
            "ref_rate_gap_day_weighted": 0.10,
            "benchmark_implied_increment": 0.25,
            "benchmark_implied_active_share": 1.0,
            "overlay_hs8_month_present": True,
            "overlay_increment": 0.25,
            "overlay_rule_code": "99038801",
            "discrepancy_type": "statutory_rate_mismatch",
            "duplicate_reference_key_rows": 1,
            "duplicate_raw_key_rows": 1,
            "diagnosed_stage": "statutory_rate_mismatch",
            "rate_provenance_stage": "benchmark_statutory_definition_mismatch",
            "raw_formula_statutory_gap_pp": 0.0,
            "raw_formula_day_weighted_gap_pp": 0.0,
            "ref_rate_gap_statutory_pp": 10.0,
            "ref_rate_gap_day_weighted_pp": 10.0,
            "timing_diagnosed_stage": "requires_full_model_review",
            "closest_candidate_timing": "unmapped",
            "closest_candidate_abs_gap": pd.NA,
        },
        {
            "cty_code": 5700,
            "hs10": "0101210100",
            "hs8": "01012101",
            "year": 2018,
            "month": 7,
            "ref_active": True,
            "ref_m_status2": 1,
            "ref_m_china_hit": 1,
            "ref_m_stattariff1": 0.40,
            "ref_m_stattariff2": 0.40,
            "ref_effective_period": "2018-07",
            "raw_key_present": True,
            "raw_panel_hs10_present": True,
            "raw_panel_hs8_month_present": True,
            "raw_panel_increment": 0.20,
            "raw_panel_rule_code": "99038802",
            "raw_panel_policy_source": "machine",
            "raw_rule_code": "99038802",
            "raw_policy_source": "machine",
            "raw_mfn_ad_val_rate": 0.02,
            "raw_base_pref_rate_raw": 0.02,
            "raw_base_statutory_rate_raw": 0.20,
            "raw_tw_active_share_raw": 1.0,
            "raw_m_statutory_tariff1": 0.40,
            "raw_m_statutory_tariff2": 0.40,
            "raw_formula_statutory_rate": 0.40,
            "raw_formula_day_weighted_rate": 0.40,
            "raw_formula_statutory_gap": 0.0,
            "raw_formula_day_weighted_gap": 0.0,
            "ref_rate_gap_statutory": 0.0,
            "ref_rate_gap_day_weighted": 0.0,
            "benchmark_implied_increment": 0.20,
            "benchmark_implied_active_share": 1.0,
            "overlay_hs8_month_present": True,
            "overlay_increment": 0.20,
            "overlay_rule_code": "99038802",
            "discrepancy_type": "statutory_rate_mismatch",
            "duplicate_reference_key_rows": 1,
            "duplicate_raw_key_rows": 1,
            "diagnosed_stage": "statutory_rate_mismatch",
            "rate_provenance_stage": "benchmark_statutory_definition_mismatch",
            "raw_formula_statutory_gap_pp": 0.0,
            "raw_formula_day_weighted_gap_pp": 0.0,
            "ref_rate_gap_statutory_pp": 20.0,
            "ref_rate_gap_day_weighted_pp": 20.0,
            "timing_diagnosed_stage": "requires_full_model_review",
            "closest_candidate_timing": "unmapped",
            "closest_candidate_abs_gap": pd.NA,
        },
        {
            "cty_code": 5700,
            "hs10": "0101210200",
            "hs8": "01012102",
            "year": 2018,
            "month": 7,
            "ref_active": True,
            "ref_m_status2": 1,
            "ref_m_china_hit": 1,
            "ref_m_stattariff1": 0.41,
            "ref_m_stattariff2": 0.41,
            "ref_effective_period": "2018-07",
            "raw_key_present": True,
            "raw_panel_hs10_present": True,
            "raw_panel_hs8_month_present": True,
            "raw_panel_increment": 0.25,
            "raw_panel_rule_code": "99038803",
            "raw_panel_policy_source": "machine_or_pdf",
            "raw_rule_code": "99038803",
            "raw_policy_source": "machine_or_pdf",
            "raw_mfn_ad_val_rate": 0.02,
            "raw_base_pref_rate_raw": 0.02,
            "raw_base_statutory_rate_raw": 0.02,
            "raw_tw_active_share_raw": 1.0,
            "raw_m_statutory_tariff1": 0.27,
            "raw_m_statutory_tariff2": 0.27,
            "raw_formula_statutory_rate": 0.27,
            "raw_formula_day_weighted_rate": 0.27,
            "raw_formula_statutory_gap": 0.0,
            "raw_formula_day_weighted_gap": 0.0,
            "ref_rate_gap_statutory": 0.14,
            "ref_rate_gap_day_weighted": 0.14,
            "benchmark_implied_increment": 0.27,
            "benchmark_implied_active_share": 1.0,
            "overlay_hs8_month_present": True,
            "overlay_increment": 0.25,
            "overlay_rule_code": "99038803",
            "discrepancy_type": "statutory_rate_mismatch",
            "duplicate_reference_key_rows": 1,
            "duplicate_raw_key_rows": 1,
            "diagnosed_stage": "statutory_rate_mismatch",
            "rate_provenance_stage": "benchmark_statutory_definition_mismatch",
            "raw_formula_statutory_gap_pp": 0.0,
            "raw_formula_day_weighted_gap_pp": 0.0,
            "ref_rate_gap_statutory_pp": 14.0,
            "ref_rate_gap_day_weighted_pp": 14.0,
            "timing_diagnosed_stage": "requires_full_model_review",
            "closest_candidate_timing": "unmapped",
            "closest_candidate_abs_gap": pd.NA,
        },
    ]
    frame = pd.DataFrame(rows)
    frame.to_csv(trace_dir / "raw_replication_china_301_rate_trace.csv", index=False)
    frame.to_csv(trace_dir / "raw_replication_china_301_rate_provenance.csv", index=False)

    pd.DataFrame(
        {
            "rule_code": ["99038801", "99038802", "99038802"],
            "year": [2018, 2018, 2018],
            "month": [7, 7, 7],
            "increment_rate": [0.25, 0.20, 0.20],
        }
    ).to_parquet(cfg.reference_dir / "tradewar_rule_attributes.parquet", index=False)

    result = build_china_301_statutory_component_trace_from_artifacts(cfg)
    trace = pd.read_csv(
        trace_dir / "raw_replication_china_301_statutory_component_trace.csv",
        dtype={"raw_rule_code": "string", "overlay_rule_code": "string", "diagnosed_component": "string"},
    )
    summary = pd.read_csv(trace_dir / "raw_replication_china_301_statutory_component_summary.csv")
    clusters = pd.read_csv(trace_dir / "raw_replication_china_301_statutory_component_top_clusters.csv")
    freshness = build_raw_replication_artifact_freshness(cfg)

    assert result["rows"] == 3
    assert trace.loc[trace["raw_rule_code"].eq("99038801"), "diagnosed_component"].item() == "exact_raw_components_match"
    assert trace.loc[trace["raw_rule_code"].eq("99038802"), "diagnosed_component"].item() == "duplicate_rule_attribute"
    assert trace.loc[trace["raw_rule_code"].eq("99038803"), "diagnosed_component"].item() == "missing_rule_attribute"
    assert "rows" in summary.columns
    assert len(clusters) == 3
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_statutory_component_trace.csv"), "exists"].item() is True


def test_china_301_benchmark_definition_trace_separates_increment_and_timing_differences(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    cfg.verification_dir.mkdir(parents=True, exist_ok=True)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)

    rate_trace = pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "hs8": "01012100",
                "year": 2018,
                "month": 7,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.25,
                "ref_m_stattariff2": 0.25,
                "ref_effective_period": "2018-07",
                "raw_key_present": True,
                "raw_panel_hs10_present": True,
                "raw_panel_hs8_month_present": True,
                "raw_panel_increment": 0.25,
                "raw_panel_rule_code": "99038801",
                "raw_panel_policy_source": "machine_or_pdf|deterministic_grouping",
                "raw_mfn_ad_val_rate": 0.02,
                "raw_base_pref_rate_raw": 0.02,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_tw_active_share_raw": 1.0,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "raw_formula_statutory_rate": 0.27,
                "raw_formula_day_weighted_rate": 0.27,
                "raw_formula_statutory_gap": 0.0,
                "raw_formula_day_weighted_gap": 0.0,
                "ref_rate_gap_statutory": 0.02,
                "ref_rate_gap_day_weighted": 0.02,
                "benchmark_implied_increment": 0.23,
                "benchmark_implied_active_share": 0.80,
                "benchmark_implied_active_share_gap": 0.20,
                "overlay_hs8_month_present": True,
                "overlay_increment": 0.25,
                "overlay_rule_code": "99038801",
                "discrepancy_type": "statutory_rate_mismatch",
                "duplicate_reference_key_rows": 1,
                "duplicate_raw_key_rows": 1,
                "diagnosed_stage": "statutory_rate_mismatch",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "hs8": "01012101",
                "year": 2018,
                "month": 7,
                "ref_active": True,
                "ref_m_status2": 1,
                "ref_m_china_hit": 1,
                "ref_m_stattariff1": 0.25,
                "ref_m_stattariff2": 0.2016129046678543,
                "ref_effective_period": "2018-07",
                "raw_key_present": True,
                "raw_panel_hs10_present": True,
                "raw_panel_hs8_month_present": True,
                "raw_panel_increment": 0.25,
                "raw_panel_rule_code": "99038801",
                "raw_panel_policy_source": "machine_or_pdf|deterministic_grouping",
                "raw_mfn_ad_val_rate": 0.00,
                "raw_base_pref_rate_raw": 0.00,
                "raw_base_statutory_rate_raw": 0.00,
                "raw_tw_active_share_raw": 0.6774193548387096,
                "raw_m_statutory_tariff1": 0.25,
                "raw_m_statutory_tariff2": 0.1693548387096774,
                "raw_formula_statutory_rate": 0.25,
                "raw_formula_day_weighted_rate": 0.1693548387096774,
                "raw_formula_statutory_gap": 0.0,
                "raw_formula_day_weighted_gap": 0.0,
                "ref_rate_gap_statutory": 0.0,
                "ref_rate_gap_day_weighted": 0.0322580659581769,
                "benchmark_implied_increment": 0.25,
                "benchmark_implied_active_share": 0.8064516186714172,
                "benchmark_implied_active_share_gap": 0.1290322638327076,
                "overlay_hs8_month_present": True,
                "overlay_increment": 0.25,
                "overlay_rule_code": "99038801",
                "discrepancy_type": "day_weighted_rate_mismatch",
                "duplicate_reference_key_rows": 1,
                "duplicate_raw_key_rows": 1,
                "diagnosed_stage": "day_weighted_rate_mismatch",
            },
        ]
    )
    rate_trace.to_csv(trace_dir / "raw_replication_china_301_rate_trace.csv", index=False)

    component_trace = pd.DataFrame(
        [
            {
                "cty_code": 5700,
                "hs10": "0101210000",
                "hs8": "01012100",
                "year": 2018,
                "month": 7,
                "ref_effective_period": "2018-07",
                "discrepancy_type": "statutory_rate_mismatch",
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "raw_policy_source": "machine_or_pdf|deterministic_grouping",
                "rule_attribute_increment": 0.25,
                "raw_panel_increment": 0.25,
                "raw_m_statutory_tariff1": 0.27,
                "raw_m_statutory_tariff2": 0.27,
                "ref_m_stattariff1": 0.25,
                "ref_m_stattariff2": 0.25,
                "benchmark_implied_increment": 0.23,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 2.0,
                "duplicate_rule_attribute_rows": 1,
                "diagnosed_component": "exact_raw_components_match",
            },
            {
                "cty_code": 5700,
                "hs10": "0101210100",
                "hs8": "01012101",
                "year": 2018,
                "month": 7,
                "ref_effective_period": "2018-07",
                "discrepancy_type": "day_weighted_rate_mismatch",
                "raw_rule_code": "99038801",
                "overlay_rule_code": "99038801",
                "raw_policy_source": "machine_or_pdf|deterministic_grouping",
                "rule_attribute_increment": 0.25,
                "raw_panel_increment": 0.25,
                "raw_m_statutory_tariff1": 0.25,
                "raw_m_statutory_tariff2": 0.1693548387096774,
                "ref_m_stattariff1": 0.25,
                "ref_m_stattariff2": 0.2016129046678543,
                "benchmark_implied_increment": 0.25,
                "raw_vs_rule_attribute_increment_gap_pp": 0.0,
                "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
                "duplicate_rule_attribute_rows": 1,
                "diagnosed_component": "exact_raw_components_match",
            },
        ]
    )
    component_trace.to_csv(trace_dir / "raw_replication_china_301_statutory_component_trace.csv", index=False)

    result = build_china_301_benchmark_definition_trace_from_artifacts(cfg)
    trace = pd.read_csv(
        trace_dir / "raw_replication_china_301_benchmark_definition_trace.csv",
        dtype={"hs10": "string", "hs8": "string", "diagnosed_stage": "string"},
    )

    assert result["rows"] == 2
    assert trace.loc[trace["hs10"].eq("0101210000"), "diagnosed_stage"].item() == "benchmark_increment_definition_difference"
    assert trace.loc[trace["hs10"].eq("0101210100"), "diagnosed_stage"].item() == "benchmark_timing_convention_difference"
    assert trace.loc[trace["hs10"].eq("0101210000"), "closest_reference_formulation"].item() == "raw_increment_only"
    assert trace.loc[trace["hs10"].eq("0101210100"), "closest_reference_formulation"].item() == "raw_total_statutory_rate"
    assert (trace_dir / "raw_replication_china_301_benchmark_definition_by_rule.csv").exists()
    assert (trace_dir / "raw_replication_china_301_benchmark_definition_quantiles.csv").exists()
    freshness = build_raw_replication_artifact_freshness(cfg)
    assert freshness.loc[freshness["artifact"].eq("raw_replication_china_301_benchmark_definition_trace.csv"), "exists"].item() is True


def test_china_301_benchmark_definition_stage_requires_scaled_decimal_tolerances() -> None:
    statutory_row = pd.Series(
        {
            "cty_code": 5700,
            "ref_active": True,
            "ref_m_china_hit": 1,
            "duplicate_reference_key_rows": 1,
            "duplicate_raw_key_rows": 1,
            "raw_key_present": True,
            "discrepancy_type": "statutory_rate_mismatch",
            "ref_rate_gap_statutory": 0.02,
            "ref_rate_gap_day_weighted": 0.02,
            "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
            "benchmark_implied_active_share_gap_pp": 0.0,
            "raw_rule_code": "99038801",
            "overlay_rule_code": "99038801",
        }
    )
    day_row = pd.Series(
        {
            "cty_code": 5700,
            "ref_active": True,
            "ref_m_china_hit": 1,
            "duplicate_reference_key_rows": 1,
            "duplicate_raw_key_rows": 1,
            "raw_key_present": True,
            "discrepancy_type": "day_weighted_rate_mismatch",
            "ref_rate_gap_statutory": 0.02,
            "ref_rate_gap_day_weighted": 0.02,
            "benchmark_implied_vs_rule_attribute_gap_pp": 0.0,
            "benchmark_implied_active_share_gap_pp": 0.0,
            "raw_rule_code": "99038801",
            "overlay_rule_code": "99038801",
        }
    )

    assert _classify_china_301_benchmark_definition_stage(statutory_row) == "statutory_rate_aligned_to_raw_formula"
    assert _classify_china_301_benchmark_definition_stage(day_row) == "day_weighted_rate_aligned_to_raw_formula"


def test_china_301_rule_assignment_stage_classifier_covers_core_cases() -> None:
    assert (
        _classify_china_301_rule_assignment_stage(
            pd.Series(
                {
                    "candidate_rule_count": 4,
                    "cross_family_candidate_count": 2,
                    "raw_panel_increment": pd.NA,
                    "raw_panel_rule_code": pd.NA,
                    "overlay_rule_code": pd.NA,
                    "earliest_301_rule": "99038801",
                    "latest_301_rule": "99038803",
                    "source_file": "2019HTSARev10_extracted_rows.csv",
                    "source_page": 3510,
                    "source_row": 20637,
                    "matched_rule_text": "Articles the product of China, as provided for in U.S. note 20(e)",
                    "extraction_method": "product_line_context_excerpt",
                    "year": 2018,
                    "month": 7,
                    "discrepancy_type": "missing_raw_policy_scope",
                }
            )
        )
        == "parser_cross_family_context_bleed"
    )
    assert (
        _classify_china_301_rule_assignment_stage(
            pd.Series(
                {
                    "candidate_rule_count": 1,
                    "cross_family_candidate_count": 0,
                    "raw_panel_increment": pd.NA,
                    "raw_panel_rule_code": pd.NA,
                    "overlay_rule_code": pd.NA,
                    "earliest_301_rule": "99038801",
                    "latest_301_rule": "99038801",
                    "source_file": "2018HTSARevision7_1_extracted_rows.csv",
                    "source_page": 3337,
                    "source_row": 14109,
                    "matched_rule_text": "Articles the product of China, as enumerated in U.S. note 20 to this subchapter",
                    "extraction_method": "chapter99_nearby_note_fallback",
                    "year": 2018,
                    "month": 7,
                    "discrepancy_type": "missing_raw_policy_scope",
                }
            )
        )
        == "early_wave_link_missing"
    )
    assert (
        _classify_china_301_rule_assignment_stage(
            pd.Series(
                {
                    "candidate_rule_count": 1,
                    "cross_family_candidate_count": 0,
                    "raw_panel_increment": 0.1,
                    "raw_panel_rule_code": "99038803",
                    "overlay_rule_code": "99038803",
                    "earliest_301_rule": "99038803",
                    "latest_301_rule": "99038803",
                    "source_file": "2019HTSARev10_extracted_rows.csv",
                    "source_page": 3510,
                    "source_row": 20639,
                    "matched_rule_text": "Articles the product of China, as provided for in U.S. note 20(e)",
                    "extraction_method": "chapter99_enumeration_link",
                    "year": 2019,
                    "month": 4,
                    "discrepancy_type": "day_weighted_rate_mismatch",
                }
            )
        )
        == "timing_convention_only"
    )


def test_china_301_rule_assignment_trace_builds_from_artifacts(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)
    reference_dir = cfg.reference_dir
    reference_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "hs10": ["8501640021"],
            "hs8": ["85016400"],
            "year": [2018],
            "month": [7],
            "ref_effective_period": ["2018-07"],
            "ref_m_status2": [2],
            "ref_m_stattariff1": [0.25],
            "raw_base_statutory_rate_raw": [0.02],
            "raw_panel_increment": [pd.NA],
            "raw_panel_rule_code": [pd.NA],
            "overlay_increment": [pd.NA],
            "overlay_rule_code": [pd.NA],
            "discrepancy_type": ["missing_raw_policy_scope"],
            "ref_m_effective_mdate2": ["2018-07-06"],
        }
    ).to_csv(trace_dir / "raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv", index=False)

    pd.DataFrame(
        {
            "release_name": ["2018HTSARevision7_1", "2018HTSARevision7_1"],
            "release_start_date": ["2018-07-06", "2018-07-06"],
            "release_end_date": ["2018-08-07", "2018-08-07"],
            "source_file": ["2018HTSARevision7_1_extracted_rows.csv", "2018HTSARevision7_1_extracted_rows.csv"],
            "source_page": [3337, 3337],
            "source_row": [14109, 14109],
            "hs8": ["85016400", "85016400"],
            "rule_code": ["99038801", "99038002"],
            "extraction_method": ["chapter99_nearby_note_fallback", "chapter99_nearby_note_fallback"],
            "rule_found_in_same_row": [True, True],
            "rule_found_only_in_context": [False, False],
            "matched_rule_text": [
                "Articles the product of China, as enumerated in U.S. note 20 to this subchapter",
                "Articles the product of China, as enumerated in U.S. note 20 to this subchapter",
            ],
        }
    ).to_csv(trace_dir / "raw_replication_china_301_wave_link_audit.csv", index=False)

    pd.DataFrame(columns=["hs8", "rule_code", "release_name"]).to_parquet(reference_dir / "tradewar_machine_links.parquet", index=False)
    pd.DataFrame(
        {
            "hs8": ["85016400", "85016400"],
            "rule_code": ["99038801", "99038002"],
            "release_name": ["2018HTSARevision7_1", "2018HTSARevision7_1"],
        }
    ).to_parquet(reference_dir / "tradewar_pdf_links.parquet", index=False)

    result = build_china_301_rule_assignment_trace_from_artifacts(cfg)
    path = trace_dir / "raw_replication_china_301_rule_assignment_trace.csv"
    trace = pd.read_csv(path, dtype={"hs10": "string", "hs8": "string", "diagnosed_stage": "string"})
    assert result["rows"] == 1
    assert path.exists()
    assert trace.loc[trace["hs10"].eq("8501640021"), "diagnosed_stage"].item() == "parser_cross_family_context_bleed"
    assert trace.loc[trace["hs10"].eq("8501640021"), "structural_note_identifier"].item() == "U.S. note 20"
    assert trace.loc[trace["hs10"].eq("8501640021"), "candidate_rule_count"].item() == 2


def test_china_301_rule_assignment_trace_keeps_increment_disagreements_and_joins_status(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    trace_dir = cfg.verification_dir / "raw_replication_imports"
    trace_dir.mkdir(parents=True, exist_ok=True)
    reference_dir = cfg.reference_dir
    reference_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "hs10": "8501640021",
                "year": 2018,
                "month": 7,
                "ref_effective_period": "2018-07",
                "ref_m_status2": pd.NA,
                "ref_m_stattariff1": 0.25,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "raw_panel_rule_code": "99038801",
                "overlay_increment": 0.25,
                "overlay_rule_code": "99038801",
                "discrepancy_type": "missing_raw_policy_scope",
            },
            {
                "hs10": "8424201000",
                "year": 2018,
                "month": 7,
                "ref_effective_period": "2018-07",
                "ref_m_status2": pd.NA,
                "ref_m_stattariff1": 0.37,
                "raw_base_statutory_rate_raw": 0.02,
                "raw_panel_increment": 0.25,
                "raw_panel_rule_code": "99038801",
                "overlay_increment": 0.25,
                "overlay_rule_code": "99038801",
                "discrepancy_type": "statutory_rate_mismatch",
                "diagnosed_stage": "benchmark_increment_definition_difference",
            },
        ]
    ).to_csv(trace_dir / "raw_replication_china_301_benchmark_definition_trace_china_301_semantics_corrected.csv", index=False)

    pd.DataFrame(
        {
            "cty_code": [5700, 5700],
            "hs10": ["8501640021", "8424201000"],
            "year": [2018, 2018],
            "month": [7, 7],
            "ref_m_status2": [2, 2],
        }
    ).to_csv(trace_dir / "raw_replication_china_301_key_trace.csv", index=False)

    pd.DataFrame(
        {
            "release_name": ["2018HTSARevision7_1", "2018HTSARevision7_1"],
            "release_start_date": ["2018-07-06", "2018-07-06"],
            "release_end_date": ["2018-08-07", "2018-08-07"],
            "source_file": ["2018HTSARevision7_1_extracted_rows.csv", "2018HTSARevision7_1_extracted_rows.csv"],
            "source_page": [3337, 3337],
            "source_row": [14109, 14109],
            "hs8": ["85016400", "84242010"],
            "rule_code": ["99038801", "99038801"],
            "extraction_method": ["chapter99_nearby_note_fallback", "chapter99_nearby_note_fallback"],
            "rule_found_in_same_row": [True, True],
            "rule_found_only_in_context": [False, False],
            "matched_rule_text": [
                "Articles the product of China, as enumerated in U.S. note 20 to this subchapter",
                "Articles the product of China, as enumerated in U.S. note 20 to this subchapter",
            ],
        }
    ).to_csv(trace_dir / "raw_replication_china_301_wave_link_audit.csv", index=False)

    pd.DataFrame(columns=["hs8", "rule_code", "release_name"]).to_parquet(reference_dir / "tradewar_machine_links.parquet", index=False)
    pd.DataFrame(columns=["hs8", "rule_code", "release_name"]).to_parquet(reference_dir / "tradewar_pdf_links.parquet", index=False)

    result = build_china_301_rule_assignment_trace_from_artifacts(cfg)
    trace = pd.read_csv(trace_dir / "raw_replication_china_301_rule_assignment_trace.csv", dtype={"hs10": "string", "hs8": "string", "diagnosed_stage": "string"})
    candidates = pd.read_csv(trace_dir / "raw_replication_china_301_rule_assignment_candidates.csv")

    assert result["rows"] == 2
    assert trace.loc[trace["hs10"].eq("8424201000"), "ref_m_status2"].item() == 2
    assert trace.loc[trace["hs10"].eq("8501640021"), "ref_m_status2"].item() == 2
    assert not candidates.empty
    assert set(candidates.columns) >= {
        "hs8",
        "candidate_rule_code",
        "policy_family",
        "release_name",
        "release_start_date",
        "source_file",
        "source_page",
        "source_row",
        "structural_note_identifier",
        "extraction_method",
        "matched_rule_text",
        "rule_found_in_same_row",
        "rule_found_only_in_context",
        "same_structural_note_block",
        "cross_family_candidate",
        "source_priority",
    }


def test_china_301_rate_timing_stage_classifier_covers_candidate_cases() -> None:
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.01,
                    "raw_formula_day_weighted_gap": 0.0,
                }
            )
        )
        == "raw_formula_bug"
    )
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.0,
                    "raw_formula_day_weighted_gap": 0.0,
                    "raw_vs_ref_increment_gap": 0.0,
                    "raw_vs_ref_active_share_gap": 0.0,
                    "ref_rate_gap_statutory": 0.0,
                    "ref_rate_gap_day_weighted": 0.0,
                    "closest_candidate_timing": "full_month",
                    "closest_candidate_abs_gap": 0.0,
                    "ref_implied_active_share": 1.0,
                }
            )
        )
        == "benchmark_uses_full_month"
    )
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.0,
                    "raw_formula_day_weighted_gap": 0.0,
                    "raw_vs_ref_increment_gap": 0.0,
                    "raw_vs_ref_active_share_gap": 0.0,
                    "ref_rate_gap_statutory": 0.0,
                    "ref_rate_gap_day_weighted": 0.0,
                    "closest_candidate_timing": "previous_day",
                    "closest_candidate_abs_gap": 0.0,
                    "ref_implied_active_share": 0.32,
                }
            )
        )
        == "benchmark_uses_previous_day"
    )
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.0,
                    "raw_formula_day_weighted_gap": 0.0,
                    "raw_vs_ref_increment_gap": 0.02,
                    "raw_vs_ref_active_share_gap": 0.01,
                    "ref_rate_gap_statutory": 0.0,
                    "ref_rate_gap_day_weighted": 0.0,
                    "closest_candidate_timing": "next_day",
                    "closest_candidate_abs_gap": 0.0,
                    "ref_implied_active_share": 0.26,
                }
            )
        )
        == "increment_rate_mismatch"
    )
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.0,
                    "raw_formula_day_weighted_gap": 0.0,
                    "raw_vs_ref_increment_gap": 0.0,
                    "raw_vs_ref_active_share_gap": 0.0,
                    "ref_rate_gap_statutory": 0.02,
                    "ref_rate_gap_day_weighted": 0.021,
                    "closest_candidate_timing": "next_day",
                    "closest_candidate_abs_gap": 0.0,
                    "ref_implied_active_share": 0.26,
                }
            )
        )
        == "base_rate_mismatch"
    )
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.0,
                    "raw_formula_day_weighted_gap": 0.0,
                    "raw_vs_ref_increment_gap": 0.0,
                    "raw_vs_ref_active_share_gap": 0.0,
                    "ref_rate_gap_statutory": 0.0,
                    "ref_rate_gap_day_weighted": 0.0,
                    "closest_candidate_timing": "action_month_start",
                    "closest_candidate_abs_gap": 0.0,
                    "ref_implied_active_share": 0.81,
                }
            )
        )
        == "benchmark_uses_action_month_start"
    )
    assert (
        _classify_china_301_rate_timing_stage(
            pd.Series(
                {
                    "raw_formula_statutory_gap": 0.0,
                    "raw_formula_day_weighted_gap": 0.0,
                    "raw_vs_ref_increment_gap": 0.0,
                    "raw_vs_ref_active_share_gap": 0.0,
                    "ref_rate_gap_statutory": 0.0,
                    "ref_rate_gap_day_weighted": 0.0,
                    "closest_candidate_timing": "unmapped",
                    "closest_candidate_abs_gap": pd.NA,
                    "ref_implied_active_share": pd.NA,
                }
            )
        )
        == "requires_full_model_review"
    )
