from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import zipfile

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from scr.data_construction.config import PipelineConfig as DataConfig
from scr.data_construction.extension_2025 import (
    DETAIL_COLSPECS,
    DETAIL_NAMES,
    build_trade_month,
)
from scr.pass_through.extension_2025 import (
    EPISODES,
    EventCurveSpec,
    ExtendedEventCurveSpec,
    _base_horizon_is_promotable,
    _common_iv_sample,
    _hash_payload,
    _horizon_paths,
    _valid_horizon,
    _write_horizon_checkpoint,
    build_local_projection_sample,
    build_quarterly_panel,
    event_grid,
    event_estimator_fingerprint,
    extended_event_grid,
    fit_quarterly_iv,
    load_quarterly_source,
    prepare_event_panel,
    prepare_extended_event_panel,
)


def _put(line: list[str], start: int, end: int, value: str | int | None) -> None:
    if value is None:
        return
    text = str(value)
    if isinstance(value, int):
        text = f"{value:0{end-start}d}"
    line[start:end] = text.ljust(end - start)[: end - start]


def _fixed_width_import_line(
    *,
    rate_prov: str = "69",
    con_value: int = 1_000,
    con_quantity: int = 10,
    calculated_duty: int | None = 120,
    general_value: int = 9_000,
) -> str:
    line = [" "] * 223
    fields = {
        (0, 10): "0801001090",
        (10, 14): "5700",
        (14, 16): "00",
        (16, 18): "01",
        (18, 20): "01",
        (20, 22): rate_prov,
        (22, 26): "2025",
        (26, 28): "02",
        (43, 58): con_quantity,
        (58, 73): 0,
        (73, 88): con_value,
        (88, 103): con_value,
        (103, 118): calculated_duty,
        (118, 133): 20,
        (133, 148): con_value + 20,
        (148, 163): con_quantity * 10,
        (163, 178): 0,
        (178, 193): general_value,
        (193, 208): 200,
        (208, 223): general_value + 200,
    }
    for (start, end), value in fields.items():
        _put(line, start, end, value)
    return "".join(line)


def _archive(config: DataConfig, lines: list[str]) -> Path:
    path = config.raw_dir / "trade" / "imports" / "IMDB2502.ZIP"
    path.parent.mkdir(parents=True)
    country = "5700" + " " * 7 + "CHINA".ljust(50)
    with zipfile.ZipFile(path, "w") as handle:
        handle.writestr("IMP_DETL.TXT", "\n".join(lines) + "\n")
        handle.writestr("COUNTRY.TXT", country + "\n")
    return path


def test_census_layout_retains_rate_provision_and_consumption_fields() -> None:
    mapping = dict(zip(DETAIL_NAMES, DETAIL_COLSPECS))
    assert mapping["rate_prov"] == (20, 22)
    assert mapping["con_qy1_mo"] == (43, 58)
    assert mapping["con_val_mo"] == (73, 88)
    assert mapping["cal_dut_mo"] == (103, 118)
    assert mapping["gen_val_mo"] == (178, 193)


def test_archive_build_uses_consumption_not_general_imports(tmp_path: Path) -> None:
    config = DataConfig.default(tmp_path)
    _archive(config, [_fixed_width_import_line()])
    audit = build_trade_month(config, "2025-02")
    assert audit["reconciliation_pass"] is True
    path = (
        config.processed_trade_dir
        / "fk2025"
        / "variety_month"
        / "year=2025"
        / "month=02"
        / "part.parquet"
    )
    frame = pq.ParquetFile(path).read().to_pandas()
    row = frame.iloc[0]
    assert row["con_val_mo"] == 1_000
    assert row["gen_val_mo"] == 9_000
    assert row["import_value"] == 1_000
    assert row["applied_tariff"] == 0.12
    assert row["before_tariff_unit_value"] == 100
    assert row["duty_inclusive_unit_value"] == 112
    assert row["rate_provision_count"] == 1
    metadata = pq.ParquetFile(path).metadata
    assert {
        metadata.row_group(0).column(i).compression
        for i in range(metadata.row_group(0).num_columns)
    } == {"ZSTD"}


def test_rate_provision79_missing_duty_is_flagged_not_imputed(tmp_path: Path) -> None:
    config = DataConfig.default(tmp_path)
    _archive(
        config,
        [
            _fixed_width_import_line(rate_prov="69", con_value=900, calculated_duty=90),
            _fixed_width_import_line(rate_prov="79", con_value=100, calculated_duty=None),
        ],
    )
    build_trade_month(config, "2025-02")
    path = (
        config.processed_trade_dir
        / "fk2025"
        / "variety_month"
        / "year=2025"
        / "month=02"
        / "part.parquet"
    )
    row = pd.read_parquet(path).iloc[0]
    assert row["cal_dut_mo"] == 90
    assert row["applied_tariff"] == 0.09
    assert row["rate_provision79_value_share"] == 0.10
    assert bool(row["duty_measure_incomplete"])
    assert row["duty_missing_rate_rows"] == 1


def test_trade_month_can_write_separate_horizon_namespace(
    tmp_path: Path,
) -> None:
    config = DataConfig.default(tmp_path)
    _archive(config, [_fixed_width_import_line()])
    audit = build_trade_month(
        config,
        "2025-02",
        output_namespace="fk2025_event_horizon_extension",
    )
    assert audit["output_namespace"] == "fk2025_event_horizon_extension"
    assert (
        config.processed_trade_dir
        / "fk2025_event_horizon_extension"
        / "variety_month"
        / "year=2025"
        / "month=02"
        / "part.parquet"
    ).exists()


def _event_panel() -> pd.DataFrame:
    rows = []
    for partner, treated_at in (("1000", None), ("2000", "2025-03"), ("3000", "2025-05")):
        for period in pd.period_range("2024-01", "2025-12", freq="M"):
            tariff = 0.01
            if treated_at is not None and period >= pd.Period(treated_at, freq="M"):
                tariff = 0.06
            rows.append({
                "partner_code": partner,
                "hs10": "0101210000",
                "hs8": "01012100",
                "year": period.year,
                "month": period.month,
                "period": str(period),
                "applied_tariff": tariff,
                "import_value": 100.0,
                "quantity": 10.0,
                "before_tariff_unit_value": 10.0,
                "duty_inclusive_unit_value": 10.0 * (1 + tariff),
                "duty_measure_incomplete": False,
                "rate_provision79_value_share": 0.0,
            })
    return pd.DataFrame(rows)


def test_treatment_is_first_two_point_crossing_and_absorbing() -> None:
    spec = EventCurveSpec("tariffs_2025", "all", "tariff")
    prepared = prepare_event_panel(_event_panel(), spec)
    treated = prepared.loc[prepared["partner_code"].eq("2000")]
    assert treated["newly_treated"].sum() == 1
    assert treated.loc[treated["newly_treated"], "period"].iloc[0] == "2025-03"
    never = prepared.loc[prepared["partner_code"].eq("1000")]
    assert never["first_treatment_index"].isna().all()


def test_local_projection_uses_never_and_not_yet_controls() -> None:
    spec = EventCurveSpec("tariffs_2025", "all", "value")
    prepared = prepare_event_panel(_event_panel(), spec)
    sample = build_local_projection_sample(prepared, 1)
    march = 2025 * 12 + 3 - 1
    march_rows = sample.loc[sample["base_index"].eq(march)]
    # The March cohort is treated; the never-treated and May cohort are valid
    # controls because the May cohort is still untreated at t+1.
    assert set(march_rows["partner_code"]) == {"1000", "2000", "3000"}
    assert march_rows["delta_treatment"].sum() == 1


def test_pretrend_controls_are_untreated_at_event_base() -> None:
    spec = EventCurveSpec("tariffs_2025", "all", "value")
    prepared = prepare_event_panel(_event_panel(), spec)
    sample = build_local_projection_sample(prepared, -2)
    may = 2025 * 12 + 5 - 1
    # The reference LP-DiD pretrend loop uses `treat==0` at base t. A cohort
    # treated in March is therefore not a clean control for a May base even
    # though the outcome horizon is March.
    may_rows = sample.loc[sample["base_index"].eq(may)]
    assert "2000" not in set(may_rows["partner_code"])


def test_local_projection_minus_one_is_normalized_baseline() -> None:
    spec = EventCurveSpec("tariffs_2025", "all", "value")
    prepared = prepare_event_panel(_event_panel(), spec)
    sample = build_local_projection_sample(prepared, -1)
    assert not sample.empty
    assert sample["delta_outcome"].eq(0.0).all()


def test_china_curve_keeps_full_control_pool() -> None:
    spec = EventCurveSpec("tariffs_2025", "china", "value")
    source = _event_panel()
    source.loc[source["partner_code"].eq("2000"), "partner_code"] = "5700"
    prepared = prepare_event_panel(source, spec)
    assert prepared["partner_code"].nunique() > 1
    treated = prepared.loc[prepared["newly_treated"]]
    assert not treated.empty
    assert treated["partner_code"].eq("5700").all()


def test_annual_horizon_is_year_over_year_quarterly_change() -> None:
    source = _quarterly_source()
    annual = build_quarterly_panel(source, cutoff="2025-11", frequency="annual")
    assert set(annual["time"].str[-2:].unique()) <= {"Q1", "Q2", "Q3", "Q4"}
    first_variety = annual.loc[annual["variety_id"].eq(annual["variety_id"].iloc[0])]
    assert first_variety.sort_values("time_index")["d_log_applied"].iloc[:4].isna().all()


def test_event_grid_matches_figure4_scope() -> None:
    grid = event_grid()
    assert len(grid) == 16
    assert sum(len(spec.horizons) for spec in grid) == 232
    assert set(EventCurveSpec("trade_war_2018", "all", "value").horizons) == set(range(-6, 13))
    assert set(EventCurveSpec("tariffs_2025", "all", "value").horizons) == set(range(-6, 7))


def test_extended_event_grid_registers_requested_horizons() -> None:
    grid = extended_event_grid()
    assert len(grid) == 8
    by_episode = {
        spec.episode: set(spec.requested_horizons)
        for spec in grid
    }
    assert by_episode["trade_war_2018"] == set(range(-6, 25))
    assert by_episode["tariffs_2025"] == set(range(-6, 13))


def test_only_identical_2025_paper_window_horizons_are_promotable() -> None:
    extended_2025 = ExtendedEventCurveSpec("tariffs_2025", "value")
    assert _base_horizon_is_promotable(extended_2025, -6)
    assert _base_horizon_is_promotable(extended_2025, 6)
    assert not _base_horizon_is_promotable(extended_2025, 7)
    assert not _base_horizon_is_promotable(
        ExtendedEventCurveSpec("trade_war_2018", "value"),
        6,
    )


def test_extended_2018_cohorts_exclude_later_tariff_episodes() -> None:
    rows = []
    for partner, treated_at in (
        ("1000", None),
        ("2000", "2018-03"),
        ("3000", "2020-03"),
    ):
        for period in pd.period_range("2017-01", "2021-12", freq="M"):
            tariff = 0.01
            if (
                treated_at is not None
                and period >= pd.Period(treated_at, freq="M")
            ):
                tariff = 0.06
            rows.append(
                {
                    "partner_code": partner,
                    "hs10": "0101210000",
                    "hs8": "01012100",
                    "year": period.year,
                    "month": period.month,
                    "period": str(period),
                    "applied_tariff": tariff,
                    "import_value": 100.0,
                    "quantity": 10.0,
                    "before_tariff_unit_value": 10.0,
                    "duty_inclusive_unit_value": 10.0 * (1 + tariff),
                    "duty_measure_incomplete": False,
                    "rate_provision79_value_share": 0.0,
                }
            )
    spec = ExtendedEventCurveSpec(
        "trade_war_2018",
        "value",
    )
    prepared = prepare_extended_event_panel(pd.DataFrame(rows), spec)
    first = (
        prepared.loc[prepared["newly_treated"]]
        .set_index("partner_code")["period"]
        .to_dict()
    )
    assert first == {"2000": "2018-03"}


def test_extended_2025_request_is_right_censored_without_2026_data() -> None:
    rows = []
    for partner, treated in (("1000", False), ("2000", True)):
        for period in pd.period_range("2024-01", "2025-12", freq="M"):
            tariff = (
                0.06
                if treated and period >= pd.Period("2025-01", freq="M")
                else 0.01
            )
            rows.append(
                {
                    "partner_code": partner,
                    "hs10": "0101210000",
                    "hs8": "01012100",
                    "year": period.year,
                    "month": period.month,
                    "period": str(period),
                    "applied_tariff": tariff,
                    "import_value": 100.0,
                    "quantity": 10.0,
                    "before_tariff_unit_value": 10.0,
                    "duty_inclusive_unit_value": 10.0 * (1 + tariff),
                    "duty_measure_incomplete": False,
                    "rate_provision79_value_share": 0.0,
                }
            )
    spec = ExtendedEventCurveSpec("tariffs_2025", "value")
    prepared = prepare_extended_event_panel(pd.DataFrame(rows), spec)
    at_11 = build_local_projection_sample(prepared, 11)
    at_12 = build_local_projection_sample(prepared, 12)
    assert int(at_11["delta_treatment"].sum()) == 1
    assert int(at_12["delta_treatment"].sum()) == 0


def test_event_horizon_checkpoint_is_independently_resumable(tmp_path: Path) -> None:
    config = DataConfig.default(tmp_path)
    spec = EventCurveSpec("tariffs_2025", "all", "value")
    row = {
        "horizon": 0,
        "estimate": -1.0,
        "std_error": 0.2,
        "conf_low": -1.4,
        "conf_high": -0.6,
        "nobs": 120,
        "treated_rows": 20,
        "control_rows": 100,
        "products": 8,
        "origins": 5,
    }
    _write_horizon_checkpoint(config, spec, 0, "source-hash", row)
    valid, reason = _valid_horizon(config, spec, 0, "source-hash")
    assert (valid, reason) == (True, "valid")
    coefficient, manifest = _horizon_paths(config, spec, 0)
    assert coefficient.exists()
    assert manifest.exists()
    stale, stale_reason = _valid_horizon(config, spec, 0, "other-source")
    assert stale is False
    assert stale_reason == "mismatch:source_hash"


def test_event_horizon_checkpoint_rejects_zero_nonbaseline_se(
    tmp_path: Path,
) -> None:
    config = DataConfig.default(tmp_path)
    spec = EventCurveSpec("tariffs_2025", "all", "value")
    row = {
        "horizon": 0,
        "estimate": -1.0,
        "std_error": 0.0,
        "conf_low": -1.0,
        "conf_high": -1.0,
        "nobs": 120,
        "treated_rows": 20,
        "control_rows": 100,
        "products": 8,
        "origins": 5,
    }
    coefficient, manifest = _horizon_paths(config, spec, 0)
    coefficient.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_parquet(coefficient, compression="zstd", index=False)
    manifest.write_text(
        """{
          "fit_id": "event|tariffs_2025|all|value",
          "horizon": 0,
          "source_hash": "source-hash",
          "estimator_fingerprint": "invalid-on-purpose",
          "specification_fingerprint": "invalid-on-purpose"
        }""",
        encoding="utf-8",
    )
    # First exercise the scientific row validator without relying on a valid
    # manifest. The manifest mismatch must be resolved before row validation.
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["estimator_fingerprint"] = event_estimator_fingerprint()
    payload["specification_fingerprint"] = _hash_payload(asdict(spec))
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    valid, reason = _valid_horizon(config, spec, 0, "source-hash")
    assert valid is False
    assert reason == "nonpositive_nonbaseline_standard_error"


def _quarterly_source() -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(42)
    for partner_number in range(1, 13):
        partner = f"{partner_number:04d}"
        for product_number in range(1, 7):
            hs10 = f"{product_number:08d}00"
            statutory = 0.01
            for period in pd.period_range("2024-01", "2025-11", freq="M"):
                if period >= pd.Period("2025-02", freq="M"):
                    statutory = 0.03 + 0.003 * partner_number + 0.001 * product_number
                applied = 0.42 * statutory
                quantity = 100 + rng.normal(0, 2)
                price = 10 * np.exp(-0.10 * np.log1p(applied))
                value = quantity * price
                rows.append({
                    "partner_code": partner,
                    "hs10": hs10,
                    "hs8": hs10[:8],
                    "period": str(period),
                    "con_val_mo": value,
                    "con_qy1_mo": quantity,
                    "cal_dut_mo": value * applied,
                    "statutory_value_numerator": value * statutory,
                    "statutory_value_denominator": value,
                })
    return pd.DataFrame(rows)


def test_quarterly_panel_reconstructs_rates_and_adjacent_differences() -> None:
    panel = build_quarterly_panel(_quarterly_source(), cutoff="2025-11")
    assert panel["applied_tariff"].notna().all()
    assert panel["statutory_tariff"].notna().all()
    assert panel["d_log_applied"].notna().any()
    assert panel["d_log_pduty"].notna().any()


def test_quarterly_source_modes_select_distinct_statutory_instruments(
    tmp_path: Path,
) -> None:
    config = DataConfig.default(tmp_path)
    path = (
        config.processed_trade_dir
        / "fk2025"
        / "workhorse_2025.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "partner_code": "5700",
                "hs10": "0101210000",
                "period": "2025-04",
                "con_val_mo": 100.0,
                "con_qy1_mo": 10.0,
                "cal_dut_mo": 20.0,
                "statutory_paper_coverage_rate": 0.25,
                "statutory_deterministic_rate": 0.10,
                "dynamic_scope_eligible": True,
            },
            {
                "partner_code": "2010",
                "hs10": "8703230000",
                "period": "2025-04",
                "con_val_mo": 200.0,
                "con_qy1_mo": 2.0,
                "cal_dut_mo": 10.0,
                "statutory_paper_coverage_rate": 0.15,
                "statutory_deterministic_rate": None,
                "dynamic_scope_eligible": False,
            },
        ]
    ).to_parquet(path, compression="zstd", index=False)

    paper = load_quarterly_source(config, instrument_scope="paper_coverage")
    strict = load_quarterly_source(config, instrument_scope="deterministic")
    assert len(paper) == 2
    assert len(strict) == 1
    assert paper["statutory_value_numerator"].sum() == 55.0
    assert strict["statutory_value_numerator"].sum() == 10.0


def test_common_iv_sample_removes_nonfinite_values() -> None:
    columns = [
        "d_log_applied",
        "d_log_statutory",
        "d_log_import_value",
        "d_log_quantity",
        "d_log_p",
        "d_log_pduty",
    ]
    frame = pd.DataFrame(
        [
            dict.fromkeys(columns, 1.0),
            {**dict.fromkeys(columns, 1.0), "d_log_p": np.inf},
            {**dict.fromkeys(columns, 1.0), "d_log_pduty": -np.inf},
        ]
    )
    clean = _common_iv_sample(frame)
    assert len(clean) == 1
    assert np.isfinite(clean[columns].to_numpy()).all()


def test_quarterly_iv_preserves_price_identity() -> None:
    panel = build_quarterly_panel(_quarterly_source(), cutoff="2025-11")
    result = fit_quarterly_iv(panel)
    p = result.loc[result["outcome"].eq("p"), "estimate"].iloc[0]
    pduty = result.loc[result["outcome"].eq("pduty"), "estimate"].iloc[0]
    assert np.isclose(pduty - p, 1.0, atol=1e-7)
    assert result["nobs"].nunique() == 1


def test_no_superseded_february_event_contract_remains() -> None:
    assert "EVENT_PERIOD" not in globals()
    assert EPISODES["tariffs_2025"]["baseline_start"] == "2024-01"
