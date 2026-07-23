from __future__ import annotations

from pathlib import Path
import zipfile

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from scr.data_construction.config import PipelineConfig as DataConfig
from scr.data_construction.extension_2025 import (
    POLICY_SOURCES,
    build_trade_month,
    day_weighted_rate,
    paper_event_period,
    supported_post_horizon,
)
from scr.data_construction.io_utils import write_metadata_json
from scr.pass_through.config import PipelineConfig
from scr.pass_through.extension_2025 import (
    FitSpec,
    build_dynamic_design,
    build_event_design,
    fit_grid,
    horizon_contract,
    preflight,
    rank_preflight,
)


def test_paper_clock_uses_day_fifteen_cutoff() -> None:
    assert paper_event_period("2025-02-15") == "2025-02"
    assert paper_event_period("2025-02-16") == "2025-03"


def test_day_weighted_rate_counts_exact_days_in_force() -> None:
    value = day_weighted_rate([("2025-02-04", "2025-02-28", 0.10)], "2025-02")
    assert np.isclose(value, 0.10 * 24 / 28)
    full_month = day_weighted_rate([("2025-02-04", None, 0.10)], "2025-03")
    assert np.isclose(full_month, 0.10)


def test_supported_long_horizon_is_censored_not_zero_padded() -> None:
    assert supported_post_horizon("2025-12") == 10
    assert supported_post_horizon("2027-02") == 24
    contract = horizon_contract("2025-12")
    assert contract["short"]["post"] == 6
    assert contract["long"]["actual_post"] == 10
    assert contract["right_censored"] is True


def test_fit_grid_contains_separate_short_and_long_specs() -> None:
    grid = fit_grid("2025-12")
    china_event = [row for row in grid if row.analysis == "china_hk" and row.specification == "event" and row.outcome == "val" and row.design == "bilateral"]
    assert {row.horizon_kind: row.post_horizon for row in china_event} == {"short": 6, "long": 10}
    china_dynamic = [row for row in grid if row.analysis == "china_hk" and row.specification == "dynamic"]
    assert china_dynamic
    assert {row.clock for row in china_dynamic} == {"legal_path"}
    assert {row.design for row in grid} == {"bilateral"}


def test_locked_common_feb_design_is_declared_absorbed() -> None:
    locked = FitSpec("china_hk", "event", "val", "locked", "common_feb", "short", 6, 6)
    bilateral = FitSpec("china_hk", "event", "val", "bilateral", "common_feb", "short", 6, 6)
    assert rank_preflight(locked)["identified"] is False
    assert rank_preflight(bilateral)["identified"] is True


def _panel() -> pd.DataFrame:
    rows = []
    for partner in ("5700", "1000"):
        for hs10 in ("0101210000", "0201100010"):
            for period in pd.period_range("2024-07", "2026-01", freq="M"):
                rows.append({
                    "partner_code": partner,
                    "hs10": hs10,
                    "year": period.year,
                    "month": period.month,
                    "m_val": 2.0,
                    "m_q1": 1.0,
                    "m_p": 2.0,
                    "m_pduty": 2.2,
                    "statutory_total_rate": 0.1 + (0.1 if partner == "5700" and period >= pd.Period("2025-02", freq="M") else 0.0),
                })
    return pd.DataFrame(rows)


def test_short_event_window_drops_later_months_instead_of_topcoding() -> None:
    spec = FitSpec("china_hk", "event", "val", "bilateral", "common_feb", "short", 6, 6)
    prepared = build_event_design(_panel(), spec)
    assert prepared["event_time"].min() == -6
    assert prepared["event_time"].max() == 6
    assert not (prepared["event_time"] > 6).any()


def test_dynamic_uses_exact_calendar_shifts() -> None:
    spec = FitSpec("china_hk", "dynamic", "val", "bilateral", "common_feb", "short", 6, 6)
    panel = _panel()
    panel = panel.loc[~((panel["partner_code"] == "5700") & (panel["hs10"] == "0101210000") & (panel["year"] == 2024) & (panel["month"] == 12))]
    prepared = build_dynamic_design(panel, spec)
    january = prepared.loc[(prepared["partner_code"] == "5700") & (prepared["hs10"] == "0101210000") & (prepared["year"] == 2025) & (prepared["month"] == 1)]
    assert january["x"].isna().all()


def test_policy_sources_never_encode_unknown_rates_as_zero() -> None:
    blocked = [source for source in POLICY_SOURCES if source.scope_status.startswith("blocked")]
    assert blocked
    assert all(source.additional_rate is not None for source in blocked)


def test_preflight_keeps_trade_and_policy_gates_separate(tmp_path: Path) -> None:
    data = DataConfig.default(tmp_path)
    trade = data.processed_trade_dir / "extension_2025" / "trade_extension_manifest.json"
    policy = data.processed_tariff_dir / "extension_2025" / "policy_extension_manifest.json"
    write_metadata_json(trade, {"status": "passed", "end_period": "2025-12"})
    write_metadata_json(policy, {"status": "blocked_incomplete_product_ledger", "policy_gate": "failed", "event_estimation_authorized": False})
    result = preflight(PipelineConfig.default(tmp_path))
    assert result["trade_status"] == "passed"
    assert result["policy_gate"] == "failed"
    assert result["event_estimation_authorized"] is False
    assert result["horizon_contract"]["long"]["actual_post"] == 10


def _fixed_width_import_line() -> str:
    line = [" "] * 223
    values = {
        (0, 10): "0801001090",
        (10, 14): "5700",
        (22, 26): "2025",
        (26, 28): "02",
        (88, 103): f"{100:015d}",
        (103, 118): f"{120:015d}",
        (148, 163): f"{10:015d}",
        (178, 193): f"{900:015d}",
        (208, 223): f"{1000:015d}",
    }
    for (start, end), value in values.items():
        line[start:end] = value
    return "".join(line)


def test_archive_native_month_preserves_duty_and_reconciles(tmp_path: Path) -> None:
    config = DataConfig.default(tmp_path)
    archive = config.raw_dir / "trade" / "imports" / "IMDB2502.ZIP"
    archive.parent.mkdir(parents=True)
    country = "5700" + " " * 7 + "CHINA".ljust(50)
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("IMP_DETL.TXT", _fixed_width_import_line() + "\n")
        handle.writestr("COUNTRY.TXT", country + "\n")
    audit = build_trade_month(config, "2025-02")
    assert audit["reconciliation_pass"] is True
    assert audit["duplicate_keys"] == 0
    assert audit["calculated_duty_nonmissing_rows"] == 1
    output_path = tmp_path / "data" / "processed" / "trade" / "extension_2025" / "year=2025" / "month=02" / "part.parquet"
    output = pq.ParquetFile(output_path).read().to_pandas()
    assert output.loc[0, "hs10"] == "0801001090"
    assert output.loc[0, "cal_dut_mo"] == 120
    assert output.loc[0, "m_pduty"] == 112.0
    assert not any("tariff" in column or "treatment" in column for column in output.columns)
