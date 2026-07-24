from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyfixest as pf

from scr.pass_through.config import PipelineConfig
from scr.pass_through.cumulative_lp_iv import (
    CumulativeLPSpec,
    EPISODES,
    fit_cumulative_lp_iv,
    grid,
    load_horizon_sample,
    panel_path,
    specification_fingerprint,
)


def test_cumulative_lp_grid_has_registered_episode_horizons() -> None:
    specs = grid()
    assert len(specs) == 37
    assert {
        spec.horizon
        for spec in specs
        if spec.episode == "trade_war_2018"
    } == set(range(25))
    assert {
        spec.horizon
        for spec in specs
        if spec.episode == "tariffs_2025"
    } == set(range(12))


def test_specification_fingerprint_records_distinct_policy_clocks() -> None:
    historical = specification_fingerprint(
        CumulativeLPSpec("trade_war_2018", 0)
    )
    extension = specification_fingerprint(
        CumulativeLPSpec("tariffs_2025", 0)
    )
    assert historical != extension
    assert EPISODES["trade_war_2018"]["statutory_clock"].startswith(
        "independent"
    )


def test_horizon_sample_uses_common_price_endpoints_and_identity(
    tmp_path: Path,
) -> None:
    config = PipelineConfig.default(tmp_path)
    destination = panel_path(config, "tariffs_2025")
    destination.parent.mkdir(parents=True)
    rows = []
    for partner_index, partner in enumerate(("1000", "2000")):
        for hs_index, hs10 in enumerate(("0101010101", "0202020202")):
            for month in range(1, 5):
                value = 100 + partner_index * 10 + hs_index + month
                quantity = 10 + hs_index
                applied = 0.01 * (partner_index + month)
                statutory = 0.008 * (partner_index + month)
                rows.append(
                    {
                        "episode": "tariffs_2025",
                        "partner_code": partner,
                        "hs10": hs10,
                        "hs8": hs10[:8],
                        "year": 2025,
                        "month": month,
                        "period": f"2025-{month:02d}",
                        "month_index": 2025 * 12 + month,
                        "import_value": float(value),
                        "quantity": float(quantity),
                        "calculated_duty": value * applied,
                        "applied_tariff": applied,
                        "pre_duty_price": value / quantity,
                        "duty_inclusive_price": (
                            value * (1 + applied) / quantity
                        ),
                        "statutory_rate": statutory,
                        "statutory_source_period": f"2025-{month:02d}",
                        "statutory_carried_forward": False,
                    }
                )
    pd.DataFrame(rows).to_parquet(
        destination,
        index=False,
        compression="zstd",
    )
    spec = CumulativeLPSpec("tariffs_2025", 1)
    sample, audit = load_horizon_sample(config, spec)
    assert not sample.empty
    assert audit["price_identity_max_abs_error"] < 1e-10
    assert (
        sample["delta_log_pduty"]
        - sample["delta_log_p"]
        - sample["delta_log_applied"]
    ).abs().max() < 1e-10


def test_direct_duty_iv_and_derived_preduty_preserve_identity() -> None:
    rng = np.random.default_rng(721)
    n = 3_000
    partner = rng.integers(0, 30, n)
    product_time = rng.integers(0, 100, n)
    statutory = rng.normal(size=n)
    applied = 0.8 * statutory + rng.normal(scale=0.4, size=n)
    preduty = -0.15 * applied + rng.normal(scale=0.8, size=n)
    sample = pd.DataFrame(
        {
            "partner_code": partner.astype(str),
            "hs8": rng.integers(0, 20, n).astype(str),
            "product_time": product_time.astype(str),
            "delta_log_statutory": statutory,
            "delta_log_applied": applied,
            "delta_log_p": preduty,
            "delta_log_pduty": preduty + applied,
        }
    )
    result = fit_cumulative_lp_iv(
        sample,
        CumulativeLPSpec("tariffs_2025", 0),
    ).set_index("outcome")
    assert result.loc["pduty", "estimate"] - result.loc["p", "estimate"] == 1
    assert (
        result.loc["pduty", "std_error"]
        == result.loc["p", "std_error"]
    )
    assert result.loc["first_stage", "first_stage_f"] > 10

    reference = pf.feols(
        (
            "delta_log_pduty ~ 1 | product_time + partner_code | "
            "delta_log_applied ~ delta_log_statutory"
        ),
        sample,
        vcov={"CRV1": "partner_code + hs8"},
        copy_data=False,
        store_data=False,
        lean=True,
    ).tidy()
    assert np.isclose(
        result.loc["pduty", "estimate"],
        reference.loc["delta_log_applied", "Estimate"],
        atol=1e-10,
    )
    assert np.isclose(
        result.loc["pduty", "std_error"],
        reference.loc["delta_log_applied", "Std. Error"],
        rtol=0.02,
    )
