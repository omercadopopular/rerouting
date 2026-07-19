import json
from pathlib import Path

import pandas as pd

from scr.passthru_data.config import PipelineConfig
from scr.passthru_data.plot_replication_event_studies import (
    OUTCOMES,
    _bridge_fit,
    plot_replication_event_studies,
)


def _config(tmp_path: Path) -> PipelineConfig:
    data_root = tmp_path / "data"
    verification = data_root / "verification" / "passthru_data"
    config = PipelineConfig(
        repo_root=tmp_path,
        raw_dir=data_root / "raw" / "passthru_data",
        staging_dir=data_root / "staging" / "passthru_data",
        reference_dir=data_root / "reference" / "passthru_data",
        analysis_dir=data_root / "analysis" / "passthru_data",
        verification_dir=verification,
        fajgelbaum_root=data_root / "fajgelbaum",
        fajgelbaum_analysis_dir=data_root / "fajgelbaum" / "data" / "analysis",
        manual_input_dir=data_root / "raw" / "passthru_data" / "manual",
        logs_dir=verification / "logs",
    )
    config.ensure_directories()
    return config


def test_bridge_fit_uses_exact_fit_id() -> None:
    coefficients = pd.DataFrame(
        {
            "fit_id": [
                "raw_outcomes_package_policy|event|p",
                "raw_outcomes_package_policy|event|pduty",
            ],
            "event_time": [-6, -6],
            "horizon": [-6, -6],
            "estimate": [1.0, 2.0],
        }
    )
    selected = _bridge_fit(coefficients, "raw_outcomes_package_policy", "event", "p")
    assert selected["estimate"].tolist() == [1.0]


def test_plotter_materializes_both_specs_and_discloses_policy_source(tmp_path: Path) -> None:
    config = _config(tmp_path)
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    bridge_root = root / "common_sample_v3" / "bridge_resumable"
    bridge_root.mkdir(parents=True, exist_ok=True)

    comparison_rows = []
    bridge_rows = []
    for spec in ("event", "dynamic"):
        for outcome in OUTCOMES:
            for horizon in range(-6, 7):
                comparison_rows.append(
                    {
                        "spec": spec,
                        "outcome": outcome,
                        "horizon": horizon,
                        "reference_value": float(horizon),
                        "reference_conf_low": float(horizon - 1),
                        "reference_conf_high": float(horizon + 1),
                        "estimate": float(horizon) + 0.1,
                    }
                )
                for source_mode, offset in (
                    ("package_common_sample_anchor", 0.2),
                    ("raw_outcomes_package_policy", 0.3),
                ):
                    bridge_rows.append(
                        {
                            "fit_id": f"{source_mode}|{spec}|{outcome}",
                            "event_time": horizon,
                            "horizon": horizon,
                            "estimate": float(horizon) + offset,
                            "conf_low": float(horizon - 1) + offset,
                            "conf_high": float(horizon + 1) + offset,
                        }
                    )

    pd.DataFrame(comparison_rows).to_parquet(root / "package_pdf_comparison.parquet", index=False)
    bridge_frame = pd.DataFrame(bridge_rows)
    bridge_frame.to_parquet(bridge_root / "bridge_coefficients.parquet", index=False)
    for spec in ("event", "dynamic"):
        corrected = bridge_frame.loc[
            bridge_frame["fit_id"].eq(f"raw_outcomes_package_policy|{spec}|pduty")
        ].copy()
        corrected_dir = root / "common_sample_v3" / "pduty_diagnosis" / "fits" / spec
        corrected_dir.mkdir(parents=True, exist_ok=True)
        corrected.to_parquet(corrected_dir / "coefficients.parquet", index=False)

    result = plot_replication_event_studies(config)

    assert result["status"] == "complete"
    assert result["independent_raw_policy_included"] is False
    assert result["pduty_realized_calculated_duty_used"] is True
    for name in (
        "figure2_event_paper_package_raw_overlay.png",
        "figure2_event_paper_package_raw_overlay.pdf",
        "figure4a_dynamic_paper_package_raw_overlay.png",
        "figure4a_dynamic_paper_package_raw_overlay.pdf",
    ):
        assert (root / "figures" / name).is_file()
    manifest = json.loads((root / "figures" / "replication_event_study_overlay_manifest.json").read_text())
    assert manifest["series"][-1] == "raw_outcomes_package_policy"
    assert manifest["independent_raw_policy_included"] is False
    assert manifest["pduty_outcome_formula"] == "(gen_cif_mo + cal_dut_mo) / gen_qy1_mo"
