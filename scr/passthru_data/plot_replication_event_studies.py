"""Plot the paper, package replication, common-sample, and raw-outcome curves."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json


VERSION = "replication_event_study_overlay_v1"
OUTCOMES = ("val", "q1", "p", "pduty")
OUTCOME_LABELS = {
    "val": "Import value",
    "q1": "Quantity",
    "p": "Pre-duty price",
    "pduty": "Duty-inclusive price",
}


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _bridge_fit(coefficients: pd.DataFrame, source_mode: str, spec: str, outcome: str) -> pd.DataFrame:
    fit_id = f"{source_mode}|{spec}|{outcome}"
    frame = coefficients.loc[coefficients["fit_id"].eq(fit_id)].copy()
    frame["plot_horizon"] = frame["event_time"] if spec == "event" else frame["horizon"]
    return frame.sort_values("plot_horizon")


def _plot_spec(
    comparison: pd.DataFrame,
    bridge: pd.DataFrame,
    spec: str,
    destination: Path,
    *,
    realized_pduty: bool,
) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.2), sharex=True)
    for axis, outcome in zip(axes.flat, OUTCOMES):
        package_full = comparison.loc[
            comparison["spec"].eq(spec) & comparison["outcome"].eq(outcome)
        ].sort_values("horizon")
        package_common = _bridge_fit(bridge, "package_common_sample_anchor", spec, outcome)
        raw = _bridge_fit(bridge, "raw_outcomes_package_policy", spec, outcome)

        x = package_full["horizon"].astype(float).to_numpy()
        reference = package_full["reference_value"].astype(float).to_numpy()
        reference_low = package_full["reference_conf_low"].astype(float).to_numpy()
        reference_high = package_full["reference_conf_high"].astype(float).to_numpy()
        axis.fill_between(x, reference_low, reference_high, color="0.65", alpha=0.20, linewidth=0)
        axis.plot(x, reference, color="black", marker="o", markersize=3.5, linewidth=1.8, label="Paper PDF reference")

        axis.plot(
            x,
            package_full["estimate"].astype(float).to_numpy(),
            color="#3165a8",
            linestyle="--",
            linewidth=1.8,
            label="Package-only replication",
        )
        axis.plot(
            package_common["plot_horizon"].astype(float).to_numpy(),
            package_common["estimate"].astype(float).to_numpy(),
            color="#2b8c6b",
            linestyle=":",
            linewidth=2.0,
            label="Package common sample",
        )
        raw_x = raw["plot_horizon"].astype(float).to_numpy()
        axis.fill_between(
            raw_x,
            raw["conf_low"].astype(float).to_numpy(),
            raw["conf_high"].astype(float).to_numpy(),
            color="#d97706",
            alpha=0.13,
            linewidth=0,
        )
        axis.plot(
            raw_x,
            raw["estimate"].astype(float).to_numpy(),
            color="#d35f00",
            marker="s",
            markersize=3.0,
            linewidth=1.8,
            label="Raw outcomes, package policy",
        )
        axis.axhline(0, color="0.25", linewidth=0.7)
        axis.axvline(0, color="0.55", linewidth=0.8, linestyle="--")
        axis.set_title(OUTCOME_LABELS[outcome])
        axis.set_xticks(range(-6, 7, 2))
        axis.grid(axis="y", color="0.9", linewidth=0.6)
        axis.set_ylabel("Coefficient (log points)")
    for axis in axes[-1, :]:
        axis.set_xlabel("Event horizon (months)")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.5, 0.945), fontsize=9)
    title = "Figure 2-style import event studies" if spec == "event" else "Figure 4a-style dynamic import responses"
    fig.suptitle(title, fontsize=14, y=0.995)
    fig.text(
        0.5,
        0.012,
        (
            "Paper reference is the frozen local vector extraction; shaded regions show paper and raw-outcome confidence intervals. "
            + ("Duty-inclusive raw price uses Census calculated duty." if realized_pduty else "")
        ),
        ha="center",
        fontsize=8.5,
        color="0.35",
    )
    fig.tight_layout(rect=(0.02, 0.045, 0.98, 0.88))
    destination.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(destination, dpi=220, bbox_inches="tight")
    fig.savefig(destination.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def plot_replication_event_studies(config: PipelineConfig) -> dict[str, Any]:
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    comparison_path = root / "package_pdf_comparison.parquet"
    bridge_path = root / "common_sample_v3" / "bridge_resumable" / "bridge_coefficients.parquet"
    if not comparison_path.exists() or not bridge_path.exists():
        raise FileNotFoundError(f"Missing package comparison or v3 bridge coefficients: {comparison_path}, {bridge_path}")
    comparison = pd.read_parquet(comparison_path)
    bridge = pd.read_parquet(bridge_path)
    pduty_fit_paths = {
        spec: root / "common_sample_v3" / "pduty_diagnosis" / "fits" / spec / "coefficients.parquet"
        for spec in ("event", "dynamic")
    }
    realized_pduty = all(path.exists() for path in pduty_fit_paths.values())
    pduty_fingerprints: dict[str, str] = {}
    if realized_pduty:
        replacements = []
        for spec, path in pduty_fit_paths.items():
            frame = pd.read_parquet(path)
            frame["fit_id"] = f"raw_outcomes_package_policy|{spec}|pduty"
            replacements.append(frame)
            pduty_fingerprints[spec] = sha256_file(path)
            bridge = bridge.loc[bridge["fit_id"] != f"raw_outcomes_package_policy|{spec}|pduty"]
        bridge = pd.concat([bridge, *replacements], ignore_index=True, sort=False)
    output = root / "figures"
    event_path = output / "figure2_event_paper_package_raw_overlay.png"
    dynamic_path = output / "figure4a_dynamic_paper_package_raw_overlay.png"
    _plot_spec(comparison, bridge, "event", event_path, realized_pduty=realized_pduty)
    _plot_spec(comparison, bridge, "dynamic", dynamic_path, realized_pduty=realized_pduty)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "package_comparison_path": _relative(config, comparison_path),
        "package_comparison_sha256": sha256_file(comparison_path),
        "bridge_coefficients_path": _relative(config, bridge_path),
        "bridge_coefficients_sha256": sha256_file(bridge_path),
        "event_figure_png": _relative(config, event_path),
        "event_figure_pdf": _relative(config, event_path.with_suffix(".pdf")),
        "dynamic_figure_png": _relative(config, dynamic_path),
        "dynamic_figure_pdf": _relative(config, dynamic_path.with_suffix(".pdf")),
        "series": [
            "paper_pdf_reference",
            "package_full_benchmark",
            "package_common_sample_anchor",
            "raw_outcomes_package_policy",
        ],
        "independent_raw_policy_included": False,
        "pduty_realized_calculated_duty_used": realized_pduty,
        "pduty_fit_paths": {
            spec: _relative(config, path) for spec, path in pduty_fit_paths.items() if path.exists()
        },
        "pduty_fit_sha256": pduty_fingerprints,
        "pduty_outcome_formula": "(trade_value + cal_dut_mo) / quantity" if realized_pduty else "legacy statutory multiplier",
        "status": "complete",
    }
    write_metadata_json(output / "replication_event_study_overlay_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(plot_replication_event_studies(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
