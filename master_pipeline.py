"""Top-level pipeline for the locked historical replication.

The two stages are intentionally separate:

1. ``scr.data_construction`` creates raw-trade outcomes and independently
   reconstructed tariff panels.
2. ``scr.pass_through`` estimates and plots the locked pass-through design.

Large empirical outputs remain local under ``data/processed``.  Portable
manifests, source documentation, code, and publication figures are versioned.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from scr.data_construction.config import PipelineConfig as DataConfig
from scr.data_construction.pipeline import run as run_data_construction
from scr.data_construction.extension_2025 import (
    build_event_horizon_extension,
    run as run_extension_2025_construction,
)
from scr.data_construction.io_utils import write_metadata_json
from scr.pass_through.config import PipelineConfig as RegressionConfig
from scr.pass_through.extension_2025 import (
    finalize_extended_event,
    finalize as finalize_extension_2025,
)
from scr.pass_through.extension_2025 import (
    fit_grid as extension_2025_fit_grid,
)
from scr.pass_through.extension_2025 import (
    preflight as preflight_extension_2025,
)
from scr.pass_through.extension_2025 import (
    run_fits as run_extension_2025_fits,
    run_extended_event_fits,
)
from scr.pass_through.cumulative_lp_iv import (
    build_source_panels as build_cumulative_lp_panels,
    finalize as finalize_cumulative_lp,
    run_fits as run_cumulative_lp_fits,
)
from scr.pass_through.extended import plot_dynamic_h12
from scr.pass_through.pipeline import run as run_pass_through

VERSION = "master_pipeline_v3"


def run(
    *,
    rebuild_tariffs: bool = False,
    build_archives: bool = False,
    extended: bool = False,
    extension_2025: bool = False,
    estimate_extension_2025: bool = False,
    extend_event_horizons: bool = False,
    cumulative_pass_through: bool = False,
    overwrite: bool = False,
) -> dict:
    data_config = DataConfig.default()
    regression_config = RegressionConfig.default()
    construction = run_data_construction(
        data_config,
        rebuild_tariffs=rebuild_tariffs,
        build_archives=build_archives,
        overwrite=overwrite,
    )
    replication = run_pass_through(
        regression_config,
        use_validated_checkpoints=True,
        run_extended=extended,
        overwrite=overwrite,
    )
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "construction": construction,
        "replication": replication,
        "locked_historical_window": [-6, 6],
        "requested_long_horizon": [-6, 24] if extended else None,
        "policy_scope": ["MFN", "Section 201", "Section 232", "Section 301"],
        "extension_2025_gates": {
            "applied_tariff_event_ready": False,
            "statutory_instrument_constructed": False,
            "quarterly_iv_execution_ready": False,
            "registered_result_gate": "not_run",
        },
    }
    if extension_2025:
        construction_2025 = run_extension_2025_construction(data_config, build_trade=True, overwrite=overwrite)
        preflight_2025 = preflight_extension_2025(regression_config)
        extension_result: dict = {
            "construction": construction_2025,
            "preflight": preflight_2025,
            "estimation": {"status": "not_requested"},
        }
        manifest["extension_2025_gates"] = {
            "applied_tariff_event_ready": bool(
                preflight_2025["applied_tariff_event_ready"]
            ),
            "statutory_instrument_constructed": (
                preflight_2025["statutory_instrument_status"]
                == "passed_with_documented_statutory_ambiguities"
            ),
            "quarterly_iv_execution_ready": bool(
                preflight_2025["quarterly_iv_ready"]
            ),
            "registered_result_gate": "not_run",
        }
        if estimate_extension_2025:
            if not preflight_2025["applied_tariff_event_ready"]:
                extension_result["estimation"] = {
                    "status": "blocked_trade_gate",
                    "reason": "the FK-2025 consumption/applied-tariff panel has not passed",
                }
            else:
                grid = extension_2025_fit_grid()
                extension_result["estimation"] = run_extension_2025_fits(regression_config, grid, resume=True)
                if preflight_2025["quarterly_iv_ready"]:
                    extension_result["finalization"] = finalize_extension_2025(regression_config)
                    manifest["extension_2025_gates"][
                        "registered_result_gate"
                    ] = extension_result["finalization"][
                        "quarterly_iv_paper_gate"
                    ]
                else:
                    extension_result["finalization"] = {
                        "status": "blocked_statutory_instrument",
                        "reason": "event curves may run, but the quarterly IV requires a validated statutory instrument",
                    }
        if extend_event_horizons:
            extension_result["horizon_extension_construction"] = (
                build_event_horizon_extension(
                    data_config,
                    overwrite=overwrite,
                )
            )
            extension_result["horizon_extension_estimation"] = (
                run_extended_event_fits(
                    regression_config,
                    resume=not overwrite,
                )
            )
            extension_result["horizon_extension_finalization"] = (
                finalize_extended_event(regression_config)
            )
            extension_result["historical_dynamic_h12_figure"] = (
                plot_dynamic_h12(regression_config)
            )
        if cumulative_pass_through:
            extension_result["cumulative_pass_through_panels"] = (
                build_cumulative_lp_panels(
                    regression_config,
                    overwrite=overwrite,
                )
            )
            extension_result["cumulative_pass_through_fits"] = (
                run_cumulative_lp_fits(
                    regression_config,
                    resume=not overwrite,
                )
            )
            extension_result["cumulative_pass_through_finalization"] = (
                finalize_cumulative_lp(regression_config)
            )
        manifest["extension_2025"] = extension_result
    else:
        manifest["extension_2025"] = {"status": "not_requested", "command": "--extension-2025"}
    write_metadata_json(data_config.repo_root / "data" / "processed" / "master_pipeline_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-tariffs", action="store_true", help="Reconstruct tariffs from locally stored official sources")
    parser.add_argument("--build-archives", action="store_true", help="Reparse archive-native Census imports")
    parser.add_argument("--extended", action="store_true", help="Estimate the separately identified -6 to +24 specifications")
    parser.add_argument("--extension-2025", action="store_true", help="Build the FK-2025 consumption, applied-tariff, and statutory-IV inputs")
    parser.add_argument("--estimate-extension-2025", action="store_true", help="Estimate the FK-2025 local projections and quarterly IV")
    parser.add_argument(
        "--extend-event-horizons",
        action="store_true",
        help=(
            "Extend the FK event comparison to requested +24/+12 "
            "horizons and Appendix Figure 2 to +12"
        ),
    )
    parser.add_argument(
        "--cumulative-pass-through",
        action="store_true",
        help=(
            "Estimate monthly cumulative local-projection IV "
            "pass-through for the 2018 and 2025 episodes"
        ),
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(
        rebuild_tariffs=args.rebuild_tariffs,
        build_archives=args.build_archives,
        extended=args.extended,
        extension_2025=(
            args.extension_2025
            or args.estimate_extension_2025
            or args.extend_event_horizons
            or args.cumulative_pass_through
        ),
        estimate_extension_2025=args.estimate_extension_2025,
        extend_event_horizons=args.extend_event_horizons,
        cumulative_pass_through=args.cumulative_pass_through,
        overwrite=args.overwrite,
    ), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
