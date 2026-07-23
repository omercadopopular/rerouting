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
from scr.data_construction.io_utils import write_metadata_json
from scr.data_construction.extension_2025 import run as run_extension_2025_construction
from scr.pass_through.config import PipelineConfig as RegressionConfig
from scr.pass_through.extension_2025 import preflight as preflight_extension_2025
from scr.pass_through.extension_2025 import fit_grid as extension_2025_fit_grid
from scr.pass_through.extension_2025 import run_fits as run_extension_2025_fits
from scr.pass_through.extension_2025 import finalize as finalize_extension_2025
from scr.pass_through.pipeline import run as run_pass_through

VERSION = "master_pipeline_v2"


def run(
    *,
    rebuild_tariffs: bool = False,
    build_archives: bool = False,
    extended: bool = False,
    extension_2025: bool = False,
    estimate_extension_2025: bool = False,
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
        "independent_2025_policy_actions_included": False,
    }
    if extension_2025:
        construction_2025 = run_extension_2025_construction(data_config, build_trade=True, overwrite=overwrite)
        preflight_2025 = preflight_extension_2025(regression_config)
        extension_result: dict = {
            "construction": construction_2025,
            "preflight": preflight_2025,
            "estimation": {"status": "not_requested"},
        }
        if estimate_extension_2025:
            if not preflight_2025["event_estimation_authorized"]:
                extension_result["estimation"] = {
                    "status": "blocked_policy_gate",
                    "reason": "independent 2025 product/date/rate/exclusion/stacking ledger has not passed",
                }
            else:
                grid = extension_2025_fit_grid(preflight_2025["horizon_contract"]["latest_trade_period"])
                extension_result["estimation"] = run_extension_2025_fits(regression_config, grid, resume=True)
                extension_result["finalization"] = finalize_extension_2025(regression_config)
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
    parser.add_argument("--extension-2025", action="store_true", help="Build and validate the February-2025 extension inputs")
    parser.add_argument("--estimate-extension-2025", action="store_true", help="Estimate only if the independent 2025 policy gate passes")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(
        rebuild_tariffs=args.rebuild_tariffs,
        build_archives=args.build_archives,
        extended=args.extended,
        extension_2025=args.extension_2025 or args.estimate_extension_2025,
        estimate_extension_2025=args.estimate_extension_2025,
        overwrite=args.overwrite,
    ), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
