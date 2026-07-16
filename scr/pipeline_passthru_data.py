"""Top-level CLI orchestrator for the passthrough rebuild pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import sys

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from passthru_data.build_cpi_hs6x import run_cpi_hs6x_build
from passthru_data.build_hts_monthly_schedule import run_hts_monthly_schedule_build
from passthru_data.build_hs10_codes import run_hs10_code_build
from passthru_data.build_hs6_bec import run_hs6_bec_build
from passthru_data.build_us_products_partner_panel import run_raw_tradewar_overlay_build, run_us_products_partner_panel_build
from passthru_data.download_policy_sources import build_policy_inventory, run_policy_source_download, run_policy_update_download
from passthru_data.build_trade_panels import run_trade_panel_build
from passthru_data.build_imports_with_package_shocks import run_build_imports_with_package_shocks
from passthru_data.section301 import build_section301_import_panel
from passthru_data.rtp_long_horizon import build_frozen_long_horizon_panel
from passthru_data.rtp_long_horizon import build_2025_ieepa_event_panel
from passthru_data.raw_replication_validation import (
    build_china_301_trace_from_artifacts,
    run_raw_replication_validation,
    run_raw_replication_validation_china_current,
    run_raw_replication_validation_china_semantics_corrected,
)
from passthru_data.build_rerouting_controls import run_build_rerouting_controls
from passthru_data.build_trade_workhorse_panels import run_trade_workhorse_panel_build
from passthru_data.config import PipelineConfig, selected_steps, validate_only_step_inputs
from passthru_data.download_concordances import run_concordance_download
from passthru_data.download_cpi import build_cpi_inventory, run_cpi_download
from passthru_data.download_trade import build_trade_inventory, run_trade_download
from passthru_data.logging_utils import setup_logging
from passthru_data.trade_regression_sources import run_trade_regression_source_audit
from passthru_data.verify_data import run_verification
from passthru_data.io_utils import write_metadata_json


def run_rerouting_regressions(config: PipelineConfig):
    """Load optional estimation dependencies only when the regression step is selected."""
    from passthru_data.run_rerouting_regressions import run_rerouting_regressions as runner

    return runner(config)


def run_trade_regressions(config: PipelineConfig):
    """Load optional estimation dependencies only when the regression step is selected."""
    from passthru_data.trade_regressions import run_trade_regressions as runner

    return runner(config)


def run_package_full_benchmark(config: PipelineConfig):
    """Run the package-only import benchmark without raw-key joins."""
    from passthru_data.package_benchmark import run_package_benchmark as runner

    return runner(config)


def run_package_common_sample_benchmark(config: PipelineConfig):
    """Run the package/raw common-sample anchor."""
    from passthru_data.package_benchmark import run_package_common_sample_benchmark as runner

    return runner(config)


def plot_trade_regressions(config: PipelineConfig):
    """Load optional plotting and estimation dependencies only when selected."""
    from passthru_data.trade_regressions import plot_trade_regressions as runner

    return runner(config)


def run_rtp_long_horizon_2018_event(config: PipelineConfig):
    """Load fixed-effect estimation dependencies only for the long-horizon run."""
    from passthru_data.rtp_long_horizon import run_long_horizon_2018_event as runner

    return runner(config)


def run_section301_regression_sensitivity_step(config: PipelineConfig):
    """Load regression dependencies only when the Section 301 sensitivity step is selected."""
    from passthru_data.section301_regression_sensitivity_v5 import run_section301_regression_sensitivity as runner

    return runner(config)

STEP_RUNNERS = {
    "download_trade": run_trade_download,
    "download_cpi": run_cpi_download,
    "download_concordances": run_concordance_download,
    "download_policy_sources": run_policy_source_download,
    "download_policy_updates": run_policy_update_download,
    "build_hs10_codes": run_hs10_code_build,
    "build_hs6_bec": run_hs6_bec_build,
    "build_cpi_hs6x": run_cpi_hs6x_build,
    "build_trade_panels": run_trade_panel_build,
    "build_imports_with_package_shocks": run_build_imports_with_package_shocks,
    "build_section301_import_panel": build_section301_import_panel,
    "build_rtp_long_horizon_panel": build_frozen_long_horizon_panel,
    "run_rtp_long_horizon_2018_event": run_rtp_long_horizon_2018_event,
    "build_rtp_2025_ieepa_event_panel": build_2025_ieepa_event_panel,
    "validate_raw_replication_imports": run_raw_replication_validation,
    "validate_raw_replication_imports_china_current": run_raw_replication_validation_china_current,
    "validate_raw_replication_imports_china_semantics_corrected": run_raw_replication_validation_china_semantics_corrected,
    "build_china_301_trace": build_china_301_trace_from_artifacts,
    "build_rerouting_controls": run_build_rerouting_controls,
    "run_rerouting_regressions": run_rerouting_regressions,
    "build_hts_monthly_schedule": run_hts_monthly_schedule_build,
    "build_tradewar_overlay_raw": run_raw_tradewar_overlay_build,
    "build_us_products_partner_hs10_panel": run_us_products_partner_panel_build,
    "audit_trade_regression_sources": run_trade_regression_source_audit,
    "build_trade_workhorse_panels": run_trade_workhorse_panel_build,
    "run_trade_regressions": run_trade_regressions,
    "run_package_full_benchmark": run_package_full_benchmark,
    "run_package_common_sample_benchmark": run_package_common_sample_benchmark,
    "run_section301_regression_sensitivity": run_section301_regression_sensitivity_step,
    "plot_trade_regressions": plot_trade_regressions,
    "verify_data": run_verification,
}


def main(argv: list[str] | None = None) -> int:
    config = PipelineConfig.from_args(argv)
    config.ensure_directories()
    logger, log_path = setup_logging(config)
    logger.info("Starting passthrough pipeline.")

    manifest: dict[str, object] = {
        "started_at_utc": datetime.utcnow().isoformat() + "Z",
        "config": config.to_dict(),
        "log_path": str(log_path),
        "steps": {},
    }

    try:
        if config.only_step:
            validate_only_step_inputs(config, config.only_step)
        if config.inventory_only:
            inventory = {
                "trade": build_trade_inventory(config),
                "cpi": build_cpi_inventory(config),
                "policy": build_policy_inventory(config),
            }
            manifest["inventory"] = inventory
            manifest["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"
            manifest_path = config.verification_dir / "pipeline_inventory.json"
            write_metadata_json(manifest_path, manifest)
            logger.info("Inventory complete. Manifest saved to %s", manifest_path)
            print(f"Pipeline inventory complete. Manifest: {manifest_path}")
            print(f"Log file: {log_path}")
            return 0

        for step in selected_steps(config):
            if config.skip_downloads and step.startswith("download_"):
                logger.info("Skipping %s because --skip-downloads was set.", step)
                manifest["steps"][step] = {"status": "skipped"}
                continue
            if config.skip_verification and step == "verify_data":
                logger.info("Skipping verification because --skip-verification was set.")
                manifest["steps"][step] = {"status": "skipped"}
                continue

            logger.info("Running step: %s", step)
            result = STEP_RUNNERS[step](config)
            manifest["steps"][step] = {"status": "completed", "result": result}

        manifest["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"
        manifest_path = config.verification_dir / "pipeline_manifest.json"
        write_metadata_json(manifest_path, manifest)
        logger.info("Pipeline complete. Manifest saved to %s", manifest_path)
        print(f"Pipeline complete. Manifest: {manifest_path}")
        print(f"Log file: {log_path}")
        return 0
    except Exception as exc:
        manifest["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"
        manifest["error"] = {"type": type(exc).__name__, "message": str(exc)}
        manifest_path = config.verification_dir / "pipeline_manifest.json"
        write_metadata_json(manifest_path, manifest)
        logger.exception("Pipeline failed: %s", exc)
        print(f"Pipeline failed. See log: {log_path}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
