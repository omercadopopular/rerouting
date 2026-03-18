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
from passthru_data.build_hs10_codes import run_hs10_code_build
from passthru_data.build_hs6_bec import run_hs6_bec_build
from passthru_data.build_trade_panels import run_trade_panel_build
from passthru_data.config import PipelineConfig, selected_steps
from passthru_data.download_concordances import run_concordance_download
from passthru_data.download_cpi import run_cpi_download
from passthru_data.download_trade import run_trade_download
from passthru_data.logging_utils import setup_logging
from passthru_data.verify_data import run_verification
from passthru_data.io_utils import write_metadata_json

STEP_RUNNERS = {
    "download_trade": run_trade_download,
    "download_cpi": run_cpi_download,
    "download_concordances": run_concordance_download,
    "build_hs10_codes": run_hs10_code_build,
    "build_hs6_bec": run_hs6_bec_build,
    "build_cpi_hs6x": run_cpi_hs6x_build,
    "build_trade_panels": run_trade_panel_build,
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
