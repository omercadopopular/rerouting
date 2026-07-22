"""Command-line orchestration for the locked data-construction pipeline."""

from __future__ import annotations

import argparse
import json

from .config import PipelineConfig
from .sample import build_replication_outcomes
from .tariffs import materialize_validated, rebuild_from_sources, write_source_ledger
from .trade import build_trade


def run(
    config: PipelineConfig,
    *,
    rebuild_tariffs: bool = False,
    build_archives: bool = False,
    overwrite: bool = False,
) -> dict:
    config.ensure_directories()
    outputs = {
        "tariffs": rebuild_from_sources(config, overwrite=overwrite) if rebuild_tariffs else materialize_validated(config, overwrite=overwrite),
        "source_ledger": str(write_source_ledger(config)),
        "replication_outcomes": build_replication_outcomes(config, overwrite=overwrite),
    }
    if build_archives:
        outputs["archive_native_trade"] = build_trade(
            config,
            start_period=config.start_period,
            end_period=config.end_period,
            overwrite=overwrite,
        )
    else:
        outputs["archive_native_trade"] = {
            "status": "not_requested",
            "command": "--build-archives",
            "default_period": [config.start_period, config.end_period],
        }
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-tariffs", action="store_true")
    parser.add_argument("--build-archives", action="store_true")
    parser.add_argument("--start", default="2017-01")
    parser.add_argument("--end", default="2020-10")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.default()
    config.start_period = args.start
    config.end_period = args.end
    print(json.dumps(run(config, rebuild_tariffs=args.rebuild_tariffs, build_archives=args.build_archives, overwrite=args.overwrite), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
