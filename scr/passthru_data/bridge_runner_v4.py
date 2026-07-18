"""Run the 16-fit bridge against the v4 realized-duty source panel.

The implementation delegates estimator mechanics to the tested v3 runner but
uses a new namespace, source panel, version, and fingerprints.  This preserves
v3 as historical evidence while preventing accidental resume across formulas.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
import argparse

from .config import PipelineConfig
from . import bridge_runner as _impl


VERSION = "bridge_v4_realized_calculated_duty"


def bridge_root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4" / "bridge_resumable"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _source_paths(config: PipelineConfig) -> dict[str, Path]:
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4"
    return {
        "package_common_sample_anchor": root / "package_common_sample_anchor.parquet",
        "raw_outcomes_package_policy": root / "raw_outcomes_package_policy_realized_duty.parquet",
    }


@contextmanager
def _v4_bindings() -> Iterator[None]:
    saved = {
        "VERSION": _impl.VERSION,
        "bridge_root": _impl.bridge_root,
        "_source_paths": _impl._source_paths,
    }
    _impl.VERSION = VERSION
    _impl.bridge_root = bridge_root
    _impl._source_paths = _source_paths
    try:
        yield
    finally:
        for name, value in saved.items():
            setattr(_impl, name, value)


def run_bridge(config: PipelineConfig, **kwargs: Any) -> dict[str, Any]:
    with _v4_bindings():
        return _impl.run_bridge(config, **kwargs)


def finalize_bridge(config: PipelineConfig) -> dict[str, Any]:
    with _v4_bindings():
        return _impl.finalize_bridge(config)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-mode", choices=(*_impl.SOURCE_MODES, "all"), default="all")
    parser.add_argument("--spec", choices=(*_impl.SPECS, "all"), default="all")
    parser.add_argument("--outcome", choices=(*_impl.OUTCOMES, "all"), default="all")
    parser.add_argument("--only-fit")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args(argv)
    modes = _impl.SOURCE_MODES if args.source_mode == "all" else (args.source_mode,)
    specs = _impl.SPECS if args.spec == "all" else (args.spec,)
    outcomes = _impl.OUTCOMES if args.outcome == "all" else (args.outcome,)
    result = run_bridge(
        PipelineConfig.default(), source_modes=modes, specs=specs, outcomes=outcomes,
        only_fit=args.only_fit, resume=args.resume, preflight_only=args.preflight_only,
        finalize_only=args.finalize_only,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
