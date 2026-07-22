"""Configuration helpers for the passthrough rebuild pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Sequence
import argparse
from datetime import date
import json
import logging

DEFAULT_EXPORT_FORMATS = ("parquet",)
STEP_NAMES = (
    "download_trade",
    "download_cpi",
    "download_concordances",
    "build_hs10_codes",
    "build_hs6_bec",
    "build_cpi_hs6x",
    "build_trade_panels",
    "build_imports_with_package_shocks",
    "build_rerouting_controls",
    "run_rerouting_regressions",
    "audit_trade_regression_sources",
    "build_trade_workhorse_panels",
    "run_trade_regressions",
    "run_package_full_benchmark",
    "run_package_common_sample_benchmark",
    "plot_trade_regressions",
    "run_section301_regression_sensitivity",
    "verify_data",
    "download_policy_sources",
    "download_policy_updates",
    "build_hts_monthly_schedule",
    "build_tradewar_overlay_raw",
    "build_us_products_partner_hs10_panel",
    "build_section301_import_panel",
    "build_rtp_long_horizon_panel",
    "run_rtp_long_horizon_2018_event",
    "build_rtp_2025_ieepa_event_panel",
    "validate_raw_replication_imports",
    "validate_raw_replication_imports_china_current",
    "validate_raw_replication_imports_china_semantics_corrected",
    "build_china_301_trace",
)

# Explicit dependency graph for the production pipeline.  Archived policy
# steps remain opt-in, but their prerequisites are recorded so ``--only-step``
# can fail clearly instead of running an unrelated subset implicitly.
PIPELINE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "download_trade": (),
    "download_cpi": (),
    "download_concordances": ("download_trade",),
    "download_policy_sources": (),
    "download_policy_updates": ("download_policy_sources",),
    "build_hs10_codes": ("download_trade", "download_concordances"),
    "build_hs6_bec": ("download_concordances",),
    "build_cpi_hs6x": ("download_cpi", "build_hs10_codes"),
    "build_trade_panels": ("download_trade", "build_hs10_codes"),
    "build_imports_with_package_shocks": ("build_trade_panels",),
    "build_rerouting_controls": ("build_trade_panels",),
    "run_rerouting_regressions": ("build_rerouting_controls", "build_imports_with_package_shocks"),
    "audit_trade_regression_sources": ("build_trade_panels",),
    "build_trade_workhorse_panels": ("build_trade_panels",),
    "run_trade_regressions": ("build_trade_workhorse_panels", "audit_trade_regression_sources"),
    "run_package_full_benchmark": (),
    "run_package_common_sample_benchmark": ("run_package_full_benchmark",),
    "plot_trade_regressions": ("run_trade_regressions",),
    "verify_data": ("build_trade_panels",),
    "build_hts_monthly_schedule": ("download_policy_sources", "download_policy_updates"),
    "build_tradewar_overlay_raw": ("build_hts_monthly_schedule",),
    "build_us_products_partner_hs10_panel": ("build_trade_panels", "build_tradewar_overlay_raw"),
    "build_section301_import_panel": ("build_us_products_partner_hs10_panel",),
    "validate_raw_replication_imports": ("build_section301_import_panel",),
    "validate_raw_replication_imports_china_current": ("build_section301_import_panel",),
    "validate_raw_replication_imports_china_semantics_corrected": ("build_section301_import_panel",),
    "build_china_301_trace": ("validate_raw_replication_imports_china_semantics_corrected",),
    "run_section301_regression_sensitivity": ("build_section301_import_panel", "build_trade_workhorse_panels"),
    "build_rtp_long_horizon_panel": ("build_trade_workhorse_panels",),
    "run_rtp_long_horizon_2018_event": ("build_rtp_long_horizon_panel",),
    "build_rtp_2025_ieepa_event_panel": ("build_rtp_long_horizon_panel",),
}


def pipeline_topological_order() -> tuple[str, ...]:
    """Return a deterministic topological ordering and reject cycles."""
    visiting: set[str] = set()
    visited: set[str] = set()
    order: list[str] = []

    def visit(step: str) -> None:
        if step in visiting:
            raise ValueError(f"Pipeline dependency cycle detected at '{step}'")
        if step in visited:
            return
        visiting.add(step)
        for dependency in PIPELINE_DEPENDENCIES.get(step, ()):
            if dependency not in STEP_NAMES:
                raise ValueError(f"Unknown pipeline dependency '{dependency}' for '{step}'")
            visit(dependency)
        visiting.remove(step)
        visited.add(step)
        order.append(step)

    for step in STEP_NAMES:
        visit(step)
    return tuple(order)


def required_artifacts_for_step(config: "PipelineConfig", step: str) -> tuple[Path, ...]:
    """Return only concrete prerequisites that can be checked locally.

    The graph records logical dependencies; this function supplies a small set
    of canonical artifacts for the most commonly invoked ``--only-step`` paths.
    """
    checks: dict[str, tuple[Path, ...]] = {
        "build_trade_panels": (config.staging_dir / "trade" / "imports.parquet",),
        "build_trade_workhorse_panels": (config.analysis_dir / "trade_imports_hs10.parquet",),
        "build_section301_import_panel": (config.analysis_dir / "tradewar_overlay_raw.parquet",),
        "run_trade_regressions": (config.analysis_dir / "trade_imports_hs10.parquet",),
        "run_section301_regression_sensitivity": (
            config.verification_dir / "raw_replication_imports" / "china_301_semantics_corrected_gate.json",
        ),
    }
    return checks.get(step, ())


def validate_only_step_inputs(config: "PipelineConfig", step: str) -> None:
    missing = [str(path) for path in required_artifacts_for_step(config, step) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"--only-step {step} requires missing or stale prerequisite artifact(s): " + ", ".join(missing)
        )

ARCHIVED_POLICY_STEPS = {
    "download_policy_sources",
    "download_policy_updates",
    "build_hts_monthly_schedule",
    "build_tradewar_overlay_raw",
    "build_us_products_partner_hs10_panel",
    "build_section301_import_panel",
}

OPT_IN_STEPS = {
    "build_rtp_long_horizon_panel",
    "run_rtp_long_horizon_2018_event",
    "build_rtp_2025_ieepa_event_panel",
    "validate_raw_replication_imports",
    "validate_raw_replication_imports_china_current",
    "validate_raw_replication_imports_china_semantics_corrected",
    "build_china_301_trace",
    "run_section301_regression_sensitivity",
    "run_package_full_benchmark",
    "run_package_common_sample_benchmark",
}


@dataclass(slots=True)
class PipelineConfig:
    """Central configuration for the passthrough pipeline."""

    repo_root: Path
    raw_dir: Path
    staging_dir: Path
    reference_dir: Path
    analysis_dir: Path
    verification_dir: Path
    fajgelbaum_root: Path
    fajgelbaum_analysis_dir: Path
    manual_input_dir: Path
    logs_dir: Path
    start_period: str = "2013-01"
    end_period: str = "2019-12"
    validation_end_period: str = "2019-12"
    export_formats: tuple[str, ...] = field(default_factory=lambda: DEFAULT_EXPORT_FORMATS)
    overwrite: bool = False
    log_level: str = "INFO"
    skip_downloads: bool = False
    skip_verification: bool = False
    latest_available: bool = False
    inventory_only: bool = False
    trade_flow: str | None = None
    regression_spec: str | None = None
    regression_outcome: str | None = None
    only_step: str | None = None
    enable_archived_policy_pipeline: bool = False
    analysis_window: str = "benchmark"

    @classmethod
    def default(cls, repo_root: Path | None = None) -> "PipelineConfig":
        root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
        data_root = root / "data"
        passthru_raw = data_root / "raw" / "passthru_data"
        verification_dir = data_root / "verification" / "passthru_data"
        return cls(
            repo_root=root,
            raw_dir=passthru_raw,
            staging_dir=data_root / "staging" / "passthru_data",
            reference_dir=data_root / "reference" / "passthru_data",
            analysis_dir=data_root / "analysis" / "passthru_data",
            verification_dir=verification_dir,
            fajgelbaum_root=data_root / "fajgelbaum",
            fajgelbaum_analysis_dir=data_root / "fajgelbaum" / "data" / "analysis",
            manual_input_dir=passthru_raw / "manual",
            logs_dir=verification_dir / "logs",
        )

    @classmethod
    def from_args(cls, args: Sequence[str] | None = None) -> "PipelineConfig":
        parser = build_arg_parser()
        parsed = parser.parse_args(args=args)
        cfg = cls.default()
        formats = list(DEFAULT_EXPORT_FORMATS)
        if parsed.export_dta:
            formats.append("dta")
        cfg.start_period = parsed.start
        cfg.end_period = parsed.end
        cfg.overwrite = parsed.overwrite
        cfg.log_level = parsed.log_level.upper()
        cfg.skip_downloads = parsed.skip_downloads
        cfg.skip_verification = parsed.skip_verification
        cfg.latest_available = parsed.latest_available
        cfg.inventory_only = parsed.inventory_only
        cfg.trade_flow = parsed.trade_flow
        cfg.regression_spec = parsed.regression_spec
        cfg.regression_outcome = parsed.regression_outcome
        cfg.only_step = parsed.only_step
        cfg.enable_archived_policy_pipeline = parsed.enable_archived_policy_pipeline
        cfg.analysis_window = parsed.analysis_window
        cfg.export_formats = tuple(formats)
        if parsed.latest_available and parsed.end == "2019-12":
            cfg.end_period = inferred_latest_complete_period()
        return cfg

    def ensure_directories(self) -> None:
        for path in (
            self.raw_dir,
            self.staging_dir,
            self.reference_dir,
            self.analysis_dir,
            self.verification_dir,
            self.manual_input_dir,
            self.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        for child in ("trade", "cpi", "concordances", "policy"):
            (self.manual_input_dir / child).mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        return {key: str(value) if isinstance(value, Path) else value for key, value in payload.items()}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    def should_run(self, step_name: str) -> bool:
        if step_name not in STEP_NAMES:
            raise ValueError(f"Unknown step: {step_name}")
        return self.only_step is None or self.only_step == step_name

    def export_dta(self) -> bool:
        return "dta" in self.export_formats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild passthrough Phase 1 datasets.")
    parser.add_argument("--start", default="2013-01", help="Start period in YYYY-MM format.")
    parser.add_argument("--end", default="2019-12", help="End period in YYYY-MM format.")
    parser.add_argument("--latest-available", action="store_true", help="Use the latest likely complete period for downloads/builds.")
    parser.add_argument("--inventory-only", action="store_true", help="Write raw-data inventory diagnostics and exit without running pipeline steps.")
    parser.add_argument("--trade-flow", choices=("imports", "exports"), help="Restrict trade download/build inventory to one flow.")
    parser.add_argument("--regression-spec", choices=("event", "dynamic"), help="Restrict regression steps to one specification.")
    parser.add_argument("--regression-outcome", choices=("val", "q1", "p", "pduty"), help="Restrict regression steps to one outcome.")
    parser.add_argument("--skip-downloads", action="store_true", help="Skip download and ingest steps.")
    parser.add_argument("--skip-verification", action="store_true", help="Skip validation against reference files.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--export-dta", action="store_true", help="Also export Stata .dta files.")
    parser.add_argument("--only-step", choices=STEP_NAMES, help="Run only a single pipeline step.")
    parser.add_argument(
        "--analysis-window",
        choices=("benchmark", "current"),
        default="benchmark",
        help="Use the paper window or retain the full available current-data panel in regression steps.",
    )
    parser.add_argument(
        "--enable-archived-policy-pipeline",
        action="store_true",
        help="Enable archived raw-policy reconstruction steps (machine-readable/PDF HTS reconstruction).",
    )
    parser.add_argument(
        "--log-level",
        default=logging.getLevelName(logging.INFO),
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level.",
    )
    return parser


def selected_steps(config: PipelineConfig) -> Iterable[str]:
    # Always iterate in dependency order.  For ``--only-step`` retain the
    # historical meaning: emit exactly the requested step and never run its
    # prerequisites implicitly.
    steps = (config.only_step,) if config.only_step else pipeline_topological_order()
    for step in steps:
        if step is None:
            continue
        if step in ARCHIVED_POLICY_STEPS and not config.enable_archived_policy_pipeline:
            continue
        if step in OPT_IN_STEPS and config.only_step != step:
            continue
        if config.should_run(step):
            yield step


def inferred_latest_complete_period(today: date | None = None) -> str:
    current = today or date.today()
    year = current.year
    month = current.month - 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"
