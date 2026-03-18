"""Configuration helpers for the passthrough rebuild pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Sequence
import argparse
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
    "verify_data",
)


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
    export_formats: tuple[str, ...] = field(default_factory=lambda: DEFAULT_EXPORT_FORMATS)
    overwrite: bool = False
    log_level: str = "INFO"
    skip_downloads: bool = False
    skip_verification: bool = False
    only_step: str | None = None

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
        cfg.only_step = parsed.only_step
        cfg.export_formats = tuple(formats)
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
    parser.add_argument("--skip-downloads", action="store_true", help="Skip download and ingest steps.")
    parser.add_argument("--skip-verification", action="store_true", help="Skip validation against reference files.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--export-dta", action="store_true", help="Also export Stata .dta files.")
    parser.add_argument("--only-step", choices=STEP_NAMES, help="Run only a single pipeline step.")
    parser.add_argument(
        "--log-level",
        default=logging.getLevelName(logging.INFO),
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level.",
    )
    return parser


def selected_steps(config: PipelineConfig) -> Iterable[str]:
    for step in STEP_NAMES:
        if config.should_run(step):
            yield step
