"""Portable paths for the locked historical replication."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class PipelineConfig:
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
    processed_trade_dir: Path
    processed_tariff_dir: Path
    start_period: str = "2017-01"
    end_period: str = "2020-10"
    validation_end_period: str = "2020-10"
    overwrite: bool = False
    trade_flow: str | None = None
    analysis_window: str = "benchmark"

    @classmethod
    def default(cls, repo_root: Path | None = None) -> "PipelineConfig":
        root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
        data = root / "data"
        raw = data / "raw" / "passthru_data"
        tariffs = data / "processed" / "tariffs"
        return cls(
            repo_root=root,
            raw_dir=raw,
            staging_dir=tariffs / "intermediate",
            reference_dir=data / "reference" / "passthru_data",
            analysis_dir=tariffs / "intermediate",
            verification_dir=tariffs / "verification",
            fajgelbaum_root=data / "fajgelbaum",
            fajgelbaum_analysis_dir=data / "fajgelbaum" / "data" / "analysis",
            manual_input_dir=raw / "manual",
            logs_dir=tariffs / "verification" / "logs",
            processed_trade_dir=data / "processed" / "trade",
            processed_tariff_dir=tariffs,
        )

    def ensure_directories(self) -> None:
        for path in (
            self.staging_dir,
            self.analysis_dir,
            self.verification_dir,
            self.logs_dir,
            self.processed_trade_dir,
            self.processed_tariff_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
