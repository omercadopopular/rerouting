"""Regression-side path configuration."""

from dataclasses import replace
from pathlib import Path

from scr.data_construction.config import PipelineConfig as DataConfig


class PipelineConfig(DataConfig):
    @classmethod
    def default(cls, repo_root: Path | None = None) -> "PipelineConfig":
        base = DataConfig.default(repo_root)
        trade = base.processed_trade_dir
        values = {
            field: getattr(base, field)
            for field in base.__dataclass_fields__
        }
        values.update(
            analysis_dir=trade,
            verification_dir=trade / "regressions",
            logs_dir=trade / "regressions" / "logs",
        )
        return cls(**values)
