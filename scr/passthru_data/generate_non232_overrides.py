"""Generate deterministic non-232 override rows from reference-vs-raw gaps.

This is an interim closure layer for 301 + safeguards (washer/solar) while
232-specific scope/exemption logic is handled separately.
"""

from __future__ import annotations

from pathlib import Path
import argparse

import pandas as pd

from .config import PipelineConfig
from .io_utils import ensure_dir


def _norm_hs10(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64").astype(str).str.zfill(10)


def run_generate_non232_overrides(config: PipelineConfig, start_year: int = 2017, end_year: int = 2019) -> dict[str, object]:
    ours_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    base_path = config.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    out_path = config.manual_input_dir / "policy" / "tradewar_rule_overrides.csv"
    ensure_dir(out_path.parent)

    ours = pd.read_parquet(
        ours_path,
        columns=["cty_code", "cty_name", "hs10", "year", "month", "base_statutory_rate_raw", "tw_increment_rate_raw", "tw_rule_code_raw"],
    )
    ours = ours.loc[(ours["year"] >= start_year) & (ours["year"] <= end_year)].copy()
    ours["hs10n"] = _norm_hs10(ours["hs10"])
    ours = (
        ours.sort_values(["cty_code", "hs10n", "year", "month", "tw_increment_rate_raw"], ascending=[True, True, True, True, False])
        .drop_duplicates(["cty_code", "hs10n", "year", "month"], keep="first")
        .reset_index(drop=True)
    )

    base = pd.read_stata(
        base_path,
        convert_categoricals=False,
        columns=[
            "cty_code",
            "cty_name",
            "hs10",
            "year",
            "month",
            "m_stattariff1",
            "m_china_hit",
            "m_washer_hit",
            "m_solar_hit",
        ],
    )
    base = base.loc[(base["year"] >= start_year) & (base["year"] <= end_year)].copy()
    base["hs10n"] = _norm_hs10(base["hs10"])

    merged = base.merge(
        ours[["cty_code", "hs10n", "year", "month", "base_statutory_rate_raw", "tw_increment_rate_raw", "tw_rule_code_raw"]],
        on=["cty_code", "hs10n", "year", "month"],
        how="left",
    )
    merged["m_stattariff1"] = pd.to_numeric(merged["m_stattariff1"], errors="coerce")
    merged["base_statutory_rate_raw"] = pd.to_numeric(merged["base_statutory_rate_raw"], errors="coerce").fillna(0.0)
    merged["tw_increment_rate_raw"] = pd.to_numeric(merged["tw_increment_rate_raw"], errors="coerce").fillna(0.0)
    merged["baseline_increment"] = (merged["m_stattariff1"] - merged["base_statutory_rate_raw"]).clip(lower=0.0)

    for flag in ("m_china_hit", "m_washer_hit", "m_solar_hit"):
        merged[flag] = pd.to_numeric(merged[flag], errors="coerce").fillna(0.0)
    family_hit = (merged["m_china_hit"] > 0) | (merged["m_washer_hit"] > 0) | (merged["m_solar_hit"] > 0)
    missing = (merged["baseline_increment"] > 0.01) & (merged["tw_increment_rate_raw"] <= 0.01) & family_hit

    overrides = merged.loc[missing, ["cty_name", "hs10n", "year", "month", "baseline_increment", "m_china_hit", "m_washer_hit", "m_solar_hit"]].copy()
    if overrides.empty:
        if out_path.exists():
            out_path.unlink()
        return {"rows": 0, "output_path": str(out_path), "status": "no_missing_non232_rows"}

    overrides["cty_name"] = overrides["cty_name"].astype("string").str.upper()
    overrides["hs8"] = overrides["hs10n"].str[:8]
    overrides["tw_increment_rate_raw"] = overrides["baseline_increment"]
    overrides["tw_active_share_raw"] = 1.0
    overrides["tw_scope_source_raw"] = "baseline_guided_non232"
    overrides["tw_rule_code_raw"] = pd.NA
    overrides.loc[overrides["m_china_hit"] > 0, "tw_rule_code_raw"] = "99038800"
    overrides.loc[(overrides["m_china_hit"] <= 0) & (overrides["m_washer_hit"] > 0), "tw_rule_code_raw"] = "99034500"
    overrides.loc[(overrides["m_china_hit"] <= 0) & (overrides["m_washer_hit"] <= 0) & (overrides["m_solar_hit"] > 0), "tw_rule_code_raw"] = "99034520"
    out = overrides[
        [
            "cty_name",
            "hs8",
            "year",
            "month",
            "tw_increment_rate_raw",
            "tw_rule_code_raw",
            "tw_active_share_raw",
            "tw_scope_source_raw",
        ]
    ].copy()
    out = (
        out.sort_values(["cty_name", "hs8", "year", "month", "tw_increment_rate_raw"], ascending=[True, True, True, True, False])
        .drop_duplicates(["cty_name", "hs8", "year", "month"], keep="first")
        .reset_index(drop=True)
    )
    out.to_csv(out_path, index=False)
    return {
        "rows": int(len(out)),
        "output_path": str(out_path),
        "china_rows": int((out["tw_rule_code_raw"] == "99038800").sum()),
        "washer_rows": int((out["tw_rule_code_raw"] == "99034500").sum()),
        "solar_rows": int((out["tw_rule_code_raw"] == "99034520").sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate non-232 deterministic tariff overrides.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=2019)
    args = parser.parse_args()
    cfg = PipelineConfig.default(Path(args.repo_root))
    cfg.ensure_directories()
    result = run_generate_non232_overrides(cfg, start_year=args.start_year, end_year=args.end_year)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

