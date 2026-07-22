"""Finalize policy panel by applying schedule rates (with forward-fill) to existing panel rows."""

from __future__ import annotations

from pathlib import Path
import argparse

import pandas as pd


def _norm_hs10(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64").astype(str).str.zfill(10)


def run(repo_root: Path) -> dict[str, object]:
    root = repo_root.resolve()
    analysis_dir = root / "data" / "analysis" / "passthru_data"
    reference_dir = root / "data" / "reference" / "passthru_data"
    verification_dir = root / "data" / "verification" / "passthru_data"
    verification_dir.mkdir(parents=True, exist_ok=True)

    panel_path = analysis_dir / "us_products_partner_hs10_monthly.parquet"
    schedule_path = reference_dir / "hts_monthly_hs10_schedule.parquet"

    panel = pd.read_parquet(panel_path)
    schedule = pd.read_parquet(schedule_path)

    panel["hs10n"] = _norm_hs10(panel["hs10"])
    schedule["hs10n"] = _norm_hs10(schedule["hs10"])
    for col in ("year", "month"):
        panel[col] = pd.to_numeric(panel[col], errors="coerce").astype("Int64")
        schedule[col] = pd.to_numeric(schedule[col], errors="coerce").astype("Int64")

    schedule = (
        schedule.sort_values(["hs10n", "year", "month"])
        .drop_duplicates(["hs10n", "year", "month"], keep="last")
        .copy()
    )
    schedule["mfn_ad_val_rate"] = pd.to_numeric(schedule.get("mfn_ad_val_rate"), errors="coerce")

    merge_cols = ["hs10n", "year", "month", "mfn_text_rate", "mfn_ad_val_rate", "additional_duty", "source_type", "release_name"]
    if "mfn_ad_val_rate_ffilled" in schedule.columns:
        merge_cols.append("mfn_ad_val_rate_ffilled")
    updated = panel.merge(schedule[merge_cols], on=["hs10n", "year", "month"], how="left", suffixes=("", "_sched"))

    for col in ("mfn_text_rate", "mfn_ad_val_rate", "additional_duty", "source_type", "release_name"):
        sched_col = f"{col}_sched"
        if sched_col in updated.columns:
            updated[col] = updated[sched_col].where(updated[sched_col].notna(), updated.get(col))
            updated = updated.drop(columns=[sched_col], errors="ignore")

    updated["mfn_ad_val_rate"] = pd.to_numeric(updated.get("mfn_ad_val_rate"), errors="coerce")
    updated["base_pref_rate_raw"] = pd.to_numeric(updated.get("base_pref_rate_raw"), errors="coerce")
    updated["tw_increment_rate_raw"] = pd.to_numeric(updated.get("tw_increment_rate_raw"), errors="coerce")
    updated["tw_active_share_raw"] = pd.to_numeric(updated.get("tw_active_share_raw"), errors="coerce").fillna(1.0)

    updated["base_statutory_rate_raw"] = updated["base_pref_rate_raw"].where(updated["base_pref_rate_raw"].notna(), updated["mfn_ad_val_rate"])
    updated["m_statutory_tariff1"] = updated["base_statutory_rate_raw"].fillna(0.0) + updated["tw_increment_rate_raw"].fillna(0.0)
    updated["m_statutory_tariff2"] = updated["base_statutory_rate_raw"].fillna(0.0) + updated["tw_increment_rate_raw"].fillna(0.0) * updated["tw_active_share_raw"]

    # Keep WORLD rows in intermediate/full panel.
    updated = updated.drop(columns=["hs10n"], errors="ignore")
    updated.to_parquet(panel_path, index=False)

    # Regression-ready subset excludes nonpositive country codes.
    cty_code = pd.to_numeric(updated["cty_code"], errors="coerce").fillna(-9999)
    reg = updated.loc[cty_code > 0].copy()
    reg_path = analysis_dir / "us_products_partner_hs10_monthly_regression.parquet"
    reg.to_parquet(reg_path, index=False)

    out = {
        "full_rows": int(len(updated)),
        "regression_rows": int(len(reg)),
        "full_world_rows": int((cty_code <= 0).sum()),
        "output_full": str(panel_path),
        "output_regression": str(reg_path),
        "schedule_has_ffill_flag": bool("mfn_ad_val_rate_ffilled" in schedule.columns),
    }
    pd.DataFrame([out]).to_csv(verification_dir / "policy_panel_finalize_summary.csv", index=False)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Finalize policy panel from schedule outputs.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]))
    args = parser.parse_args()
    result = run(Path(args.repo_root))
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
