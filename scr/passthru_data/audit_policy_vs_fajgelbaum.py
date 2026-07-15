"""Audit tariff-construction differences versus the Fajgelbaum replication data."""

from __future__ import annotations

from pathlib import Path
import argparse

import numpy as np
import pandas as pd


def _norm_hs10(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64").astype(str).str.zfill(10)


def run_audit(repo_root: Path, start_year: int = 2017, end_year: int = 2019, exclude_world: bool = False) -> dict[str, object]:
    root = repo_root.resolve()
    ours_path = root / "data" / "analysis" / "passthru_data" / "us_products_partner_hs10_monthly.parquet"
    base_path = root / "data" / "fajgelbaum" / "data" / "analysis" / "m_flow_hs10_fm_new.dta"
    out_dir = root / "data" / "verification" / "passthru_data"
    out_dir.mkdir(parents=True, exist_ok=True)

    ours = pd.read_parquet(
        ours_path,
        columns=[
            "cty_code",
            "hs10",
            "year",
            "month",
            "m_statutory_tariff1",
            "m_statutory_tariff2",
            "m_policy_source",
            "mfn_ad_val_rate",
        ],
    )
    ours = ours.loc[(ours["year"] >= start_year) & (ours["year"] <= end_year)].copy()
    ours["hs10n"] = _norm_hs10(ours["hs10"])
    ours = ours.dropna(subset=["m_statutory_tariff1"])
    ours["source_priority"] = np.where(ours["m_policy_source"].astype("string").eq("trade_war_raw_overlay"), 1, 0)
    ours = (
        ours.sort_values(["cty_code", "hs10n", "year", "month", "source_priority"], ascending=[True, True, True, True, False])
        .drop_duplicates(["cty_code", "hs10n", "year", "month"], keep="first")
        .rename(columns={"m_statutory_tariff1": "our_rate", "m_statutory_tariff2": "our_rate2"})
    )

    base = pd.read_stata(base_path, convert_categoricals=False)
    base = base.loc[(base["year"] >= start_year) & (base["year"] <= end_year)].copy()
    if exclude_world:
        base = base.loc[pd.to_numeric(base["cty_code"], errors="coerce").fillna(-9999) > 0].copy()
    base["hs10n"] = _norm_hs10(base["hs10"])
    for column in [
        "m_stattariff1",
        "m_stattariff2",
        "m_hit",
        "m_china_hit",
        "m_steel_hit",
        "m_alum_hit",
        "m_washer_hit",
        "m_solar_hit",
    ]:
        if column in base.columns:
            base[column] = pd.to_numeric(base[column], errors="coerce")

    merged = base.merge(
        ours[["cty_code", "hs10n", "year", "month", "our_rate", "our_rate2", "m_policy_source", "mfn_ad_val_rate"]],
        on=["cty_code", "hs10n", "year", "month"],
        how="left",
    )

    coverage = {
        "baseline_rows": int(len(base)),
        "baseline_nonmissing_st1": int(base["m_stattariff1"].notna().sum()),
        "rows_with_our_rate": int(merged["our_rate"].notna().sum()),
        "st1_rows_with_our_rate": int(merged.loc[merged["m_stattariff1"].notna(), "our_rate"].notna().sum()),
    }

    diff_all = merged.loc[merged["m_stattariff1"].notna() & merged["our_rate"].notna(), ["m_stattariff1", "our_rate"]].copy()
    diff_all["absdiff"] = (diff_all["our_rate"] - diff_all["m_stattariff1"]).abs()
    diff_all["is_sentinel"] = diff_all["our_rate"] >= 9999

    diff_no_sentinel = diff_all.loc[~diff_all["is_sentinel"]].copy()

    cells = merged.loc[merged["m_stattariff1"].notna() & merged["our_rate"].notna()].copy()
    cells["absdiff"] = (cells["our_rate"] - cells["m_stattariff1"]).abs()
    cells["is_sentinel"] = cells["our_rate"] >= 9999

    source_mix = ours["m_policy_source"].value_counts(dropna=False).rename_axis("source_type").reset_index(name="cells")
    source_mix["share"] = source_mix["cells"] / source_mix["cells"].sum()
    source_mix.to_csv(out_dir / "policy_source_mix_cells.csv", index=False)

    top_all = cells.sort_values("absdiff", ascending=False).head(20000)
    top_all.to_csv(out_dir / "policy_diff_top_cells.csv", index=False)
    top_non_sentinel = cells.loc[~cells["is_sentinel"]].sort_values("absdiff", ascending=False).head(20000)
    top_non_sentinel.to_csv(out_dir / "policy_diff_top_cells_no_sentinel.csv", index=False)

    metrics: list[dict[str, object]] = [
        {"metric": "coverage_baseline_rows", "value": coverage["baseline_rows"]},
        {"metric": "coverage_baseline_nonmissing_st1", "value": coverage["baseline_nonmissing_st1"]},
        {"metric": "coverage_rows_with_our_rate", "value": coverage["rows_with_our_rate"]},
        {"metric": "coverage_st1_rows_with_our_rate", "value": coverage["st1_rows_with_our_rate"]},
        {"metric": "diff_all_n", "value": int(len(diff_all))},
        {"metric": "diff_all_mean_abs", "value": float(diff_all["absdiff"].mean())},
        {"metric": "diff_all_median_abs", "value": float(diff_all["absdiff"].median())},
        {"metric": "diff_all_share_le_0_01", "value": float((diff_all["absdiff"] <= 0.01).mean())},
        {"metric": "diff_all_share_sentinel", "value": float(diff_all["is_sentinel"].mean())},
        {"metric": "diff_no_sentinel_n", "value": int(len(diff_no_sentinel))},
        {"metric": "diff_no_sentinel_mean_abs", "value": float(diff_no_sentinel["absdiff"].mean())},
        {"metric": "diff_no_sentinel_median_abs", "value": float(diff_no_sentinel["absdiff"].median())},
        {"metric": "diff_no_sentinel_p90_abs", "value": float(diff_no_sentinel["absdiff"].quantile(0.9))},
        {"metric": "diff_no_sentinel_share_le_0_01", "value": float((diff_no_sentinel["absdiff"] <= 0.01).mean())},
        {"metric": "diff_no_sentinel_share_le_0_05", "value": float((diff_no_sentinel["absdiff"] <= 0.05).mean())},
        {"metric": "diff_no_sentinel_share_le_0_10", "value": float((diff_no_sentinel["absdiff"] <= 0.10).mean())},
        {"metric": "overlay_share_rows", "value": float((merged["m_policy_source"] == "trade_war_raw_overlay").mean())},
        {"metric": "overlay_share_matched_rows", "value": float((merged.loc[merged["our_rate"].notna(), "m_policy_source"] == "trade_war_raw_overlay").mean()) if (merged["our_rate"].notna().any()) else 0.0},
    ]

    by_hit_rows: list[dict[str, object]] = []
    for hit_column in ["m_china_hit", "m_steel_hit", "m_alum_hit", "m_washer_hit", "m_solar_hit"]:
        if hit_column not in merged.columns:
            continue
        temp = merged.loc[
            merged["m_stattariff1"].notna() & merged["our_rate"].notna() & (merged["our_rate"] < 9999),
            [hit_column, "m_stattariff1", "our_rate"],
        ].copy()
        if temp.empty:
            continue
        temp["absdiff"] = (temp["our_rate"] - temp["m_stattariff1"]).abs()
        grouped = temp.groupby(hit_column)["absdiff"].agg(["count", "mean", "median"]).reset_index()
        grouped["hit_variable"] = hit_column
        by_hit_rows.extend(grouped.to_dict("records"))

    by_hit = pd.DataFrame(by_hit_rows)
    if not by_hit.empty:
        by_hit.to_csv(out_dir / "policy_diff_by_hit.csv", index=False)

    metrics_frame = pd.DataFrame(metrics)
    metrics_frame.to_csv(out_dir / "policy_diff_audit_metrics.csv", index=False)

    report_path = out_dir / "policy_diff_audit_report.md"
    report_lines = [
        "# Policy Diff Audit (2017-2019)",
        "",
        f"- Scope: `{'bilateral_only_cty_code>0' if exclude_world else 'all_rows'}`",
        "",
        "## Coverage",
        f"- Baseline rows (`m_flow_hs10_fm_new`): `{coverage['baseline_rows']:,}`",
        f"- Baseline rows with `m_stattariff1`: `{coverage['baseline_nonmissing_st1']:,}`",
        f"- Rows with our tariff rate: `{coverage['rows_with_our_rate']:,}`",
        f"- Baseline-`m_stattariff1` rows with our tariff rate: `{coverage['st1_rows_with_our_rate']:,}`",
        "",
        "## Main Differences",
        f"- Mean absolute difference (all matched rows): `{diff_all['absdiff'].mean():.4f}`",
        f"- Mean absolute difference (excluding `9999.99` sentinel rates): `{diff_no_sentinel['absdiff'].mean():.4f}`",
        f"- Share within 0.01 (excluding sentinels): `{(diff_no_sentinel['absdiff'] <= 0.01).mean():.2%}`",
        f"- Share within 0.05 (excluding sentinels): `{(diff_no_sentinel['absdiff'] <= 0.05).mean():.2%}`",
        f"- Share with sentinel `9999.99` in our rates: `{diff_all['is_sentinel'].mean():.2%}`",
        "",
        "## Interpretation Against Paper Construction",
        "- Paper tariff variable labels define `m_stattariff1` as **Statutory Tariff Rate** and `m_stattariff2` as day-scaled statutory rate.",
        "- Online Appendix A.2.2 states identification uses **ad-valorem tariff increases in USITC revisions** (trade-war changes), not raw full MFN baseline levels.",
        "- This audit compares a bilateral corrected variable (`m_statutory_tariff1`) to the paper baseline on the full key `cty_code × hs10 × year × month`.",
        "- Remaining differences after overlay indicate either schedule-level fallback rows, sentinel rate handling, or timing/coding mismatches.",
        "",
        "## Output Files",
        "- `policy_diff_audit_metrics.csv`",
        "- `policy_source_mix_cells.csv`",
        "- `policy_diff_top_cells.csv`",
        "- `policy_diff_top_cells_no_sentinel.csv`",
        "- `policy_diff_by_hit.csv` (if available)",
        "",
    ]
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    return {
        "metrics_path": str(out_dir / "policy_diff_audit_metrics.csv"),
        "source_mix_path": str(out_dir / "policy_source_mix_cells.csv"),
        "top_path": str(out_dir / "policy_diff_top_cells.csv"),
        "top_no_sentinel_path": str(out_dir / "policy_diff_top_cells_no_sentinel.csv"),
        "report_path": str(report_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit policy-tariff differences vs Fajgelbaum baseline.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=2019)
    parser.add_argument("--exclude-world", action="store_true", help="Exclude cty_code<=0 rows from baseline before audit merge.")
    args = parser.parse_args()
    result = run_audit(Path(args.repo_root), start_year=args.start_year, end_year=args.end_year, exclude_world=args.exclude_world)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
