#!/usr/bin/env python3
"""Summary statistics and time-series plots for rerouted shares."""

from __future__ import annotations

from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


PERCENTILES = [0.10, 0.25, 0.50, 0.75, 0.90]
PERCENTILE_RENAME = {0.10: "p10", 0.25: "p25", 0.50: "p50", 0.75: "p75", 0.90: "p90"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ensure_dirs(root: Path) -> tuple[Path, Path]:
    fig_dir = root / "figs"
    out_dir = root / "code" / "shr_ts" / "output"
    fig_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    return fig_dir, out_dir


def _load_share_data(path: Path) -> pd.DataFrame:
    df = pd.read_stata(path)
    required = {"hs_6dig", "modate_imports", "hs_section_name", "share_rerouted"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in share file: {sorted(missing)}")

    df = df.copy()
    df["hs6"] = df["hs_6dig"].astype(str).str.zfill(6)
    df["date"] = pd.to_datetime(df["modate_imports"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    return df


def _annual_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("year")["share_rerouted"]
        .quantile(PERCENTILES)
        .unstack()
        .rename(columns=PERCENTILE_RENAME)
        .sort_index()
        .reset_index()
    )
    return out


def _plot_percentiles(df: pd.DataFrame, fig_path: Path) -> None:
    pct_cols = [PERCENTILE_RENAME[p] for p in PERCENTILES]
    fig, ax = plt.subplots(figsize=(10, 5))
    for col in pct_cols:
        ax.plot(df["year"], df[col], marker="o", linewidth=1.8, label=col.upper())
    ax.set_title("Annual Percentiles of share_rerouted")
    ax.set_xlabel("Year")
    ax.set_ylabel("share_rerouted")
    ax.legend(ncol=5, fontsize=8, frameon=False)
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(fig_path, dpi=300)
    plt.close(fig)


def _section_year_median(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["year", "hs_section_name"], as_index=False)["share_rerouted"]
        .median()
        .rename(columns={"share_rerouted": "median_share_rerouted"})
    )
    return out


def _plot_section_median(df: pd.DataFrame, fig_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(13, 7))
    sns.lineplot(
        data=df,
        x="year",
        y="median_share_rerouted",
        hue="hs_section_name",
        marker="o",
        linewidth=1.6,
        ax=ax,
    )
    ax.set_title("Median share_rerouted by HS Section and Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Median share_rerouted")
    ax.grid(alpha=0.2)
    ax.legend(
        title="HS Section",
        bbox_to_anchor=(1.02, 1.0),
        loc="upper left",
        borderaxespad=0,
        fontsize=8,
        title_fontsize=9,
        frameon=False,
    )
    fig.tight_layout()
    fig.savefig(fig_path, dpi=300)
    plt.close(fig)


def _load_2014_china_hs6_imports(path: Path) -> pd.DataFrame:
    cols = ["cty_name", "year", "hs6", "m_val"]
    df = pd.read_stata(path, columns=cols)
    required = set(cols)
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in flow file: {sorted(missing)}")

    df = df.loc[(df["cty_name"] == "CHINA") & (df["year"] == 2014)].copy()
    df["hs6"] = df["hs6"].astype("Int64").astype(str).str.zfill(6)
    out = (
        df.groupby("hs6", as_index=False)["m_val"]
        .sum()
        .rename(columns={"m_val": "m2014_import_china"})
    )
    return out


def _build_hs6_section_map(share_df: pd.DataFrame) -> pd.DataFrame:
    map_df = share_df[["hs6", "hs_section_name"]].drop_duplicates()
    counts = map_df.groupby("hs6")["hs_section_name"].nunique()
    conflict_hs6 = counts[counts > 1].index.tolist()
    if conflict_hs6:
        warnings.warn(
            f"{len(conflict_hs6)} hs6 codes map to multiple sections; using modal section assignment."
        )
        map_df = (
            share_df.groupby(["hs6", "hs_section_name"], as_index=False)
            .size()
            .sort_values(["hs6", "size", "hs_section_name"], ascending=[True, False, True])
            .drop_duplicates("hs6")
            .drop(columns="size")
        )
    return map_df


def _compute_2014_weights(
    share_df: pd.DataFrame, hs6_imports_2014: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    hs6_section = _build_hs6_section_map(share_df)
    weights = hs6_imports_2014.merge(hs6_section, on="hs6", how="inner")

    section_totals = (
        weights.groupby("hs_section_name", as_index=False)["m2014_import_china"]
        .sum()
        .rename(columns={"m2014_import_china": "section_import_2014"})
    )
    weights = weights.merge(section_totals, on="hs_section_name", how="left")
    weights["weight_2014"] = weights["m2014_import_china"] / weights["section_import_2014"]

    return weights[["hs6", "hs_section_name", "weight_2014"]], hs6_section


def _weighted_section_series(share_df: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
    merged = share_df.merge(weights, on=["hs6", "hs_section_name"], how="left")
    merged["weight_2014"] = merged["weight_2014"].fillna(0.0)
    merged["weighted_share"] = merged["share_rerouted"] * merged["weight_2014"]

    out = (
        merged.groupby(["hs_section_name", "date", "year", "month"], as_index=False)["weighted_share"]
        .sum()
        .rename(columns={"weighted_share": "weighted_share_rerouted"})
    )
    return out


def _plot_weighted_series(df: pd.DataFrame, fig_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(13, 7))
    sns.lineplot(
        data=df,
        x="date",
        y="weighted_share_rerouted",
        hue="hs_section_name",
        linewidth=1.6,
        ax=ax,
    )
    ax.set_title("HS Section Weighted share_rerouted (Fixed 2014 China Import Weights)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Weighted share_rerouted")
    ax.grid(alpha=0.2)
    ax.legend(
        title="HS Section",
        bbox_to_anchor=(1.02, 1.0),
        loc="upper left",
        borderaxespad=0,
        fontsize=8,
        title_fontsize=9,
        frameon=False,
    )
    fig.tight_layout()
    fig.savefig(fig_path, dpi=300)
    plt.close(fig)


def _extra_stats(share_df: pd.DataFrame, weighted_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    year_stats = (
        share_df.groupby("year")
        .agg(
            n_obs=("share_rerouted", "size"),
            share_nonzero=("share_rerouted", lambda x: (x > 0).mean()),
            mean_share=("share_rerouted", "mean"),
            median_share=("share_rerouted", "median"),
            p90=("share_rerouted", lambda x: x.quantile(0.9)),
            p10=("share_rerouted", lambda x: x.quantile(0.1)),
        )
        .reset_index()
    )
    year_stats["p90_p10_spread"] = year_stats["p90"] - year_stats["p10"]

    weighted_top10 = (
        weighted_df.assign(year=weighted_df["date"].dt.year, month=weighted_df["date"].dt.month)
        .groupby(["hs_section_name", "year", "month"], as_index=False)["weighted_share_rerouted"]
        .sum()
    )

    return year_stats, weighted_top10


def _diagnostics(
    share_df: pd.DataFrame,
    hs6_section_map: pd.DataFrame,
    imports_2014: pd.DataFrame,
    weights_df: pd.DataFrame,
) -> pd.DataFrame:
    share_hs6 = set(share_df["hs6"].unique())
    import_hs6 = set(imports_2014["hs6"].unique())
    matched_hs6 = set(weights_df["hs6"].unique())

    checks = [
        {"check": "share_rows", "value": float(len(share_df))},
        {"check": "share_hs6_unique", "value": float(len(share_hs6))},
        {"check": "import_hs6_unique_2014_china", "value": float(len(import_hs6))},
        {"check": "matched_hs6", "value": float(len(matched_hs6))},
        {"check": "share_hs6_coverage_rate", "value": float(len(matched_hs6) / max(len(share_hs6), 1))},
        {"check": "import_hs6_coverage_rate", "value": float(len(matched_hs6) / max(len(import_hs6), 1))},
    ]

    weight_sums = (
        weights_df.groupby("hs_section_name", as_index=False)["weight_2014"]
        .sum()
        .rename(columns={"weight_2014": "weight_sum"})
    )
    for _, row in weight_sums.iterrows():
        checks.append(
            {
                "check": f"weight_sum_{row['hs_section_name']}",
                "value": float(row["weight_sum"]),
            }
        )

    return pd.DataFrame(checks), hs6_section_map


def main() -> None:
    root = _repo_root()
    fig_dir, out_dir = _ensure_dirs(root)

    share_path = root / "data" / "rerouted_shares" / "data_share_rerouted.dta"
    flow_path = root / "data" / "m_flow_hs10_fm_new.dta"

    share_df = _load_share_data(share_path)

    annual_pct = _annual_percentiles(share_df)
    annual_pct.to_csv(out_dir / "annual_percentiles_share_rerouted.csv", index=False)
    _plot_percentiles(annual_pct, fig_dir / "share_rerouted_annual_percentiles.png")

    section_median = _section_year_median(share_df)
    section_median.to_csv(out_dir / "annual_section_median_share_rerouted.csv", index=False)
    _plot_section_median(section_median, fig_dir / "share_rerouted_section_median_by_year.png")

    imports_2014 = _load_2014_china_hs6_imports(flow_path)
    weights_df, hs6_section_map = _compute_2014_weights(share_df, imports_2014)
    weights_df.to_csv(out_dir / "weights_hs6_section_2014_china_imports.csv", index=False)

    weighted_series = _weighted_section_series(share_df, weights_df)
    weighted_series.to_csv(out_dir / "weighted_share_rerouted_by_section_month.csv", index=False)
    _plot_weighted_series(weighted_series, fig_dir / "share_rerouted_weighted_by_section_month_import_weights2014.png")

    diag_df, hs6_map_df = _diagnostics(share_df, hs6_section_map, imports_2014, weights_df)
    diag_df.to_csv(out_dir / "diagnostics.csv", index=False)
    hs6_map_df.to_csv(out_dir / "hs6_to_section_map_used.csv", index=False)

    year_stats, weighted_top10 = _extra_stats(share_df, weighted_series)
    year_stats.to_csv(out_dir / "extra_annual_summary_stats.csv", index=False)
    weighted_top10.to_csv(out_dir / "extra_weighted_section_month_totals.csv", index=False)

    print(f"Figures saved to: {fig_dir}")
    print(f"Tables saved to: {out_dir}")


if __name__ == "__main__":
    main()
