
#!/usr/bin/env python3
"""merge_mex_china_imports.py

Reads two Census International Trade CSV outputs:
  * chnoutput.csv  – imports from China
  * mexoutput.csv  – imports from Mexico

Each CSV must contain: I_COMMODITY, period, GEN_VAL_MO
(which is what the data‑download script produces).

Outputs (in current directory):

  * merged_imports.csv            – China & Mexico dollar values + Mexico share
  * mex_share_percentiles.csv     – 10th,25th,50th,75th,90th pct Mexico share per period
  * mex_share_percentiles.png     – Time‑series plot with shaded percentile bands
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

path = r'C:\Users\cbezerradegoes\OneDrive\research\rerouting\code'

# ---------------------------------------------------------------------
# Configuration – edit if your file names/locations differ
# ---------------------------------------------------------------------
CHINA_FILE = Path(path + "\\chnoutput.csv")
MEX_FILE   = Path(path + "\\mexoutput.csv")
OUT_DIR    = Path(path)           # directory for outputs
OUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------
# Load & keep only needed columns
# ---------------------------------------------------------------------
def load_clean(path: Path, tag: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"I_COMMODITY": str})
    keep = [c for c in df.columns if c in ("I_COMMODITY", "period", "GEN_VAL_MO")]
    df = df[keep].rename(columns={"GEN_VAL_MO": f"{tag}_val"})
    return df

china = load_clean(CHINA_FILE, "china")
mex   = load_clean(MEX_FILE,   "mex")

# ---------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------
merged = pd.merge(china, mex, on=["I_COMMODITY", "period"], how="outer").fillna(0)
merged["total_val"] = merged["china_val"] + merged["mex_val"]
merged["mex_share"] = merged["mex_val"] / merged["total_val"].where(merged["total_val"] > 0)

merged.to_csv(OUT_DIR / "merged_imports.csv", index=False)

# ---------------------------------------------------------------------
# Percentiles
# ---------------------------------------------------------------------
percentiles = (
    merged.groupby("period")["mex_share"]
          .quantile([0.10, 0.25, 0.50, 0.75, 0.90])
          .unstack()
          .rename(columns={0.10: "p10", 0.25: "p25", 0.50: "p50", 0.75: "p75", 0.90: "p90"})
          .sort_index()
)
#percentiles.to_csv(OUT_DIR / "mex_share_percentiles.csv")
percentiles['mean'] = merged.groupby("period")["mex_share"].mean()

# ---------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------
# Convert period strings to datetime for plotting
periods_dt = pd.to_datetime([p if len(p)==7 else f"{p}-01" for p in percentiles.index])

fig, ax = plt.subplots(figsize=(10,5))
ax.axhline(0)
ax.axvline(pd.to_datetime("2018-07-01"), color='black', linestyle=':')
ax.plot(periods_dt, percentiles["mean"], label="Mean")
ax.plot(periods_dt, percentiles["p50"], label="Median")
#ax.fill_between(periods_dt, percentiles["p25"], percentiles["p50"], alpha=0.3, label="25th-50th")
#ax.fill_between(periods_dt, percentiles["p50"], percentiles["p75"], alpha=0.3, label="50th-75th")
#ax.fill_between(periods_dt, percentiles["p10"], percentiles["p90"], alpha=0.15, label="10th-90th")

ax.set_ylabel("Mexico share of imports")
ax.set_xlabel("Time")
ax.set_title("Distribution of Mexico's Share in China+Mexico US Imports (HS10 level)")
ax.legend()
fig.tight_layout()
plt.show()

#fig.savefig(OUT_DIR / "mex_share_percentiles.png", dpi=300)

print("Outputs written to:", OUT_DIR.resolve())



def add_cum_change(df: pd.DataFrame,
                   initial_period: str = "2018-07",
                   period_col: str = "period",
                   group_col: str = "I_COMMODITY",
                   share_col: str = "mex_share",
                   out_col: str = "cum_change") -> pd.DataFrame:
    """
    For each HS-10 code, compute the change in `share_col`
    relative to the same period one year earlier.

    Returns the original DataFrame with an extra `out_col`.
    Rows that have no matching lag (e.g., series starts < 12 months ago)
    get NaN in `out_col`.
    """
    # Ensure period column is real datetime for robust alignment
    df = df.copy()
    df["_dt"] = pd.to_datetime(
        df[period_col].where(df[period_col].str.len() == 7,
                             df[period_col] + "-01")
    )

    # Sort for correct alignment
    df = df.sort_values([group_col, "_dt"])

    # Compute year-ago value within each HS-10 series
    df[f"{share_col}_lag"] = (
        df.set_index(group_col)
          .groupby(level=0)[share_col]
          .shift(12)
          .values
    )

    # Compute baseline value for share_col    
    df = df.merge(df[df[period_col] == initial_period][[group_col, share_col]],
             on=group_col,
             how='left',
             suffixes=('', '_base'))

    # YoY change
    df[out_col] = df[share_col] - df[f"{share_col}_base"]

    # Clean up helper columns
    df.drop(columns=["_dt", f"{share_col}_lag", f"{share_col}_base"], inplace=True)
    return df

merged = add_cum_change(merged)

percentiles = (
    merged.groupby("period")["cum_change"]
          .quantile([0.10, 0.25, 0.50, 0.75, 0.90])
          .unstack()
          .rename(columns={0.10: "p10", 0.25: "p25", 0.50: "p50", 0.75: "p75", 0.90: "p90"})
          .sort_index()
)

percentiles['mean'] = merged.groupby("period")["cum_change"].mean()



# ---------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------
# Convert period strings to datetime for plotting
periods_dt = pd.to_datetime([p if len(p)==7 else f"{p}-01" for p in percentiles.index])

fig, ax = plt.subplots(figsize=(10,5))
ax.axhline(0)
ax.axvline(pd.to_datetime("2018-07-01"), color='black', linestyle=':')
ax.plot(periods_dt, percentiles["mean"], label="Mean")
#ax.plot(periods_dt, percentiles["p50"], label="Median")
#ax.fill_between(periods_dt, percentiles["p25"], percentiles["p75"], alpha=0.3, label="25th-75th")
ax.fill_between(periods_dt, percentiles["p10"], percentiles["p90"], alpha=0.15, label="10th-90th")

ax.set_ylabel("Cumulative change in mexico share of imports")
ax.set_xlabel("Time")
ax.set_title("Distribution of Mexico's Share in China+Mexico US Imports (HS10 level)")
ax.legend()
fig.tight_layout()
plt.show()


#------------------------

last_slide_df = merged[merged.period == merged.period.iloc[-1]]

def plot_kdensity(df: pd.DataFrame, var: str):       
    
    # Filter for years
    data = df[f'{var}'].dropna()

    # Create the plot
    plt.figure(figsize=(8, 6))
    sns.kdeplot(data, bw_adjust=1)

    plt.title("")
    plt.xlabel("")
    plt.legend()

    # Save plot
#    output_path = f"figs/emp_{var}_shr_f_{year}.pdf"
#    os.makedirs("figs", exist_ok=True)
#    plt.savefig(output_path, format='pdf')
    plt.show()

plot_kdensity(last_slide_df, 'cum_change')
