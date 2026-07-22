"""China Section 301 policy registry and current-window import panel builder."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet


@dataclass(frozen=True, slots=True)
class Section301Action:
    """One published Section 301 rate action; scope is resolved from HTS Chapter 99 data."""

    action_id: str
    effective_date: str
    rate: float
    description: str
    source_url: str


# The registry intentionally records policy actions, not a hand-maintained product list.
# Product scope and exclusions are read from the raw HTS revision artifacts already used by
# the bilateral policy builder.
SECTION301_ACTIONS = (
    Section301Action("list1_initial", "2018-07-06", 0.25, "List 1 additional duty", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
    Section301Action("list2_initial", "2018-08-23", 0.25, "List 2 additional duty", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
    Section301Action("list3_initial", "2018-09-24", 0.10, "List 3 initial additional duty", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
    Section301Action("list3_increase", "2019-05-10", 0.25, "List 3 rate increase", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
    Section301Action("list4a_initial", "2019-09-01", 0.15, "List 4A initial additional duty", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
    Section301Action("phase_one_reduction", "2020-02-14", 0.075, "Phase One List 4A rate reduction", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
    Section301Action("four_year_review", "2024-09-27", np.nan, "Four-year review product-specific rate increases", "https://ustr.gov/issue-areas/enforcement/section-301-investigations/tariff-actions"),
)


def section301_action_frame() -> pd.DataFrame:
    """Return a serializable action registry with a normalized effective period."""
    frame = pd.DataFrame([asdict(action) for action in SECTION301_ACTIONS])
    frame["effective_date"] = pd.to_datetime(frame["effective_date"])
    frame["effective_period"] = frame["effective_date"].dt.to_period("M").astype(str)
    return frame


def _month_active_share(effective_date: pd.Timestamp, year: int, month: int) -> float:
    """Share of a calendar month for which an action is in force."""
    period = pd.Period(year=year, month=month, freq="M")
    start = effective_date.to_period("M")
    if period < start:
        return 0.0
    if period > start:
        return 1.0
    days = monthrange(year, month)[1]
    return float((days - effective_date.day + 1) / days)


def _section301_mask(frame: pd.DataFrame) -> pd.Series:
    rule = frame.get("tw_rule_code_raw", pd.Series(pd.NA, index=frame.index, dtype="string"))
    return rule.astype("string").str.replace(r"\D", "", regex=True).str.startswith("990388", na=False)


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return a numeric column or a same-index missing series."""
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _load_policy_panel(config: PipelineConfig) -> tuple[pd.DataFrame, str]:
    full_panel = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    regression_panel = config.analysis_dir / "us_products_partner_hs10_monthly_regression.parquet"
    candidates = [full_panel, regression_panel] if config.analysis_window == "current" else [regression_panel, full_panel]
    for path in candidates:
        if path.exists():
            return read_table(path), str(path)
    raise FileNotFoundError(
        "Section 301 build requires the raw bilateral policy panel. Run the archived HTS policy steps "
        "and build_us_products_partner_hs10_panel first."
    )


def _latest_period_from_rows(frame: pd.DataFrame) -> pd.Period | None:
    valid = frame[["year", "month"]].copy()
    valid["year"] = pd.to_numeric(valid["year"], errors="coerce")
    valid["month"] = pd.to_numeric(valid["month"], errors="coerce")
    valid = valid.dropna().sort_values(["year", "month"])
    if valid.empty:
        return None
    row = valid.iloc[-1]
    return pd.Period(year=int(row["year"]), month=int(row["month"]), freq="M")


def _assert_current_policy_is_fresh(config: PipelineConfig, policy: pd.DataFrame) -> None:
    """Prevent current-window estimates from silently using an old policy snapshot."""
    if config.analysis_window != "current":
        return
    raw_path = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
    if not raw_path.exists():
        return
    raw_period = _latest_period_from_rows(read_table(raw_path, columns=["year", "month"]))
    policy_period = _latest_period_from_rows(policy)
    if raw_period is not None and (policy_period is None or policy_period < raw_period):
        raise RuntimeError(
            "Current Section 301 build refused: the raw bilateral policy panel ends at "
            f"{policy_period}, but the Census import panel ends at {raw_period}. Rebuild the archived "
            "HTS schedule and bilateral policy panel through the discovered Census cutoff first."
        )


def build_section301_import_panel(config: PipelineConfig) -> dict[str, Any]:
    """Materialize a China-only, Section-301-enriched HS10 import panel.

    The output is deliberately separate from the paper workhorse panel: it is suitable for
    current-window estimation while preserving the Fajgelbaum package as a benchmark only.
    """
    policy, source_path = _load_policy_panel(config)
    _assert_current_policy_is_fresh(config, policy)
    required = {"cty_code", "cty_name", "hs10", "year", "month", "m_val", "m_q1"}
    missing = sorted(required - set(policy.columns))
    if missing:
        raise ValueError(f"Raw bilateral policy panel is missing required columns: {missing}")

    out = policy.copy()
    out["cty_name"] = out["cty_name"].astype("string").str.upper()
    out = out.loc[out["cty_name"].eq("CHINA")].copy()
    out["hs10"] = out["hs10"].map(lambda value: normalize_hs_code(value, 10)).astype("string")
    out["hs8"] = out["hs10"].str.slice(0, 8)
    out["hs6"] = out["hs10"].str.slice(0, 6)
    out["hs4"] = out["hs10"].str.slice(0, 4)
    out["hs2"] = out["hs10"].str.slice(0, 2)
    out["m_val"] = pd.to_numeric(out["m_val"], errors="coerce")
    out["m_q1"] = pd.to_numeric(out["m_q1"], errors="coerce")
    out["section301_increment"] = _numeric_column(out, "tw_increment_rate_raw").where(_section301_mask(out), 0.0).fillna(0.0)
    out["section301_active_share"] = _numeric_column(out, "tw_active_share_raw").where(out["section301_increment"].gt(0), 0.0).fillna(0.0)
    out["section301_increment_effective"] = out["section301_increment"] * out["section301_active_share"]
    out["m_stattariff2"] = _numeric_column(out, "m_statutory_tariff2")
    out["m_stattariff1"] = _numeric_column(out, "m_statutory_tariff1")
    out["m_p"] = np.where(out["m_q1"] > 0, out["m_val"] / out["m_q1"], np.nan)
    out["m_pduty"] = out["m_p"] * (1.0 + out["m_stattariff2"].fillna(0.0))
    out["mdate"] = pd.to_datetime(dict(year=pd.to_numeric(out["year"]), month=pd.to_numeric(out["month"]), day=1), errors="coerce")
    out["mdate_index"] = pd.to_numeric(out["year"], errors="coerce") * 12 + pd.to_numeric(out["month"], errors="coerce") - 1
    out["id"] = pd.factorize(out["cty_code"].astype("string") + "|" + out["hs10"], sort=False)[0].astype("int64")
    out["m_ess"] = out.groupby("id", sort=False)["section301_increment"].transform("max").gt(0).astype("int8") * 2
    first_treatment = out.loc[out["section301_increment"].gt(0)].groupby("id")["mdate_index"].min()
    out["m_effective_mdate2"] = out["id"].map(first_treatment).astype("Int64")
    out["m_status2"] = np.where(
        out["m_effective_mdate2"].notna() & out["mdate_index"].ge(out["m_effective_mdate2"]), 2, 0
    ).astype("int8")
    # A transparent fallback until an official HS10-to-NAICS concordance is added.
    out["naics_str"] = out["hs4"].astype("string") + "00"
    out["policy_source"] = out.get("tw_scope_source_raw", pd.Series(pd.NA, index=out.index, dtype="string")).astype("string")
    out = out.sort_values(["hs10", "year", "month"]).reset_index(drop=True)

    if out.loc[out["mdate"].ge(pd.Timestamp("2018-07-01")), "section301_increment"].le(0).all():
        raise RuntimeError(
            "Section 301 build refused: no 9903.88 scope was found after the initial 2018 action. "
            "Rebuild the raw HTS trade-war overlay before estimating Section 301 effects."
        )

    action_path = config.reference_dir / "section301_action_registry.parquet"
    output_path = config.analysis_dir / "section301_imports_hs10.parquet"
    actions = section301_action_frame()
    write_parquet(actions, action_path, overwrite=True)
    write_data_dictionary(actions, action_path.with_suffix(".dictionary.json"), key_columns=["action_id"])
    write_parquet(out, output_path, overwrite=True)
    write_data_dictionary(out, output_path.with_suffix(".dictionary.json"), key_columns=["cty_code", "hs10", "year", "month"])

    observed_periods = set((out.loc[out["section301_increment"].gt(0), "mdate"].dt.to_period("M").astype(str)).dropna())
    expected_actions = actions.loc[actions["effective_date"] <= out["mdate"].max(), ["action_id", "effective_period"]].copy()
    expected_actions["observed_policy_month"] = expected_actions["effective_period"].isin(observed_periods)
    coverage_path = config.verification_dir / "section301_policy_coverage.csv"
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    expected_actions.to_csv(coverage_path, index=False)
    metadata = {
        "source_path": source_path,
        "output_path": str(output_path),
        "registry_path": str(action_path),
        "coverage_path": str(coverage_path),
        "rows": int(len(out)),
        "treated_rows": int(out["section301_increment"].gt(0).sum()),
        "treated_products": int(out.loc[out["m_ess"].eq(2), "hs10"].nunique()),
        "period_min": None if out.empty else str(out["mdate"].min().to_period("M")),
        "period_max": None if out.empty else str(out["mdate"].max().to_period("M")),
        "uncovered_actions": expected_actions.loc[~expected_actions["observed_policy_month"], "action_id"].tolist(),
        "naics_mapping": "hs4_placeholder_pending_official_concordance",
    }
    write_metadata_json(config.analysis_dir / "section301_imports_hs10.metadata.json", metadata)
    return metadata
