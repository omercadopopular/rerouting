import json
from pathlib import Path
import shutil
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.config import PipelineConfig
from passthru_data.section301_regression_sensitivity_v3 import (
    VARIANTS,
    _duplicate_source_variant,
    _expected_fit_count,
    _build_master_panel,
    _prepare_event_frame,
    run_section301_regression_sensitivity,
)
from passthru_data.section301_regression_sensitivity_v4 import (
    VARIANTS as VARIANTS_V4,
    _bridge_master as _bridge_master_v4,
    _target_column as _target_column_v4,
)


def _config(tmp_path: Path) -> PipelineConfig:
    data_root = tmp_path / "data"
    cfg = PipelineConfig(
        repo_root=tmp_path,
        raw_dir=data_root / "raw" / "passthru_data",
        staging_dir=data_root / "staging" / "passthru_data",
        reference_dir=data_root / "reference" / "passthru_data",
        analysis_dir=data_root / "analysis" / "passthru_data",
        verification_dir=data_root / "verification" / "passthru_data",
        fajgelbaum_root=data_root / "fajgelbaum",
        fajgelbaum_analysis_dir=data_root / "fajgelbaum" / "data" / "analysis",
        manual_input_dir=data_root / "raw" / "passthru_data" / "manual",
        logs_dir=data_root / "verification" / "passthru_data" / "logs",
    )
    cfg.ensure_directories()
    cfg.fajgelbaum_analysis_dir.mkdir(parents=True, exist_ok=True)
    return cfg


def _stata_month_index(year: int, month: int) -> int:
    return year * 12 + month - 1


def _tiny_master() -> pd.DataFrame:
    rows = [
        {
            "cty_code": 5700,
            "hs10": "0101210000",
            "year": 2018,
            "month": 2,
            "mdate_index": _stata_month_index(2018, 2),
            "naics_str": "111100",
            "naics4": "1111",
            "naics3": "111",
            "naics2": "11",
            "pkg_m_val": 1000.0,
            "pkg_m_q1": 100.0,
            "pkg_m_p": 10.0,
            "pkg_m_pduty": 12.0,
            "pkg_m_stattariff2": 0.20,
            "pkg_m_status2": 0,
            "pkg_m_ess": 2,
            "pkg_m_china_hit": 1,
            "pkg_first_active_mdate2": pd.NA,
            "pkg_paper_first_active_mdate2": pd.NA,
            "raw_m_val": 1000.0,
            "raw_m_q1": 100.0,
            "raw_m_p": 10.0,
            "raw_m_pduty": 13.0,
            "raw_m_stattariff2": 0.25,
            "raw_tw_increment_rate_raw": 0.25,
            "raw_tw_active_share_raw": 0.50,
            "raw_first_active_mdate2": _stata_month_index(2018, 2),
            "raw_paper_first_active_mdate2": _stata_month_index(2018, 3),
        },
        {
            "cty_code": 5700,
            "hs10": "0101210000",
            "year": 2018,
            "month": 3,
            "mdate_index": _stata_month_index(2018, 3),
            "naics_str": "111100",
            "naics4": "1111",
            "naics3": "111",
            "naics2": "11",
            "pkg_m_val": 1020.0,
            "pkg_m_q1": 102.0,
            "pkg_m_p": 10.0,
            "pkg_m_pduty": 12.0,
            "pkg_m_stattariff2": 0.20,
            "pkg_m_status2": 2,
            "pkg_m_ess": 2,
            "pkg_m_china_hit": 1,
            "pkg_first_active_mdate2": _stata_month_index(2018, 3),
            "pkg_paper_first_active_mdate2": _stata_month_index(2018, 3),
            "raw_m_val": 1020.0,
            "raw_m_q1": 102.0,
            "raw_m_p": 10.0,
            "raw_m_pduty": 13.0,
            "raw_m_stattariff2": 0.25,
            "raw_tw_increment_rate_raw": 0.25,
            "raw_tw_active_share_raw": 1.00,
            "raw_first_active_mdate2": _stata_month_index(2018, 2),
            "raw_paper_first_active_mdate2": _stata_month_index(2018, 3),
        },
        {
            "cty_code": 1240,
            "hs10": "0101210000",
            "year": 2018,
            "month": 2,
            "mdate_index": _stata_month_index(2018, 2),
            "naics_str": "111100",
            "naics4": "1111",
            "naics3": "111",
            "naics2": "11",
            "pkg_m_val": 2000.0,
            "pkg_m_q1": 200.0,
            "pkg_m_p": 10.0,
            "pkg_m_pduty": 10.5,
            "pkg_m_stattariff2": 0.05,
            "pkg_m_status2": 0,
            "pkg_m_ess": 0,
            "pkg_m_china_hit": 0,
            "pkg_first_active_mdate2": pd.NA,
            "pkg_paper_first_active_mdate2": pd.NA,
            "raw_m_val": 2000.0,
            "raw_m_q1": 200.0,
            "raw_m_p": 10.0,
            "raw_m_pduty": 10.5,
            "raw_m_stattariff2": 0.05,
            "raw_tw_increment_rate_raw": 0.00,
            "raw_tw_active_share_raw": 0.00,
            "raw_first_active_mdate2": pd.NA,
            "raw_paper_first_active_mdate2": pd.NA,
        },
        {
            "cty_code": 1240,
            "hs10": "0101210000",
            "year": 2018,
            "month": 3,
            "mdate_index": _stata_month_index(2018, 3),
            "naics_str": "111100",
            "naics4": "1111",
            "naics3": "111",
            "naics2": "11",
            "pkg_m_val": 2020.0,
            "pkg_m_q1": 202.0,
            "pkg_m_p": 10.0,
            "pkg_m_pduty": 10.5,
            "pkg_m_stattariff2": 0.05,
            "pkg_m_status2": 0,
            "pkg_m_ess": 0,
            "pkg_m_china_hit": 0,
            "pkg_first_active_mdate2": pd.NA,
            "pkg_paper_first_active_mdate2": pd.NA,
            "raw_m_val": 2020.0,
            "raw_m_q1": 202.0,
            "raw_m_p": 10.0,
            "raw_m_pduty": 10.5,
            "raw_m_stattariff2": 0.05,
            "raw_tw_increment_rate_raw": 0.00,
            "raw_tw_active_share_raw": 0.00,
            "raw_first_active_mdate2": pd.NA,
            "raw_paper_first_active_mdate2": pd.NA,
        },
    ]
    frame = pd.DataFrame(rows)
    frame["id"] = pd.factorize(frame["cty_code"].astype(str) + "|" + frame["hs10"], sort=False)[0]
    frame["ct"] = pd.factorize(frame["cty_code"].astype(str) + "|" + frame["mdate_index"].astype(str), sort=False)[0]
    frame["ht"] = pd.factorize(frame["hs10"].astype(str) + "|" + frame["mdate_index"].astype(str), sort=False)[0]
    return frame


def test_prepare_event_frame_fills_frozen_package_calendar_for_raw_only_rows() -> None:
    master = _tiny_master().copy()
    master.loc[(master["cty_code"].eq(1240)) & (master["month"].eq(2)), "pkg_first_active_mdate2"] = _stata_month_index(2018, 3)
    master.loc[(master["cty_code"].eq(1240)) & (master["month"].eq(3)), "pkg_first_active_mdate2"] = pd.NA
    master.loc[(master["cty_code"].eq(1240)) & (master["month"].eq(2)), "pkg_m_status2"] = 0
    master.loc[(master["cty_code"].eq(1240)) & (master["month"].eq(3)), "pkg_m_status2"] = 0
    variant = next(spec for spec in VARIANTS if spec.code == "C_map_only")
    frame, meta = _prepare_event_frame(master, variant, "paper_6m", "val")
    assert meta["calendar_col"] == "d_index"
    assert set(frame["cty_code"].tolist()) == {5700, 1240}
    assert frame.loc[frame["cty_code"].eq(1240), "event_time"].notna().all()


def test_prepare_event_frame_tariff_switching_only_affects_duty_outcome() -> None:
    master = _tiny_master().copy()
    variant_pkg = next(spec for spec in VARIANTS if spec.code == "C_legal")
    variant_raw = next(spec for spec in VARIANTS if spec.code == "D_legal")
    frame_val_pkg, _ = _prepare_event_frame(master, variant_pkg, "paper_6m", "val")
    frame_val_raw, _ = _prepare_event_frame(master, variant_raw, "paper_6m", "val")
    frame_pduty_pkg, _ = _prepare_event_frame(master, variant_pkg, "paper_6m", "pduty")
    frame_pduty_raw, _ = _prepare_event_frame(master, variant_raw, "paper_6m", "pduty")
    assert frame_val_pkg["l_outcome"].equals(frame_val_raw["l_outcome"])
    assert not frame_pduty_pkg["l_outcome"].equals(frame_pduty_raw["l_outcome"])


def test_duplicate_source_variants_skip_non_duty_d_regressions() -> None:
    assert _duplicate_source_variant(next(spec for spec in VARIANTS if spec.code == "D_legal"), "val") == "C_legal"
    assert _duplicate_source_variant(next(spec for spec in VARIANTS if spec.code == "D_paper"), "q1") == "C_paper"
    assert _duplicate_source_variant(next(spec for spec in VARIANTS if spec.code == "D_legal"), "pduty") is None
    assert _expected_fit_count() == 60


def test_v4_pooled_target_is_not_china_restricted() -> None:
    master = _tiny_master().copy()
    master["pkg_pair_target_all"] = 0
    master["pkg_pair_target"] = 0
    master.loc[master["cty_code"].eq(5700), "pkg_pair_target_all"] = 1
    master.loc[master["cty_code"].eq(5700), "pkg_m_china_hit"] = 0
    pooled = next(spec for spec in VARIANTS_V4 if spec.code == "A")
    assert _target_column_v4(master, pooled, analysis="pooled_outcome_bridge").sum() > 0
    assert _target_column_v4(master, pooled, analysis="china301_policy_bridge").sum() == 0


def test_v4_raw_target_requires_990388_rule() -> None:
    master = _tiny_master().copy()
    master["raw_tw_rule_code_raw"] = "99038002"
    master["raw_pair_target"] = 0
    raw_variant = next(spec for spec in VARIANTS_V4 if spec.code == "C_legal")
    assert _target_column_v4(master, raw_variant, analysis="china301_policy_bridge").sum() == 0
    master["raw_tw_rule_code_raw"] = "99038803"
    master["raw_pair_target"] = 1
    assert _target_column_v4(master, raw_variant, analysis="china301_policy_bridge").sum() > 0


def test_v4_china_bridge_excludes_cross_family_pairs() -> None:
    master = _tiny_master().copy()
    master["pkg_pair_target"] = 1
    master["raw_pair_target"] = 1
    master["pkg_cross_family_pair"] = 0
    master["raw_cross_family_pair"] = 0
    master.loc[master["cty_code"].eq(5700), "pkg_cross_family_pair"] = 1
    scoped = _bridge_master_v4(master, "china301_policy_bridge")
    assert scoped.loc[scoped["cty_code"].eq(5700)].empty


def test_section301_regression_sensitivity_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    package_frame = _tiny_master()[[
        "cty_code",
        "hs10",
        "year",
        "month",
        "pkg_m_val",
        "pkg_m_q1",
        "pkg_m_p",
        "pkg_m_pduty",
        "pkg_m_stattariff2",
        "pkg_m_status2",
        "pkg_m_ess",
        "pkg_m_china_hit",
        "pkg_first_active_mdate2",
        "pkg_paper_first_active_mdate2",
        "naics_str",
    ]].copy()
    raw_frame = _tiny_master()[[
        "cty_code",
        "hs10",
        "year",
        "month",
        "raw_m_val",
        "raw_m_q1",
        "raw_m_p",
        "raw_m_pduty",
        "raw_m_stattariff2",
        "raw_tw_increment_rate_raw",
        "raw_tw_active_share_raw",
        "raw_first_active_mdate2",
        "raw_paper_first_active_mdate2",
        "naics_str",
    ]].copy()
    package_path = cfg.fajgelbaum_analysis_dir / "m_flow_hs10_fm_new.dta"
    raw_path = cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    package_path.parent.mkdir(parents=True, exist_ok=True)
    package_path.write_bytes(b"stub")
    package_frame.to_parquet(cfg.verification_dir / "package_cache_stub.parquet", index=False)
    raw_frame.to_parquet(raw_path, index=False)
    package_meta = {
        "source_path": str(package_path),
        "sha256": "package",
        "size": 1,
        "modified_time": "2026-07-12T00:00:00+00:00",
        "rows": int(len(package_frame)),
        "treated_rows": int(package_frame["pkg_m_status2"].gt(0).sum()),
        "treated_products": int(package_frame.loc[package_frame["pkg_m_status2"].gt(0), "hs10"].nunique()),
        "period_min": "2018-02",
        "period_max": "2019-04",
    }
    raw_meta = {
        "source_path": str(raw_path),
        "sha256": "raw",
        "size": 1,
        "modified_time": "2026-07-12T00:00:00+00:00",
        "rows": int(len(raw_frame)),
        "treated_rows": int(raw_frame["raw_tw_increment_rate_raw"].gt(0).sum()),
        "treated_products": int(raw_frame.loc[raw_frame["raw_tw_increment_rate_raw"].gt(0), "hs10"].nunique()),
        "period_min": "2018-02",
        "period_max": "2019-04",
    }

    monkeypatch.setattr("passthru_data.section301_regression_sensitivity_v3._build_package_paper_window_cache", lambda _cfg, overwrite=False: (cfg.verification_dir / "package_cache_stub.parquet", package_meta))
    monkeypatch.setattr("passthru_data.section301_regression_sensitivity_v3._build_raw_paper_window_cache", lambda _cfg, overwrite=False: (raw_path, raw_meta))
    monkeypatch.setattr("passthru_data.section301_regression_sensitivity_v3._build_master_panel", lambda _cfg, _p, _r: _tiny_master())
    def _fake_preflight(_cfg, _package_meta, _raw_meta, _package_path, _raw_path):
        preflight_csv = cfg.verification_dir / "preflight.csv"
        preflight_json = cfg.verification_dir / "preflight.json"
        pd.DataFrame(
            [
                {"role": "package_dta_source", "valid_for_run": True, "failure_reason": None},
                {"role": "current_panel_raw_source", "valid_for_run": True, "failure_reason": None},
                {"role": "package_cache", "valid_for_run": True, "failure_reason": None},
                {"role": "raw_cache", "valid_for_run": True, "failure_reason": None},
            ]
        ).to_csv(preflight_csv, index=False)
        preflight_json.write_text(json.dumps({"version": "v3", "records": []}), encoding="utf-8")
        return preflight_csv, preflight_json, [
            {"role": "package_dta_source", "valid_for_run": True, "failure_reason": None},
            {"role": "current_panel_raw_source", "valid_for_run": True, "failure_reason": None},
            {"role": "package_cache", "valid_for_run": True, "failure_reason": None},
            {"role": "raw_cache", "valid_for_run": True, "failure_reason": None},
        ]

    monkeypatch.setattr("passthru_data.section301_regression_sensitivity_v3._write_preflight", _fake_preflight)

    class FakeFit:
        def __init__(self, terms: list[str], nobs: int) -> None:
            self._terms = terms
            self._N = nobs
            self._r2 = 0.42

        def tidy(self) -> pd.DataFrame:
            rows = []
            for idx, term in enumerate(self._terms):
                rows.append({"Coefficient": term, "Estimate": 0.1 * (idx + 1), "Std. Error": 0.01, "2.5%": 0.1 * (idx + 1) - 0.02, "97.5%": 0.1 * (idx + 1) + 0.02})
            return pd.DataFrame(rows).set_index("Coefficient")

    def fake_feols(formula, data, **kwargs):  # noqa: ANN001
        rhs = formula.split("~", 1)[1].split("|", 1)[0]
        terms = [term.strip() for term in rhs.split("+") if term.strip()]
        return FakeFit(terms, len(data))

    monkeypatch.setattr("passthru_data.section301_regression_sensitivity_v3.pf.feols", fake_feols)

    result = run_section301_regression_sensitivity(cfg)
    coeffs = pd.read_csv(result["coefficients_path"])
    summary = json.loads(Path(result["summary_path"]).read_text(encoding="utf-8"))
    preflight = pd.read_csv(summary["preflight_csv"])
    v2_status = json.loads((cfg.verification_dir / "raw_replication_imports" / "v2" / "status.json").read_text(encoding="utf-8"))

    assert "A" in set(coeffs["variant"])
    assert "B" in set(coeffs["variant"])
    assert not bool(summary["ready_for_extension"])
    assert preflight.loc[preflight["role"].eq("package_cache"), "valid_for_run"].item()
    assert preflight.loc[preflight["role"].eq("raw_cache"), "valid_for_run"].item()
    assert v2_status["valid"] is False
