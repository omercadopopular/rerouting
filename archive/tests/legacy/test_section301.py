from pathlib import Path
import zipfile
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scr"
if str(SCR) not in sys.path:
    sys.path.insert(0, str(SCR))

from passthru_data.config import PipelineConfig
from passthru_data.build_us_products_partner_panel import _load_raw_tradewar_overlay, _load_tradewar_pdf_csv_links, _load_tradewar_rule_attributes
from passthru_data.section301 import _month_active_share, build_section301_import_panel, section301_action_frame


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
        analysis_window="current",
    )
    cfg.ensure_directories()
    return cfg


def test_month_active_share_uses_inclusive_effective_day() -> None:
    assert _month_active_share(pd.Timestamp("2018-07-06"), 2018, 6) == 0.0
    assert _month_active_share(pd.Timestamp("2018-07-06"), 2018, 8) == 1.0
    assert _month_active_share(pd.Timestamp("2018-07-06"), 2018, 7) == 26 / 31


def test_registry_contains_published_initial_2018_actions() -> None:
    actions = section301_action_frame()
    assert {"list1_initial", "list2_initial", "list3_initial"}.issubset(set(actions["action_id"]))
    assert actions.loc[actions["action_id"] == "list1_initial", "effective_period"].item() == "2018-07"


def test_build_section301_import_panel_filters_china_and_flags_990388(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    source = pd.DataFrame(
        {
            "cty_code": [5700, 5700, 2010],
            "cty_name": ["CHINA", "CHINA", "MEXICO"],
            "hs10": ["0101210000", "0101210000", "0101210000"],
            "year": [2018, 2018, 2018],
            "month": [6, 7, 7],
            "m_val": [100.0, 110.0, 20.0],
            "m_q1": [10.0, 10.0, 2.0],
            "tw_rule_code_raw": [pd.NA, "99038801", "99038801"],
            "tw_increment_rate_raw": [pd.NA, 0.25, 0.25],
            "tw_active_share_raw": [pd.NA, 26 / 31, 26 / 31],
            "m_statutory_tariff1": [0.02, 0.27, 0.27],
            "m_statutory_tariff2": [0.02, 0.02 + 0.25 * (26 / 31), 0.02 + 0.25 * (26 / 31)],
            "tw_scope_source_raw": [pd.NA, "machine", "machine"],
        }
    )
    source.to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly_regression.parquet", index=False)
    metadata = build_section301_import_panel(cfg)
    result = pd.read_parquet(cfg.analysis_dir / "section301_imports_hs10.parquet")
    assert metadata["treated_rows"] == 1
    assert len(result) == 2
    assert result["m_ess"].eq(2).all()
    assert result.loc[result["month"] == 7, "section301_increment_effective"].item() == 0.25 * (26 / 31)


def test_current_build_rejects_stale_policy_panel(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    pd.DataFrame({"year": [2025], "month": [12]}).to_parquet(cfg.analysis_dir / "m_flow_hs10_fm_new.parquet", index=False)
    pd.DataFrame(
        {
            "cty_code": [5700], "cty_name": ["CHINA"], "hs10": ["0101210000"],
            "year": [2019], "month": [12], "m_val": [1.0], "m_q1": [1.0],
        }
    ).to_parquet(cfg.analysis_dir / "us_products_partner_hs10_monthly.parquet", index=False)
    with pytest.raises(RuntimeError, match="ends at 2019-12"):
        build_section301_import_panel(cfg)


def test_raw_tradewar_overlay_prefers_complete_link_rows_and_carries_china_301_months(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    machine_links = pd.DataFrame(
        {
            "release_name": ["2019HTSABASICA"],
            "release_start_date": [pd.NaT],
            "release_end_date": [pd.NaT],
            "hs8": ["84818090"],
            "rule_code": ["99038801"],
        }
    )
    reference_links = pd.DataFrame(
        {
            "release_name": ["2019HTSABASICA"],
            "release_start_date": [pd.Timestamp("2019-03-25")],
            "release_end_date": [pd.Timestamp("2019-04-18")],
            "hs8": ["84818090"],
            "rule_code": ["99038801"],
        }
    )
    rule_attrs = pd.DataFrame(
        {
            "rule_code": ["99038801", "99038801"],
            "year": [2019, 2019],
            "month": [3, 4],
            "increment_rate": [0.25, 0.25],
            "description": ["List 1", "List 1"],
        }
    )

    empty_links = pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    empty_overrides = pd.DataFrame(
        columns=[
            "cty_name",
            "hs8",
            "year",
            "month",
            "tw_increment_rate_raw",
            "tw_rule_code_raw",
            "tw_active_share_raw",
            "tw_scope_source_raw",
        ]
    )

    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_machine_links", lambda _cfg: machine_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_pdf_csv_links", lambda _cfg: empty_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_pdf_links", lambda _cfg: empty_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_reference_tradewar_links", lambda _cfg, _filename, _prefixes: reference_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_rule_attributes", lambda _cfg: rule_attrs)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_manual_tradewar_overrides", lambda _cfg: empty_overrides)

    overlay = _load_raw_tradewar_overlay(cfg, pd.Series(["CHINA", "MEXICO"]))

    china = overlay.loc[overlay["cty_name"].eq("CHINA") & overlay["hs8"].eq("84818090")].copy()
    assert set(china["month"].astype(int).tolist()) == {3, 4}
    assert china["tw_increment_rate_raw"].notna().all()
    assert set(overlay["cty_name"].astype(str).unique().tolist()) == {"CHINA"}


def test_raw_tradewar_overlay_uses_effective_date_range_for_partial_months(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    machine_links = pd.DataFrame(
        {
            "release_name": ["2019HTSABASICA"],
            "release_start_date": [pd.Timestamp("2019-05-10")],
            "release_end_date": [pd.Timestamp("2019-05-31")],
            "hs8": ["84818090"],
            "rule_code": ["99038804"],
        }
    )
    reference_links = pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    rule_attrs = pd.DataFrame(
        {
            "rule_code": ["99038804", "99038804"],
            "year": [2019, 2019],
            "month": [3, 4],
            "increment_rate": [0.25, 0.25],
            "description": ["List 3", "List 3"],
            "effective_start": [pd.Timestamp("2019-03-25"), pd.Timestamp("2019-03-25")],
            "effective_end": [pd.Timestamp("2019-04-18"), pd.Timestamp("2019-04-18")],
        }
    )

    empty_links = pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    empty_overrides = pd.DataFrame(
        columns=[
            "cty_name",
            "hs8",
            "year",
            "month",
            "tw_increment_rate_raw",
            "tw_rule_code_raw",
            "tw_active_share_raw",
            "tw_scope_source_raw",
        ]
    )

    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_machine_links", lambda _cfg: machine_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_pdf_csv_links", lambda _cfg: empty_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_pdf_links", lambda _cfg: empty_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_reference_tradewar_links", lambda _cfg, _filename, _prefixes: reference_links)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_tradewar_rule_attributes", lambda _cfg: rule_attrs)
    monkeypatch.setattr("passthru_data.build_us_products_partner_panel._load_manual_tradewar_overrides", lambda _cfg: empty_overrides)

    overlay = _load_raw_tradewar_overlay(cfg, pd.Series(["CHINA"]))

    china = overlay.loc[overlay["cty_name"].eq("CHINA") & overlay["hs8"].eq("84818090")].copy()
    assert set(china["month"].astype(int).tolist()) == {3, 4}
    march_share = china.loc[china["month"].astype(int).eq(3), "tw_active_share_raw"].item()
    april_share = china.loc[china["month"].astype(int).eq(4), "tw_active_share_raw"].item()
    assert march_share == 7 / 31
    assert april_share == 18 / 30


def test_rule_attribute_loader_scans_all_text_members_in_archive(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    annual_dir = cfg.raw_dir / "policy" / "annual"
    annual_dir.mkdir(parents=True, exist_ok=True)
    zip_path = annual_dir / "tariff_data_2019.zip"
    with zipfile.ZipFile(zip_path, mode="w") as handle:
        handle.writestr(
            "first.txt",
            "hts8,mfn_text_rate,mfn_ad_val_rate,brief_description,begin_effect_date,end_effective_date\n"
            "00000000,0%,0.00,Irrelevant,2019-01-01,2019-12-31\n",
        )
        handle.writestr(
            "second.txt",
            "hts8,mfn_text_rate,mfn_ad_val_rate,brief_description,begin_effect_date,end_effective_date\n"
            "99038805,25%,0.25,China 301,2019-02-21,2019-03-25\n"
            "99038812,10%,0.10,China 301,2019-04-18,2019-05-09\n",
        )

    rule_attrs = _load_tradewar_rule_attributes(cfg)
    codes = set(rule_attrs["rule_code"].astype(str).unique().tolist())
    assert {"99038805", "99038812"}.issubset(codes)


def test_pdf_csv_loader_uses_nearby_notes_fallback_for_chapter_99_rows(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    extract_dir = cfg.staging_dir / "policy" / "pdf_extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "hs_code": ["9903.88.04", "8481.80.90"],
            "page": [3510, 3469],
            "description_blob": [
                "Articles the product of China, as provided for in U.S. note 20(g) to this subchapter",
                "U.S. Notes (con.)",
            ],
            "context_excerpt": [
                "9903.88.04 | The duty | provided in the | applicable | subheading + | 25%",
                "8481.80.90 | 8481.80.50 | 8483.20.80 | 8483.20.40",
            ],
        }
    ).to_csv(extract_dir / "2019HTSARev10_extracted_rows.csv", index=False)

    monkeypatch.setattr(
        "passthru_data.build_us_products_partner_panel._load_tradewar_release_catalog",
        lambda _cfg: pd.DataFrame(
            {
                "release_name": ["2019HTSARev10"],
                "year": [2019],
                "release_start_date": [pd.Timestamp("2019-05-10")],
                "release_end_date": [pd.Timestamp("2050-12-31")],
            }
        ),
    )

    links = _load_tradewar_pdf_csv_links(cfg, include_candidate_fallback=True)
    sub = links.loc[links["rule_code"].astype(str).eq("99038804")]
    assert "84818090" in set(sub["hs8"].astype(str).tolist())


def test_pdf_csv_loader_uses_nearby_notes_fallback_for_early_china_301_rows(tmp_path: Path, monkeypatch) -> None:
    cfg = _config(tmp_path)
    extract_dir = cfg.staging_dir / "policy" / "pdf_extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "hs_code": ["9903.88.01", "8501.64.00", "8501.63.00"],
            "page": [3337, 3314, 3314],
            "description_blob": [
                "Articles the product of China, as enumerated in note 20(b) to subchapter III of Chapter 99",
                "U.S. Notes (con.)",
                "U.S. Notes (con.)",
            ],
            "context_excerpt": [
                "9903.88.01 | The duty | provided in the | applicable | subheading plus | 25%",
                "8501.52.80 | 8501.51.60 | 8502.11.00 | 8501.64.00 | 8501.63.00 | 8501.62.00",
                "8501.51.60 | 8502.11.00 | 8501.64.00 | 8501.63.00 | 8501.62.00 | 8502.39.00",
            ],
        }
    ).to_csv(extract_dir / "2018HTSARevision7_1_extracted_rows.csv", index=False)

    monkeypatch.setattr(
        "passthru_data.build_us_products_partner_panel._load_tradewar_release_catalog",
        lambda _cfg: pd.DataFrame(
            {
                "release_name": ["2018HTSARevision7_1"],
                "year": [2018],
                "release_start_date": [pd.Timestamp("2018-07-06")],
                "release_end_date": [pd.Timestamp("2018-08-07")],
            }
        ),
    )

    links = _load_tradewar_pdf_csv_links(cfg, include_candidate_fallback=True)
    sub = links.loc[links["rule_code"].astype(str).eq("99038801")]
    assert "85016400" in set(sub["hs8"].astype(str).tolist())
