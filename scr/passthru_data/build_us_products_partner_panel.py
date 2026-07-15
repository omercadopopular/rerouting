"""Build full partner-HS10 monthly panel for U.S. products."""

from __future__ import annotations

from typing import Any
import re
import zipfile

import pandas as pd

from .config import PipelineConfig
from .io_utils import add_hierarchy_codes, normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet


def _filter_period(df: pd.DataFrame, start_period: str, end_period: str) -> pd.DataFrame:
    start = pd.Period(start_period, freq="M")
    end = pd.Period(end_period, freq="M")
    period = pd.to_datetime(
        df["year"].astype("Int64").astype(str) + "-" + df["month"].astype("Int64").astype(str).str.zfill(2) + "-01",
        errors="coerce",
    ).dt.to_period("M")
    mask = (period >= start) & (period <= end)
    return df.loc[mask].copy()


def _clean_date_field(series: pd.Series) -> pd.Series:
    text = series.astype("string")
    token = text.str.extract(r"((?:\d{1,2}/\d{1,2}/\d{4})|(?:\d{4}-\d{2}-\d{2}))", expand=False)
    return pd.to_datetime(token, errors="coerce")


def _month_active_share_from_range(effective_start: pd.Timestamp, effective_end: pd.Timestamp, year: int, month: int) -> float:
    """Share of a calendar month covered by an effective date range."""
    period = pd.Period(year=year, month=month, freq="M")
    start_period = effective_start.to_period("M")
    end_period = effective_end.to_period("M")
    if period < start_period or period > end_period:
        return 0.0
    month_start = pd.Timestamp(year=year, month=month, day=1)
    month_end = month_start + pd.offsets.MonthEnd(0)
    active_start = max(effective_start, month_start)
    active_end = min(effective_end, month_end)
    if active_end < active_start:
        return 0.0
    return float((active_end - active_start).days + 1) / float(month_end.day)


def _load_trade_panel(config: PipelineConfig, flow: str) -> pd.DataFrame:
    path = config.analysis_dir / f"{flow}_flow_hs10_fm_new.parquet"
    value_col = "m_val" if flow == "m" else "x_val"
    qty_col = "m_q1" if flow == "m" else "x_q1"
    df = read_table(path, columns=["cty_code", "cty_name", "hs10", "year", "month", value_col, qty_col])
    df["hs10"] = df["hs10"].map(lambda value: normalize_hs_code(value, 10))
    return _filter_period(df, config.start_period, config.end_period)


_REF_9903_RE = re.compile(r"9903\.\d{2}\.\d{2}", re.I)
_PCT_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*%")
_TRADEWAR_RULE_RE = re.compile(r"^9903(?:45|46|80|85|88)\d{2}$")

_EU28: set[str] = {
    "AUSTRIA",
    "BELGIUM",
    "BULGARIA",
    "CROATIA",
    "CYPRUS",
    "CZECH REPUBLIC",
    "DENMARK",
    "ESTONIA",
    "FINLAND",
    "FRANCE",
    "GERMANY",
    "GREECE",
    "HUNGARY",
    "IRELAND",
    "ITALY",
    "LATVIA",
    "LITHUANIA",
    "LUXEMBOURG",
    "MALTA",
    "NETHERLANDS",
    "POLAND",
    "PORTUGAL",
    "ROMANIA",
    "SLOVAKIA",
    "SLOVENIA",
    "SPAIN",
    "SWEDEN",
    "UNITED KINGDOM",
}

_GSP_LDC_COUNTRIES: set[str] = {
    "AFGHANISTAN",
    "ANGOLA",
    "BANGLADESH",
    "BENIN",
    "BHUTAN",
    "BURKINA FASO",
    "BURUNDI",
    "CAMBODIA",
    "CENTRAL AFRICAN REPUBLIC",
    "CHAD",
    "COMOROS",
    "CONGO (KINSHASA)",
    "DJIBOUTI",
    "ETHIOPIA",
    "GAMBIA",
    "GUINEA",
    "GUINEA-BISSAU",
    "HAITI",
    "KIRIBATI",
    "LAOS",
    "LESOTHO",
    "LIBERIA",
    "MADAGASCAR",
    "MALAWI",
    "MALI",
    "MAURITANIA",
    "MOZAMBIQUE",
    "MYANMAR",
    "NEPAL",
    "NIGER",
    "RWANDA",
    "SAO TOME AND PRINCIPE",
    "SIERRA LEONE",
    "SOLOMON ISLANDS",
    "SOMALIA",
    "SOUTH SUDAN",
    "SUDAN",
    "TANZANIA",
    "TIMOR-LESTE",
    "TOGO",
    "TUVALU",
    "UGANDA",
    "VANUATU",
    "YEMEN",
    "ZAMBIA",
}


def _parse_increment_rate(text_rate: Any, adval_rate: Any) -> float | None:
    numeric = pd.to_numeric(pd.Series([adval_rate]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric) < 100:
        return float(numeric)
    text = str(text_rate or "")
    matches = _PCT_RE.findall(text)
    if matches:
        try:
            return float(matches[-1]) / 100.0
        except Exception:
            return None
    return None


def _canonical_country(name: str) -> str:
    text = str(name or "").upper().strip()
    if "KOREA" in text:
        return "SOUTH KOREA"
    if text in {"CHINA", "TURKEY", "ARGENTINA", "BRAZIL", "AUSTRALIA", "CANADA", "MEXICO", "RUSSIA", "INDIA"}:
        return text
    return text


def _extract_countries_from_rule(description: str, rule_code: str | None = None) -> tuple[list[str], list[str]]:
    text = str(description or "").upper()
    known = ["CHINA", "TURKEY", "ARGENTINA", "BRAZIL", "SOUTH KOREA", "AUSTRALIA", "CANADA", "MEXICO", "RUSSIA", "INDIA"]
    present = [country for country in known if country in text]
    include: list[str] = []
    exclude: list[str] = []
    rule = str(rule_code or "")
    if "PRODUCT OF CHINA" in text or "PRODUCTS OF CHINA" in text:
        include = ["CHINA"]
    elif "PRODUCT OF TURKEY" in text or "PRODUCTS OF TURKEY" in text:
        include = ["TURKEY"]
    elif rule in {"99038505", "99038506", "99038511"}:
        # 232 aluminum quota-specific lines are Argentina-specific in the underlying Chapter 99 notes.
        include = ["ARGENTINA"]
    elif rule == "99038002":
        include = ["TURKEY"]
    elif "EXCEPT PRODUCTS OF" in text:
        exclude = present
    elif "EXCEPT FROM" in text:
        exclude = present
    elif "PRODUCT OF" in text or "PRODUCTS OF" in text:
        include = present
    return include, exclude


def _rule_family(rule_code: str) -> str:
    rule = str(rule_code or "")
    if rule.startswith("990388"):
        return "china_301"
    if rule.startswith("990380"):
        return "steel_232"
    if rule.startswith("990385"):
        return "aluminum_232"
    if rule.startswith("990345"):
        return "washer_201"
    if rule.startswith("990346"):
        return "solar_201"
    return "other"


def _eligible_countries_by_deterministic_grouping(
    rule_code: str,
    year: int,
    month: int,
    country_values: list[str],
) -> list[str]:
    family = _rule_family(rule_code)
    universe = set(country_values)

    if family == "china_301":
        return [cty for cty in country_values if cty == "CHINA"]

    if family == "washer_201":
        excluded = {"CANADA"} | _GSP_LDC_COUNTRIES
        return [cty for cty in country_values if cty in universe and cty not in excluded]

    if family == "solar_201":
        excluded = set(_GSP_LDC_COUNTRIES)
        return [cty for cty in country_values if cty in universe and cty not in excluded]

    if family == "steel_232":
        excluded = {"ARGENTINA", "AUSTRALIA", "BRAZIL", "SOUTH KOREA"}
        # Exemptions for Canada/Mexico/EU are treated as active through 2018-05.
        if (int(year), int(month)) < (2018, 6):
            excluded |= {"CANADA", "MEXICO"} | _EU28
        return [cty for cty in country_values if cty in universe and cty not in excluded]

    if family == "aluminum_232":
        excluded = {"ARGENTINA", "AUSTRALIA"}
        # Exemptions for Canada/Mexico/EU are treated as active through 2018-05.
        if (int(year), int(month)) < (2018, 6):
            excluded |= {"CANADA", "MEXICO"} | _EU28
        return [cty for cty in country_values if cty in universe and cty not in excluded]

    return country_values


_PREFERENCE_MAPPINGS: tuple[dict[str, Any], ...] = (
    {"country": "CANADA", "indicator_col": "nafta_canada_ind", "rate_col": None, "default_rate": 0.0},
    {"country": "MEXICO", "indicator_col": "nafta_mexico_ind", "rate_col": "mexico_ad_val_rate", "default_rate": 0.0},
    {"country": "AUSTRALIA", "indicator_col": "australia_indicator", "rate_col": "australia_ad_val_rate"},
    {"country": "BAHRAIN", "indicator_col": "bahrain_indicator", "rate_col": "bahrain_ad_val_rate"},
    {"country": "CHILE", "indicator_col": "chile_indicator", "rate_col": "chile_ad_val_rate"},
    {"country": "COLOMBIA", "indicator_col": "colombia_indicator", "rate_col": "colombia_ad_val_rate"},
    {"country": "ISRAEL", "indicator_col": "israel_fta_indicator", "rate_col": None, "default_rate": 0.0},
    {"country": "JORDAN", "indicator_col": "jordan_indicator", "rate_col": "jordan_ad_val_rate"},
    {"country": "SOUTH KOREA", "indicator_col": "korea_indicator", "rate_col": "korea_ad_val_rate"},
    {"country": "MOROCCO", "indicator_col": "morocco_indicator", "rate_col": "morocco_ad_val_rate"},
    {"country": "OMAN", "indicator_col": "oman_indicator", "rate_col": "oman_ad_val_rate"},
    {"country": "PANAMA", "indicator_col": "panama_indicator", "rate_col": "panama_ad_val_rate"},
    {"country": "PERU", "indicator_col": "peru_indicator", "rate_col": "peru_ad_val_rate"},
    {"country": "SINGAPORE", "indicator_col": "singapore_indicator", "rate_col": "singapore_ad_val_rate"},
)

_PROGRAM_COUNTRY_GROUPS: dict[str, set[str]] = {
    "DR_CAFTA": {
        "COSTA RICA",
        "DOMINICAN REPUBLIC",
        "EL SALVADOR",
        "GUATEMALA",
        "HONDURAS",
        "NICARAGUA",
    },
    "CBI": {
        "ANTIGUA AND BARBUDA",
        "ARUBA",
        "BAHAMAS",
        "BARBADOS",
        "BELIZE",
        "COSTA RICA",
        "CURACAO",
        "DOMINICA",
        "DOMINICAN REPUBLIC",
        "EL SALVADOR",
        "GRENADA",
        "GUATEMALA",
        "GUYANA",
        "HAITI",
        "HONDURAS",
        "JAMAICA",
        "MONTSERRAT",
        "NICARAGUA",
        "PANAMA",
        "ST KITTS AND NEVIS",
        "ST LUCIA",
        "ST VINCENT AND THE GRENADINES",
        "SURINAME",
        "TRINIDAD AND TOBAGO",
    },
    "CBTPA": {"COLOMBIA"},
    "AGOA": {
        "ANGOLA",
        "BENIN",
        "BOTSWANA",
        "BURKINA FASO",
        "BURUNDI",
        "CAMEROON",
        "CABO VERDE",
        "CENTRAL AFRICAN REPUBLIC",
        "CHAD",
        "COMOROS",
        "CONGO (BRAZZAVILLE)",
        "CONGO (KINSHASA)",
        "COTE D'IVOIRE",
        "DJIBOUTI",
        "ETHIOPIA",
        "GABON",
        "GAMBIA",
        "GHANA",
        "GUINEA",
        "GUINEA-BISSAU",
        "KENYA",
        "LESOTHO",
        "LIBERIA",
        "MADAGASCAR",
        "MALAWI",
        "MALI",
        "MAURITANIA",
        "MAURITIUS",
        "MOZAMBIQUE",
        "NAMIBIA",
        "NIGER",
        "NIGERIA",
        "RWANDA",
        "SAO TOME AND PRINCIPE",
        "SENEGAL",
        "SEYCHELLES",
        "SIERRA LEONE",
        "SOUTH AFRICA",
        "SWAZILAND",
        "TANZANIA",
        "TOGO",
        "UGANDA",
        "ZAMBIA",
    },
    "GSP": {
        "ALGERIA",
        "ANGOLA",
        "ARGENTINA",
        "ARMENIA",
        "AZERBAIJAN",
        "BELIZE",
        "BENIN",
        "BOLIVIA",
        "BOSNIA AND HERZEGOVINA",
        "BRAZIL",
        "BULGARIA",
        "CAMBODIA",
        "CAMEROON",
        "COLOMBIA",
        "COSTA RICA",
        "COTE D'IVOIRE",
        "DOMINICAN REPUBLIC",
        "ECUADOR",
        "EGYPT",
        "EL SALVADOR",
        "FIJI",
        "GEORGIA",
        "GHANA",
        "GUATEMALA",
        "GUYANA",
        "HONDURAS",
        "INDIA",
        "INDONESIA",
        "IRAQ",
        "JAMAICA",
        "JORDAN",
        "KAZAKHSTAN",
        "KENYA",
        "KYRGYZSTAN",
        "MOLDOVA",
        "MONGOLIA",
        "MOROCCO",
        "NICARAGUA",
        "NIGERIA",
        "PAKISTAN",
        "PANAMA",
        "PARAGUAY",
        "PERU",
        "PHILIPPINES",
        "SOUTH AFRICA",
        "SRI LANKA",
        "THAILAND",
        "TUNISIA",
        "TURKEY",
        "UKRAINE",
        "UZBEKISTAN",
        "VENEZUELA",
    },
}

_PROGRAM_MAPPINGS: tuple[dict[str, Any], ...] = (
    {"program": "GSP", "indicator_col": "gsp_indicator", "rate_col": None, "default_rate": 0.0, "exclude_col": "gsp_ctry_excluded"},
    {"program": "CBI", "indicator_col": "cbi_indicator", "rate_col": "cbi_ad_val_rate", "default_rate": 0.0},
    {"program": "AGOA", "indicator_col": "agoa_indicator", "rate_col": None, "default_rate": 0.0},
    {"program": "CBTPA", "indicator_col": "cbtpa_indicator", "rate_col": "cbtpa_ad_val_rate", "default_rate": 0.0},
    {"program": "DR_CAFTA", "indicator_col": "dr_cafta_indicator", "rate_col": "dr_cafta_ad_val_rate", "default_rate": 0.0},
    {"program": "DR_CAFTA", "indicator_col": "dr_cafta_plus_indicator", "rate_col": "dr_cafta_plus_ad_val_rate", "default_rate": 0.0},
)

_ISO2_TO_COUNTRY: dict[str, str] = {
    "AR": "ARGENTINA",
    "BR": "BRAZIL",
    "EC": "ECUADOR",
    "ID": "INDONESIA",
    "IN": "INDIA",
    "JM": "JAMAICA",
    "KZ": "KAZAKHSTAN",
    "PH": "PHILIPPINES",
    "TH": "THAILAND",
    "TR": "TURKEY",
    "UA": "UKRAINE",
}


def _decode_exclusion_iso_tokens(value: Any) -> set[str]:
    text = str(value or "").strip().upper()
    if not text:
        return set()
    codes: set[str] = set()
    only_letters = re.sub(r"[^A-Z]", "", text)
    if len(only_letters) >= 2:
        for idx in range(0, len(only_letters) - 1, 2):
            token = only_letters[idx : idx + 2]
            if token in _ISO2_TO_COUNTRY:
                codes.add(token)
    return {_canonical_country(_ISO2_TO_COUNTRY[token]) for token in codes}


_SPECIAL_TOKEN_COUNTRY: dict[str, str] = {
    "AU": "AUSTRALIA",
    "BH": "BAHRAIN",
    "CA": "CANADA",
    "CL": "CHILE",
    "CO": "COLOMBIA",
    "IL": "ISRAEL",
    "JO": "JORDAN",
    "K": "SOUTH KOREA",
    "KR": "SOUTH KOREA",
    "MA": "MOROCCO",
    "MX": "MEXICO",
    "OM": "OMAN",
    "PA": "PANAMA",
    "PE": "PERU",
    "SG": "SINGAPORE",
}

_SPECIAL_TOKEN_PROGRAM: dict[str, str] = {
    "A": "GSP",
    "A+": "GSP",
    "A*": "GSP",
    "D": "AGOA",
    "E": "CBI",
    "E*": "CBI",
    "P": "DR_CAFTA",
    "P+": "DR_CAFTA",
    "R": "CBTPA",
}


def _decode_special_tokens_to_countries(token_text: str, universe: set[str]) -> set[str]:
    tokens = [token.strip().upper() for token in str(token_text or "").split(",") if token.strip()]
    countries: set[str] = set()
    for token in tokens:
        if token in _SPECIAL_TOKEN_COUNTRY:
            countries.add(_canonical_country(_SPECIAL_TOKEN_COUNTRY[token]))
            continue
        if token in _SPECIAL_TOKEN_PROGRAM:
            program = _SPECIAL_TOKEN_PROGRAM[token]
            countries |= {_canonical_country(value) for value in _PROGRAM_COUNTRY_GROUPS.get(program, set())}
    return countries & universe


def _special_text_overrides(special_text: Any, universe: set[str]) -> dict[str, float]:
    text = str(special_text or "").strip()
    if not text:
        return {}
    upper = text.upper()
    result: dict[str, float] = {}

    # Baseline "Free (...)" assignment by listed country/program tokens.
    if upper.startswith("FREE"):
        base_match = re.search(r"FREE\s*\(([^)]*)\)", upper, re.I)
        if base_match:
            countries = _decode_special_tokens_to_countries(base_match.group(1), universe)
            for country in countries:
                result[country] = 0.0

    # Country/program-specific ad valorem override terms such as "0.6% (KR)".
    for rate_str, tokens in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*%\s*\(([^)]*)\)", upper):
        try:
            rate = float(rate_str) / 100.0
        except Exception:
            continue
        countries = _decode_special_tokens_to_countries(tokens, universe)
        for country in countries:
            result[country] = rate

    return result


def _load_raw_partner_preference_overrides(config: PipelineConfig, countries: pd.Series) -> pd.DataFrame:
    """Construct bilateral base statutory rates from annual HTS preference columns."""
    annual_dir = config.raw_dir / "policy" / "annual"
    country_values = sorted({_canonical_country(value) for value in countries.dropna().astype(str).str.upper().unique()})
    country_set = set(country_values)
    rows: list[dict[str, Any]] = []
    for zip_path in sorted(annual_dir.glob("tariff_data_*.zip")):
        year_match = re.search(r"(19|20)\d{2}", zip_path.name)
        if not year_match:
            continue
        year = int(year_match.group(0))
        if year < int(config.start_period[:4]) or year > int(config.end_period[:4]):
            continue
        with zipfile.ZipFile(zip_path) as handle:
            txt_candidates = [name for name in handle.namelist() if name.lower().endswith(".txt")]
            if not txt_candidates:
                continue
            with handle.open(sorted(txt_candidates)[0]) as raw:
                df = pd.read_csv(raw, dtype=str, engine="python", sep=None, on_bad_lines="skip", encoding="latin1")
        for col in ("hts8", "begin_effect_date", "end_effective_date"):
            if col not in df.columns:
                df[col] = pd.NA
        df["hs8"] = df["hts8"].map(lambda value: normalize_hs_code(value, 8))
        df["begin_effect_date"] = _clean_date_field(df["begin_effect_date"])
        df["end_effective_date"] = _clean_date_field(df["end_effective_date"])
        df["begin_effect_date"] = df["begin_effect_date"].fillna(pd.Timestamp(year=year, month=1, day=1))
        df["end_effective_date"] = df["end_effective_date"].fillna(pd.Timestamp(year=year, month=12, day=31))

        for mapping in _PREFERENCE_MAPPINGS:
            indicator_col = str(mapping["indicator_col"])
            if indicator_col not in df.columns:
                continue
            candidate = df[df[indicator_col].notna() & (df[indicator_col].astype(str).str.strip() != "")].copy()
            if candidate.empty:
                continue
            rate_col = mapping.get("rate_col")
            if rate_col and rate_col in candidate.columns:
                rate = pd.to_numeric(candidate[rate_col], errors="coerce")
                if "default_rate" in mapping:
                    rate = rate.fillna(float(mapping["default_rate"]))
                candidate["base_rate"] = rate
            elif "default_rate" in mapping:
                candidate["base_rate"] = float(mapping["default_rate"])
            else:
                continue
            candidate = candidate[candidate["base_rate"].notna()].copy()
            if candidate.empty:
                continue
            for _, row in candidate.iterrows():
                if pd.isna(row["hs8"]):
                    continue
                start = row["begin_effect_date"]
                end = row["end_effective_date"]
                start_month = int(start.month)
                end_month = int(end.month) if int(end.year) == year else 12
                for month in range(start_month, end_month + 1):
                    rows.append(
                        {
                            "cty_name": _canonical_country(str(mapping["country"])),
                            "hs8": str(row["hs8"]),
                            "year": year,
                            "month": month,
                            "base_pref_rate_raw": float(row["base_rate"]),
                            "base_pref_source": indicator_col,
                        }
                    )

        # Parse statutory special-rate text directly when it maps to ad valorem bilateral rates.
        if "col1_special_text" in df.columns:
            special = df[df["col1_special_text"].notna() & (df["col1_special_text"].astype(str).str.strip() != "")].copy()
            for _, row in special.iterrows():
                if pd.isna(row["hs8"]):
                    continue
                overrides = _special_text_overrides(row.get("col1_special_text"), country_set)
                if not overrides:
                    continue
                start = row["begin_effect_date"]
                end = row["end_effective_date"]
                start_month = int(start.month)
                end_month = int(end.month) if int(end.year) == year else 12
                for month in range(start_month, end_month + 1):
                    for country, rate in overrides.items():
                        rows.append(
                            {
                                "cty_name": country,
                                "hs8": str(row["hs8"]),
                                "year": year,
                                "month": month,
                                "base_pref_rate_raw": float(rate),
                                "base_pref_source": "col1_special_text",
                            }
                        )

        for mapping in _PROGRAM_MAPPINGS:
            indicator_col = str(mapping["indicator_col"])
            if indicator_col not in df.columns:
                continue
            candidate = df[df[indicator_col].notna() & (df[indicator_col].astype(str).str.strip() != "")].copy()
            if candidate.empty:
                continue
            rate_col = mapping.get("rate_col")
            if rate_col and rate_col in candidate.columns:
                rate = pd.to_numeric(candidate[rate_col], errors="coerce")
                if "default_rate" in mapping:
                    rate = rate.fillna(float(mapping["default_rate"]))
                candidate["base_rate"] = rate
            elif "default_rate" in mapping:
                candidate["base_rate"] = float(mapping["default_rate"])
            else:
                continue
            candidate = candidate[candidate["base_rate"].notna()].copy()
            if candidate.empty:
                continue
            program = str(mapping["program"])
            program_countries = {_canonical_country(value) for value in _PROGRAM_COUNTRY_GROUPS.get(program, set())}
            program_countries = program_countries & country_set
            if not program_countries:
                continue
            exclude_col = mapping.get("exclude_col")
            for _, row in candidate.iterrows():
                if pd.isna(row["hs8"]):
                    continue
                excluded: set[str] = set()
                if exclude_col and exclude_col in candidate.columns:
                    excluded = _decode_exclusion_iso_tokens(row.get(exclude_col))
                eligible = [country for country in program_countries if country not in excluded]
                if not eligible:
                    continue
                start = row["begin_effect_date"]
                end = row["end_effective_date"]
                start_month = int(start.month)
                end_month = int(end.month) if int(end.year) == year else 12
                for month in range(start_month, end_month + 1):
                    for country in eligible:
                        rows.append(
                            {
                                "cty_name": country,
                                "hs8": str(row["hs8"]),
                                "year": year,
                                "month": month,
                                "base_pref_rate_raw": float(row["base_rate"]),
                                "base_pref_source": f"{program}:{indicator_col}",
                            }
                        )

    if not rows:
        return pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "base_pref_rate_raw", "base_pref_source"])
    out = pd.DataFrame(rows)
    out = out[pd.to_numeric(out["base_pref_rate_raw"], errors="coerce").fillna(9999.99) < 9000].copy()
    out = (
        out.sort_values(["cty_name", "hs8", "year", "month", "base_pref_rate_raw"], ascending=[True, True, True, True, True])
        .drop_duplicates(["cty_name", "hs8", "year", "month"], keep="first")
        .reset_index(drop=True)
    )
    return out


def _load_tradewar_release_catalog(config: PipelineConfig) -> pd.DataFrame:
    catalog_path = config.reference_dir / "policy_release_catalog.csv"
    frame = pd.DataFrame()
    if catalog_path.exists():
        try:
            frame = pd.read_csv(catalog_path)
        except Exception:
            frame = pd.DataFrame()
    if frame.empty:
        parquet_path = config.reference_dir / "policy_release_catalog.parquet"
        if parquet_path.exists():
            try:
                frame = pd.read_parquet(parquet_path)
            except Exception:
                frame = pd.DataFrame()
    if frame.empty:
        return frame
    for col in ("release_start_date", "release_end_date", "release_date"):
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")
    frame = frame.loc[frame["year"].between(int(config.start_period[:4]), int(config.end_period[:4]))].copy()
    return frame


def _locate_release_pdf(archive_pdf_dir: pd.PathLike[str] | Any, release_name: str) -> str | None:
    from pathlib import Path

    base = Path(archive_pdf_dir)
    direct = base / f"{release_name}.pdf"
    if direct.exists():
        return str(direct)
    target = str(release_name).lower()
    candidates = sorted(base.glob("*.pdf"))
    # Prefer exact stem match, then prefix match.
    for candidate in candidates:
        if candidate.stem.lower() == target:
            return str(candidate)
    for candidate in candidates:
        stem = candidate.stem.lower()
        if stem.startswith(target) or target.startswith(stem):
            return str(candidate)
    return None


def _parse_pdf_rule_hs_links(pdf_path: str) -> pd.DataFrame:
    """Extract 9903-rule to HS8 links from Chapter 99 revision PDF text.

    This parser focuses on the trade-war families (301/232/safeguards):
    - direct line-level "See 9903.xx.xx" references to HS lines
    - U.S.-note blocks (e.g., 20(b)/(d)/(f)/(g), 16/19 notes) used by Chapter 99 rules
    """
    try:
        import fitz
    except Exception:
        return pd.DataFrame(columns=["hs8", "rule_code"])

    rule_re = re.compile(r"9903\.(?:45|46|80|85|88)\.\d{2}")
    hs8_re = re.compile(r"\b\d{4}\.\d{2}\.\d{2}\b")
    note_key_re = re.compile(r"(\d+\([a-z]\))", re.I)
    rows: list[dict[str, str]] = []
    doc = fitz.open(pdf_path)
    page_texts: list[str] = []

    # Pass 1: line-context extraction around explicit rule mentions.
    for page_index in range(doc.page_count):
        page_text = doc.load_page(page_index).get_text("text")
        page_texts.append(page_text)
        lines = page_text.splitlines()
        for idx, line in enumerate(lines):
            rules = [match.replace(".", "") for match in rule_re.findall(line)]
            rules = [rule for rule in rules if _TRADEWAR_RULE_RE.match(rule)]
            if rules:
                lo = idx - 1
                while lo >= 0:
                    prev = lines[lo]
                    prev_upper = prev.upper()
                    if rule_re.search(prev):
                        break
                    if prev_upper.startswith("RATES OF DUTY") or "HARMONIZED TARIFF SCHEDULE" in prev_upper:
                        break
                    lo -= 1
                context_lines = lines[lo + 1 : idx + 1]
                context = " ".join(context_lines)
                hs8_values = sorted({code.replace(".", "") for code in hs8_re.findall(context) if not code.startswith("9903")})
                for rule in rules:
                    for hs8 in hs8_values:
                        rows.append({"hs8": hs8, "rule_code": rule})

    # Pass 2: note-block extraction.
    full_text = "\n".join(page_texts)
    norm_text = " ".join(full_text.split())
    if norm_text:
        rule_to_notes: dict[str, set[str]] = {}
        note_to_rules: dict[str, set[str]] = {}
        note_rule_patterns = (
            re.compile(
                r"(?:heading|subheading)\s*(9903\.(?:45|46|80|85|88)\.\d{2}).{0,320}?U\.S\.\s*note\s*(\d+\([a-z]\))",
                re.I,
            ),
            re.compile(
                r"U\.S\.\s*note\s*(\d+\([a-z]\)).{0,320}?(?:heading|subheading)\s*(9903\.(?:45|46|80|85|88)\.\d{2})",
                re.I,
            ),
            re.compile(
                r"(9903\.(?:45|46|80|85|88)\.\d{2}).{0,220}?enumerated in\s+U\.S\.\s*note\s*(\d+\([a-z]\))",
                re.I,
            ),
            re.compile(
                r"enumerated in\s+U\.S\.\s*note\s*(\d+\([a-z]\)).{0,220}?(9903\.(?:45|46|80|85|88)\.\d{2})",
                re.I,
            ),
        )
        def _note_rule_family_match(note_key: str, rule_code: str) -> bool:
            major = note_key.split("(", 1)[0]
            if rule_code.startswith("990388"):
                return major == "20"
            if rule_code.startswith("990380"):
                return major == "16"
            if rule_code.startswith("990385"):
                return major == "19"
            if rule_code.startswith("990345") or rule_code.startswith("990346"):
                return major in {"17", "18"}
            return True

        for pattern in note_rule_patterns:
            for match in pattern.findall(norm_text):
                if len(match) != 2:
                    continue
                a, b = match
                rule, note = (a, b) if a.startswith("9903.") else (b, a)
                rule_code = rule.replace(".", "")
                if not _TRADEWAR_RULE_RE.match(rule_code):
                    continue
                note_match = note_key_re.search(str(note))
                if not note_match:
                    continue
                note_key = note_match.group(1).lower()
                if not _note_rule_family_match(note_key, rule_code):
                    continue
                rule_to_notes.setdefault(rule_code, set()).add(note_key)
                note_to_rules.setdefault(note_key, set()).add(rule_code)

        hs8_only_re = re.compile(r"\b(?!99)\d{4}\.\d{2}\.\d{2}\b")

        def _extract_note_hs8(note_key: str) -> set[str]:
            major, letter_part = note_key.split("(")
            letter = letter_part.rstrip(")")
            # Accept "20. (b)", "20.(b)" and similar whitespace variants.
            start_pattern = re.compile(rf"{re.escape(major)}\s*\.\s*\({re.escape(letter)}\)", re.I)
            next_pattern = re.compile(rf"{re.escape(major)}\s*\.\s*\([a-z]\)", re.I)
            starts = [m.start() for m in start_pattern.finditer(full_text)]
            best_codes: set[str] = set()
            best_score = 0
            for start in starts:
                end = min(start + 140_000, len(full_text))
                for candidate in next_pattern.finditer(full_text, start + 1):
                    if candidate.start() > start:
                        end = candidate.start()
                        break
                segment = full_text[start:end]
                hits = {code.replace(".", "") for code in hs8_only_re.findall(segment)}
                score = len(hits)
                if score > best_score:
                    best_score = score
                    best_codes = hits
            # Keep only meaningful note blocks (avoid tiny false-match snippets).
            if len(best_codes) < 10:
                return set()
            return best_codes

        for note_key, rules in note_to_rules.items():
            hs8_values = _extract_note_hs8(note_key)
            if not hs8_values:
                continue
            for rule_code in rules:
                for hs8 in hs8_values:
                    rows.append({"hs8": hs8, "rule_code": rule_code})

    if not rows:
        return pd.DataFrame(columns=["hs8", "rule_code"])
    out = pd.DataFrame(rows).drop_duplicates(["hs8", "rule_code"]).reset_index(drop=True)
    return out


def _load_tradewar_pdf_links(config: PipelineConfig) -> pd.DataFrame:
    """Load HS8->rule links from 2018-2019 revision PDFs and attach release windows."""
    cache_dir = config.staging_dir / "policy"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "tradewar_pdf_links.parquet"
    if cache_path.exists() and not config.overwrite:
        return pd.read_parquet(cache_path)

    catalog = _load_tradewar_release_catalog(config)
    if catalog.empty:
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])

    # Focus on paper window where trade-war revisions matter.
    catalog = catalog.loc[catalog["year"].between(2018, 2019)].copy()
    pdf_dir = config.raw_dir / "policy" / "archive" / "pdf"
    rows: list[pd.DataFrame] = []
    for _, rel in catalog.iterrows():
        release_name = str(rel.get("release_name") or "").strip()
        if not release_name:
            continue
        pdf_path = _locate_release_pdf(pdf_dir, release_name)
        if not pdf_path:
            continue
        links = _parse_pdf_rule_hs_links(pdf_path)
        if links.empty:
            continue
        links["release_name"] = release_name
        links["release_start_date"] = rel.get("release_start_date")
        links["release_end_date"] = rel.get("release_end_date")
        rows.append(links)
    if not rows:
        empty = pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
        empty.to_parquet(cache_path, index=False)
        return empty
    out = pd.concat(rows, ignore_index=True).drop_duplicates().reset_index(drop=True)
    out.to_parquet(cache_path, index=False)
    return out


def _load_tradewar_pdf_csv_links(config: PipelineConfig, include_candidate_fallback: bool = False) -> pd.DataFrame:
    """Extract trusted HS8->rule links from pre-extracted PDF CSV rows.

    Nearby-note matches are retained only in the provenance audit as candidate
    evidence. They are not production scope because page distance does not
    establish a structural Chapter 99 relationship.
    """
    provenance = _load_tradewar_pdf_csv_link_provenance(config, include_candidate_fallback=include_candidate_fallback)
    if provenance.empty:
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    return provenance[["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"]].drop_duplicates().reset_index(drop=True)


def _load_tradewar_pdf_csv_link_provenance(config: PipelineConfig, include_candidate_fallback: bool = False) -> pd.DataFrame:
    """Extract HS8->rule links from pre-extracted PDF CSV rows with row-level provenance."""
    extract_dir = config.staging_dir / "policy" / "pdf_extract"
    if not extract_dir.exists():
        return pd.DataFrame(
            columns=[
                "release_name",
                "release_start_date",
                "release_end_date",
                "source_file",
                "source_page",
                "source_row",
                "hs8",
                "rule_code",
                "extraction_method",
                "rule_found_in_same_row",
                "rule_found_only_in_context",
                "matched_rule_text",
            ]
        )
    catalog = _load_tradewar_release_catalog(config)
    if catalog.empty:
        return pd.DataFrame(
            columns=[
                "release_name",
                "release_start_date",
                "release_end_date",
                "source_file",
                "source_page",
                "source_row",
                "hs8",
                "rule_code",
                "extraction_method",
                "rule_found_in_same_row",
                "rule_found_only_in_context",
                "matched_rule_text",
            ]
        )
    catalog = catalog.loc[catalog["year"].between(2017, 2019)].copy()
    release_dates = {
        str(row["release_name"]): (row.get("release_start_date"), row.get("release_end_date"))
        for _, row in catalog.iterrows()
        if str(row.get("release_name") or "").strip()
    }
    rule_re = re.compile(r"9903\.(?:45|46|80|85|88)\.\d{2}", re.I)
    empty_columns = [
        "release_name",
        "release_start_date",
        "release_end_date",
        "source_file",
        "source_page",
        "source_row",
        "hs8",
        "rule_code",
        "extraction_method",
        "rule_found_in_same_row",
        "rule_found_only_in_context",
        "matched_rule_text",
    ]
    rows: list[dict[str, Any]] = []
    for csv_path in sorted(extract_dir.glob("*_extracted_rows.csv")):
        release_name = csv_path.stem.replace("_extracted_rows", "")
        if release_name not in release_dates:
            continue
        try:
            frame = pd.read_csv(csv_path, dtype=str)
        except Exception:
            continue
        if frame.empty:
            continue
        if "hs_code" not in frame.columns:
            continue
        frame["hs_code"] = frame["hs_code"].astype("string")
        frame["hs_digits"] = frame["hs_code"].str.replace(".", "", regex=False)
        if "description_blob" not in frame.columns:
            frame["description_blob"] = ""
        if "context_excerpt" not in frame.columns:
            frame["context_excerpt"] = ""
        page_numbers = pd.to_numeric(frame.get("page"), errors="coerce")
        note_mask = frame["description_blob"].astype("string").str.contains("U.S. Notes", case=False, na=False)
        for source_row, row in frame.iterrows():
            hs_code = str(row.get("hs_code") or "").strip()
            hs_digits = str(row.get("hs_digits") or "").strip()
            if not hs_code or not hs_digits:
                continue
            description_blob = str(row.get("description_blob") or "")
            context_excerpt = str(row.get("context_excerpt") or "")
            description_rules = sorted({hit.replace(".", "") for hit in rule_re.findall(description_blob) if _TRADEWAR_RULE_RE.match(hit.replace(".", ""))})
            context_rules = sorted({hit.replace(".", "") for hit in rule_re.findall(context_excerpt) if _TRADEWAR_RULE_RE.match(hit.replace(".", ""))})
            rules = description_rules if description_rules else context_rules
            same_row_text = description_blob if description_rules else ""
            context_text = context_excerpt if (not description_rules and context_rules) else ""
            # Product line with explicit reference -> direct hs8 scope link.
            if len(hs_digits) == 8 and not hs_digits.startswith("99"):
                hs8 = hs_digits[:8]
                for rule in rules:
                    rows.append(
                        {
                            "release_name": release_name,
                            "release_start_date": release_dates[release_name][0],
                            "release_end_date": release_dates[release_name][1],
                            "source_file": csv_path.name,
                            "source_page": pd.to_numeric(row.get("page"), errors="coerce"),
                            "source_row": int(source_row),
                            "hs8": hs8,
                            "rule_code": rule,
                            "extraction_method": "product_line_same_row_text" if description_rules else "product_line_context_excerpt",
                            "rule_found_in_same_row": bool(rule in description_rules),
                            "rule_found_only_in_context": bool(rule not in description_rules and rule in context_rules),
                            "matched_rule_text": same_row_text or context_text,
                        }
                    )
            # Chapter 99 row may enumerate covered HS in text.
            if len(hs_digits) == 8 and hs_digits.startswith("9903"):
                rule_code = normalize_hs_code(hs_digits, 8)
                if _TRADEWAR_RULE_RE.match(str(rule_code)):
                    text_blob = f"{description_blob} {context_excerpt}".strip()
                    hs_hits = re.findall(r"\b(?!99)\d{4}\.\d{2}\.\d{2}\b", str(text_blob))
                    extraction_method = "chapter99_enumeration_link"
                    if include_candidate_fallback and not hs_hits and str(rule_code) in {"99038801", "99038802", "99038803", "99038804"}:
                        page_value = pd.to_numeric(row.get("page"), errors="coerce")
                        if pd.notna(page_value):
                            nearby = frame.loc[
                                page_numbers.between(float(page_value) - 80, float(page_value))
                                & note_mask
                            ]
                            hs_hits = sorted(
                                {
                                    normalize_hs_code(value, 8)
                                    for value in nearby.get("hs_code", pd.Series(dtype="string")).astype("string")
                                    if str(value).strip() and not str(value).strip().startswith("99")
                                }
                            )
                            extraction_method = "chapter99_nearby_note_fallback"
                    for hs in hs_hits:
                        rows.append(
                            {
                                "release_name": release_name,
                                "release_start_date": release_dates[release_name][0],
                                "release_end_date": release_dates[release_name][1],
                                "source_file": csv_path.name,
                                "source_page": pd.to_numeric(row.get("page"), errors="coerce"),
                                "source_row": int(source_row),
                                "hs8": hs.replace(".", ""),
                                "rule_code": str(rule_code),
                                "extraction_method": extraction_method,
                                "rule_found_in_same_row": True,
                                "rule_found_only_in_context": False,
                                "matched_rule_text": ";".join(sorted(set(hs_hits))),
                            }
                        )
    if not rows:
        return pd.DataFrame(columns=empty_columns)
    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)


def _load_reference_tradewar_links(config: PipelineConfig, filename: str, rule_prefixes: tuple[str, ...]) -> pd.DataFrame:
    path = config.reference_dir / filename
    if not path.exists():
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    try:
        frame = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    if frame.empty or "rule_code" not in frame.columns:
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    frame = frame.copy()
    frame["rule_code"] = frame["rule_code"].map(lambda value: normalize_hs_code(value, 8))
    frame = frame.loc[frame["rule_code"].astype("string").str.startswith(rule_prefixes, na=False)].copy()
    for column in ("release_name", "release_start_date", "release_end_date", "hs8"):
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"]].drop_duplicates().reset_index(drop=True)


def _load_tradewar_machine_links(config: PipelineConfig) -> pd.DataFrame:
    """Extract HS8->rule links from machine-readable archive release files.

    This is the deterministic first source for rule scope. PDF links are fallback.
    """
    cache_dir = config.staging_dir / "policy"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "tradewar_machine_links.parquet"
    if cache_path.exists() and not config.overwrite:
        cached = pd.read_parquet(cache_path)
        if not cached.empty:
            return cached

    catalog = _load_tradewar_release_catalog(config)
    if not catalog.empty:
        catalog = catalog.loc[catalog["year"].between(2017, 2019)].copy()

    archive_data_dir = config.raw_dir / "policy" / "archive" / "data"
    if not archive_data_dir.exists():
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    index_path = config.reference_dir / "policy_archive_revision_index.csv"
    index = pd.DataFrame()
    if index_path.exists():
        try:
            index = pd.read_csv(index_path)
        except Exception:
            index = pd.DataFrame()
    if index.empty:
        parquet_path = config.reference_dir / "policy_archive_revision_index.parquet"
        if parquet_path.exists():
            try:
                index = pd.read_parquet(parquet_path)
            except Exception:
                index = pd.DataFrame()
    if index.empty:
        return pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
    index["year"] = pd.to_numeric(index.get("year"), errors="coerce")
    index = index.loc[index["year"].between(2017, 2019)].copy()
    if "file_ext" in index.columns:
        index = index.loc[index["file_ext"].astype("string").str.lower() == "csv"].copy()
    index["file_name"] = index.get("file_name", pd.Series(index.index, dtype="string")).astype("string")
    index["archive_release_name"] = index.get("archive_release_name", pd.Series(index.index, dtype="string")).astype("string")

    rule_text_re = re.compile(r"9903\.(?:45|46|80|85|88)\.\d{2}", re.I)
    hs8_text_re = re.compile(r"\b(?!99)\d{4}\.\d{2}\.\d{2}\b")
    hs10_text_re = re.compile(r"^\d{4}\.\d{2}\.\d{2}\.\d{2}$")
    rows: list[dict[str, Any]] = []

    catalog_release_dates: dict[str, tuple[Any, Any]] = {}
    if not catalog.empty:
        for _, rel in catalog.iterrows():
            key = str(rel.get("release_name") or "").strip()
            if key:
                catalog_release_dates[key] = (rel.get("release_start_date"), rel.get("release_end_date"))

    for _, idx_row in index.iterrows():
        file_name = str(idx_row.get("file_name") or "").strip()
        if not file_name:
            continue
        csv_path = archive_data_dir / file_name
        if not csv_path.exists():
            continue
        release_raw = idx_row.get("archive_release_name")
        release_name = "" if pd.isna(release_raw) else str(release_raw).strip()
        start_end = catalog_release_dates.get(release_name, (pd.NaT, pd.NaT))
        if pd.isna(start_end[0]):
            yr = pd.to_numeric(idx_row.get("year"), errors="coerce")
            if pd.notna(yr):
                start_end = (pd.Timestamp(year=int(yr), month=1, day=1), pd.Timestamp(year=int(yr), month=12, day=31))

        try:
            frame = pd.read_csv(csv_path, dtype=str, encoding="latin1", on_bad_lines="skip")
        except Exception:
            continue
        if frame.empty:
            continue
        # normalize column names across historical files
        col_map = {str(col).strip().lower().replace('"', ""): col for col in frame.columns}

        def _find_col(*needles: str) -> str | None:
            for key, original in col_map.items():
                key_simple = key.replace("_", " ").replace("-", " ")
                if all(needle in key_simple for needle in needles):
                    return original
            return None

        hts_col = _find_col("hts", "number") or _find_col("htsno")
        desc_col = _find_col("description") or _find_col("brief", "description")
        add_col = _find_col("additional", "duties") or _find_col("additional", "duty")
        if hts_col is None:
            continue
        desc_series = frame[desc_col] if desc_col in frame.columns else pd.Series("", index=frame.index, dtype="string")
        add_series = frame[add_col] if add_col in frame.columns else pd.Series("", index=frame.index, dtype="string")
        hts_series = frame[hts_col].astype("string")

        # Direct references on regular HS lines: "See 9903.xx.xx".
        for hts_raw, desc, add in zip(hts_series, desc_series, add_series):
            hts_text = "" if pd.isna(hts_raw) else str(hts_raw).strip().replace('"', "")
            if not hs10_text_re.match(hts_text):
                continue
            if hts_text.startswith("99"):
                continue
            text_blob = f"{desc} {add}"
            rule_hits = sorted({hit.replace(".", "") for hit in rule_text_re.findall(str(text_blob))})
            if not rule_hits:
                continue
            hs8 = hts_text[:10].replace(".", "")
            for rule in rule_hits:
                if _TRADEWAR_RULE_RE.match(rule):
                    rows.append(
                        {
                            "release_name": release_name,
                            "release_start_date": start_end[0],
                            "release_end_date": start_end[1],
                            "hs8": hs8,
                            "rule_code": rule,
                        }
                    )

        # Chapter 99 rows that enumerate covered HS headings/subheadings.
        for hts_raw, desc in zip(hts_series, desc_series):
            hts_text = "" if pd.isna(hts_raw) else str(hts_raw).strip().replace('"', "")
            if not hts_text.startswith("9903."):
                continue
            rule_code = normalize_hs_code(hts_text, 8)
            if not _TRADEWAR_RULE_RE.match(str(rule_code)):
                continue
            hs8_hits = sorted({hit.replace(".", "") for hit in hs8_text_re.findall(str(desc))})
            for hs8 in hs8_hits:
                rows.append(
                    {
                        "release_name": release_name,
                        "release_start_date": start_end[0],
                        "release_end_date": start_end[1],
                        "hs8": hs8,
                        "rule_code": str(rule_code),
                    }
                )

    if not rows:
        empty = pd.DataFrame(columns=["release_name", "release_start_date", "release_end_date", "hs8", "rule_code"])
        empty.to_parquet(cache_path, index=False)
        return empty
    out = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)
    out.to_parquet(cache_path, index=False)
    return out


def _load_tradewar_rule_attributes(config: PipelineConfig) -> pd.DataFrame:
    """Load active 9903 trade-war rule attributes from annual files."""
    annual_dir = config.raw_dir / "policy" / "annual"
    rows: list[dict[str, Any]] = []
    for zip_path in sorted(annual_dir.glob("tariff_data_*.zip")):
        year_match = re.search(r"(19|20)\d{2}", zip_path.name)
        if not year_match:
            continue
        year = int(year_match.group(0))
        if year < 2017 or year > int(config.end_period[:4]):
            continue
        try:
            with zipfile.ZipFile(zip_path) as handle:
                txt_candidates = [name for name in handle.namelist() if name.lower().endswith(".txt")]
                if not txt_candidates:
                    continue
                df_parts: list[pd.DataFrame] = []
                for txt_name in sorted(txt_candidates):
                    try:
                        with handle.open(txt_name) as raw:
                            part = pd.read_csv(raw, dtype=str, engine="python", sep=",", on_bad_lines="skip", encoding="latin1")
                    except Exception:
                        continue
                    if not part.empty:
                        df_parts.append(part)
                if not df_parts:
                    continue
                df = pd.concat(df_parts, ignore_index=True)
        except Exception:
            continue
        for col in ("hts8", "mfn_text_rate", "mfn_ad_val_rate", "brief_description", "begin_effect_date", "end_effective_date"):
            if col not in df.columns:
                df[col] = pd.NA
        df["rule_code"] = df["hts8"].map(lambda value: normalize_hs_code(value, 8))
        df = df[df["rule_code"].astype(str).map(lambda value: bool(_TRADEWAR_RULE_RE.match(str(value))))].copy()
        if df.empty:
            continue
        df["effective_start"] = _clean_date_field(df["begin_effect_date"]).fillna(pd.Timestamp(year=year, month=1, day=1))
        df["effective_end"] = _clean_date_field(df["end_effective_date"]).fillna(pd.Timestamp(year=year, month=12, day=31))
        df["increment_rate"] = df.apply(lambda row: _parse_increment_rate(row.get("mfn_text_rate"), row.get("mfn_ad_val_rate")), axis=1)
        df = df[df["increment_rate"].notna()].copy()
        if df.empty:
            continue
        for _, row in df.iterrows():
            rows.append(
                {
                    "rule_code": str(row["rule_code"]),
                    "effective_start": row["effective_start"],
                    "effective_end": row["effective_end"],
                    "increment_rate": float(row["increment_rate"]),
                    "description": str(row.get("brief_description") or ""),
                }
            )
    if not rows:
        parquet_path = config.reference_dir / "tradewar_rule_attributes.parquet"
        if parquet_path.exists():
            try:
                cached = pd.read_parquet(parquet_path)
                if not cached.empty:
                    for column in ("effective_start", "effective_end"):
                        if column not in cached.columns:
                            cached[column] = pd.NaT
                    return cached
            except Exception:
                pass
        return pd.DataFrame(columns=["rule_code", "year", "month", "increment_rate", "description", "effective_start", "effective_end"])
    rules = pd.DataFrame(rows)
    expanded: list[dict[str, Any]] = []
    for _, row in rules.iterrows():
        start = pd.Period(row["effective_start"], freq="M")
        end = pd.Period(row["effective_end"], freq="M")
        for period in pd.period_range(start, end, freq="M"):
            expanded.append(
                {
                    "rule_code": row["rule_code"],
                    "year": int(period.year),
                    "month": int(period.month),
                    "increment_rate": float(row["increment_rate"]),
                    "description": str(row["description"]),
                    "effective_start": row["effective_start"],
                    "effective_end": row["effective_end"],
                }
            )
    out = pd.DataFrame(expanded).drop_duplicates(["rule_code", "year", "month"], keep="last").reset_index(drop=True)
    return out


def _load_manual_tradewar_overrides(config: PipelineConfig) -> pd.DataFrame:
    """Load manual deterministic overrides for unresolved policy cells."""
    path = config.manual_input_dir / "policy" / "tradewar_rule_overrides.csv"
    if not path.exists():
        return pd.DataFrame(
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
    frame = pd.read_csv(path, dtype=str)
    if frame.empty:
        return pd.DataFrame(
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
    for col in ("cty_name", "hs8", "tw_rule_code_raw"):
        if col not in frame.columns:
            frame[col] = pd.NA
    frame["cty_name"] = frame["cty_name"].map(_canonical_country)
    frame["hs8"] = frame["hs8"].map(lambda value: normalize_hs_code(value, 8))
    frame["year"] = pd.to_numeric(frame.get("year"), errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(frame.get("month"), errors="coerce").astype("Int64")
    frame["tw_increment_rate_raw"] = pd.to_numeric(frame.get("tw_increment_rate_raw"), errors="coerce")
    frame["tw_active_share_raw"] = pd.to_numeric(frame.get("tw_active_share_raw"), errors="coerce").fillna(1.0)
    frame["tw_rule_code_raw"] = frame["tw_rule_code_raw"].map(lambda value: normalize_hs_code(value, 8))
    frame["tw_scope_source_raw"] = frame.get("tw_scope_source_raw", pd.Series("manual_override", index=frame.index))
    out = frame.dropna(subset=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw"]).copy()
    return out[
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
    ].reset_index(drop=True)


def _load_raw_tradewar_overlay(config: PipelineConfig, countries: pd.Series) -> pd.DataFrame:
    """Construct bilateral trade-war increments from revision PDF note links + annual Chapter 99 rule metadata."""
    machine_links = _load_tradewar_machine_links(config)
    pdf_csv_links = _load_tradewar_pdf_csv_links(config)
    pdf_links = _load_tradewar_pdf_links(config)
    keys = ["release_name", "hs8", "rule_code"]

    def _append_missing_links(base: pd.DataFrame, fallback: pd.DataFrame) -> pd.DataFrame:
        if fallback.empty:
            return base
        if base.empty:
            return fallback.copy()
        base_keys = base[keys].drop_duplicates()
        tmp = fallback.merge(base_keys.assign(_in_base=1), on=keys, how="left")
        add = tmp[tmp["_in_base"].isna()].drop(columns=["_in_base"])
        if add.empty:
            return base
        return pd.concat([base, add], ignore_index=True).drop_duplicates(keys, keep="first").reset_index(drop=True)

    links = _load_reference_tradewar_links(config, "tradewar_pdf_links.parquet", ("990388",))
    links = _append_missing_links(links, pdf_links)
    links = _append_missing_links(links, pdf_csv_links)
    links = _append_missing_links(links, machine_links)

    rule_attrs = _load_tradewar_rule_attributes(config)
    if links.empty or rule_attrs.empty:
        return pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_active_share_raw", "tw_scope_source_raw"])

    links = links.copy()
    links["release_start_date"] = pd.to_datetime(links["release_start_date"], errors="coerce")
    links["release_end_date"] = pd.to_datetime(links["release_end_date"], errors="coerce")
    links["hs8"] = links["hs8"].map(lambda value: normalize_hs_code(value, 8))
    links["rule_code"] = links["rule_code"].map(lambda value: normalize_hs_code(value, 8))
    links["tw_scope_source_raw"] = "machine_or_pdf"
    links = links.dropna(subset=["hs8", "rule_code"]).copy()

    start_period = pd.Period(config.start_period, freq="M")
    end_period = pd.Period(config.end_period, freq="M")
    rule_attrs = rule_attrs.copy()
    rule_attrs["rule_code"] = rule_attrs["rule_code"].map(lambda value: normalize_hs_code(value, 8))
    rule_attrs["year"] = pd.to_numeric(rule_attrs["year"], errors="coerce").astype("Int64")
    rule_attrs["month"] = pd.to_numeric(rule_attrs["month"], errors="coerce").astype("Int64")
    rule_attrs["increment_rate"] = pd.to_numeric(rule_attrs["increment_rate"], errors="coerce")
    if "effective_start" in rule_attrs.columns:
        rule_attrs["effective_start"] = pd.to_datetime(rule_attrs["effective_start"], errors="coerce")
    else:
        rule_attrs["effective_start"] = pd.NaT
    if "effective_end" in rule_attrs.columns:
        rule_attrs["effective_end"] = pd.to_datetime(rule_attrs["effective_end"], errors="coerce")
    else:
        rule_attrs["effective_end"] = pd.NaT
    expanded = links.merge(rule_attrs, on="rule_code", how="inner", suffixes=("", "_rule"))
    expanded = expanded.dropna(subset=["year", "month", "increment_rate"]).copy()
    expanded = expanded.loc[pd.to_numeric(expanded["increment_rate"], errors="coerce").fillna(0.0) > 0.0].copy()
    if expanded.empty:
        return pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_active_share_raw", "tw_scope_source_raw"])
    expanded["year"] = pd.to_numeric(expanded["year"], errors="coerce").astype("Int64")
    expanded["month"] = pd.to_numeric(expanded["month"], errors="coerce").astype("Int64")
    expanded = expanded.loc[expanded["year"].between(int(config.start_period[:4]), int(config.end_period[:4]))].copy()
    if expanded.empty:
        return pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_active_share_raw", "tw_scope_source_raw"])

    country_values = sorted({_canonical_country(value) for value in countries.dropna().astype(str).str.upper().unique()})
    bilateral_rows: list[dict[str, Any]] = []
    for _, row in expanded.iterrows():
        rule_code = str(row.get("rule_code") or "")
        year = int(row["year"])
        month = int(row["month"])
        eligible = _eligible_countries_by_deterministic_grouping(rule_code, year, month, country_values)
        if _rule_family(rule_code) == "other":
            include, exclude = _extract_countries_from_rule(row.get("description"), rule_code)
            include_set = set(_canonical_country(value) for value in include)
            exclude_set = set(_canonical_country(value) for value in exclude)
            if include_set:
                eligible = [cty for cty in eligible if cty in include_set]
            if exclude_set:
                eligible = [cty for cty in eligible if cty not in exclude_set]
        for country in eligible:
            effective_start = pd.to_datetime(row.get("effective_start"), errors="coerce")
            if pd.isna(effective_start):
                effective_start = pd.to_datetime(row.get("release_start_date"), errors="coerce")
            effective_end = pd.to_datetime(row.get("effective_end"), errors="coerce")
            if pd.isna(effective_end):
                effective_end = pd.to_datetime(row.get("release_end_date"), errors="coerce")
            if pd.isna(effective_start):
                continue
            if pd.isna(effective_end):
                effective_end = effective_start
            bilateral_rows.append(
                {
                    "cty_name": country,
                    "hs8": str(row["hs8"]),
                    "year": year,
                    "month": month,
                    "tw_increment_rate_raw": float(row["increment_rate"]),
                    "tw_rule_code_raw": rule_code,
                    "tw_active_share_raw": float(_month_active_share_from_range(effective_start, effective_end, year, month)),
                    "tw_scope_source_raw": str(row.get("tw_scope_source_raw") or "machine_or_pdf") + "|deterministic_grouping",
                }
            )

    if not bilateral_rows:
        return pd.DataFrame(columns=["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_rule_code_raw", "tw_active_share_raw", "tw_scope_source_raw"])
    overlay = pd.DataFrame(bilateral_rows)
    overlay = (
        overlay.sort_values(
            ["cty_name", "hs8", "year", "month", "tw_increment_rate_raw", "tw_active_share_raw", "tw_scope_source_raw"],
            ascending=[True, True, True, True, False, False, True],
        )
        .drop_duplicates(["cty_name", "hs8", "year", "month"], keep="first")
        .reset_index(drop=True)
    )
    manual = _load_manual_tradewar_overrides(config)
    if not manual.empty:
        overlay = pd.concat([overlay, manual], ignore_index=True)
        source_priority = {"manual_override": 0, "machine_or_pdf": 1}
        overlay["_source_priority"] = overlay["tw_scope_source_raw"].map(lambda value: source_priority.get(str(value), 2))
        overlay = (
            overlay.sort_values(
                ["cty_name", "hs8", "year", "month", "_source_priority", "tw_increment_rate_raw", "tw_active_share_raw"],
                ascending=[True, True, True, True, True, False, False],
            )
            .drop_duplicates(["cty_name", "hs8", "year", "month"], keep="first")
            .drop(columns=["_source_priority"], errors="ignore")
            .reset_index(drop=True)
        )
    return overlay


def run_raw_tradewar_overlay_build(config: PipelineConfig) -> dict[str, Any]:
    """Rebuild only the raw 9903 trade-war overlay from cached/raw policy sources."""
    panel_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(
            f"Cannot rebuild trade-war overlay without the country universe from {panel_path}"
        )
    countries = read_table(panel_path, columns=["cty_name"])["cty_name"].astype("string").str.upper().map(_canonical_country)

    overlay = _load_raw_tradewar_overlay(config, countries)
    overlay_path = config.analysis_dir / "tradewar_overlay_raw.parquet"
    write_parquet(overlay, overlay_path, overwrite=True)

    panel = read_table(panel_path)
    panel["cty_name"] = panel["cty_name"].astype("string").str.upper()
    panel["cty_name_canon"] = panel["cty_name"].map(_canonical_country)
    replace_columns = [
        "tw_increment_rate_raw",
        "tw_rule_code_raw",
        "tw_active_share_raw",
        "tw_scope_source_raw",
        "cty_name_tw",
    ]
    panel = panel.drop(columns=[column for column in replace_columns if column in panel.columns], errors="ignore")
    if not overlay.empty:
        panel = panel.merge(
            overlay,
            left_on=["cty_name_canon", "hs8", "year", "month"],
            right_on=["cty_name", "hs8", "year", "month"],
            how="left",
            suffixes=("", "_tw"),
        )
    else:
        panel["tw_increment_rate_raw"] = pd.NA
        panel["tw_rule_code_raw"] = pd.NA
        panel["tw_active_share_raw"] = pd.NA
        panel["tw_scope_source_raw"] = pd.NA

    panel["mfn_ad_val_rate"] = pd.to_numeric(panel["mfn_ad_val_rate"], errors="coerce")
    panel["base_pref_rate_raw"] = pd.to_numeric(panel["base_pref_rate_raw"], errors="coerce")
    panel["tw_increment_rate_raw"] = pd.to_numeric(panel["tw_increment_rate_raw"], errors="coerce")
    panel["tw_active_share_raw"] = pd.to_numeric(panel["tw_active_share_raw"], errors="coerce").fillna(1.0)
    panel["base_statutory_rate_raw"] = panel["base_pref_rate_raw"].where(panel["base_pref_rate_raw"].notna(), panel["mfn_ad_val_rate"])
    panel["m_statutory_tariff1"] = panel["base_statutory_rate_raw"].fillna(0.0) + panel["tw_increment_rate_raw"].fillna(0.0)
    panel["m_statutory_tariff2"] = panel["base_statutory_rate_raw"].fillna(0.0) + panel["tw_increment_rate_raw"].fillna(0.0) * panel["tw_active_share_raw"]
    panel["m_policy_source"] = pd.Series("mfn_schedule_only", index=panel.index, dtype="string")
    panel.loc[panel["base_pref_rate_raw"].notna(), "m_policy_source"] = "base_preference_raw"
    panel.loc[panel["tw_increment_rate_raw"].notna(), "m_policy_source"] = "trade_war_raw_overlay"
    panel = panel.drop(columns=[column for column in ["cty_name_canon", "cty_name_tw"] if column in panel.columns])
    write_parquet(panel, panel_path, overwrite=True)

    return {
        "tradewar_overlay_raw_path": str(overlay_path),
        "tradewar_overlay_raw_rows": int(len(overlay)),
        "tradewar_overlay_raw_china_rows": int(overlay["cty_name"].eq("CHINA").sum()) if "cty_name" in overlay.columns else 0,
        "panel_path": str(panel_path),
        "panel_rows": int(len(panel)),
    }


def run_us_products_partner_panel_build(config: PipelineConfig) -> dict[str, Any]:
    """Merge imports, exports, and monthly HTS schedule into one wide panel."""
    imports = _load_trade_panel(config, "m")
    exports = _load_trade_panel(config, "x")

    key_cols = ["cty_code", "cty_name", "hs10", "year", "month"]
    imports_idx = imports.set_index(key_cols)
    exports_idx = exports.set_index(key_cols)
    panel = imports_idx.join(exports_idx, how="outer").reset_index()
    panel["hs10"] = panel["hs10"].map(lambda value: normalize_hs_code(value, 10))
    panel = add_hierarchy_codes(panel, source_column="hs10")

    schedule_path = config.reference_dir / "hts_monthly_hs10_schedule.parquet"
    if schedule_path.exists():
        schedule = read_table(schedule_path, columns=["hs10", "year", "month", "mfn_text_rate", "mfn_ad_val_rate", "additional_duty", "source_type", "release_name"])
        schedule["hs10"] = schedule["hs10"].map(lambda value: normalize_hs_code(value, 10))
        panel = panel.merge(schedule, on=["hs10", "year", "month"], how="left")
    else:
        panel["mfn_text_rate"] = pd.NA
        panel["mfn_ad_val_rate"] = pd.NA
        panel["additional_duty"] = pd.NA
        panel["source_type"] = pd.NA
        panel["release_name"] = pd.NA

    # Bilateral raw-source base preference rates from annual HTS (FTA/NAFTA country columns).
    panel["cty_name"] = panel["cty_name"].astype("string").str.upper()
    panel["cty_name_canon"] = panel["cty_name"].map(_canonical_country)

    # Persist intermediate raw-source policy artifacts for auditability.
    tradewar_links = _load_tradewar_pdf_links(config)
    if not tradewar_links.empty:
        write_parquet(tradewar_links, config.reference_dir / "tradewar_pdf_links.parquet", overwrite=True)
    tradewar_machine_links = _load_tradewar_machine_links(config)
    if not tradewar_machine_links.empty:
        write_parquet(tradewar_machine_links, config.reference_dir / "tradewar_machine_links.parquet", overwrite=True)
    tradewar_rule_attrs = _load_tradewar_rule_attributes(config)
    if not tradewar_rule_attrs.empty:
        write_parquet(tradewar_rule_attrs, config.reference_dir / "tradewar_rule_attributes.parquet", overwrite=True)

    pref_overrides = _load_raw_partner_preference_overrides(config, panel["cty_name_canon"])
    if not pref_overrides.empty:
        panel = panel.merge(
            pref_overrides,
            left_on=["cty_name_canon", "hs8", "year", "month"],
            right_on=["cty_name", "hs8", "year", "month"],
            how="left",
            suffixes=("", "_pref"),
        )
    else:
        panel["base_pref_rate_raw"] = pd.NA
        panel["base_pref_source"] = pd.NA

    # Bilateral raw-source policy overlay: 9903 trade-war increments on top of base statutory rate.
    tw_overlay = _load_raw_tradewar_overlay(config, panel["cty_name_canon"])
    if not tw_overlay.empty:
        write_parquet(tw_overlay, config.analysis_dir / "tradewar_overlay_raw.parquet", overwrite=True)
    if not tw_overlay.empty:
        panel = panel.merge(
            tw_overlay,
            left_on=["cty_name_canon", "hs8", "year", "month"],
            right_on=["cty_name", "hs8", "year", "month"],
            how="left",
            suffixes=("", "_tw"),
        )
    else:
        panel["tw_increment_rate_raw"] = pd.NA
        panel["tw_rule_code_raw"] = pd.NA
        panel["tw_active_share_raw"] = pd.NA
        panel["tw_scope_source_raw"] = pd.NA

    panel["mfn_ad_val_rate"] = pd.to_numeric(panel["mfn_ad_val_rate"], errors="coerce")
    panel["base_pref_rate_raw"] = pd.to_numeric(panel["base_pref_rate_raw"], errors="coerce")
    panel["tw_increment_rate_raw"] = pd.to_numeric(panel["tw_increment_rate_raw"], errors="coerce")
    panel["tw_active_share_raw"] = pd.to_numeric(panel["tw_active_share_raw"], errors="coerce").fillna(1.0)
    panel["base_statutory_rate_raw"] = panel["base_pref_rate_raw"].where(panel["base_pref_rate_raw"].notna(), panel["mfn_ad_val_rate"])
    panel["m_statutory_tariff1"] = panel["base_statutory_rate_raw"].fillna(0.0) + panel["tw_increment_rate_raw"].fillna(0.0)
    panel["m_statutory_tariff2"] = panel["base_statutory_rate_raw"].fillna(0.0) + panel["tw_increment_rate_raw"].fillna(0.0) * panel["tw_active_share_raw"]
    panel["m_policy_source"] = pd.Series("mfn_schedule_only", index=panel.index, dtype="string")
    panel.loc[panel["base_pref_rate_raw"].notna(), "m_policy_source"] = "base_preference_raw"
    panel.loc[panel["tw_increment_rate_raw"].notna(), "m_policy_source"] = "trade_war_raw_overlay"
    panel = panel.drop(columns=[column for column in ["cty_name_canon", "cty_name_tw", "cty_name_pref"] if column in panel.columns])

    panel["period"] = panel["year"].astype(int).astype(str).str.zfill(4) + "-" + panel["month"].astype(int).astype(str).str.zfill(2)
    panel["mdate"] = pd.to_datetime(panel["period"] + "-01")
    panel = panel.sort_values(["cty_code", "hs10", "year", "month"]).reset_index(drop=True)

    output_path = config.analysis_dir / "us_products_partner_hs10_monthly.parquet"
    write_parquet(panel, output_path, overwrite=True)
    write_data_dictionary(panel, config.analysis_dir / "us_products_partner_hs10_monthly.dictionary.json", key_columns=["cty_code", "hs10", "year", "month"])

    scope_counts: dict[str, int] = {}
    if "tw_scope_source_raw" in panel.columns:
        counts = panel["tw_scope_source_raw"].astype("string").fillna("missing").value_counts(dropna=False)
        scope_counts = {str(key): int(value) for key, value in counts.to_dict().items()}

    diagnostics = {
        "rows": int(len(panel)),
        "imports_rows": int(len(imports)),
        "exports_rows": int(len(exports)),
        "unique_keys": int(panel[["cty_code", "hs10", "year", "month"]].drop_duplicates().shape[0]),
        "duplicate_keys": int(panel.duplicated(["cty_code", "hs10", "year", "month"]).sum()),
        "tariff_non_null_rows": int(panel["mfn_text_rate"].notna().sum()) if "mfn_text_rate" in panel.columns else 0,
        "m_base_pref_rows": int(panel["base_pref_rate_raw"].notna().sum()) if "base_pref_rate_raw" in panel.columns else 0,
        "m_policy_overlay_rows": int(panel["tw_increment_rate_raw"].notna().sum()) if "tw_increment_rate_raw" in panel.columns else 0,
        "m_policy_overlay_partial_rows": int((panel["tw_increment_rate_raw"].notna() & (panel["tw_active_share_raw"] < 0.999)).sum()) if {"tw_increment_rate_raw", "tw_active_share_raw"}.issubset(panel.columns) else 0,
        "m_policy_overlay_scope_source_counts": scope_counts,
        "m_statutory_tariff1_non_null_rows": int(panel["m_statutory_tariff1"].notna().sum()) if "m_statutory_tariff1" in panel.columns else 0,
        "output_path": str(output_path),
        "period_start": config.start_period,
        "period_end": config.end_period,
    }
    write_metadata_json(config.analysis_dir / "us_products_partner_hs10_monthly.metadata.json", diagnostics)
    return diagnostics
