#!/usr/bin/env python3
"""
tariff_hs10_download.py  —  Build bilateral tariff panel (HTSUS) at HS-10 × country × year.

CHOICES (pick exactly one focus, mirroring your flows script intent):
  --countries  Mexico,China        # fetch ALL HS-10 lines for those partners
  --hs10       8542391020,9405604000  # fetch these HS-10 codes (requires --countries, e.g., ALL or a list)

COMMON OPTIONS
  --start   2019           # first year (inclusive)
  --end     2025           # last year  (inclusive)
  --outfile path/to/file.csv
  --parquet path/to/file.parquet  # optional
  --cache   hts_cache      # where yearly HTS CSVs are stored/cached
  --countries ALL          # expands to all known partners (FTAs + program sets)

Notes on "ALL":
- "ALL" expands to: all FTA partners we hard-code below (USMCA, CAFTA-DR, AU, CL, CO, JP, KR, ...),
  PLUS every country you list in an optional CSV file named pref_country_lists.csv
  with columns: program,country (e.g., ALL_GSP,Albania). A template is easy to create.
"""

import argparse
import io
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests

# ---------------------------------------------------------------------
# USITC HTS CSV archive pattern (stable naming)
# ---------------------------------------------------------------------
ARCHIVE_TMPL = (
    "https://www.usitc.gov/sites/default/files/tata/hts/"
    "hts_{year}_revision_{rev}_csv.csv"
)

# ---------------------------------------------------------------------
# Column 2 countries (as of 2025). Keep uppercase for matching.
# ---------------------------------------------------------------------
COLUMN2_COUNTRIES = {
    "AFGHANISTAN", "CUBA", "NORTH KOREA", "RUSSIA", "BELARUS", "SYRIA"
}

# ---------------------------------------------------------------------
# Country name normalization (map common aliases -> HTS canonical names)
# ---------------------------------------------------------------------
COUNTRY_ALIASES = {
    "KOREA": "KOREA, REPUBLIC OF",
    "SOUTH KOREA": "KOREA, REPUBLIC OF",
    "IRAN": "IRAN, ISLAMIC REPUBLIC OF",
    "VIETNAM": "VIET NAM",
    "LAOS": "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
    "BOLIVIA": "BOLIVIA (PLURINATIONAL STATE OF)",
    "CAR": "CENTRAL AFRICAN REPUBLIC",
    "CZECHIA": "CZECH REPUBLIC",
    "ESWATINI": "SWAZILAND",
    "MYANMAR": "BURMA",
    "IVORY COAST": "COTE D'IVOIRE",
    "TANZANIA": "TANZANIA, UNITED REPUBLIC OF",
    "UK": "UNITED KINGDOM",
    "UAE": "UNITED ARAB EMIRATES",
    "US": "UNITED STATES",
    "USA": "UNITED STATES",
}
def norm_name(n: str) -> str:
    n_up = n.strip().upper()
    return COUNTRY_ALIASES.get(n_up, n_up)

# ---------------------------------------------------------------------
# Explicit FTA/preference mappings used in HTS "Special" column tags
# (program code -> list of canonical country names)
# ---------------------------------------------------------------------
PREF_TO_COUNTRIES: Dict[str, List[str]] = {
    # USMCA
    "MX": ["MEXICO"],
    "CA": ["CANADA"],

    # FTAs
    "AU": ["AUSTRALIA"],
    "BH": ["BAHRAIN"],
    "CL": ["CHILE"],
    "CO": ["COLOMBIA"],
    "IL": ["ISRAEL"],
    "JO": ["JORDAN"],
    "JP": ["JAPAN"],
    "KR": ["KOREA, REPUBLIC OF"],
    "MA": ["MOROCCO"],
    "OM": ["OMAN"],
    "PA": ["PANAMA"],
    "PE": ["PERU"],
    "SG": ["SINGAPORE"],

    # CAFTA-DR (shares single code "P")
    "P": ["COSTA RICA", "DOMINICAN REPUBLIC", "EL SALVADOR", "GUATEMALA", "HONDURAS", "NICARAGUA"],

    # Small, fixed sets
    "J": ["BOLIVIA (PLURINATIONAL STATE OF)", "ECUADOR"],  # legacy ATPDEA
    "B": ["CANADA"],  # Automotive Products Trade Act
}

# ---------------------------------------------------------------------
# Program sets defined in General Notes (expand via CSV)
#   - Provide a local CSV "pref_country_lists.csv" with columns:
#       program,country
#     where program ∈ {ALL_GSP, ALL_GSP_LDC, ALL_AGOA, ALL_AGOA_APPAREL, ALL_CBI, CIVIL_AIRCRAFT_PARTIES}
# ---------------------------------------------------------------------
PROGRAM_SETS_KEYS = {
    "ALL_GSP", "ALL_GSP_LDC", "ALL_AGOA", "ALL_AGOA_APPAREL", "ALL_CBI", "CIVIL_AIRCRAFT_PARTIES"
}
PROGRAM_CODE_TO_SET = {  # HTS special-column code -> program set name
    "A": "ALL_GSP",
    "A*": "ALL_GSP_LDC",
    "D": "ALL_AGOA",
    "L": "ALL_AGOA_APPAREL",
    "E": "ALL_CBI",
    "C": "CIVIL_AIRCRAFT_PARTIES",
}
PROGRAM_SETS_PATH = Path("pref_country_lists.csv")

def load_program_sets(path: Path = PROGRAM_SETS_PATH) -> Dict[str, List[str]]:
    """
    Load dynamic program membership; returns {program_key: [COUNTRY, ...]} (UPPERCASE).
    If the file is absent or malformed, returns empty lists (script still works).
    """
    if not path.exists():
        return {k: [] for k in PROGRAM_SETS_KEYS}
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return {k: [] for k in PROGRAM_SETS_KEYS}
    if not {"program", "country"}.issubset(df.columns):
        return {k: [] for k in PROGRAM_SETS_KEYS}
    out = {k: [] for k in PROGRAM_SETS_KEYS}
    for prog, grp in df.groupby("program"):
        prog = str(prog).strip()
        if prog in out:
            out[prog] = [norm_name(x) for x in grp["country"].dropna().tolist()]
    return out

PROGRAM_SETS = load_program_sets()

# Build reverse map: country -> program code (explicit + program sets)
COUNTRY_TO_PREF: Dict[str, str] = {}
for pcode, clist in PREF_TO_COUNTRIES.items():
    for c in clist:
        COUNTRY_TO_PREF[norm_name(c)] = pcode
for pcode, set_name in PROGRAM_CODE_TO_SET.items():
    for c in PROGRAM_SETS.get(set_name, []):
        COUNTRY_TO_PREF[norm_name(c)] = pcode

# ---------------------------------------------------------------------
# Fetch latest revision CSV for a given year
# ---------------------------------------------------------------------
def fetch_best_hts_csv(year: int, session: requests.Session, max_rev: int = 30) -> Tuple[int, bytes]:
    last_url = None
    for rev in range(max_rev, -1, -1):
        url = ARCHIVE_TMPL.format(year=year, rev=rev)
        last_url = url
        r = session.get(url, timeout=60)
        if r.status_code == 200 and r.content.startswith(b'"HTS Number"'):
            return rev, r.content
    raise RuntimeError(f"No HTS CSV found for {year}. Last tried: {last_url}")

# ---------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------
CODE_SPLIT_RE = re.compile(r"[^\w\*]+")   # split on non-alnum, keep asterisk in A*
PAREN_RE = re.compile(r"\((.*?)\)")

def extract_program_codes(special_rate: str) -> List[str]:
    """
    From a 'Special Rate of Duty' cell, extract program codes in parentheses.
    Returns normalized list like ['MX','CA','A','A*','P',...].
    """
    if not isinstance(special_rate, str) or not special_rate:
        return []
    matches = PAREN_RE.findall(special_rate)
    if not matches:
        return []
    codes = []
    for grp in matches:
        for part in CODE_SPLIT_RE.split(grp):
            if part:
                codes.append(part.strip().upper())
    return sorted(set(codes))

def normalize_hts10(hts_number: str) -> str:
    if not isinstance(hts_number, str):
        hts_number = str(hts_number)
    s = hts_number.replace(".", "").replace(" ", "")
    return s.zfill(10)[:10]

# ---------------------------------------------------------------------
# Duty selection
# ---------------------------------------------------------------------
def duty_for_country(row: pd.Series, country_upper: str) -> Tuple[str, str]:
    """
    Return (duty_string, source) for the country on this row.
    source ∈ {'COLUMN2','SPECIAL','GENERAL'}
    """
    gen = str(row.get("General Rate of Duty", "")).strip()
    col2 = str(row.get("General Rate of Duty, Column 2", gen)).strip()
    special = str(row.get("Special Rate of Duty", "")).strip()

    if country_upper in COLUMN2_COUNTRIES:
        return (col2 if col2 else gen, "COLUMN2")

    pcode = COUNTRY_TO_PREF.get(country_upper)  # e.g., MX, CA, A, A*, P, ...
    if pcode and pcode in row.get("_codes", []):
        # keep only the text before '(' to strip "(MX,CA,...)" suffix
        return (special.split("(")[0].strip() or special.strip(), "SPECIAL")

    return (gen, "GENERAL")

# ---------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------
def build_panel(years: Iterable[int],
                countries: List[str],
                hs10_filter: List[str],
                cache_dir: Path,
                sleep: float = 0.2) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    frames = []

    norm_countries = [norm_name(c) for c in countries]

    for yr in years:
        rev, content = fetch_best_hts_csv(yr, s)
        (cache_dir / f"hts_{yr}_rev{rev}.csv").write_bytes(content)

        df = pd.read_csv(io.StringIO(content.decode("utf-8")), dtype=str)
        needed = {"HTS Number", "General Rate of Duty", "Special Rate of Duty"}
        if not needed.issubset(df.columns):
            raise RuntimeError(f"Unexpected HTS columns for {yr}. Saw: {list(df.columns)[:10]} ...")

        df["_codes"] = df["Special Rate of Duty"].apply(extract_program_codes)
        df["hts10"] = df["HTS Number"].apply(normalize_hts10)

        if hs10_filter:
            df = df[df["hts10"].isin(hs10_filter)].copy()

        rows = []
        for c_up in norm_countries:
            duty_pair = df.apply(duty_for_country, axis=1, args=(c_up,))
            duty_vals = duty_pair.map(lambda t: t[0])
            src_vals  = duty_pair.map(lambda t: t[1])
            rows.append(pd.DataFrame({
                "period": yr,
                "hts10": df["hts10"],
                "country": c_up.title(),
                "duty_string": duty_vals,
                "duty_source": src_vals,
            }))
        frames.append(pd.concat(rows, ignore_index=True))

        time.sleep(sleep)

    panel = pd.concat(frames, ignore_index=True).sort_values(["period","country","hts10"])
    panel.reset_index(drop=True, inplace=True)
    return panel

# ---------------------------------------------------------------------
# CLI and normalization (mirrors your flows script ergonomics)
# ---------------------------------------------------------------------
def parse_list(arg: str) -> List[str]:
    """Accept comma-separated list or @file to read one item per line."""
    if not arg:
        return []
    arg = arg.strip()
    if arg.startswith("@"):
        path = Path(arg[1:])
        items = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        return items
    return [x.strip() for x in arg.split(",") if x.strip()]

def all_known_countries() -> List[str]:
    """Union of explicit FTA lists + all program-set members + Column 2 set."""
    explicit = set()
    for lst in PREF_TO_COUNTRIES.values():
        for c in lst:
            explicit.add(norm_name(c))
    for set_key in PROGRAM_SETS_KEYS:
        for c in PROGRAM_SETS.get(set_key, []):
            explicit.add(norm_name(c))
    for c in COLUMN2_COUNTRIES:
        explicit.add(norm_name(c))
    # Return pretty-cased names
    return sorted({c.title() for c in explicit})

def main():
    ap = argparse.ArgumentParser(description="Download bilateral tariffs (HTSUS) by HS-10 × country × year")
    ap.add_argument("--countries", help="Comma list (or @file). Use ALL to expand to known partners.")
    ap.add_argument("--hs10", help="Comma list (or @file) of HS-10 codes (10 digits). If omitted, all HS-10 are used.")
    ap.add_argument("--start", required=True, type=int, help="First year, e.g., 2015")
    ap.add_argument("--end", required=True, type=int, help="Last year, e.g., 2025")
    ap.add_argument("--outfile", default="bilateral_tariffs.csv", help="Output CSV path")
    ap.add_argument("--parquet", default=None, help="Optional Parquet path")
    ap.add_argument("--cache", default="hts_cache", help="Folder to cache yearly HTS CSVs")
    args = ap.parse_args()

    if not args.countries and not args.hs10:
        sys.exit("Specify --countries (for all HS-10) OR --hs10 (requires --countries; pass ALL to expand).")

    # Countries
    if args.countries:
        if args.countries.strip().upper() == "ALL":
            countries = all_known_countries()
            if not countries:
                sys.exit("ALL requested but no known partners. Provide pref_country_lists.csv or a list via --countries.")
        else:
            countries = parse_list(args.countries)
    else:
        # If only HS-10 provided but no countries, default to ALL known (mirrors 'all partners')
        countries = all_known_countries()
        if not countries:
            sys.exit("No countries provided and none known; pass --countries or supply pref_country_lists.csv.")

    if not countries:
        sys.exit("Empty country set after parsing.")
    countries = [c for c in countries if c]  # clean

    # HS-10 filter (optional)
    hs10_filter = []
    if args.hs10:
        hs10_filter = parse_list(args.hs10)
        bad = [x for x in hs10_filter if not (x.isdigit() and len(x) == 10)]
        if bad:
            sys.exit(f"Invalid HS-10 codes: {bad}")

    years = range(int(args.start), int(args.end) + 1)
    panel = build_panel(years, countries, hs10_filter, Path(args.cache))

    out_csv = Path(args.outfile)
    panel.to_csv(out_csv, index=False)
    if args.parquet:
        panel.to_parquet(Path(args.parquet), index=False)

    print(f"Wrote {len(panel):,} rows to {out_csv}")
    if args.parquet:
        print(f"Also wrote Parquet -> {args.parquet}")

if __name__ == "__main__":
    main()
