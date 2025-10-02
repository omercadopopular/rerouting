
#!/usr/bin/env python3
"""
census_trade_download.py
------------------------

Download U.S. import data (monthly or annual) from the U.S. Census
International Trade Time‑Series API at the HS‑10 level.

Two mutually exclusive modes
----------------------------
(a) --countries  : one or many partner countries, **all** products
(b) --hs10       : one or many HS‑10 codes, **all** countries

Required arguments
------------------
--start YYYY‑MM or YYYY
--end   YYYY‑MM or YYYY

Optional
--------
--countries      Comma‑separated list of partner names (e.g. "China,Mexico")
                 or codes (e.g. "5700,2010")
--hs10           Comma‑separated list of HS10 codes  (e.g. "8542391020,2709001000")
--max-rows       API row limit per call (default 5000)
--sleep          Seconds to wait between calls      (default 0.2)
--outfile        CSV path (default "census_imports.csv")
--apikey         Census API key (optional but recommended)

Examples
--------
# 1. All HS‑10 products imported from China & Mexico, 2020‑01 → 2024‑12
python census_trade_download.py --countries China,Mexico --start 2020‑01 --end 2024‑12

# 2. HS 3002140000 and 8703230130 from **all** partners in 2023
python census_trade_download.py --hs10 3002140000,8703230130 --start 2023 --end 2023

Author: Carlos Góes and OpenAI ChatGPT‑o3 • 2025‑06‑12
"""

import argparse
import itertools
import sys
import time
from datetime import datetime

import pandas as pd
import requests

API_ROOT = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
DEFAULT_GET = ",".join(
    [
        "CTY_CODE",
        "CTY_NAME",
        "I_COMMODITY",
        "I_COMMODITY_SDESC",
        "GEN_VAL_MO",
        "GEN_VAL_YR",
    ]
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download U.S. import data (Census API).")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--countries",
        help="Comma‑separated list of partner countries (names or CTY_CODEs).",
    )
    group.add_argument(
        "--hs10",
        help="Comma‑separated list of HS‑10 codes (10 digits).",
    )
    p.add_argument("--start", required=True, help="Start period YYYY‑MM or YYYY")
    p.add_argument("--end", required=True, help="End period YYYY‑MM or YYYY")
    p.add_argument(
        "--max-rows",
        type=int,
        default=5000,
        help="Maximum rows per API call (default 5000).",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep seconds between API calls (default 0.2).",
    )
    p.add_argument(
        "--outfile",
        default="census_imports.csv",
        help='Output CSV (default "census_imports.csv").',
    )
    p.add_argument("--apikey", help="Your Census API key (recommended).")
    return p.parse_args()


# ----------------------------------------------------------------------
# Helpers: country <‑> code mapping
# ----------------------------------------------------------------------
def fetch_country_table(api_key: str | None = None) -> pd.DataFrame:
    """
    Build a dataframe mapping CTY_CODE -> CTY_NAME by querying a dummy
    HS‑10 and month that returns all partners (code 00 is crude oil).
    """
    params = {
        "get": "CTY_CODE,CTY_NAME",
        "COMM_LVL": "HS2",
        "I_COMMODITY": "00",
        "time": "2024-01",
    }
    if api_key:
        params["key"] = api_key
    url = API_ROOT
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0]).drop_duplicates()
    return df


def name_to_code(country_list: list[str], cty_tbl: pd.DataFrame) -> list[str]:
    """
    Accept CTY_NAME or CTY_CODE strings and return CTY_CODE list.
    Case‑insensitive substring match for names; pass‑through for codes.
    """
    codes = []
    for item in country_list:
        item = item.strip()
        if item.isdigit():
            codes.append(item)
            continue
        # fuzzy case‑insensitive match
        match = cty_tbl[
            cty_tbl["CTY_NAME"].str.lower().str.contains(item.lower())
        ]["CTY_CODE"]
        if match.empty:
            raise ValueError(f"Country '{item}' not found in CTY_NAME list.")
        codes.extend(match.tolist())
    return list(dict.fromkeys(codes))  # preserve order, dedupe


# ----------------------------------------------------------------------
# Period handling
# ----------------------------------------------------------------------
def build_period_list(start: str, end: str) -> list[str]:
    """
    Return list of period strings accepted by API ('YYYY‑MM' or 'YYYY').
    """
    monthly = len(start) == 7 and len(end) == 7
    if monthly:
        periods = pd.period_range(start, end, freq="M")
        return [p.strftime("%Y-%m") for p in periods]
    else:
        periods = range(int(start), int(end) + 1)
        return [str(y) for y in periods]


# ----------------------------------------------------------------------
# API query core
# ----------------------------------------------------------------------
def api_call(
    countries: list[str],
    commodities: list[str],
    periods: list[str],
    api_key: str | None,
    max_rows: int,
    sleep_sec: float,
) -> pd.DataFrame:
    """
    Fetch data in chunks not exceeding `max_rows` rows.
    Returns concatenated dataframe.
    """
    dfs = []
    # chunk generator
    for (c_block, h_block, t_block) in chunk_iterables(
        countries, commodities, periods, max_rows
    ):
        params = {
            "get": DEFAULT_GET,
            "COMM_LVL": "HS10",
            "CTY_CODE": ",".join(c_block),
            "I_COMMODITY": ",".join(h_block),
            "time": ",".join(t_block),
        }
        if api_key:
            params["key"] = api_key
        r = requests.get(API_ROOT, params=params, timeout=30)
        if r.status_code != 200:
            sys.stderr.write(
                f"API error {r.status_code}: {r.text[:200]}…\nSkipping block.\n"
            )
            continue
        data = r.json()
        dfs.append(pd.DataFrame(data[1:], columns=data[0]))
        time.sleep(sleep_sec)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def chunk_iterables(
    countries: list[str], hs_codes: list[str], periods: list[str], limit_rows: int
):
    """
    Yield (country_block, hs_block, time_block) with estimated rows ≤ limit_rows.
    Greedy chunking by gradually adding dimensions.
    """
    def chunks(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    est = lambda c, h, t: len(c) * len(h) * len(t)
    # Get max sizes for each dimension st product <= limit_rows
    # Start with optimistic full-size packages, reduce dimensions progressively
    c_total, h_total, t_total = len(countries), len(hs_codes), len(periods)
    # basic min sizing
    h_chunk_size = max(1, limit_rows // (c_total * 1))
    t_chunk_size = max(1, limit_rows // (c_total * h_chunk_size))
    # iterate
    for h_block in chunks(hs_codes, h_chunk_size):
        for t_block in chunks(periods, t_chunk_size):
            c_chunk_size = max(1, limit_rows // (len(h_block) * len(t_block)))
            for c_block in chunks(countries, c_chunk_size):
                yield (c_block, h_block, t_block)


# ----------------------------------------------------------------------
# Utility: get all HS‑10 for country
# ----------------------------------------------------------------------
def get_all_hs10_for_country(cty_code: str, sample_period: str, api_key: str | None):
    """
    Hit API once to fetch HS‑10 list for the given country & period.
    """
    params = {
        "get": "I_COMMODITY",
        "COMM_LVL": "HS10",
        "CTY_CODE": cty_code,
        "time": sample_period,
    }
    if api_key:
        params["key"] = api_key
    r = requests.get(API_ROOT, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    codes = sorted({row[0] for row in data[1:] if row[0]})
    return codes


# ----------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    periods = build_period_list(args.start, args.end)
    cty_tbl = fetch_country_table(args.apikey)

    if args.countries:
        country_list_in = [x.strip() for x in args.countries.split(",")]
        countries = name_to_code(country_list_in, cty_tbl)
        hs_codes = get_all_hs10_for_country(countries[0], periods[0], args.apikey)
    else:
        hs_codes = [c.strip() for c in args.hs10.split(",")]
        countries = cty_tbl["CTY_CODE"].tolist()

    print(
        f"Querying {len(countries)} countries × "
        f"{len(hs_codes)} HS‑10 codes × "
        f"{len(periods)} periods."
    )

    df = api_call(
        countries,
        hs_codes,
        periods,
        args.apikey,
        args.max_rows,
        args.sleep,
    )
    if df.empty:
        print("No data returned.")
        return
    df.to_csv(args.outfile, index=False)
    print(f"Saved {len(df):,} rows to {args.outfile}")


if __name__ == "__main__":
    main()
