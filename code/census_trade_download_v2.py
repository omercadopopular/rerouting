
#!/usr/bin/env python3
"""census_trade_download.py  –  Fetch U.S. Census International‑Trade HS‑10 imports.

CHOICES (pick exactly one):
  --countries  Mexico,China          # fetch all HS‑10 codes for those partners
  --hs10       8542391020,9405604000 # fetch those codes for all partners

COMMON OPTIONS
  --start  YYYY   or  YYYY-MM   (inclusive)
  --end    YYYY   or  YYYY-MM   (inclusive)
  --outfile path/to/file.csv
  --apikey  YOUR_CENSUS_KEY      (optional but strongly recommended)
"""

import argparse
import csv
import sys
import time
import datetime as dt
from typing import List, Dict
import requests

BASE_URL = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
MEASURE  = "GEN_VAL_MO"   # Monthly general-import value
COMM_LVL = "HS10"          # Always pull 10-digit detail
BATCH_CTRY = 30            # Country codes per request (well below 5000-row cap)
REQUEST_SLEEP = 0.15       # Seconds between API hits; tweak if throttled


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_period(p: str) -> dt.date:
    """Accept '2024' or '2024-07'; return date(YYYY,MM,1)."""
    if len(p) == 4 and p.isdigit():
        return dt.date(int(p), 1, 1)
    try:
        year, month = map(int, p.split("-", 1))
        return dt.date(year, month, 1)
    except ValueError:
        raise ValueError(f"Bad period format '{p}'. Use YYYY or YYYY-MM.")


def month_range(start: dt.date, end: dt.date) -> List[str]:
    out = []
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        out.append(cur.strftime("%Y-%m"))
        # advance 1 month
        cur = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return out


# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

def _try_country_query(params: Dict[str, str]) -> List[List[str]]:
    """Return JSON rows or empty list on HTTP 204."""
    r = requests.get(BASE_URL, params=params, timeout=30)
    if r.status_code == 204:
        return []
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text[:200]}")
    return r.json()


def fetch_country_table(api_key: str = "", ref_time: str = "2024-01") -> Dict[str, str]:
    """
    Return {upper-name: code} mapping plus direct {code: code}.
    Strategy: hit the API with a TOTAL commodity for one month.
    Fallback to an HS2 aggregate if the endpoint still returns 204.
    """
    attempts = [
        {
            "get": f"CTY_CODE,CTY_NAME,{MEASURE}",
            "I_COMMODITY": "TOTAL",
            "time": ref_time,
        },
        {
            "get": f"CTY_CODE,CTY_NAME,{MEASURE}",
            "COMM_LVL": "HS2",
            "I_COMMODITY": "01",
            "time": ref_time,
        },
    ]
    if api_key:
        for p in attempts:
            p["key"] = api_key

    rows = []
    for p in attempts:
        try:
            rows = _try_country_query(p)
        except RuntimeError:
            continue
        if rows:
            break

    if not rows:
        raise RuntimeError("Country lookup failed (all attempts returned 204).")

    out = {}
    for code, name, *_ in rows[1:]:
        if code.strip() == "-" or not name.strip():
            continue
        out[name.upper()] = code
        out[code] = code
    return out


def normalize_countries(arg: str, ctry_map: Dict[str, str]) -> List[str]:
    # NEW – treat TOTAL and bare '-' as the aggregate row
    ctry_map["TOTAL"] = "-"
    ctry_map["-"]     = "-"

    out = []
    for token in arg.split(","):
        token = token.strip()
        if not token:
            continue
        key = token.upper()
        if key not in ctry_map:
            raise ValueError(f"Unknown country: {token}")
        out.append(ctry_map[key])
    return out


def normalize_hs10(arg: str) -> List[str]:
    out = []
    for tok in arg.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if not (tok.isdigit() and len(tok) == 10):
            raise ValueError(f"Invalid HS-10 code '{tok}'")
        out.append(tok)
    return out


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def build_params(
    ctry_list: List[str], hs_list: List[str], period: str, api_key: str
) -> Dict[str, str]:
    p = {
        "get": f"CTY_CODE,CTY_NAME,I_COMMODITY,{MEASURE}",
        "time": period,
        "COMM_LVL": COMM_LVL,
    }
    # NEW – if the list is exactly ["-"] we skip CTY_CODE so the API returns totals
    if ctry_list and not (len(ctry_list) == 1 and ctry_list[0] == "-"):
        p["CTY_CODE"] = "+".join(ctry_list)

    if hs_list:
        p["I_COMMODITY"] = "+".join(hs_list)
    if api_key:
        p["key"] = api_key
    return p


def query_batch(params: Dict[str, str]) -> List[List[str]]:
    r = requests.get(BASE_URL, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"API error {r.status_code}\nURL: {r.url}\n{r.text[:300]}")
    return r.json()


# ---------------------------------------------------------------------------
# Main download logic
# ---------------------------------------------------------------------------

def download(args):
    start = parse_period(args.start)
    end = parse_period(args.end)
    if start > end:
        sys.exit("Error: --start must be <= --end.")
    periods = month_range(start, end)

    mode_ctry = bool(args.countries)
    mode_hs = bool(args.hs10)
    if mode_ctry == mode_hs:
        sys.exit("Specify exactly one of --countries or --hs10.")

    ctry_map = fetch_country_table(args.apikey or "")
    if mode_ctry:
        countries = normalize_countries(args.countries, ctry_map)
        hs_codes = []
    else:
        countries = []
        hs_codes = normalize_hs10(args.hs10)

    header_written = False
    with open(args.outfile, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        for per in periods:
            batches = (
                [countries[i : i + BATCH_CTRY] for i in range(0, len(countries), BATCH_CTRY)]
                if countries
                else [None]
            )
            for c_batch in batches:
                params = build_params(c_batch or [], hs_codes, per, args.apikey or "")
                rows = query_batch(params)
                if not header_written:
                    writer.writerow(rows[0] + ["period"])
                    header_written = True
                for row in rows[1:]:
                    writer.writerow(row + [per])
                time.sleep(REQUEST_SLEEP)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Fetch U.S. import values (HS-10) from Census ITTS API.")
    ap.add_argument("--countries", help="Comma-separated partner names or codes")
    ap.add_argument("--hs10", help="Comma-separated HS-10 codes")
    ap.add_argument("--start", required=True, help="Start period (YYYY or YYYY-MM)")
    ap.add_argument("--end", required=True, help="End period (YYYY or YYYY-MM)")
    ap.add_argument("--outfile", default="imports.csv", help="Destination CSV path")
    ap.add_argument("--apikey", help="Census API key (optional)")
    args = ap.parse_args()
    download(args)

"""
args = ap.parse_args(["--countries", "Mexico,China",
                      "--start", "2020-01",
                      "--end",   "2024-12"])
"""

if __name__ == "__main__":
    main()
