"""Trade data ingestion for passthrough rebuilds using official Census bulk archives."""

from __future__ import annotations

from collections import OrderedDict
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import Any
import logging
import re
import zipfile

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import PipelineConfig
from .io_utils import add_hierarchy_codes, ensure_dir, normalize_hs_code, normalize_period, sha256_file, write_data_dictionary, write_metadata_json, write_parquet

LOGGER = logging.getLogger("passthru_data.trade")

FLOW_SPECS = {
    "imports": {
        "page": "https://www.census.gov/foreign-trade/data/IMDB.html",
        "zip_re": re.compile(r"IMDB(\d{2})(\d{2})\.ZIP$", re.I),
        "detail_member": "IMP_DETL.TXT",
        "country_member": "COUNTRY.TXT",
        "concord_member": "CONCORD.TXT",
        "country_colspecs": [(0, 4), (11, 61)],
        "country_names": ["cty_code", "cty_name"],
        "detail_colspecs": [(0, 10), (10, 14), (22, 26), (26, 28), (148, 163), (178, 193), (88, 103), (103, 118)],
        "detail_names": ["hs10", "cty_code", "year", "month", "quantity", "trade_value", "dut_val_mo", "cal_dut_mo"],
    },
    "exports": {
        "page": "https://www.census.gov/foreign-trade/data/EXDB.html",
        "zip_re": re.compile(r"EXDB(\d{2})(\d{2})\.ZIP$", re.I),
        "detail_member": "EXP_DETL.TXT",
        "country_member": "COUNTRY.TXT",
        "concord_member": "CONCORD.TXT",
        "country_colspecs": [(0, 4), (11, 61)],
        "country_names": ["cty_code", "cty_name"],
        "detail_colspecs": [(1, 11), (11, 15), (17, 21), (21, 23), (38, 53), (68, 83)],
        "detail_names": ["hs10", "cty_code", "year", "month", "quantity", "trade_value"],
    },
}

CONCORD_COLSPECS = [(0, 10), (10, 160)]
CONCORD_NAMES = ["hs10", "hs10_desc"]


def _period_key(period: str) -> str:
    period = normalize_period(period)
    return period[2:4] + period[5:7]


def _discover_monthly_urls(flow: str) -> dict[str, str]:
    spec = FLOW_SPECS[flow]
    response = requests.get(spec["page"], timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    discovered: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        match = spec["zip_re"].search(href)
        if not match:
            continue
        yy, mm = match.groups()
        year = 2000 + int(yy)
        discovered[f"{year:04d}-{mm}"] = href
    return discovered


def _download_zip(url: str, destination: Path) -> Path:
    ensure_dir(destination.parent)
    if destination.exists():
        return destination
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    return destination


def _read_fixed_width_from_zip(zip_path: Path, member: str, colspecs: list[tuple[int, int]], names: list[str]) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open(member) as handle:
            wrapper = TextIOWrapper(handle, encoding="latin1", errors="ignore")
            return pd.read_fwf(wrapper, colspecs=colspecs, names=names, dtype=str)


def _iter_fixed_width_chunks(zip_path: Path, member: str, colspecs: list[tuple[int, int]], names: list[str], chunksize: int) -> Any:
    archive = zipfile.ZipFile(zip_path)
    handle = archive.open(member)
    wrapper = TextIOWrapper(handle, encoding="latin1", errors="ignore")
    try:
        for chunk in pd.read_fwf(wrapper, colspecs=colspecs, names=names, dtype=str, chunksize=chunksize):
            yield chunk
    finally:
        wrapper.close()
        archive.close()


def _load_country_lookup(zip_path: Path, flow: str) -> pd.DataFrame:
    spec = FLOW_SPECS[flow]
    frame = _read_fixed_width_from_zip(zip_path, spec["country_member"], spec["country_colspecs"], spec["country_names"])
    frame["cty_code"] = frame["cty_code"].astype(str).str.zfill(4)
    frame["cty_name"] = frame["cty_name"].astype(str).str.strip().str.upper()
    return frame.drop_duplicates("cty_code")


def _load_concord(zip_path: Path, flow: str) -> pd.DataFrame:
    frame = _read_fixed_width_from_zip(zip_path, FLOW_SPECS[flow]["concord_member"], CONCORD_COLSPECS, CONCORD_NAMES)
    frame["hs10"] = frame["hs10"].map(lambda value: normalize_hs_code(value, 10))
    frame["hs10_desc"] = frame["hs10_desc"].astype(str).str.strip()
    return frame.dropna(subset=["hs10"]).drop_duplicates("hs10")


def _parse_trade_detail(zip_path: Path, flow: str) -> pd.DataFrame:
    spec = FLOW_SPECS[flow]
    chunks = _iter_fixed_width_chunks(zip_path, spec["detail_member"], spec["detail_colspecs"], spec["detail_names"], chunksize=250_000)
    grouped_chunks: list[pd.DataFrame] = []
    for chunk in chunks:
        chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
        chunk["cty_code"] = chunk["cty_code"].astype(str).str.zfill(4)
        chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce").astype("Int64")
        chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce").astype("Int64")
        chunk["quantity"] = pd.to_numeric(chunk["quantity"], errors="coerce")
        chunk["trade_value"] = pd.to_numeric(chunk["trade_value"], errors="coerce")
        grouped = chunk.groupby(["cty_code", "hs10", "year", "month"], as_index=False)[["quantity", "trade_value"]].sum()
        grouped_chunks.append(grouped)
    detail = pd.concat(grouped_chunks, ignore_index=True)
    detail = detail.groupby(["cty_code", "hs10", "year", "month"], as_index=False)[["quantity", "trade_value"]].sum()
    detail["period"] = [f"{int(y):04d}-{int(m):02d}" for y, m in zip(detail["year"], detail["month"])]
    detail["mdate"] = pd.to_datetime(detail["period"] + "-01")
    detail["flow"] = flow
    return add_hierarchy_codes(detail, "hs10")


def run_trade_download(config: PipelineConfig) -> dict[str, Any]:
    """Download Census monthly raw trade archives and stage country-HS10 panels."""
    periods = pd.period_range(normalize_period(config.start_period), normalize_period(config.end_period), freq="M")
    results: dict[str, Any] = {}
    for flow in ("imports", "exports"):
        urls = _discover_monthly_urls(flow)
        selected = OrderedDict()
        for period in periods:
            period_str = str(period)
            if period_str not in urls:
                raise FileNotFoundError(f"No official Census bulk file found for {flow} period {period_str}.")
            selected[period_str] = urls[period_str]

        raw_flow_dir = ensure_dir(config.raw_dir / "trade" / flow)
        parsed_frames: list[pd.DataFrame] = []
        concord_frames: list[pd.DataFrame] = []
        country_lookup: pd.DataFrame | None = None
        files_meta = []
        for period_str, url in selected.items():
            zip_name = Path(url).name
            zip_path = _download_zip(url, raw_flow_dir / zip_name)
            files_meta.append({"period": period_str, "path": str(zip_path), "sha256": sha256_file(zip_path), "url": url})
            parsed_frames.append(_parse_trade_detail(zip_path, flow))
            if country_lookup is None:
                country_lookup = _load_country_lookup(zip_path, flow)
            concord_frames.append(_load_concord(zip_path, flow))
            LOGGER.info("Downloaded and parsed %s %s", flow, period_str)

        if country_lookup is None:
            raise RuntimeError(f"Country lookup could not be built for {flow}.")
        panel = pd.concat(parsed_frames, ignore_index=True)
        panel = panel.groupby(["cty_code", "hs10", "year", "month", "period", "mdate", "flow", "hs8", "hs6", "hs4", "hs2"], as_index=False)[["quantity", "trade_value"]].sum()
        panel = panel.merge(country_lookup, on="cty_code", how="left")
        panel = panel.rename(columns={"cty_name": "partner_name", "cty_code": "partner_code"})
        panel = panel[["flow", "partner_code", "partner_name", "hs10", "hs8", "hs6", "hs4", "hs2", "year", "month", "period", "mdate", "trade_value", "quantity"]].sort_values(["partner_code", "hs10", "year", "month"]).reset_index(drop=True)

        staging_path = config.staging_dir / f"{flow}_trade_staging.parquet"
        write_parquet(panel, staging_path, overwrite=True)
        write_data_dictionary(panel, config.staging_dir / f"{flow}_trade_staging.dictionary.json", key_columns=["partner_code", "hs10", "year", "month"])
        write_metadata_json(config.staging_dir / f"{flow}_trade_staging.metadata.json", {"rows": int(len(panel)), "source_files": files_meta})

        concord = pd.concat(concord_frames, ignore_index=True).drop_duplicates("hs10")
        concord_path = config.raw_dir / "trade" / flow / f"{flow}_concord.parquet"
        write_parquet(concord, concord_path, overwrite=True)
        results[flow] = {"rows": int(len(panel)), "staging": str(staging_path), "downloaded_files": files_meta, "concord_path": str(concord_path)}
    return results
