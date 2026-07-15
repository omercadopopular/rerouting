"""Trade data ingestion for passthrough rebuilds using official Census bulk archives."""

from __future__ import annotations

from collections import OrderedDict
from io import TextIOWrapper
from pathlib import Path
from typing import Any
import logging
import re
import subprocess
import time
import zipfile

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import PipelineConfig
from .io_utils import add_hierarchy_codes, ensure_dir, iter_months, normalize_country_name, normalize_hs_code, normalize_period, sha256_file, write_data_dictionary, write_metadata_json, write_parquet

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


def _selected_flows(config: PipelineConfig) -> tuple[str, ...]:
    if config.trade_flow is None:
        return ("imports", "exports")
    return (config.trade_flow,)


def _period_key(period: str) -> str:
    period = normalize_period(period)
    return period[2:4] + period[5:7]


def _expected_zip_name(flow: str, period: str) -> str:
    prefix = "IMDB" if flow == "imports" else "EXDB"
    return f"{prefix}{_period_key(period)}.ZIP"


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


def _discover_monthly_urls_safe(flow: str) -> tuple[dict[str, str], str | None]:
    try:
        return _discover_monthly_urls(flow), None
    except Exception as exc:
        return {}, f"{type(exc).__name__}: {exc}"


def _download_zip(url: str, destination: Path) -> Path:
    ensure_dir(destination.parent)
    if destination.exists() and zipfile.is_zipfile(destination):
        return destination
    partial_path = destination.with_suffix(destination.suffix + ".partial")
    if destination.exists() and not zipfile.is_zipfile(destination):
        destination.unlink()
    if partial_path.exists():
        partial_path.unlink()
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            if attempt < 3:
                with requests.get(url, stream=True, timeout=300) as response:
                    response.raise_for_status()
                    with partial_path.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)
            else:
                response = requests.get(url, timeout=300)
                response.raise_for_status()
                partial_path.write_bytes(response.content)
            if not zipfile.is_zipfile(partial_path):
                raise zipfile.BadZipFile(f"Downloaded file is not a valid ZIP: {partial_path}")
            partial_path.replace(destination)
            return destination
        except Exception as exc:
            last_error = exc
            if partial_path.exists():
                partial_path.unlink()
            LOGGER.warning("Download attempt %s failed for %s: %s", attempt, url, exc)
            time.sleep(min(attempt * 2, 10))
    try:
        subprocess.run(
            ["curl.exe", "-L", "--fail", "--output", str(partial_path), url],
            check=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if not zipfile.is_zipfile(partial_path):
            raise zipfile.BadZipFile(f"curl downloaded file is not a valid ZIP: {partial_path}")
        partial_path.replace(destination)
        return destination
    except Exception as exc:
        last_error = exc
        if partial_path.exists():
            partial_path.unlink()
        LOGGER.warning("curl fallback failed for %s: %s", url, exc)
    if last_error is not None:
        raise last_error
    return destination


def _read_fixed_width_from_zip(zip_path: Path, member: str, colspecs: list[tuple[int, int]], names: list[str]) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open(_resolve_member_name(archive, member)) as handle:
            wrapper = TextIOWrapper(handle, encoding="latin1", errors="ignore")
            return pd.read_fwf(wrapper, colspecs=colspecs, names=names, dtype=str)


def _standardize_trade_frame(raw: pd.DataFrame, flow: str, source_type: str, source_file: Path) -> pd.DataFrame:
    column_map = {
        "I_COMMODITY": "hs10",
        "E_COMMODITY": "hs10",
        "COMMODITY": "hs10",
        "HS10": "hs10",
        "CTY_CODE": "partner_code",
        "CTY_NAME": "partner_name",
        "YEAR": "year",
        "MONTH": "month",
        "GEN_VAL_MO": "trade_value",
        "ALL_VAL_MO": "trade_value",
        "EXP_VAL_MO": "trade_value",
        "GEN_QY1_MO": "quantity",
        "ALL_QY1_MO": "quantity",
        "EXP_QY1_MO": "quantity",
    }
    out = raw.rename(columns={column: column_map.get(column, column) for column in raw.columns}).copy()
    out["hs10"] = out["hs10"].map(lambda value: normalize_hs_code(value, 10))
    out["partner_code"] = out["partner_code"].astype(str).str.zfill(4)
    out["partner_name"] = out["partner_name"].map(normalize_country_name)
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["month"] = pd.to_numeric(out["month"], errors="coerce").astype("Int64")
    out["trade_value"] = pd.to_numeric(out["trade_value"], errors="coerce")
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")
    out = out.dropna(subset=["hs10", "partner_code", "year", "month"])
    out["period"] = [f"{int(y):04d}-{int(m):02d}" for y, m in zip(out["year"], out["month"])]
    out["mdate"] = pd.to_datetime(out["period"] + "-01")
    out["flow"] = flow
    out["source_type"] = source_type
    out["source_file"] = str(source_file)
    out = add_hierarchy_codes(out, "hs10")
    return out


def _iter_fixed_width_chunks(zip_path: Path, member: str, colspecs: list[tuple[int, int]], names: list[str], chunksize: int) -> Any:
    archive = zipfile.ZipFile(zip_path)
    handle = archive.open(_resolve_member_name(archive, member))
    wrapper = TextIOWrapper(handle, encoding="latin1", errors="ignore")
    try:
        for chunk in pd.read_fwf(wrapper, colspecs=colspecs, names=names, dtype=str, chunksize=chunksize):
            yield chunk
    finally:
        wrapper.close()
        archive.close()


def _resolve_member_name(archive: zipfile.ZipFile, member: str) -> str:
    target = member.lower()
    for candidate in archive.namelist():
        if candidate.lower() == target:
            return candidate
    raise KeyError(f"There is no item named '{member}' in the archive")


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


def build_trade_inventory(config: PipelineConfig) -> dict[str, Any]:
    periods = iter_months(config.start_period, config.end_period)
    inventory: dict[str, Any] = {
        "requested_periods": periods,
        "requested_start_period": config.start_period,
        "requested_end_period": config.end_period,
        "validation_end_period": config.validation_end_period,
        "trade_flow": config.trade_flow,
    }
    for flow in _selected_flows(config):
        urls, discovery_error = _discover_monthly_urls_safe(flow)
        raw_flow_dir = config.raw_dir / "trade" / flow
        existing_files = {path.name.upper(): path for path in raw_flow_dir.glob("*.ZIP")}
        records = []
        missing_periods = []
        unavailable_periods = []
        for period in periods:
            zip_name = _expected_zip_name(flow, period)
            local_path = raw_flow_dir / zip_name
            source_url = urls.get(period)
            record = {
                "period": period,
                "expected_file": zip_name,
                "exists_locally": zip_name.upper() in existing_files,
                "path": str(local_path),
                "source_url": source_url,
                "available_from_source": source_url is not None,
            }
            if record["exists_locally"]:
                record["size_bytes"] = existing_files[zip_name.upper()].stat().st_size
            if not record["exists_locally"] and record["available_from_source"]:
                missing_periods.append(period)
            if not record["available_from_source"]:
                unavailable_periods.append(period)
            records.append(record)
        inventory[flow] = {
            "discovery_error": discovery_error,
            "source_periods_found": len(urls),
            "local_zip_count": len(existing_files),
            "missing_periods": missing_periods,
            "unavailable_periods": unavailable_periods,
            "latest_source_period": max(urls) if urls else None,
            "records": records,
        }
    write_metadata_json(config.verification_dir / "trade_raw_inventory.json", inventory)
    return inventory


def run_trade_download(config: PipelineConfig) -> dict[str, Any]:
    """Download Census monthly raw trade archives and stage country-HS10 panels."""
    requested_periods = pd.period_range(normalize_period(config.start_period), normalize_period(config.end_period), freq="M")
    results: dict[str, Any] = {}
    inventory = build_trade_inventory(config)
    for flow in _selected_flows(config):
        urls = _discover_monthly_urls(flow)
        if not urls:
            raise FileNotFoundError(f"No official Census bulk files could be discovered for {flow}.")
        periods = requested_periods
        latest_source_period = pd.Period(max(urls), freq="M")
        if config.latest_available:
            periods = [period for period in requested_periods if period <= latest_source_period]
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
            zip_path = raw_flow_dir / zip_name
            downloaded = not zip_path.exists()
            zip_path = _download_zip(url, zip_path)
            files_meta.append(
                {
                    "period": period_str,
                    "path": str(zip_path),
                    "sha256": sha256_file(zip_path),
                    "url": url,
                    "downloaded": downloaded,
                }
            )
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
        flow_inventory = inventory.get(flow, {})
        write_metadata_json(
            config.staging_dir / f"{flow}_trade_staging.metadata.json",
            {
                "rows": int(len(panel)),
                "source_files": files_meta,
                "requested_start_period": config.start_period,
                "requested_end_period": config.end_period,
                "validation_end_period": config.validation_end_period,
                "source_periods_found": flow_inventory.get("source_periods_found"),
                "missing_periods_before_run": flow_inventory.get("missing_periods"),
            },
        )

        concord = pd.concat(concord_frames, ignore_index=True).drop_duplicates("hs10")
        concord_path = config.raw_dir / "trade" / flow / f"{flow}_concord.parquet"
        write_parquet(concord, concord_path, overwrite=True)
        results[flow] = {"rows": int(len(panel)), "staging": str(staging_path), "downloaded_files": files_meta, "concord_path": str(concord_path)}
    return results
