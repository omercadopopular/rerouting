"""Concordance downloads for passthrough rebuilds."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any
import logging
import zipfile

import pandas as pd
import requests

from .config import PipelineConfig
from .io_utils import normalize_hs_code, read_table, sha256_file, write_data_dictionary, write_metadata_json, write_parquet

LOGGER = logging.getLogger("passthru_data.concordances")
WITS_H5_BEC_URL = 'https://wits.worldbank.org/data/public/concordance/Concordance_H5_to_BE.zip'


def _download(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return destination
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with destination.open('wb') as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    return destination


def _load_hs10_from_trade_raw(config: PipelineConfig) -> pd.DataFrame:
    concord_paths = sorted((config.raw_dir / 'trade').rglob('*_concord.parquet'))
    if not concord_paths:
        raise FileNotFoundError('No raw trade concordance parquet files were found. Run the trade downloader first.')
    frames = [read_table(path) for path in concord_paths]
    out = pd.concat(frames, ignore_index=True).drop_duplicates('hs10')
    out['hs10'] = out['hs10'].map(lambda value: normalize_hs_code(value, 10))
    return out[['hs10', 'hs10_desc']].dropna(subset=['hs10']).sort_values('hs10').reset_index(drop=True)


def _load_h5_bec(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        member = [name for name in archive.namelist() if name.lower().endswith('.csv')][0]
        with archive.open(member) as handle:
            frame = pd.read_csv(handle, dtype=str)
    hs_col = next(column for column in frame.columns if 'hs' in column.lower() and 'product code' in column.lower())
    hs_desc_col = next((column for column in frame.columns if 'hs' in column.lower() and 'description' in column.lower()), None)
    bec_col = next(column for column in frame.columns if 'bec' in column.lower() and 'code' in column.lower())
    bec_desc_col = next((column for column in frame.columns if 'bec' in column.lower() and 'description' in column.lower()), None)
    out = frame.rename(columns={hs_col: 'hs6', bec_col: 'bec'})
    out['hs6'] = out['hs6'].map(lambda value: normalize_hs_code(value, 6))
    out['bec'] = pd.to_numeric(out['bec'], errors='coerce').astype('Int64')
    out['hs6_description'] = out[hs_desc_col] if hs_desc_col else pd.NA
    out['bec_description'] = out[bec_desc_col] if bec_desc_col else pd.NA
    return out[['hs6', 'bec', 'bec_description']].dropna(subset=['hs6', 'bec']).drop_duplicates('hs6').sort_values('hs6').reset_index(drop=True)


def run_concordance_download(config: PipelineConfig) -> dict[str, Any]:
    """Download official concordance files used in Phase 1."""
    raw_concord_dir = config.raw_dir / 'concordances'
    hs10_df = _load_hs10_from_trade_raw(config)
    hs10_source_path = config.reference_dir / 'hs10_concordance_source.parquet'
    write_parquet(hs10_df, hs10_source_path, overwrite=True)
    write_data_dictionary(hs10_df, config.reference_dir / 'hs10_concordance_source.dictionary.json', key_columns=['hs10'])

    bec_zip = _download(WITS_H5_BEC_URL, raw_concord_dir / 'Concordance_H5_to_BE.zip')
    hs6_bec_df = _load_h5_bec(bec_zip)
    hs6_bec_source_path = config.reference_dir / 'hs6_bec_source.parquet'
    write_parquet(hs6_bec_df, hs6_bec_source_path, overwrite=True)
    write_data_dictionary(hs6_bec_df, config.reference_dir / 'hs6_bec_source.dictionary.json', key_columns=['hs6'])

    metadata = {
        'hs10_source': 'census_trade_concord_from_raw_archives',
        'hs6_bec_source_url': WITS_H5_BEC_URL,
        'hs6_bec_zip': {'path': str(bec_zip), 'sha256': sha256_file(bec_zip)},
    }
    write_metadata_json(config.reference_dir / 'concordance_download.metadata.json', metadata)
    return {'outputs': {'hs10': str(hs10_source_path), 'hs6_bec': str(hs6_bec_source_path)}, 'rows': {'hs10': int(len(hs10_df)), 'hs6_bec': int(len(hs6_bec_df))}, 'metadata': metadata}
