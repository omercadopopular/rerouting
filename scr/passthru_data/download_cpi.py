"""CPI download helpers for passthrough rebuilds.

This module downloads CPI observations from the official BLS public API. Because the
flat-file CPI metadata endpoints are returning 403 in the current environment, the current
series universe is seeded from the set of CPI codes used in the Fajgelbaum reference
crosswalk and then refreshed from the official API with catalog metadata.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import logging

import pandas as pd
import requests

from .config import PipelineConfig
from .io_utils import normalize_period, read_table, write_data_dictionary, write_metadata_json, write_parquet

LOGGER = logging.getLogger("passthru_data.cpi")
BLS_API = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
BATCH_SIZE = 25


def _series_ids_from_reference(config: PipelineConfig) -> pd.DataFrame:
    reference = read_table(config.fajgelbaum_analysis_dir / 'cpi_hs6x.dta')
    frame = reference[['cpi_code', 'cpi_desc', 'eli']].drop_duplicates().copy()
    frame['series_id'] = 'CUUR0000' + frame['cpi_code'].astype(str)
    return frame[['series_id', 'cpi_code', 'cpi_desc', 'eli']].sort_values('series_id').reset_index(drop=True)


def _fetch_bls_batch(series_ids: list[str], start_year: int, end_year: int) -> dict[str, Any]:
    payload = {'seriesid': series_ids, 'startyear': str(start_year), 'endyear': str(end_year), 'catalog': True}
    response = requests.post(BLS_API, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


def run_cpi_download(config: PipelineConfig) -> dict[str, Any]:
    """Download CPI data from the BLS public API."""
    raw_dir = config.raw_dir / 'cpi'
    raw_dir.mkdir(parents=True, exist_ok=True)
    seed_series = _series_ids_from_reference(config)
    start_year = int(config.start_period[:4])
    end_year = int(config.end_period[:4])

    payloads = []
    series_frames = []
    observation_frames = []
    ids = seed_series['series_id'].tolist()
    for index in range(0, len(ids), BATCH_SIZE):
        batch = ids[index:index + BATCH_SIZE]
        payload = _fetch_bls_batch(batch, start_year, end_year)
        payload_path = raw_dir / f'bls_cpi_batch_{index // BATCH_SIZE + 1:03d}.json'
        payload_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
        payloads.append(str(payload_path))
        for series in payload.get('Results', {}).get('series', []):
            catalog = series.get('catalog', {})
            series_frames.append(
                {
                    'series_id': series['seriesID'],
                    'cpi_code': series['seriesID'].replace('CUUR0000', ''),
                    'cpi_desc': catalog.get('series_title') or catalog.get('catalog_name') or series['seriesID'],
                    'eli': catalog.get('item_code'),
                }
            )
            for row in series.get('data', []):
                period_code = row.get('period')
                if not period_code or not period_code.startswith('M'):
                    continue
                period = normalize_period(f"{int(row['year']):04d}-{period_code[1:]:0>2}")
                observation_frames.append(
                    {
                        'series_id': series['seriesID'],
                        'period': period,
                        'date': pd.Timestamp(period + '-01'),
                        'year': int(row['year']),
                        'month': int(period_code[1:]),
                        'value': pd.to_numeric(row.get('value'), errors='coerce'),
                    }
                )

    series_df = pd.DataFrame(series_frames).drop_duplicates('series_id').merge(seed_series, on=['series_id', 'cpi_code'], how='outer', suffixes=('', '_seed'))
    series_df['cpi_desc'] = series_df['cpi_desc'].fillna(series_df['cpi_desc_seed'])
    series_df['eli'] = series_df['eli'].fillna(series_df['eli_seed'])
    series_df = series_df[['series_id', 'cpi_code', 'cpi_desc', 'eli']].sort_values('series_id').reset_index(drop=True)
    observations_df = pd.DataFrame(observation_frames).sort_values(['series_id', 'date']).reset_index(drop=True)

    series_path = config.staging_dir / 'cpi_series.parquet'
    obs_path = config.staging_dir / 'cpi_observations.parquet'
    write_parquet(series_df, series_path, overwrite=True)
    write_parquet(observations_df, obs_path, overwrite=True)
    write_data_dictionary(series_df, config.staging_dir / 'cpi_series.dictionary.json', key_columns=['series_id'])
    write_data_dictionary(observations_df, config.staging_dir / 'cpi_observations.dictionary.json', key_columns=['series_id', 'period'])
    write_metadata_json(config.staging_dir / 'cpi_download.metadata.json', {'raw_payloads': payloads, 'note': 'Series universe seeded from reference CPI codes because BLS flat-file metadata endpoints returned 403 in this environment.'})
    return {'outputs': {'series': str(series_path), 'observations': str(obs_path)}, 'raw_payloads': payloads, 'rows': {'series': int(len(series_df)), 'observations': int(len(observations_df))}}
