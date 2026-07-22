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
MAX_YEARS_PER_REQUEST = 10


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


def _iter_year_windows(start_year: int, end_year: int) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    current = start_year
    while current <= end_year:
        window_end = min(current + MAX_YEARS_PER_REQUEST - 1, end_year)
        windows.append((current, window_end))
        current = window_end + 1
    return windows


def _existing_observation_periods(config: PipelineConfig) -> list[str]:
    obs_path = config.staging_dir / 'cpi_observations.parquet'
    if not obs_path.exists():
        return []
    frame = read_table(obs_path, columns=['period'])
    if 'period' not in frame.columns:
        return []
    return sorted(frame['period'].dropna().astype(str).unique().tolist())


def build_cpi_inventory(config: PipelineConfig) -> dict[str, Any]:
    seed_series = _series_ids_from_reference(config)
    existing_periods = _existing_observation_periods(config)
    requested_periods = pd.period_range(normalize_period(config.start_period), normalize_period(config.end_period), freq='M')
    requested_strings = [str(period) for period in requested_periods]
    missing_periods = [period for period in requested_strings if period not in set(existing_periods)]
    raw_payloads = sorted((config.raw_dir / 'cpi').glob('bls_cpi_batch_*.json'))
    inventory = {
        'seed_series_count': int(len(seed_series)),
        'raw_payload_count': len(raw_payloads),
        'raw_payloads': [str(path) for path in raw_payloads],
        'requested_start_period': config.start_period,
        'requested_end_period': config.end_period,
        'validation_end_period': config.validation_end_period,
        'existing_period_min': min(existing_periods) if existing_periods else None,
        'existing_period_max': max(existing_periods) if existing_periods else None,
        'missing_periods': missing_periods,
    }
    write_metadata_json(config.verification_dir / 'cpi_raw_inventory.json', inventory)
    return inventory


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
    inventory = build_cpi_inventory(config)
    for batch_index in range(0, len(ids), BATCH_SIZE):
        batch = ids[batch_index:batch_index + BATCH_SIZE]
        for window_index, (window_start, window_end) in enumerate(_iter_year_windows(start_year, end_year), start=1):
            payload = _fetch_bls_batch(batch, window_start, window_end)
            payload_path = raw_dir / f'bls_cpi_batch_{batch_index // BATCH_SIZE + 1:03d}_{window_start}_{window_end}.json'
            payload_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
            payloads.append(str(payload_path))
            for series in payload.get('Results', {}).get('series', []):
                catalog = series.get('catalog') or {}
                series_frames.append(
                    {
                        'series_id': series['seriesID'],
                        'cpi_code': series['seriesID'].replace('CUUR0000', ''),
                        # The public API sometimes omits catalog metadata entirely; keep
                        # those values null here so we can fall back to the seeded
                        # replication-package descriptions below.
                        'cpi_desc': catalog.get('series_title') or catalog.get('catalog_name'),
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
    observations_df = pd.DataFrame(observation_frames).drop_duplicates(['series_id', 'period']).sort_values(['series_id', 'date']).reset_index(drop=True)

    series_path = config.staging_dir / 'cpi_series.parquet'
    obs_path = config.staging_dir / 'cpi_observations.parquet'
    write_parquet(series_df, series_path, overwrite=True)
    write_parquet(observations_df, obs_path, overwrite=True)
    write_data_dictionary(series_df, config.staging_dir / 'cpi_series.dictionary.json', key_columns=['series_id'])
    write_data_dictionary(observations_df, config.staging_dir / 'cpi_observations.dictionary.json', key_columns=['series_id', 'period'])
    write_metadata_json(
        config.staging_dir / 'cpi_download.metadata.json',
        {
            'raw_payloads': payloads,
            'requested_start_period': config.start_period,
            'requested_end_period': config.end_period,
            'validation_end_period': config.validation_end_period,
            'inventory_missing_periods_before_run': inventory['missing_periods'],
            'note': 'Series universe seeded from reference CPI codes because BLS flat-file metadata endpoints returned 403 in this environment.',
        },
    )
    return {'outputs': {'series': str(series_path), 'observations': str(obs_path)}, 'raw_payloads': payloads, 'rows': {'series': int(len(series_df)), 'observations': int(len(observations_df))}}
