"""Cumulative local-projection IV pass-through for the 2018 and 2025 episodes.

For each horizon ``h`` the dependent variable is the cumulative change in
duty-inclusive unit value from ``t-1`` through ``t+h``.  The endogenous
regressor is the corresponding cumulative change in the realized Census
applied tariff, instrumented with the independently constructed statutory
change.  The pre-duty result is recovered from the exact accounting identity

    log(p_duty) = log(p) + log(1 + applied_tariff)

on an identical estimation sample.  This is algebraically identical to a
separate pre-duty IV fit and avoids estimating the same projection twice.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import inspect
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats import t as student_t

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json, write_parquet

VERSION = "cumulative_lp_iv_v1"
EPISODES: dict[str, dict[str, Any]] = {
    "trade_war_2018": {
        "panel_start": "2017-01",
        "panel_end": "2021-12",
        "base_start": "2018-01",
        "base_end": "2019-12",
        "max_horizon": 24,
        "statutory_field": "paper_dynamic_total_tariff",
        "statutory_clock": "independent_paper_compatible",
        "post_ledger_rule": (
            "hold each partner-HS10 last nonmissing December-2019 "
            "statutory rate fixed through 2021"
        ),
    },
    "tariffs_2025": {
        "panel_start": "2024-01",
        "panel_end": "2025-12",
        "base_start": "2025-01",
        "base_end": "2025-12",
        "max_horizon": 11,
        "statutory_field": "statutory_paper_coverage_rate",
        "statutory_clock": "independent_day_weighted_legal_schedule",
        "post_ledger_rule": "not_applicable",
    },
}


@dataclass(frozen=True)
class CumulativeLPSpec:
    episode: str
    horizon: int

    @property
    def fit_id(self) -> str:
        return f"cumulative_lp_iv|{self.episode}|h{self.horizon:02d}"


def grid(episodes: Iterable[str] | None = None) -> list[CumulativeLPSpec]:
    selected = tuple(episodes or EPISODES)
    return [
        CumulativeLPSpec(episode, horizon)
        for episode in selected
        for horizon in range(EPISODES[episode]["max_horizon"] + 1)
    ]


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _sql(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()


def root(config: PipelineConfig) -> Path:
    return (
        config.processed_trade_dir
        / "regressions"
        / "fk2025"
        / "cumulative_lp_iv"
    )


def panel_path(config: PipelineConfig, episode: str) -> Path:
    return root(config) / "panels" / f"{episode}.parquet"


def panel_manifest_path(config: PipelineConfig, episode: str) -> Path:
    return panel_path(config, episode).with_suffix(".json")


def base_endpoint_path(config: PipelineConfig, episode: str) -> Path:
    return root(config) / "panels" / f"{episode}_base_lag.parquet"


def base_endpoint_manifest_path(
    config: PipelineConfig,
    episode: str,
) -> Path:
    return base_endpoint_path(config, episode).with_suffix(".json")


def fit_paths(
    config: PipelineConfig,
    spec: CumulativeLPSpec,
) -> tuple[Path, Path, Path]:
    directory = (
        root(config)
        / "checkpoints"
        / spec.episode
        / f"horizon_{spec.horizon:02d}"
    )
    return (
        directory / "coefficients.parquet",
        directory / "sample_audit.parquet",
        directory / "manifest.json",
    )


def estimator_fingerprint() -> str:
    sources = [
        inspect.getsource(load_horizon_sample),
        inspect.getsource(build_base_endpoint_panel),
        inspect.getsource(fit_cumulative_lp_iv),
        inspect.getsource(_sample_hash),
        inspect.getsource(_drop_singletons),
        inspect.getsource(_residualize_two_way),
        inspect.getsource(_clustered_scalar_iv),
    ]
    normalized = "\n".join(
        source.replace("\r\n", "\n").rstrip() for source in sources
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


def specification_fingerprint(spec: CumulativeLPSpec) -> str:
    definition = EPISODES[spec.episode]
    payload = {
        **asdict(spec),
        "dependent": (
            "100*(log(duty_inclusive_unit_value_t+h)"
            "-log(duty_inclusive_unit_value_t-1))"
        ),
        "endogenous": (
            "100*(log1p(applied_tariff_t+h)"
            "-log1p(applied_tariff_t-1))"
        ),
        "instrument": (
            "100*(log1p(statutory_rate_t+h)"
            "-log1p(statutory_rate_t-1))"
        ),
        "fixed_effects": ["hs10_x_base_month", "partner_code"],
        "clusters": ["partner_code", "hs8"],
        "common_sample": ["pre_duty_price", "duty_inclusive_price"],
        "statutory_clock": definition["statutory_clock"],
        "post_ledger_rule": definition["post_ledger_rule"],
    }
    return _hash_payload(payload)


def _validate_parquet(path: Path) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata
    compression = {
        metadata.row_group(group).column(column).compression
        for group in range(metadata.num_row_groups)
        for column in range(metadata.row_group(group).num_columns)
    }
    result = {
        "rows": int(metadata.num_rows),
        "columns": list(parquet.schema_arrow.names),
        "compression": sorted(compression),
    }
    del metadata
    del parquet
    if result["rows"] <= 0:
        raise ValueError(f"empty Parquet artifact: {path}")
    if result["compression"] != ["ZSTD"]:
        raise ValueError(
            f"expected ZSTD Parquet at {path}, got {result['compression']}"
        )
    return result


def _atomic_copy(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    destination: Path,
) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{uuid.uuid4().hex}.tmp"
    )
    try:
        connection.execute(
            f"COPY ({query}) TO '{_sql(temporary)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        validation = _validate_parquet(temporary)
        temporary.replace(destination)
        return validation
    finally:
        temporary.unlink(missing_ok=True)


def _historical_panel_query(config: PipelineConfig) -> tuple[str, list[Path]]:
    base_glob = (
        config.processed_trade_dir
        / "fk2025"
        / "variety_month"
        / "year=*"
        / "month=*"
        / "part.parquet"
    )
    extension_glob = (
        config.processed_trade_dir
        / "fk2025_event_horizon_extension"
        / "variety_month"
        / "year=*"
        / "month=*"
        / "part.parquet"
    )
    tariffs = (
        config.processed_tariff_dir
        / "final"
        / "historical_tariffs.parquet"
    )
    query = f"""
    WITH trade AS (
      SELECT lpad(cast(partner_code AS VARCHAR),4,'0') AS partner_code,
             hs10, substring(hs10,1,8) AS hs8,
             cast(year AS INTEGER) AS year,
             cast(month AS INTEGER) AS month,
             period, con_val_mo, con_qy1_mo, cal_dut_mo
      FROM read_parquet('{_sql(base_glob)}', hive_partitioning=false)
      WHERE period BETWEEN '2017-01' AND '2019-12'
      UNION ALL
      SELECT lpad(cast(partner_code AS VARCHAR),4,'0') AS partner_code,
             hs10, substring(hs10,1,8) AS hs8,
             cast(year AS INTEGER) AS year,
             cast(month AS INTEGER) AS month,
             period, con_val_mo, con_qy1_mo, cal_dut_mo
      FROM read_parquet(
        '{_sql(extension_glob)}',
        hive_partitioning=false
      )
      WHERE period BETWEEN '2020-01' AND '2021-12'
    ), monthly_policy AS (
      SELECT cast(cty_code AS INTEGER) AS cty_code, hs10,
             cast(year AS INTEGER) AS year,
             cast(month AS INTEGER) AS month,
             paper_dynamic_total_tariff AS statutory_rate
      FROM read_parquet('{_sql(tariffs)}')
      WHERE year BETWEEN 2017 AND 2019
    ), terminal_policy AS (
      SELECT cast(cty_code AS INTEGER) AS cty_code, hs10,
             arg_max(
               paper_dynamic_total_tariff,
               year*12+month
             ) FILTER (
               WHERE paper_dynamic_total_tariff IS NOT NULL
             ) AS terminal_rate
      FROM read_parquet('{_sql(tariffs)}')
      WHERE year <= 2019
      GROUP BY cty_code, hs10
    )
    SELECT 'trade_war_2018' AS episode,
           t.partner_code, t.hs10, t.hs8, t.year, t.month, t.period,
           t.year*12+t.month AS month_index,
           cast(t.con_val_mo AS DOUBLE) AS import_value,
           cast(t.con_qy1_mo AS DOUBLE) AS quantity,
           cast(t.cal_dut_mo AS DOUBLE) AS calculated_duty,
           t.cal_dut_mo/nullif(t.con_val_mo,0) AS applied_tariff,
           t.con_val_mo/nullif(t.con_qy1_mo,0) AS pre_duty_price,
           (t.con_val_mo+t.cal_dut_mo)
             /nullif(t.con_qy1_mo,0) AS duty_inclusive_price,
           CASE WHEN t.year <= 2019
                THEN m.statutory_rate
                ELSE z.terminal_rate END AS statutory_rate,
           CASE WHEN t.year <= 2019
                THEN t.period ELSE '2019-12' END
                AS statutory_source_period,
           (t.year > 2019) AS statutory_carried_forward
    FROM trade t
    LEFT JOIN monthly_policy m
      ON cast(t.partner_code AS INTEGER)=m.cty_code
     AND t.hs10=m.hs10 AND t.year=m.year AND t.month=m.month
    LEFT JOIN terminal_policy z
      ON cast(t.partner_code AS INTEGER)=z.cty_code
     AND t.hs10=z.hs10
    ORDER BY partner_code, hs10, month_index
    """
    return query, [
        tariffs,
        config.processed_trade_dir / "fk2025" / "trade_manifest.json",
        (
            config.processed_trade_dir
            / "fk2025_event_horizon_extension"
            / "trade_manifest.json"
        ),
    ]


def _extension_panel_query(config: PipelineConfig) -> tuple[str, list[Path]]:
    source = (
        config.processed_trade_dir
        / "fk2025"
        / "workhorse_2025.parquet"
    )
    query = f"""
    SELECT 'tariffs_2025' AS episode,
           lpad(cast(partner_code AS VARCHAR),4,'0') AS partner_code,
           hs10, substring(hs10,1,8) AS hs8,
           cast(year AS INTEGER) AS year,
           cast(month AS INTEGER) AS month,
           period, cast(year AS INTEGER)*12+cast(month AS INTEGER)
             AS month_index,
           cast(con_val_mo AS DOUBLE) AS import_value,
           cast(con_qy1_mo AS DOUBLE) AS quantity,
           cast(cal_dut_mo AS DOUBLE) AS calculated_duty,
           applied_tariff,
           before_tariff_unit_value AS pre_duty_price,
           duty_inclusive_unit_value AS duty_inclusive_price,
           statutory_paper_coverage_rate AS statutory_rate,
           period AS statutory_source_period,
           FALSE AS statutory_carried_forward
    FROM read_parquet('{_sql(source)}')
    WHERE period BETWEEN '2024-01' AND '2025-12'
    ORDER BY partner_code, hs10, month_index
    """
    return query, [
        source,
        config.processed_trade_dir / "fk2025" / "trade_manifest.json",
        (
            config.processed_tariff_dir
            / "fk2025"
            / "policy_extension_manifest.json"
        ),
    ]


def build_source_panel(
    config: PipelineConfig,
    episode: str,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    destination = panel_path(config, episode)
    query, direct_sources = (
        _historical_panel_query(config)
        if episode == "trade_war_2018"
        else _extension_panel_query(config)
    )
    for source in direct_sources:
        if not source.exists():
            raise FileNotFoundError(source)
    connection = duckdb.connect()
    try:
        if overwrite or not destination.exists():
            validation = _atomic_copy(connection, query, destination)
        else:
            validation = _validate_parquet(destination)
        duplicate_count = int(
            connection.execute(
                f"""
                SELECT count(*) FROM (
                  SELECT partner_code,hs10,month_index,count(*) AS n
                  FROM read_parquet('{_sql(destination)}')
                  GROUP BY partner_code,hs10,month_index
                  HAVING count(*)>1
                )
                """
            ).fetchone()[0]
        )
        coverage = connection.execute(
            f"""
            SELECT min(period),max(period),count(*),
                   count(statutory_rate),
                   count(*) FILTER(WHERE statutory_carried_forward),
                   count(*) FILTER(
                     WHERE import_value>0 AND quantity>0
                       AND calculated_duty IS NOT NULL
                       AND statutory_rate IS NOT NULL
                   )
            FROM read_parquet('{_sql(destination)}')
            """
        ).fetchone()
    finally:
        connection.close()
    if duplicate_count:
        raise ValueError(
            f"{episode} panel has {duplicate_count} duplicate keys"
        )
    payload = {
        "version": VERSION,
        "episode": episode,
        "status": "passed",
        "path": _relative(config, destination),
        "sha256": sha256_file(destination),
        "input_sources": [
            {
                "path": _relative(config, source),
                "sha256": sha256_file(source),
            }
            for source in direct_sources
        ],
        "artifact": validation,
        "period_start": coverage[0],
        "period_end": coverage[1],
        "rows": int(coverage[2]),
        "statutory_nonmissing_rows": int(coverage[3]),
        "statutory_carried_forward_rows": int(coverage[4]),
        "price_complete_rows": int(coverage[5]),
        "duplicate_keys": duplicate_count,
        "statutory_field": EPISODES[episode]["statutory_field"],
        "statutory_clock": EPISODES[episode]["statutory_clock"],
        "post_ledger_rule": EPISODES[episode]["post_ledger_rule"],
        "unresolved_rates_filled_with_zero": False,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(panel_manifest_path(config, episode), payload)
    return payload


def build_source_panels(
    config: PipelineConfig,
    episodes: Iterable[str] | None = None,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    selected = tuple(episodes or EPISODES)
    records = [
        build_source_panel(
            config,
            episode,
            overwrite=overwrite,
        )
        for episode in selected
    ]
    return {"version": VERSION, "panels": records}


def build_base_endpoint_panel(
    config: PipelineConfig,
    episode: str,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Materialize each eligible base row and its common t-1 endpoint once."""
    source = panel_path(config, episode)
    if not source.exists():
        build_source_panel(config, episode)
    source_hash = sha256_file(source)
    destination = base_endpoint_path(config, episode)
    manifest_path = base_endpoint_manifest_path(config, episode)
    code_hash = hashlib.sha256(
        inspect.getsource(build_base_endpoint_panel)
        .replace("\r\n", "\n")
        .encode()
    ).hexdigest()
    if destination.exists() and manifest_path.exists() and not overwrite:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("source_sha256") == source_hash
            and manifest.get("builder_fingerprint") == code_hash
        ):
            _validate_parquet(destination)
            return manifest
    definition = EPISODES[episode]
    spill_directory = root(config) / "duckdb_spill"
    spill_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit='4GB'")
        connection.execute(
            f"SET temp_directory='{_sql(spill_directory)}'"
        )
        connection.execute("SET preserve_insertion_order=false")
        query = f"""
        WITH base AS (
          SELECT partner_code,hs10,hs8,month_index AS base_index
          FROM read_parquet('{_sql(source)}')
          WHERE period BETWEEN '{definition["base_start"]}'
            AND '{definition["base_end"]}'
        )
        SELECT b.partner_code,b.hs10,b.hs8,b.base_index,
               l.pre_duty_price AS lag_pre_duty_price,
               l.duty_inclusive_price
                 AS lag_duty_inclusive_price,
               l.applied_tariff AS lag_applied_tariff,
               l.statutory_rate AS lag_statutory_rate
        FROM base b
        JOIN read_parquet('{_sql(source)}') l
          ON b.partner_code=l.partner_code
         AND b.hs10=l.hs10
         AND l.month_index=b.base_index-1
        WHERE l.pre_duty_price>0
          AND l.duty_inclusive_price>0
          AND l.applied_tariff>-1
          AND l.statutory_rate>-1
        """
        validation = _atomic_copy(connection, query, destination)
    finally:
        connection.close()
    manifest = {
        "version": VERSION,
        "status": "passed",
        "episode": episode,
        "path": _relative(config, destination),
        "sha256": sha256_file(destination),
        "source_path": _relative(config, source),
        "source_sha256": source_hash,
        "builder_fingerprint": code_hash,
        "artifact": validation,
        "base_period_start": definition["base_start"],
        "base_period_end": definition["base_end"],
        "lag_definition": "t_minus_1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(manifest_path, manifest)
    return manifest


def load_horizon_sample(
    config: PipelineConfig,
    spec: CumulativeLPSpec,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = panel_path(config, spec.episode)
    if not source.exists():
        raise FileNotFoundError(source)
    definition = EPISODES[spec.episode]
    base_manifest = build_base_endpoint_panel(
        config,
        spec.episode,
    )
    base_source = base_endpoint_path(config, spec.episode)
    working = (
        root(config)
        / "working"
        / (
            f"{spec.episode}_h{spec.horizon:02d}_"
            f"{uuid.uuid4().hex}.parquet"
        )
    )
    connection = duckdb.connect()
    try:
        spill_directory = root(config) / "duckdb_spill"
        spill_directory.mkdir(parents=True, exist_ok=True)
        connection.execute("SET memory_limit='4GB'")
        connection.execute(
            f"SET temp_directory='{_sql(spill_directory)}'"
        )
        connection.execute("SET preserve_insertion_order=false")
        candidate_rows = int(
            connection.execute(
                f"""
                SELECT count(*) FROM read_parquet('{_sql(source)}')
                WHERE period BETWEEN ? AND ?
                """,
                [definition["base_start"], definition["base_end"]],
            ).fetchone()[0]
        )
        sample_query = f"""
            WITH endpoints AS (
              SELECT b.*,
                     100*(
                       ln(f.duty_inclusive_price)
                       -ln(b.lag_duty_inclusive_price)
                     ) AS delta_log_pduty,
                     100*(
                       ln(f.pre_duty_price)
                       -ln(b.lag_pre_duty_price)
                     ) AS delta_log_p,
                     100*(
                       ln(1+f.applied_tariff)
                       -ln(1+b.lag_applied_tariff)
                     ) AS delta_log_applied,
                     100*(
                       ln(1+f.statutory_rate)
                       -ln(1+b.lag_statutory_rate)
                     ) AS delta_log_statutory,
                     f.statutory_carried_forward
                       AS future_statutory_carried_forward
              FROM read_parquet('{_sql(base_source)}') b
              JOIN read_parquet('{_sql(source)}') f
                ON b.partner_code=f.partner_code
               AND b.hs10=f.hs10
                AND f.month_index=b.base_index+{spec.horizon}
              WHERE f.pre_duty_price>0
                AND f.duty_inclusive_price>0
                AND f.applied_tariff>-1
                AND f.statutory_rate>-1
            )
            SELECT cast(partner_code AS INTEGER) AS partner_code,
                   cast(hs10 AS BIGINT) AS hs10,
                   cast(hs8 AS INTEGER) AS hs8,
                   base_index,
                   delta_log_pduty,
                   delta_log_p,
                   delta_log_applied,
                   delta_log_statutory,
                   future_statutory_carried_forward,
                   cast(hs10 AS BIGINT)*100000+base_index
                     AS product_time
            FROM endpoints
            WHERE isfinite(delta_log_pduty)
              AND isfinite(delta_log_p)
              AND isfinite(delta_log_applied)
              AND isfinite(delta_log_statutory)
            """
        _atomic_copy(connection, sample_query, working)
    finally:
        connection.close()
    try:
        sample = pd.read_parquet(working)
    finally:
        working.unlink(missing_ok=True)
    if sample.empty:
        raise ValueError(f"{spec.fit_id} has no complete endpoint sample")
    identity = (
        sample["delta_log_pduty"]
        - sample["delta_log_p"]
        - sample["delta_log_applied"]
    )
    identity_error = float(identity.abs().max())
    if identity_error > 1e-8:
        raise ValueError(
            f"{spec.fit_id} violates the price identity: {identity_error}"
        )
    audit = {
        "fit_id": spec.fit_id,
        "episode": spec.episode,
        "horizon": spec.horizon,
        "candidate_base_rows": candidate_rows,
        "common_sample_rows": int(len(sample)),
        "rows_lost": int(candidate_rows - len(sample)),
        "products": int(sample["hs10"].nunique()),
        "origins": int(sample["partner_code"].nunique()),
        "base_periods": int(sample["base_index"].nunique()),
        "nonzero_applied_change_rows": int(
            sample["delta_log_applied"].ne(0).sum()
        ),
        "nonzero_statutory_change_rows": int(
            sample["delta_log_statutory"].ne(0).sum()
        ),
        "future_statutory_carried_forward_rows": int(
            sample["future_statutory_carried_forward"].sum()
        ),
        "base_endpoint_source_sha256": base_manifest["sha256"],
        "price_identity_max_abs_error": identity_error,
    }
    return sample, audit


def _sample_hash(
    sample: pd.DataFrame,
    columns: tuple[str, ...] = (
        "partner_code",
        "hs10",
        "base_index",
    ),
) -> str:
    values = pd.util.hash_pandas_object(
        sample.loc[:, list(columns)],
        index=False,
    ).to_numpy(dtype="uint64", copy=False)
    values.sort()
    return hashlib.sha256(values.tobytes()).hexdigest()


def _factorize(values: np.ndarray) -> np.ndarray:
    return pd.factorize(values, sort=False)[0].astype(
        np.int32,
        copy=False,
    )


def _drop_singletons(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """Recursively remove singleton observations in either fixed effect."""
    work = frame
    original_rows = len(work)
    for _ in range(100):
        product_codes = _factorize(work["product_time"].to_numpy())
        partner_codes = _factorize(work["partner_code"].to_numpy())
        product_counts = np.bincount(product_codes)
        partner_counts = np.bincount(partner_codes)
        keep = (
            (product_counts[product_codes] > 1)
            & (partner_counts[partner_codes] > 1)
        )
        if bool(keep.all()):
            return work.reset_index(drop=True), original_rows - len(work)
        work = work.loc[keep].copy()
        if work.empty:
            raise ValueError("singleton removal eliminated the sample")
    raise RuntimeError("singleton removal did not converge")


def _residualize_two_way(
    values: np.ndarray,
    first_codes: np.ndarray,
    second_codes: np.ndarray,
    *,
    tolerance: float = 1e-8,
    max_iterations: int = 10_000,
) -> tuple[np.ndarray, int, float]:
    """Absorb two fixed effects with alternating projections."""
    residual = np.asarray(values, dtype=np.float64, order="C").copy()
    scale = max(1.0, float(np.nanmax(np.abs(residual))))
    first_counts = np.bincount(first_codes).astype(np.float64)
    second_counts = np.bincount(second_codes).astype(np.float64)
    last_adjustment = np.inf
    for iteration in range(1, max_iterations + 1):
        last_adjustment = 0.0
        for codes, counts in (
            (first_codes, first_counts),
            (second_codes, second_counts),
        ):
            for column in range(residual.shape[1]):
                sums = np.bincount(
                    codes,
                    weights=residual[:, column],
                    minlength=len(counts),
                )
                means = sums / counts
                last_adjustment = max(
                    last_adjustment,
                    float(np.max(np.abs(means))),
                )
                residual[:, column] -= means[codes]
        if last_adjustment <= tolerance * scale:
            return residual, iteration, last_adjustment
    raise RuntimeError(
        "fixed-effect absorption did not converge: "
        f"last adjustment={last_adjustment}"
    )


def _cluster_component(
    score: np.ndarray,
    clusters: np.ndarray,
) -> tuple[float, int]:
    codes = _factorize(clusters)
    groups = int(codes.max()) + 1
    sums = np.bincount(codes, weights=score, minlength=groups)
    correction = groups / (groups - 1) if groups > 1 else 1.0
    return float(correction * np.dot(sums, sums)), groups


def _clustered_score_meat(
    score: np.ndarray,
    partner: np.ndarray,
    hs8: np.ndarray,
) -> tuple[float, int]:
    partner_meat, partner_groups = _cluster_component(score, partner)
    hs8_meat, hs8_groups = _cluster_component(score, hs8)
    intersection = (
        partner.astype(np.int64, copy=False) * 100_000_000
        + hs8.astype(np.int64, copy=False)
    )
    intersection_meat, _ = _cluster_component(score, intersection)
    meat = partner_meat + hs8_meat - intersection_meat
    return max(float(meat), 0.0), min(
        partner_groups,
        hs8_groups,
    )


def _clustered_scalar_iv(
    y: np.ndarray,
    x: np.ndarray,
    z: np.ndarray,
    partner: np.ndarray,
    hs8: np.ndarray,
) -> dict[str, float]:
    zx = float(np.dot(z, x))
    zy = float(np.dot(z, y))
    zz = float(np.dot(z, z))
    if abs(zx) <= 1e-12 or zz <= 1e-12:
        raise ValueError("cumulative LP-IV is unidentified")
    beta = zy / zx
    residual = y - beta * x
    meat, cluster_df_base = _clustered_score_meat(
        z * residual,
        partner,
        hs8,
    )
    standard_error = float(np.sqrt(meat / (zx * zx)))
    first_stage = zx / zz
    first_residual = x - first_stage * z
    first_meat, _ = _clustered_score_meat(
        z * first_residual,
        partner,
        hs8,
    )
    first_se = float(np.sqrt(first_meat / (zz * zz)))
    critical = float(
        student_t.ppf(0.975, max(cluster_df_base - 1, 1))
    )
    return {
        "estimate": beta,
        "std_error": standard_error,
        "conf_low": beta - critical * standard_error,
        "conf_high": beta + critical * standard_error,
        "first_stage_estimate": first_stage,
        "first_stage_std_error": first_se,
        "first_stage_conf_low": first_stage - critical * first_se,
        "first_stage_conf_high": first_stage + critical * first_se,
        "first_stage_f": (
            (first_stage / first_se) ** 2
            if first_se > 0
            else np.inf
        ),
        "cluster_degrees_of_freedom": cluster_df_base - 1,
    }


def fit_cumulative_lp_iv(
    sample: pd.DataFrame,
    spec: CumulativeLPSpec,
) -> pd.DataFrame:
    if sample["delta_log_applied"].nunique() < 2:
        raise ValueError(f"{spec.fit_id} has no applied-tariff variation")
    if sample["delta_log_statutory"].nunique() < 2:
        raise ValueError(f"{spec.fit_id} has no statutory variation")
    required = [
        "delta_log_pduty",
        "delta_log_applied",
        "delta_log_statutory",
        "product_time",
        "partner_code",
        "hs8",
    ]
    work, singleton_losses = _drop_singletons(sample.loc[:, required])
    product_codes = _factorize(work["product_time"].to_numpy())
    partner_fe_codes = _factorize(work["partner_code"].to_numpy())
    residual, iterations, last_adjustment = _residualize_two_way(
        work[
            [
                "delta_log_pduty",
                "delta_log_applied",
                "delta_log_statutory",
            ]
        ].to_numpy(dtype=np.float64, copy=False),
        product_codes,
        partner_fe_codes,
    )
    estimates = _clustered_scalar_iv(
        residual[:, 0],
        residual[:, 1],
        residual[:, 2],
        work["partner_code"].to_numpy(dtype=np.int64, copy=False),
        work["hs8"].to_numpy(dtype=np.int64, copy=False),
    )
    duty_estimate = estimates["estimate"]
    duty_se = estimates["std_error"]
    duty_low = estimates["conf_low"]
    duty_high = estimates["conf_high"]
    first_estimate = estimates["first_stage_estimate"]
    first_se = estimates["first_stage_std_error"]
    first_low = estimates["first_stage_conf_low"]
    first_high = estimates["first_stage_conf_high"]
    first_f = estimates["first_stage_f"]
    observations = len(work)
    rows = [
        {
            "fit_id": spec.fit_id,
            "episode": spec.episode,
            "horizon": spec.horizon,
            "outcome": "first_stage",
            "estimate": first_estimate,
            "std_error": first_se,
            "conf_low": first_low,
            "conf_high": first_high,
            "nobs": observations,
            "first_stage_f": first_f,
            "estimation_method": "direct_first_stage",
            "singleton_losses": singleton_losses,
            "absorption_iterations": iterations,
            "absorption_last_adjustment": last_adjustment,
        },
        {
            "fit_id": spec.fit_id,
            "episode": spec.episode,
            "horizon": spec.horizon,
            "outcome": "p",
            "estimate": duty_estimate - 1.0,
            "std_error": duty_se,
            "conf_low": duty_low - 1.0,
            "conf_high": duty_high - 1.0,
            "nobs": observations,
            "first_stage_f": first_f,
            "estimation_method": (
                "accounting_identity_from_direct_pduty_2sls"
            ),
            "singleton_losses": singleton_losses,
            "absorption_iterations": iterations,
            "absorption_last_adjustment": last_adjustment,
        },
        {
            "fit_id": spec.fit_id,
            "episode": spec.episode,
            "horizon": spec.horizon,
            "outcome": "pduty",
            "estimate": duty_estimate,
            "std_error": duty_se,
            "conf_low": duty_low,
            "conf_high": duty_high,
            "nobs": observations,
            "first_stage_f": first_f,
            "estimation_method": "direct_scalar_2sls_after_hdfe_absorption",
            "singleton_losses": singleton_losses,
            "absorption_iterations": iterations,
            "absorption_last_adjustment": last_adjustment,
        },
    ]
    return pd.DataFrame(rows)


def _valid_checkpoint(
    config: PipelineConfig,
    spec: CumulativeLPSpec,
    source_hash: str,
) -> tuple[bool, str]:
    coefficient, audit, manifest_path = fit_paths(config, spec)
    if not all(path.exists() for path in (coefficient, audit, manifest_path)):
        return False, "missing_artifact"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False, "invalid_manifest"
    expected = {
        "fit_id": spec.fit_id,
        "source_sha256": source_hash,
        "estimator_fingerprint": estimator_fingerprint(),
        "specification_fingerprint": specification_fingerprint(spec),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False, f"mismatch:{key}"
    if manifest.get("coefficient_sha256") != sha256_file(coefficient):
        return False, "mismatch:coefficient_sha256"
    if manifest.get("sample_audit_sha256") != sha256_file(audit):
        return False, "mismatch:sample_audit_sha256"
    try:
        frame = pd.read_parquet(coefficient)
    except Exception:
        return False, "unreadable_coefficient"
    if (
        len(frame) != 3
        or set(frame["outcome"]) != {"first_stage", "p", "pduty"}
        or frame["horizon"].nunique() != 1
        or int(frame["horizon"].iloc[0]) != spec.horizon
    ):
        return False, "invalid_coefficient_schema"
    return True, "valid"


def _write_progress(
    config: PipelineConfig,
    specs: list[CumulativeLPSpec],
) -> dict[str, Any]:
    completed: list[str] = []
    stale: list[dict[str, str]] = []
    source_hashes: dict[str, str] = {}
    for spec in specs:
        source = panel_path(config, spec.episode)
        if not source.exists():
            stale.append(
                {"fit_id": spec.fit_id, "reason": "missing_source_panel"}
            )
            continue
        if spec.episode not in source_hashes:
            source_hashes[spec.episode] = sha256_file(source)
        valid, reason = _valid_checkpoint(
            config,
            spec,
            source_hashes[spec.episode],
        )
        if valid:
            completed.append(spec.fit_id)
        else:
            stale.append({"fit_id": spec.fit_id, "reason": reason})
    expected = [spec.fit_id for spec in specs]
    payload = {
        "version": VERSION,
        "expected_fit_ids": expected,
        "completed_fit_ids": completed,
        "remaining_fit_ids": sorted(set(expected) - set(completed)),
        "stale_or_missing": stale,
        "completed_fit_count": len(completed),
        "expected_fit_count": len(expected),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(root(config) / "progress.json", payload)
    return payload


def run_fits(
    config: PipelineConfig,
    specs: Iterable[CumulativeLPSpec] | None = None,
    *,
    resume: bool = True,
) -> dict[str, Any]:
    selected = list(specs or grid())
    current_path = root(config) / "current_fit.json"
    records: list[dict[str, Any]] = []
    source_hashes: dict[str, str] = {}
    for spec in selected:
        source = panel_path(config, spec.episode)
        if not source.exists():
            build_source_panel(config, spec.episode)
        if spec.episode not in source_hashes:
            source_hashes[spec.episode] = sha256_file(source)
        source_hash = source_hashes[spec.episode]
        valid, reason = _valid_checkpoint(
            config,
            spec,
            source_hash,
        )
        if resume and valid:
            records.append(
                {"fit_id": spec.fit_id, "status": "resumed"}
            )
            continue
        sample, audit = load_horizon_sample(config, spec)
        sample_hash = _sample_hash(sample)
        treatment_hash = _sample_hash(
            sample,
            (
                "partner_code",
                "hs10",
                "base_index",
                "delta_log_applied",
                "delta_log_statutory",
            ),
        )
        current = {
            "version": VERSION,
            "fit_id": spec.fit_id,
            "episode": spec.episode,
            "horizon": spec.horizon,
            "row_count": len(sample),
            "estimated_memory_bytes": int(
                sample.memory_usage(index=True, deep=True).sum()
            ),
            "formula": (
                "delta_log_pduty ~ 1 | product_time + partner_code | "
                "delta_log_applied ~ delta_log_statutory"
            ),
            "fixed_effects": ["product_time", "partner_code"],
            "clusters": ["partner_code", "hs8"],
            "start_time_utc": datetime.now(timezone.utc).isoformat(),
        }
        write_metadata_json(current_path, current)
        coefficient, audit_path, manifest_path = fit_paths(config, spec)
        started = datetime.now(timezone.utc)
        try:
            estimates = fit_cumulative_lp_iv(sample, spec)
            audit.update(
                {
                    "sample_hash": sample_hash,
                    "treatment_hash": treatment_hash,
                    "effective_observations": int(
                        estimates["nobs"].min()
                    ),
                }
            )
            write_parquet(estimates, coefficient, overwrite=True)
            write_parquet(
                pd.DataFrame([audit]),
                audit_path,
                overwrite=True,
            )
            manifest = {
                "version": VERSION,
                "status": "valid",
                "fit_id": spec.fit_id,
                "episode": spec.episode,
                "horizon": spec.horizon,
                "source_path": _relative(config, source),
                "source_sha256": source_hash,
                "estimator_fingerprint": estimator_fingerprint(),
                "specification_fingerprint": (
                    specification_fingerprint(spec)
                ),
                "sample_hash": sample_hash,
                "treatment_hash": treatment_hash,
                "observations": int(estimates["nobs"].min()),
                "coefficient_path": _relative(config, coefficient),
                "coefficient_sha256": sha256_file(coefficient),
                "sample_audit_path": _relative(config, audit_path),
                "sample_audit_sha256": sha256_file(audit_path),
                "started_at_utc": started.isoformat(),
                "completed_at_utc": (
                    datetime.now(timezone.utc).isoformat()
                ),
                "resume_reason": reason,
            }
            write_metadata_json(manifest_path, manifest)
            checkpoint_valid, checkpoint_reason = _valid_checkpoint(
                config,
                spec,
                source_hash,
            )
            if not checkpoint_valid:
                raise RuntimeError(
                    f"checkpoint validation failed: {checkpoint_reason}"
                )
            current_path.unlink(missing_ok=True)
            records.append(
                {
                    "fit_id": spec.fit_id,
                    "status": "fitted",
                    "observations": manifest["observations"],
                }
            )
        except Exception as error:
            failure = {
                **current,
                "status": "failed",
                "exception_type": type(error).__name__,
                "exception_message": str(error),
                "failed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            failure_path = (
                manifest_path.parent / "failure_manifest.json"
            )
            write_metadata_json(failure_path, failure)
            raise
        finally:
            del sample
            gc.collect()
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] "
            f"completed {spec.fit_id}",
            flush=True,
        )
    progress = _write_progress(config, grid())
    return {"version": VERSION, "fits": records, "progress": progress}


def _plot_episode(
    config: PipelineConfig,
    coefficients: pd.DataFrame,
    episode: str,
) -> list[Path]:
    line = coefficients.loc[
        coefficients["episode"].eq(episode)
        & coefficients["outcome"].isin(["p", "pduty"])
    ].copy()
    labels = {
        "trade_war_2018": "2018 tariff episode",
        "tariffs_2025": "2025 tariff episode",
    }
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), sharex=True)
    for axis, outcome, title, benchmark in (
        (
            axes[0],
            "p",
            "Pre-duty unit value",
            0.0,
        ),
        (
            axes[1],
            "pduty",
            "Duty-inclusive unit value",
            1.0,
        ),
    ):
        curve = line.loc[line["outcome"].eq(outcome)].sort_values(
            "horizon"
        )
        x = curve["horizon"].to_numpy(float)
        axis.fill_between(
            x,
            curve["conf_low"].to_numpy(float),
            curve["conf_high"].to_numpy(float),
            color="#2563eb",
            alpha=0.18,
            linewidth=0,
        )
        axis.plot(
            x,
            curve["estimate"].to_numpy(float),
            color="#2563eb",
            marker="o",
            markersize=3,
            linewidth=1.6,
        )
        axis.axhline(
            benchmark,
            color="#b91c1c",
            linestyle="--",
            linewidth=1,
            label=(
                "No foreign-price response"
                if outcome == "p"
                else "Complete pass-through"
            ),
        )
        axis.axhline(0, color="0.35", linewidth=0.7)
        axis.set_title(title)
        axis.set_xlabel("Horizon (months)")
        axis.set_ylabel("Cumulative pass-through coefficient")
        axis.grid(alpha=0.2)
        axis.legend(frameon=False, fontsize=8)
    minimum_f = float(
        line["first_stage_f"].dropna().min()
    )
    fig.suptitle(
        f"{labels[episode]}: cumulative local-projection IV "
        f"(minimum first-stage F = {minimum_f:.1f})",
        y=0.98,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    figure_root = config.repo_root / "figs" / "extension_2025"
    figure_root.mkdir(parents=True, exist_ok=True)
    stem = figure_root / f"cumulative_pass_through_{episode}"
    outputs: list[Path] = []
    for suffix in (".pdf", ".png"):
        path = stem.with_suffix(suffix)
        fig.savefig(path, dpi=220 if suffix == ".png" else None)
        outputs.append(path)
    plt.close(fig)
    return outputs


def finalize(config: PipelineConfig) -> dict[str, Any]:
    specs = grid()
    coefficient_frames: list[pd.DataFrame] = []
    audit_frames: list[pd.DataFrame] = []
    records: list[dict[str, Any]] = []
    invalid: list[dict[str, str]] = []
    source_hashes: dict[str, str] = {}
    for spec in specs:
        source = panel_path(config, spec.episode)
        if not source.exists():
            invalid.append(
                {"fit_id": spec.fit_id, "reason": "missing_source_panel"}
            )
            continue
        if spec.episode not in source_hashes:
            source_hashes[spec.episode] = sha256_file(source)
        valid, reason = _valid_checkpoint(
            config,
            spec,
            source_hashes[spec.episode],
        )
        if not valid:
            invalid.append({"fit_id": spec.fit_id, "reason": reason})
            continue
        coefficient, audit, manifest_path = fit_paths(config, spec)
        coefficient_frames.append(pd.read_parquet(coefficient))
        audit_frames.append(pd.read_parquet(audit))
        records.append(
            {
                "fit_id": spec.fit_id,
                "coefficient_path": _relative(config, coefficient),
                "sample_audit_path": _relative(config, audit),
                "manifest_path": _relative(config, manifest_path),
            }
        )
    if invalid:
        raise RuntimeError(
            f"cannot finalize cumulative LP-IV: {len(invalid)} "
            f"invalid fits; first={invalid[0]}"
        )
    coefficients = pd.concat(coefficient_frames, ignore_index=True)
    audits = pd.concat(audit_frames, ignore_index=True)
    audits["singleton_losses"] = (
        audits["common_sample_rows"]
        - audits["effective_observations"]
    )
    expected_ids = {spec.fit_id for spec in specs}
    if set(coefficients["fit_id"]) != expected_ids:
        raise RuntimeError("final coefficient fit-ID set is incomplete")
    identity = (
        coefficients.pivot(
            index=["episode", "horizon"],
            columns="outcome",
            values="estimate",
        )
    )
    identity_error = float(
        (identity["pduty"] - identity["p"] - 1.0).abs().max()
    )
    if identity_error > 1e-8:
        raise RuntimeError(
            f"coefficient accounting identity failed: {identity_error}"
        )
    coefficient_path = root(config) / "cumulative_lp_coefficients.parquet"
    audit_path = root(config) / "cumulative_lp_sample_audit.parquet"
    write_parquet(coefficients, coefficient_path, overwrite=True)
    write_parquet(audits, audit_path, overwrite=True)
    figures = {
        episode: [
            _relative(config, path)
            for path in _plot_episode(config, coefficients, episode)
        ]
        for episode in EPISODES
    }
    summary = (
        coefficients.loc[
            coefficients["outcome"].isin(["p", "pduty"])
        ]
        .groupby(["episode", "outcome"], as_index=False)
        .agg(
            horizons=("horizon", "nunique"),
            last_horizon=("horizon", "max"),
            minimum_first_stage_f=("first_stage_f", "min"),
            final_estimate=("estimate", "last"),
            final_std_error=("std_error", "last"),
        )
    )
    summary_path = root(config) / "cumulative_lp_summary.csv"
    summary.to_csv(summary_path, index=False)
    payload = {
        "version": VERSION,
        "status": "complete",
        "expected_fit_count": len(specs),
        "completed_fit_count": len(records),
        "expected_fit_ids": sorted(expected_ids),
        "completed_fit_ids": sorted(set(coefficients["fit_id"])),
        "coefficient_path": _relative(config, coefficient_path),
        "sample_audit_path": _relative(config, audit_path),
        "compact_summary_path": _relative(config, summary_path),
        "figures": figures,
        "price_identity_max_abs_error": identity_error,
        "weak_first_stage_horizons_f_below_10": (
            coefficients.loc[
                coefficients["outcome"].eq("first_stage")
                & coefficients["first_stage_f"].lt(10),
                ["episode", "horizon", "first_stage_f"],
            ].to_dict(orient="records")
        ),
        "fit_records": records,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(root(config) / "manifest.json", payload)
    (root(config) / "current_fit.json").unlink(missing_ok=True)
    _write_progress(config, specs)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-panels", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument(
        "--episode",
        choices=(*EPISODES, "all"),
        default="all",
    )
    parser.add_argument("--horizon", type=int)
    parser.add_argument("--only-fit")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--overwrite-panels", action="store_true")
    args = parser.parse_args(argv)
    config = PipelineConfig.default()
    episodes = (
        tuple(EPISODES)
        if args.episode == "all"
        else (args.episode,)
    )
    selected = [
        spec
        for spec in grid(episodes)
        if (args.horizon is None or spec.horizon == args.horizon)
        and (args.only_fit is None or spec.fit_id == args.only_fit)
    ]
    if args.build_panels:
        print(
            json.dumps(
                build_source_panels(
                    config,
                    episodes,
                    overwrite=args.overwrite_panels,
                ),
                indent=2,
            )
        )
    if args.run:
        print(
            json.dumps(
                run_fits(
                    config,
                    selected,
                    resume=not args.no_resume,
                ),
                indent=2,
            )
        )
    if args.finalize_only:
        print(json.dumps(finalize(config), indent=2))
    if not (args.build_panels or args.run or args.finalize_only):
        print(json.dumps(_write_progress(config, grid()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
