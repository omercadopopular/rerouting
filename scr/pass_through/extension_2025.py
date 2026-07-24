"""Replicate the import pass-through design in Fajgelbaum--Khandelwal (2026).

This replaces the superseded common-February event and distributed-lag
extension.  The event study uses realized Census duties and clean
not-yet-treated comparisons.  The preferred pass-through specification uses
quarterly applied tariffs instrumented by independently constructed statutory
tariffs.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import inspect
import json
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
import pyfixest as pf

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json, write_parquet

VERSION = "fajgelbaum_khandelwal_2025_pass_through_v1"
CHINA_CODE = "5700"
OUTCOMES = ("tariff", "value", "p", "pduty")
OUTCOME_LABELS = {
    "tariff": "Applied Tariff",
    "value": "Import Value",
    "p": "Before-Tariff Unit Value",
    "pduty": "Duty-Inclusive Unit Value",
}
EPISODES = {
    "trade_war_2018": {
        "sample_start": "2017-01",
        "sample_end": "2019-12",
        "baseline_start": "2017-01",
        "baseline_end": "2017-12",
        "pre": 6,
        "post": 12,
    },
    "tariffs_2025": {
        "sample_start": "2024-01",
        "sample_end": "2025-12",
        "baseline_start": "2024-01",
        "baseline_end": "2024-12",
        "pre": 6,
        "post": 6,
    },
}
EXTENDED_EVENT_EPISODES = {
    "trade_war_2018": {
        "sample_start": "2017-01",
        "sample_end": "2021-12",
        "baseline_start": "2017-01",
        "baseline_end": "2017-12",
        "treatment_start": "2018-01",
        "treatment_end": "2019-12",
        "pre": 6,
        "requested_post": 24,
    },
    "tariffs_2025": {
        "sample_start": "2024-01",
        "sample_end": "2025-12",
        "baseline_start": "2024-01",
        "baseline_end": "2024-12",
        "treatment_start": "2025-01",
        "treatment_end": "2025-12",
        "pre": 6,
        "requested_post": 12,
    },
}
GROUPS = ("all", "china", "row")
TABLE4_TARGETS = {
    "first_stage": {"estimate": 0.42, "std_error": 0.06},
    "value": {"estimate": -1.81, "std_error": 0.50},
    "quantity": {"estimate": -1.71, "std_error": 0.54},
    "p": {"estimate": -0.10, "std_error": 0.06},
    "pduty": {"estimate": 0.90, "std_error": 0.06},
}


@dataclass(frozen=True)
class EventCurveSpec:
    episode: str
    group: str
    outcome: str

    @property
    def fit_id(self) -> str:
        return f"event|{self.episode}|{self.group}|{self.outcome}"

    @property
    def horizons(self) -> range:
        definition = EPISODES[self.episode]
        return range(-definition["pre"], definition["post"] + 1)


@dataclass(frozen=True)
class ExtendedEventCurveSpec:
    episode: str
    outcome: str
    group: str = "all"

    @property
    def fit_id(self) -> str:
        requested = EXTENDED_EVENT_EPISODES[self.episode]["requested_post"]
        return (
            f"event_extended|{self.episode}|{self.group}|"
            f"{self.outcome}|requested_h{requested}"
        )

    @property
    def requested_horizons(self) -> range:
        definition = EXTENDED_EVENT_EPISODES[self.episode]
        return range(
            -definition["pre"],
            definition["requested_post"] + 1,
        )


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def event_estimator_fingerprint() -> str:
    parts = [
        inspect.getsource(prepare_event_panel),
        inspect.getsource(build_local_projection_sample),
        inspect.getsource(fit_local_projection_horizon),
    ]
    return hashlib.sha256("\n".join(part.replace("\r\n", "\n") for part in parts).encode()).hexdigest()


def extended_event_estimator_fingerprint() -> str:
    parts = [
        inspect.getsource(prepare_extended_event_panel),
        inspect.getsource(build_local_projection_sample),
        inspect.getsource(fit_local_projection_horizon),
    ]
    return hashlib.sha256(
        "\n".join(
            part.replace("\r\n", "\n")
            for part in parts
        ).encode()
    ).hexdigest()


def iv_estimator_fingerprint() -> str:
    parts = [
        inspect.getsource(load_quarterly_source),
        inspect.getsource(build_quarterly_panel),
        inspect.getsource(fit_quarterly_iv),
    ]
    return hashlib.sha256("\n".join(part.replace("\r\n", "\n") for part in parts).encode()).hexdigest()


def estimator_fingerprint() -> str:
    """Backward-compatible alias for the event-study scientific fingerprint."""
    return event_estimator_fingerprint()


def event_grid() -> list[EventCurveSpec]:
    rows = [
        EventCurveSpec(episode, "all", outcome)
        for episode in EPISODES
        for outcome in OUTCOMES
    ]
    rows.extend(
        EventCurveSpec("tariffs_2025", group, outcome)
        for group in ("china", "row")
        for outcome in OUTCOMES
    )
    return rows


def extended_event_grid() -> list[ExtendedEventCurveSpec]:
    return [
        ExtendedEventCurveSpec(episode, outcome)
        for episode in EXTENDED_EVENT_EPISODES
        for outcome in OUTCOMES
    ]


def fit_grid(_latest_period: str | None = None) -> list[EventCurveSpec]:
    """Compatibility entry point used by the master pipeline."""
    return event_grid()


def _trade_glob(config: PipelineConfig) -> str:
    return str(
        config.processed_trade_dir
        / "fk2025"
        / "variety_month"
        / "year=*"
        / "month=*"
        / "part.parquet"
    ).replace("\\", "/")


def _workhorse_path(config: PipelineConfig) -> Path:
    return config.processed_trade_dir / "fk2025" / "workhorse_2025.parquet"


def _event_source_hash(config: PipelineConfig, episode: str) -> str:
    manifest = config.processed_trade_dir / "fk2025" / "trade_manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    return _hash_payload({
        "episode": episode,
        "partition_set_fingerprint": payload["partition_set_fingerprint"],
        "version": payload["version"],
    })


def load_event_source(config: PipelineConfig, episode: str) -> pd.DataFrame:
    definition = EPISODES[episode]
    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            SELECT lpad(cast(partner_code AS VARCHAR),4,'0') AS partner_code,
                   hs10, hs8, year, month, period,
                   applied_tariff, import_value, quantity,
                   before_tariff_unit_value, duty_inclusive_unit_value,
                   duty_measure_incomplete, rate_provision79_value_share
            FROM read_parquet('{_trade_glob(config)}', hive_partitioning=false)
            WHERE period BETWEEN ? AND ?
            """,
            [definition["sample_start"], definition["sample_end"]],
        ).fetchdf()
    finally:
        con.close()


def _extended_trade_globs(
    config: PipelineConfig,
    episode: str,
) -> list[str]:
    globs = [_trade_glob(config)]
    if episode == "trade_war_2018":
        globs.append(
            str(
                config.processed_trade_dir
                / "fk2025_event_horizon_extension"
                / "variety_month"
                / "year=*"
                / "month=*"
                / "part.parquet"
            ).replace("\\", "/")
        )
    return globs


def _extended_event_source_hash(
    config: PipelineConfig,
    episode: str,
) -> str:
    base = json.loads(
        (
            config.processed_trade_dir
            / "fk2025"
            / "trade_manifest.json"
        ).read_text(encoding="utf-8")
    )
    payload: dict[str, Any] = {
        "episode": episode,
        "base_partition_set_fingerprint": base[
            "partition_set_fingerprint"
        ],
        "base_version": base["version"],
    }
    if episode == "trade_war_2018":
        extension_path = (
            config.processed_trade_dir
            / "fk2025_event_horizon_extension"
            / "trade_manifest.json"
        )
        extension = json.loads(extension_path.read_text(encoding="utf-8"))
        payload.update(
            extension_partition_set_fingerprint=extension[
                "partition_set_fingerprint"
            ],
            extension_version=extension["version"],
        )
    return _hash_payload(payload)


def load_extended_event_source(
    config: PipelineConfig,
    episode: str,
) -> pd.DataFrame:
    definition = EXTENDED_EVENT_EPISODES[episode]
    sources = ", ".join(
        f"'{path.replace(chr(39), chr(39) * 2)}'"
        for path in _extended_trade_globs(config, episode)
    )
    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            SELECT lpad(cast(partner_code AS VARCHAR),4,'0')
                       AS partner_code,
                   hs10, hs8, year, month, period,
                   applied_tariff, import_value, quantity,
                   before_tariff_unit_value,
                   duty_inclusive_unit_value,
                   duty_measure_incomplete,
                   rate_provision79_value_share
            FROM read_parquet([{sources}], hive_partitioning=false)
            WHERE period BETWEEN ? AND ?
            """,
            [
                definition["sample_start"],
                definition["sample_end"],
            ],
        ).fetchdf()
    finally:
        con.close()


def prepare_event_panel(frame: pd.DataFrame, spec: EventCurveSpec) -> pd.DataFrame:
    definition = EPISODES[spec.episode]
    out = frame.copy()
    out["partner_code"] = out["partner_code"].astype("string").str.zfill(4)
    out["hs10"] = out["hs10"].astype("string").str.zfill(10)
    out["hs8"] = out["hs10"].str[:8]
    out["month_index"] = (
        pd.to_numeric(out["year"], errors="raise").astype(int) * 12
        + pd.to_numeric(out["month"], errors="raise").astype(int)
        - 1
    )
    out["variety_id"] = (
        out["partner_code"].astype(str) + "|" + out["hs10"].astype(str)
    )
    baseline = out["period"].between(
        definition["baseline_start"], definition["baseline_end"], inclusive="both"
    )
    maxima = (
        out.loc[baseline]
        .groupby("variety_id", observed=True)["applied_tariff"]
        .max()
        .rename("baseline_max_tariff")
    )
    out = out.join(maxima, on="variety_id")
    out["above_threshold"] = (
        out["applied_tariff"] > out["baseline_max_tariff"] + 0.02
    )
    first = (
        out.loc[out["above_threshold"]]
        .groupby("variety_id", observed=True)["month_index"]
        .min()
        .rename("first_treatment_index")
    )
    out = out.join(first, on="variety_id")
    out["newly_treated"] = out["month_index"].eq(out["first_treatment_index"])
    # Figure 4B separates the treated cohort, not the estimation universe.
    # Keeping all origins is essential because HS10-by-base-month fixed effects
    # require cross-origin observations within a product-time cell.
    if spec.group == "china":
        out["newly_treated"] &= out["partner_code"].eq(CHINA_CODE)
    elif spec.group == "row":
        out["newly_treated"] &= ~out["partner_code"].eq(CHINA_CODE)
    if spec.outcome == "tariff":
        out["outcome_level"] = 100.0 * np.log1p(out["applied_tariff"])
    elif spec.outcome == "value":
        out["outcome_level"] = 100.0 * np.log(out["import_value"].where(out["import_value"] > 0))
    elif spec.outcome == "p":
        out["outcome_level"] = 100.0 * np.log(
            out["before_tariff_unit_value"].where(out["before_tariff_unit_value"] > 0)
        )
    else:
        out["outcome_level"] = 100.0 * np.log(
            out["duty_inclusive_unit_value"].where(out["duty_inclusive_unit_value"] > 0)
        )
    return out.sort_values(["variety_id", "month_index"], kind="mergesort").reset_index(drop=True)


def prepare_extended_event_panel(
    frame: pd.DataFrame,
    spec: ExtendedEventCurveSpec,
) -> pd.DataFrame:
    """Prepare a longer outcome window while freezing the episode cohorts.

    Extending the source window must not allow later tariff episodes to become
    treatment events for the 2018--19 exercise.
    """
    definition = EXTENDED_EVENT_EPISODES[spec.episode]
    base_spec = EventCurveSpec(spec.episode, spec.group, spec.outcome)
    out = prepare_event_panel(frame, base_spec)
    eligible_event = out["period"].between(
        definition["treatment_start"],
        definition["treatment_end"],
        inclusive="both",
    )
    out["above_threshold"] &= eligible_event
    first = (
        out.loc[out["above_threshold"]]
        .groupby("variety_id", observed=True)["month_index"]
        .min()
        .rename("extended_first_treatment_index")
    )
    out = out.drop(
        columns=["first_treatment_index", "newly_treated"],
    ).join(first, on="variety_id")
    out = out.rename(
        columns={
            "extended_first_treatment_index":
                "first_treatment_index",
        }
    )
    out["newly_treated"] = out["month_index"].eq(
        out["first_treatment_index"]
    )
    return out.sort_values(
        ["variety_id", "month_index"],
        kind="mergesort",
    ).reset_index(drop=True)


def _shift_values(panel: pd.DataFrame, offset: int) -> pd.DataFrame:
    shifted = panel[["variety_id", "month_index", "outcome_level"]].copy()
    shifted["base_index"] = shifted["month_index"] - offset
    return shifted.rename(columns={"outcome_level": f"outcome_{offset}"})[
        ["variety_id", "base_index", f"outcome_{offset}"]
    ]


def build_local_projection_sample(panel: pd.DataFrame, horizon: int) -> pd.DataFrame:
    base = panel[
        [
            "variety_id",
            "partner_code",
            "hs10",
            "hs8",
            "month_index",
            "first_treatment_index",
            "newly_treated",
        ]
    ].rename(columns={"month_index": "base_index"})
    work = base.merge(_shift_values(panel, -1), on=["variety_id", "base_index"], how="left")
    if horizon == -1:
        # In y(t+h) - y(t-1), h=-1 is the normalized event-study
        # reference period. Reuse the already merged lag rather than merging
        # the same column twice (which would create suffixed duplicate names).
        work["outcome_reference"] = work["outcome_-1"]
    else:
        work = work.merge(_shift_values(panel, horizon), on=["variety_id", "base_index"], how="left")
        work["outcome_reference"] = work[f"outcome_{horizon}"]
    # LP-DiD uses D(i,t+h)=0 for post-treatment horizons. For pretrend
    # horizons the reference implementation requires controls to remain
    # untreated at the event base t; h=-1 is normalized separately.
    control_cutoff = work["base_index"] + max(horizon, 0)
    untreated_control = work["first_treatment_index"].isna() | (
        work["first_treatment_index"] > control_cutoff
    )
    work = work.loc[work["newly_treated"] | untreated_control].copy()
    work["delta_treatment"] = work["newly_treated"].astype(float)
    work["delta_outcome"] = work["outcome_reference"] - work["outcome_-1"]
    work["product_time"] = (
        work["hs10"].astype(str) + "|" + work["base_index"].astype(str)
    )
    return work.dropna(subset=["delta_outcome", "delta_treatment"])


def fit_local_projection_horizon(sample: pd.DataFrame, horizon: int) -> dict[str, Any]:
    if sample["delta_treatment"].nunique() < 2:
        raise ValueError(f"horizon {horizon} has no treated/control variation")
    fit = pf.feols(
        "delta_outcome ~ delta_treatment | product_time + partner_code",
        sample,
        vcov={"CRV1": "partner_code + hs8"},
        copy_data=False,
        store_data=False,
        lean=True,
    )
    tidy = fit.tidy().reset_index()
    row = tidy.loc[tidy["Coefficient"].eq("delta_treatment")].iloc[0]
    return {
        "horizon": horizon,
        "estimate": float(row["Estimate"]),
        "std_error": float(row["Std. Error"]),
        "conf_low": float(row["2.5%"]),
        "conf_high": float(row["97.5%"]),
        "nobs": int(getattr(fit, "_N")),
        "treated_rows": int(sample["delta_treatment"].sum()),
        "control_rows": int((sample["delta_treatment"] == 0).sum()),
        "products": int(sample["hs10"].nunique()),
        "origins": int(sample["partner_code"].nunique()),
    }


def _curve_paths(config: PipelineConfig, spec: EventCurveSpec) -> tuple[Path, Path, Path]:
    root = (
        config.processed_trade_dir
        / "regressions"
        / "fk2025"
        / "event"
        / spec.fit_id.replace("|", "__")
    )
    return root / "coefficients.parquet", root / "sample_audit.parquet", root / "manifest.json"


def _horizon_paths(
    config: PipelineConfig,
    spec: EventCurveSpec,
    horizon: int,
) -> tuple[Path, Path]:
    root = _curve_paths(config, spec)[0].parent / "horizons"
    label = f"m{abs(horizon):02d}" if horizon < 0 else f"p{horizon:02d}"
    return root / f"horizon_{label}.parquet", root / f"horizon_{label}.json"


def _valid_horizon(
    config: PipelineConfig,
    spec: EventCurveSpec,
    horizon: int,
    source_hash: str,
) -> tuple[bool, str]:
    coefficient_path, manifest_path = _horizon_paths(config, spec, horizon)
    if not coefficient_path.exists() or not manifest_path.exists():
        return False, "missing_component"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {
        "fit_id": spec.fit_id,
        "horizon": horizon,
        "source_hash": source_hash,
        "estimator_fingerprint": event_estimator_fingerprint(),
        "specification_fingerprint": _hash_payload(asdict(spec)),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False, f"mismatch:{key}"
    try:
        frame = pd.read_parquet(coefficient_path)
    except Exception as exc:
        return False, f"unreadable:{type(exc).__name__}"
    required = {
        "horizon",
        "estimate",
        "std_error",
        "conf_low",
        "conf_high",
        "nobs",
        "treated_rows",
        "control_rows",
        "products",
        "origins",
    }
    if len(frame) != 1 or not required.issubset(frame.columns):
        return False, "invalid_schema_or_rows"
    if int(frame["horizon"].iloc[0]) != horizon:
        return False, "horizon_mismatch"
    if horizon != -1:
        estimate = float(frame["estimate"].iloc[0])
        standard_error = float(frame["std_error"].iloc[0])
        if not np.isfinite(estimate) or not np.isfinite(standard_error):
            return False, "nonfinite_estimate"
        if standard_error <= 0:
            return False, "nonpositive_nonbaseline_standard_error"
    return True, "valid"


def _write_horizon_checkpoint(
    config: PipelineConfig,
    spec: EventCurveSpec,
    horizon: int,
    source_hash: str,
    row: dict[str, Any],
) -> None:
    coefficient_path, manifest_path = _horizon_paths(config, spec, horizon)
    write_parquet(pd.DataFrame([row]), coefficient_path, overwrite=True)
    write_metadata_json(
        manifest_path,
        {
            "version": VERSION,
            "fit_id": spec.fit_id,
            "horizon": horizon,
            "source_hash": source_hash,
            "estimator_fingerprint": event_estimator_fingerprint(),
            "specification_fingerprint": _hash_payload(asdict(spec)),
            "coefficient_path": _relative(config, coefficient_path),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    valid, reason = _valid_horizon(config, spec, horizon, source_hash)
    if not valid:
        raise RuntimeError(f"horizon checkpoint failed validation: {reason}")


def _valid_curve(config: PipelineConfig, spec: EventCurveSpec, source_hash: str) -> tuple[bool, str]:
    coefficients, audit, manifest_path = _curve_paths(config, spec)
    if not coefficients.exists() or not audit.exists() or not manifest_path.exists():
        return False, "missing_component"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {
        "fit_id": spec.fit_id,
        "source_hash": source_hash,
        "estimator_fingerprint": event_estimator_fingerprint(),
        "specification_fingerprint": _hash_payload(asdict(spec)),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False, f"mismatch:{key}"
    frame = pd.read_parquet(coefficients)
    if set(frame["horizon"].astype(int)) != set(spec.horizons):
        return False, "incomplete_horizons"
    if frame["horizon"].duplicated().any():
        return False, "duplicate_horizons"
    nonbaseline = frame.loc[frame["horizon"].ne(-1)]
    if (
        ~np.isfinite(nonbaseline["estimate"].to_numpy(float))
    ).any() or (
        ~np.isfinite(nonbaseline["std_error"].to_numpy(float))
    ).any():
        return False, "nonfinite_estimate"
    if nonbaseline["std_error"].le(0).any():
        return False, "nonpositive_nonbaseline_standard_error"
    return True, "valid"


def fit_event_curve(
    config: PipelineConfig,
    spec: EventCurveSpec,
    *,
    resume: bool = True,
    raw_source: pd.DataFrame | None = None,
) -> dict[str, Any]:
    source_hash = _event_source_hash(config, spec.episode)
    valid, reason = _valid_curve(config, spec, source_hash)
    if resume and valid:
        return {"fit_id": spec.fit_id, "status": "resumed"}
    raw = raw_source if raw_source is not None else load_event_source(config, spec.episode)
    panel = prepare_event_panel(raw, spec)
    current = config.processed_trade_dir / "regressions" / "fk2025" / "current_fit.json"
    rows: list[dict[str, Any]] = []
    for horizon in spec.horizons:
        horizon_valid, _ = _valid_horizon(
            config, spec, horizon, source_hash
        )
        if resume and horizon_valid:
            rows.append(
                pd.read_parquet(_horizon_paths(config, spec, horizon)[0]).iloc[0].to_dict()
            )
            print(
                f"[{datetime.now().isoformat(timespec='seconds')}] "
                f"{spec.fit_id}: h={horizon} resumed",
                flush=True,
            )
            continue
        print(
            f"[{datetime.now().isoformat(timespec='seconds')}] {spec.fit_id}: h={horizon}",
            flush=True,
        )
        sample = build_local_projection_sample(panel, horizon)
        if horizon == -1:
            # The local-projection outcome is identically zero at the
            # normalization horizon. Materialize the reference point without
            # asking the estimator to fit a degenerate dependent variable.
            result = {
                "horizon": horizon,
                "estimate": 0.0,
                "std_error": 0.0,
                "conf_low": 0.0,
                "conf_high": 0.0,
                "nobs": int(len(sample)),
                "treated_rows": int(sample["delta_treatment"].sum()),
                "control_rows": int((sample["delta_treatment"] == 0).sum()),
                "products": int(sample["hs10"].nunique()),
                "origins": int(sample["partner_code"].nunique()),
            }
            _write_horizon_checkpoint(
                config, spec, horizon, source_hash, result
            )
            rows.append(result)
            continue
        marker = {
            "version": VERSION,
            "fit_id": spec.fit_id,
            "horizon": horizon,
            "row_count": len(sample),
            "estimated_memory_bytes": int(sample.memory_usage(deep=True).sum()),
            "formula": "y(t+h)-y(t-1) ~ new_treatment | HS10xbase_month + origin",
            "fixed_effects": ["hs10_x_base_month", "origin"],
            "clusters": ["origin", "hs8"],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        write_metadata_json(current, marker)
        try:
            result = fit_local_projection_horizon(sample, horizon)
            _write_horizon_checkpoint(
                config, spec, horizon, source_hash, result
            )
            rows.append(result)
        except Exception as exc:
            failure = current.with_name(
                f"failure_{spec.fit_id.replace('|','__')}_h{horizon}.json"
            )
            write_metadata_json(failure, {
                **marker,
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
            })
            raise
    coefficients = pd.DataFrame(rows)
    coefficients["fit_id"] = spec.fit_id
    coefficients["episode"] = spec.episode
    coefficients["group"] = spec.group
    coefficients["outcome"] = spec.outcome
    audit = coefficients[
        ["fit_id", "horizon", "nobs", "treated_rows", "control_rows", "products", "origins"]
    ].copy()
    coefficients_path, audit_path, manifest_path = _curve_paths(config, spec)
    write_parquet(coefficients, coefficients_path, overwrite=True)
    write_parquet(audit, audit_path, overwrite=True)
    manifest = {
        "version": VERSION,
        "fit_id": spec.fit_id,
        "source_hash": source_hash,
        "estimator_fingerprint": event_estimator_fingerprint(),
        "specification_fingerprint": _hash_payload(asdict(spec)),
        "treatment_rule": "first applied tariff above prior-year maximum by more than 0.02; absorbing",
        "outcome": spec.outcome,
        "horizons": list(spec.horizons),
        "coefficient_path": _relative(config, coefficients_path),
        "sample_audit_path": _relative(config, audit_path),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(manifest_path, manifest)
    valid, reason = _valid_curve(config, spec, source_hash)
    if not valid:
        raise RuntimeError(f"curve checkpoint failed validation: {reason}")
    current.unlink(missing_ok=True)
    return {"fit_id": spec.fit_id, "status": "fitted"}


def _extended_curve_paths(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
) -> tuple[Path, Path, Path]:
    root = (
        config.processed_trade_dir
        / "regressions"
        / "fk2025"
        / "event_horizon_extension"
        / spec.fit_id.replace("|", "__")
    )
    return (
        root / "coefficients.parquet",
        root / "sample_audit.parquet",
        root / "manifest.json",
    )


def _extended_horizon_paths(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
    horizon: int,
) -> tuple[Path, Path]:
    root = _extended_curve_paths(config, spec)[0].parent / "horizons"
    label = (
        f"m{abs(horizon):02d}"
        if horizon < 0
        else f"p{horizon:02d}"
    )
    return (
        root / f"horizon_{label}.parquet",
        root / f"horizon_{label}.json",
    )


def _valid_extended_horizon(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
    horizon: int,
    source_hash: str,
) -> tuple[bool, str]:
    coefficient_path, manifest_path = _extended_horizon_paths(
        config,
        spec,
        horizon,
    )
    if not coefficient_path.exists() or not manifest_path.exists():
        return False, "missing_component"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {
        "fit_id": spec.fit_id,
        "horizon": horizon,
        "source_hash": source_hash,
        "estimator_fingerprint":
            extended_event_estimator_fingerprint(),
        "specification_fingerprint": _hash_payload(asdict(spec)),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False, f"mismatch:{key}"
    try:
        frame = pd.read_parquet(coefficient_path)
    except Exception as exc:
        return False, f"unreadable:{type(exc).__name__}"
    required = {
        "horizon",
        "estimate",
        "std_error",
        "conf_low",
        "conf_high",
        "nobs",
        "treated_rows",
        "control_rows",
        "products",
        "origins",
    }
    if len(frame) != 1 or not required.issubset(frame.columns):
        return False, "invalid_schema_or_rows"
    if int(frame["horizon"].iloc[0]) != horizon:
        return False, "horizon_mismatch"
    if horizon != -1:
        estimate = float(frame["estimate"].iloc[0])
        standard_error = float(frame["std_error"].iloc[0])
        if not np.isfinite(estimate) or not np.isfinite(standard_error):
            return False, "nonfinite_estimate"
        if standard_error <= 0:
            return False, "nonpositive_nonbaseline_standard_error"
    return True, "valid"


def _write_extended_horizon_checkpoint(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
    horizon: int,
    source_hash: str,
    row: dict[str, Any],
) -> None:
    coefficient_path, manifest_path = _extended_horizon_paths(
        config,
        spec,
        horizon,
    )
    write_parquet(
        pd.DataFrame([row]),
        coefficient_path,
        overwrite=True,
    )
    write_metadata_json(
        manifest_path,
        {
            "version": VERSION,
            "fit_id": spec.fit_id,
            "horizon": horizon,
            "source_hash": source_hash,
            "estimator_fingerprint":
                extended_event_estimator_fingerprint(),
            "specification_fingerprint": _hash_payload(asdict(spec)),
            "coefficient_path": _relative(config, coefficient_path),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    valid, reason = _valid_extended_horizon(
        config,
        spec,
        horizon,
        source_hash,
    )
    if not valid:
        raise RuntimeError(
            f"extended horizon checkpoint failed validation: {reason}"
        )


def _base_horizon_is_promotable(
    spec: ExtendedEventCurveSpec,
    horizon: int,
) -> bool:
    """Return whether a locked paper-window fit is scientifically identical.

    The 2025 extended exercise uses the same 2024--25 source window, 2024
    baseline, all-origin cohort, and LP estimator as the locked paper-window
    exercise.  Consequently, its already validated horizons can be promoted
    without re-estimation.  The 2018 extension cannot be promoted because its
    added 2020--21 outcome rows change risk sets even at overlapping horizons.
    """
    if spec.episode != "tariffs_2025" or spec.group != "all":
        return False
    base = EPISODES["tariffs_2025"]
    extended = EXTENDED_EVENT_EPISODES["tariffs_2025"]
    same_window = (
        base["sample_start"] == extended["sample_start"]
        and base["sample_end"] == extended["sample_end"]
        and base["baseline_start"] == extended["baseline_start"]
        and base["baseline_end"] == extended["baseline_end"]
    )
    treatment_covers_postbaseline_sample = (
        extended["treatment_start"] == "2025-01"
        and extended["treatment_end"] == extended["sample_end"]
    )
    return (
        same_window
        and treatment_covers_postbaseline_sample
        and horizon in EventCurveSpec(
            spec.episode,
            spec.group,
            spec.outcome,
        ).horizons
    )


def _promote_base_horizon(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
    horizon: int,
    extended_source_hash: str,
) -> dict[str, Any] | None:
    """Promote an exactly equivalent, hash-valid paper-window checkpoint."""
    if not _base_horizon_is_promotable(spec, horizon):
        return None
    base_spec = EventCurveSpec(
        spec.episode,
        spec.group,
        spec.outcome,
    )
    base_source_hash = _event_source_hash(
        config,
        base_spec.episode,
    )
    valid, _ = _valid_horizon(
        config,
        base_spec,
        horizon,
        base_source_hash,
    )
    if not valid:
        return None
    base_coefficient, base_manifest = _horizon_paths(
        config,
        base_spec,
        horizon,
    )
    row = pd.read_parquet(base_coefficient).iloc[0].to_dict()
    _write_extended_horizon_checkpoint(
        config,
        spec,
        horizon,
        extended_source_hash,
        row,
    )
    _, promoted_manifest = _extended_horizon_paths(
        config,
        spec,
        horizon,
    )
    metadata = json.loads(
        promoted_manifest.read_text(encoding="utf-8")
    )
    metadata.update(
        checkpoint_origin="validated_paper_window_promotion",
        promoted_from_fit_id=base_spec.fit_id,
        promoted_from_source_hash=base_source_hash,
        promoted_from_estimator_fingerprint=
            event_estimator_fingerprint(),
        promoted_from_manifest=_relative(config, base_manifest),
        promotion_basis=(
            "identical_2024_2025_source_window_baseline_"
            "treatment_universe_and_local_projection_estimator"
        ),
    )
    write_metadata_json(promoted_manifest, metadata)
    return row


def _valid_extended_curve(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
    source_hash: str,
) -> tuple[bool, str]:
    coefficients, audit, manifest_path = _extended_curve_paths(
        config,
        spec,
    )
    if not coefficients.exists() or not audit.exists() or not manifest_path.exists():
        return False, "missing_component"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {
        "fit_id": spec.fit_id,
        "source_hash": source_hash,
        "estimator_fingerprint":
            extended_event_estimator_fingerprint(),
        "specification_fingerprint": _hash_payload(asdict(spec)),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            return False, f"mismatch:{key}"
    frame = pd.read_parquet(coefficients)
    completed = set(frame["horizon"].astype(int))
    unavailable = {
        int(row["horizon"])
        for row in manifest.get("unavailable_horizons", [])
    }
    requested = set(spec.requested_horizons)
    if completed & unavailable:
        return False, "completed_unavailable_overlap"
    if completed | unavailable != requested:
        return False, "incomplete_requested_horizon_accounting"
    if frame["horizon"].duplicated().any():
        return False, "duplicate_horizons"
    nonbaseline = frame.loc[frame["horizon"].ne(-1)]
    if (
        ~np.isfinite(nonbaseline["estimate"].to_numpy(float))
    ).any() or (
        ~np.isfinite(nonbaseline["std_error"].to_numpy(float))
    ).any():
        return False, "nonfinite_estimate"
    if nonbaseline["std_error"].le(0).any():
        return False, "nonpositive_nonbaseline_standard_error"
    return True, "valid"


def fit_extended_event_curve(
    config: PipelineConfig,
    spec: ExtendedEventCurveSpec,
    *,
    resume: bool = True,
    raw_source: pd.DataFrame | None = None,
) -> dict[str, Any]:
    source_hash = _extended_event_source_hash(config, spec.episode)
    valid, _ = _valid_extended_curve(
        config,
        spec,
        source_hash,
    )
    if resume and valid:
        return {"fit_id": spec.fit_id, "status": "resumed"}
    raw = (
        raw_source
        if raw_source is not None
        else load_extended_event_source(config, spec.episode)
    )
    panel = prepare_extended_event_panel(raw, spec)
    current = (
        config.processed_trade_dir
        / "regressions"
        / "fk2025"
        / "event_horizon_extension"
        / "current_fit.json"
    )
    rows: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    for horizon in spec.requested_horizons:
        horizon_valid, _ = _valid_extended_horizon(
            config,
            spec,
            horizon,
            source_hash,
        )
        if resume and horizon_valid:
            rows.append(
                pd.read_parquet(
                    _extended_horizon_paths(
                        config,
                        spec,
                        horizon,
                    )[0]
                ).iloc[0].to_dict()
            )
            print(
                f"[{datetime.now().isoformat(timespec='seconds')}] "
                f"{spec.fit_id}: h={horizon} resumed",
                flush=True,
            )
            continue
        if resume:
            promoted = _promote_base_horizon(
                config,
                spec,
                horizon,
                source_hash,
            )
            if promoted is not None:
                rows.append(promoted)
                print(
                    f"[{datetime.now().isoformat(timespec='seconds')}] "
                    f"{spec.fit_id}: h={horizon} promoted",
                    flush=True,
                )
                continue
        sample = build_local_projection_sample(panel, horizon)
        treated_rows = int(sample["delta_treatment"].sum())
        control_rows = int(
            (sample["delta_treatment"] == 0).sum()
        )
        if horizon != -1 and (
            treated_rows == 0 or control_rows == 0
        ):
            unavailable.append(
                {
                    "horizon": horizon,
                    "reason": "right_censored_no_treated_or_control_support",
                    "treated_rows": treated_rows,
                    "control_rows": control_rows,
                }
            )
            print(
                f"[{datetime.now().isoformat(timespec='seconds')}] "
                f"{spec.fit_id}: h={horizon} unavailable",
                flush=True,
            )
            del sample
            gc.collect()
            continue
        print(
            f"[{datetime.now().isoformat(timespec='seconds')}] "
            f"{spec.fit_id}: h={horizon}",
            flush=True,
        )
        if horizon == -1:
            result = {
                "horizon": horizon,
                "estimate": 0.0,
                "std_error": 0.0,
                "conf_low": 0.0,
                "conf_high": 0.0,
                "nobs": int(len(sample)),
                "treated_rows": treated_rows,
                "control_rows": control_rows,
                "products": int(sample["hs10"].nunique()),
                "origins": int(sample["partner_code"].nunique()),
            }
            _write_extended_horizon_checkpoint(
                config,
                spec,
                horizon,
                source_hash,
                result,
            )
            rows.append(result)
            del sample
            gc.collect()
            continue
        marker = {
            "version": VERSION,
            "fit_id": spec.fit_id,
            "horizon": horizon,
            "row_count": len(sample),
            "estimated_memory_bytes": int(
                sample.memory_usage(deep=True).sum()
            ),
            "formula": (
                "y(t+h)-y(t-1) ~ new_treatment | "
                "HS10xbase_month + origin"
            ),
            "fixed_effects": [
                "hs10_x_base_month",
                "origin",
            ],
            "clusters": ["origin", "hs8"],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        write_metadata_json(current, marker)
        try:
            result = fit_local_projection_horizon(sample, horizon)
            _write_extended_horizon_checkpoint(
                config,
                spec,
                horizon,
                source_hash,
                result,
            )
            rows.append(result)
            del sample
            gc.collect()
        except Exception as exc:
            failure = current.with_name(
                "failure_"
                + spec.fit_id.replace("|", "__")
                + f"_h{horizon}.json"
            )
            write_metadata_json(
                failure,
                {
                    **marker,
                    "exception_type": type(exc).__name__,
                    "exception_message": str(exc),
                },
            )
            raise
    coefficients = pd.DataFrame(rows).sort_values("horizon")
    coefficients["fit_id"] = spec.fit_id
    coefficients["episode"] = spec.episode
    coefficients["group"] = spec.group
    coefficients["outcome"] = spec.outcome
    audit = coefficients[
        [
            "fit_id",
            "horizon",
            "nobs",
            "treated_rows",
            "control_rows",
            "products",
            "origins",
        ]
    ].copy()
    coefficients_path, audit_path, manifest_path = _extended_curve_paths(
        config,
        spec,
    )
    write_parquet(coefficients, coefficients_path, overwrite=True)
    write_parquet(audit, audit_path, overwrite=True)
    manifest = {
        "version": VERSION,
        "fit_id": spec.fit_id,
        "source_hash": source_hash,
        "estimator_fingerprint":
            extended_event_estimator_fingerprint(),
        "specification_fingerprint": _hash_payload(asdict(spec)),
        "treatment_window": {
            "start": EXTENDED_EVENT_EPISODES[spec.episode][
                "treatment_start"
            ],
            "end": EXTENDED_EVENT_EPISODES[spec.episode][
                "treatment_end"
            ],
        },
        "requested_horizons": list(spec.requested_horizons),
        "completed_horizons": coefficients[
            "horizon"
        ].astype(int).tolist(),
        "unavailable_horizons": unavailable,
        "right_censored": bool(unavailable),
        "coefficient_path": _relative(config, coefficients_path),
        "sample_audit_path": _relative(config, audit_path),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_json(manifest_path, manifest)
    valid, reason = _valid_extended_curve(
        config,
        spec,
        source_hash,
    )
    if not valid:
        raise RuntimeError(
            f"extended curve failed validation: {reason}"
        )
    current.unlink(missing_ok=True)
    return {
        "fit_id": spec.fit_id,
        "status": "fitted",
        "completed_horizons": len(coefficients),
        "unavailable_horizons": unavailable,
    }


def run_extended_event_fits(
    config: PipelineConfig,
    specs: Iterable[ExtendedEventCurveSpec] | None = None,
    *,
    resume: bool = True,
) -> dict[str, Any]:
    selected = list(specs or extended_event_grid())
    records: list[dict[str, Any]] = []
    source_cache: dict[str, pd.DataFrame] = {}
    for spec in selected:
        if spec.episode not in source_cache:
            source_cache.clear()
            source_cache[spec.episode] = load_extended_event_source(
                config,
                spec.episode,
            )
        records.append(
            fit_extended_event_curve(
                config,
                spec,
                resume=resume,
                raw_source=source_cache[spec.episode],
            )
        )
    return {"version": VERSION, "fits": records}


def build_quarterly_panel(
    frame: pd.DataFrame,
    *,
    cutoff: str = "2025-11",
    frequency: str = "quarterly",
) -> pd.DataFrame:
    out = frame.loc[frame["period"].between("2024-01", cutoff, inclusive="both")].copy()
    out["date"] = pd.to_datetime(out["period"] + "-01")
    if frequency == "monthly":
        out["time"] = out["date"].dt.to_period("M").astype(str)
        lag = 1
    elif frequency == "quarterly":
        out["time"] = out["date"].dt.to_period("Q").astype(str)
        lag = 1
    elif frequency == "semiannual":
        out["time"] = (
            out["date"].dt.year.astype(str)
            + "H"
            + np.where(out["date"].dt.month <= 6, "1", "2")
        )
        lag = 1
    elif frequency == "annual":
        # Table A.3 defines the annual horizon as year-over-year changes in
        # quarterly observations (for example, 2025Q1 minus 2024Q1).
        out["time"] = out["date"].dt.to_period("Q").astype(str)
        lag = 4
    else:
        raise ValueError(f"unsupported frequency: {frequency}")
    grouped = (
        out.groupby(["partner_code", "hs10", "hs8", "time"], as_index=False)
        .agg(
            import_value=("con_val_mo", "sum"),
            quantity=("con_qy1_mo", "sum"),
            calculated_duty=("cal_dut_mo", "sum"),
            statutory_numerator=("statutory_value_numerator", "sum"),
            statutory_denominator=("statutory_value_denominator", "sum"),
        )
    )
    grouped["applied_tariff"] = grouped["calculated_duty"] / grouped["import_value"].where(grouped["import_value"] > 0)
    grouped["statutory_tariff"] = grouped["statutory_numerator"] / grouped["statutory_denominator"].where(grouped["statutory_denominator"] > 0)
    grouped["p"] = grouped["import_value"] / grouped["quantity"].where(grouped["quantity"] > 0)
    grouped["pduty"] = (grouped["import_value"] + grouped["calculated_duty"]) / grouped["quantity"].where(grouped["quantity"] > 0)
    grouped["variety_id"] = grouped["partner_code"].astype(str) + "|" + grouped["hs10"].astype(str)
    time_order = {value: i for i, value in enumerate(sorted(grouped["time"].unique()))}
    grouped["time_index"] = grouped["time"].map(time_order)
    grouped = grouped.sort_values(["variety_id", "time_index"], kind="mergesort")
    for column in ("import_value", "quantity", "p", "pduty"):
        grouped[f"log_{column}"] = np.log(grouped[column].where(grouped[column] > 0))
    grouped["log_applied"] = np.log1p(grouped["applied_tariff"])
    grouped["log_statutory"] = np.log1p(grouped["statutory_tariff"])
    current = grouped.set_index(["variety_id", "time_index"])
    prior = current.reset_index()
    prior["time_index"] += lag
    prior = prior.set_index(["variety_id", "time_index"])
    for column in ("log_applied", "log_statutory", "log_import_value", "log_quantity", "log_p", "log_pduty"):
        grouped[f"d_{column}"] = (
            current[column] - prior[column]
        ).reindex(current.index).to_numpy()
    grouped["product_time"] = grouped["hs10"].astype(str) + "|" + grouped["time"].astype(str)
    grouped["origin_time"] = grouped["partner_code"].astype(str) + "|" + grouped["time"].astype(str)
    return grouped.reset_index(drop=True)


def load_quarterly_source(
    config: PipelineConfig,
    *,
    instrument_scope: str = "paper_coverage",
) -> pd.DataFrame:
    path = _workhorse_path(config)
    if not path.exists():
        raise FileNotFoundError(path)
    if instrument_scope == "paper_coverage":
        rate_column = "statutory_paper_coverage_rate"
        scope_filter = "statutory_paper_coverage_rate IS NOT NULL"
    elif instrument_scope == "deterministic":
        rate_column = "statutory_deterministic_rate"
        scope_filter = (
            "dynamic_scope_eligible "
            "AND statutory_deterministic_rate IS NOT NULL"
        )
    else:
        raise ValueError(f"unsupported instrument scope: {instrument_scope}")
    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            SELECT lpad(cast(partner_code AS VARCHAR),4,'0') AS partner_code,
                   hs10, substring(hs10,1,8) AS hs8, period,
                   con_val_mo, con_qy1_mo, cal_dut_mo,
                   con_val_mo * {rate_column} AS statutory_value_numerator,
                   CASE WHEN {rate_column} IS NOT NULL THEN con_val_mo END
                       AS statutory_value_denominator
            FROM read_parquet(?)
            WHERE {scope_filter}
            """,
            [str(path)],
        ).fetchdf()
    finally:
        con.close()


def _common_iv_sample(panel: pd.DataFrame) -> pd.DataFrame:
    required = [
        "d_log_applied",
        "d_log_statutory",
        "d_log_import_value",
        "d_log_quantity",
        "d_log_p",
        "d_log_pduty",
    ]
    work = panel.copy()
    work[required] = work[required].replace([np.inf, -np.inf], np.nan)
    return work.dropna(subset=required).copy()


def _tidy_term(fit: Any, term: str) -> tuple[float, float, float, float]:
    tidy = fit.tidy().reset_index()
    row = tidy.loc[tidy["Coefficient"].eq(term)].iloc[0]
    return (
        float(row["Estimate"]),
        float(row["Std. Error"]),
        float(row["2.5%"]),
        float(row["97.5%"]),
    )


def fit_quarterly_iv(
    panel: pd.DataFrame,
    *,
    fixed_effects: str = "product_time + partner_code",
    outcomes: tuple[str, ...] = ("value", "quantity", "p", "pduty"),
) -> pd.DataFrame:
    work = _common_iv_sample(panel)
    vcov = {"CRV1": "partner_code + hs8"}
    first = pf.feols(
        f"d_log_applied ~ d_log_statutory | {fixed_effects}",
        work,
        vcov=vcov,
        copy_data=False,
        store_data=False,
        lean=True,
    )
    estimate, se, low, high = _tidy_term(first, "d_log_statutory")
    rows = [{
        "outcome": "first_stage",
        "estimate": estimate,
        "std_error": se,
        "conf_low": low,
        "conf_high": high,
        "nobs": int(getattr(first, "_N")),
        "first_stage_f": float(getattr(first, "_f_statistic")),
    }]
    outcome_map = {
        "value": "d_log_import_value",
        "quantity": "d_log_quantity",
        "p": "d_log_p",
        "pduty": "d_log_pduty",
    }
    for name, outcome in outcome_map.items():
        if name not in outcomes:
            continue
        fit = pf.feols(
            f"{outcome} ~ 1 | {fixed_effects} | d_log_applied ~ d_log_statutory",
            work,
            vcov=vcov,
            copy_data=False,
            store_data=False,
            lean=True,
        )
        estimate, se, low, high = _tidy_term(fit, "d_log_applied")
        rows.append({
            "outcome": name,
            "estimate": estimate,
            "std_error": se,
            "conf_low": low,
            "conf_high": high,
            "nobs": int(getattr(fit, "_N")),
            "first_stage_f": float(getattr(first, "_f_statistic")),
        })
    result = pd.DataFrame(rows)
    result["fixed_effects"] = fixed_effects
    return result


def run_quarterly_analysis(
    config: PipelineConfig,
    *,
    cutoff: str = "2025-11",
    include_robustness: bool = True,
    instrument_scope: str = "paper_coverage",
) -> dict[str, Any]:
    raw = load_quarterly_source(config, instrument_scope=instrument_scope)
    root = (
        config.processed_trade_dir
        / "regressions"
        / "fk2025"
        / "quarterly_iv"
        / instrument_scope
    )
    baseline_panel = build_quarterly_panel(raw, cutoff=cutoff, frequency="quarterly")
    baseline = fit_quarterly_iv(baseline_panel)
    baseline["sample_cutoff"] = cutoff
    baseline["specification"] = "table4_panel_b"
    write_parquet(baseline, root / f"table4_panel_b_{cutoff}.parquet", overwrite=True)

    if include_robustness:
        robustness: list[pd.DataFrame] = []
        for label, fixed in {
            "product_time_origin": "product_time + partner_code",
            "product_time": "product_time",
            "product_time_origin_time": "product_time + origin_time",
            "product_time_separate_origin": "hs10 + time + partner_code",
        }.items():
            estimate = fit_quarterly_iv(
                baseline_panel,
                fixed_effects=fixed,
                outcomes=("p",),
            )
            estimate["robustness_spec"] = label
            robustness.append(estimate)
        robust = pd.concat(robustness, ignore_index=True)
        write_parquet(robust, root / f"table_a2_{cutoff}.parquet", overwrite=True)

        horizons: list[pd.DataFrame] = []
        for frequency in ("monthly", "quarterly", "semiannual", "annual"):
            prepared = build_quarterly_panel(raw, cutoff=cutoff, frequency=frequency)
            estimate = fit_quarterly_iv(prepared, outcomes=("p",))
            estimate = estimate.loc[estimate["outcome"].eq("p")].copy()
            estimate["frequency"] = frequency
            horizons.append(estimate)
        horizon_frame = pd.concat(horizons, ignore_index=True)
        write_parquet(horizon_frame, root / f"table_a3_{cutoff}.parquet", overwrite=True)

    comparisons = []
    for row in baseline.itertuples(index=False):
        target = TABLE4_TARGETS[row.outcome]
        comparisons.append({
            "outcome": row.outcome,
            "estimate": row.estimate,
            "paper_estimate": target["estimate"],
            "paper_std_error": target["std_error"],
            "absolute_difference": abs(row.estimate - target["estimate"]),
            "within_one_paper_se": abs(row.estimate - target["estimate"]) <= target["std_error"],
        })
    comparison = pd.DataFrame(comparisons)
    write_parquet(comparison, root / f"table4_comparison_{cutoff}.parquet", overwrite=True)
    comparison.to_csv(root / f"table4_comparison_{cutoff}.csv", index=False)
    identity = float(
        baseline.loc[baseline["outcome"].eq("pduty"), "estimate"].iloc[0]
        - baseline.loc[baseline["outcome"].eq("p"), "estimate"].iloc[0]
    )
    manifest = {
        "version": VERSION,
        "status": "passed" if (
            comparison["within_one_paper_se"].all()
            and abs(int(baseline["nobs"].min()) - 1_192_687) / 1_192_687 <= 0.05
            and abs(identity - 1.0) <= 1e-8
        ) else "failed",
        "iv_estimator_fingerprint": iv_estimator_fingerprint(),
        "sample_cutoff": cutoff,
        "instrument_scope": instrument_scope,
        "sample_definition": (
            "common complete origin-HS10-quarter sample using the "
            f"{instrument_scope} statutory instrument"
        ),
        "observations": int(baseline["nobs"].min()),
        "paper_observations": 1_192_687,
        "observation_count_within_5pct": abs(int(baseline["nobs"].min()) - 1_192_687) / 1_192_687 <= 0.05,
        "duty_inclusive_minus_preduty": identity,
        "duty_identity_pass": abs(identity - 1.0) <= 1e-8,
        "exchange_rate_robustness": "blocked_missing_exact_source",
        "robustness_completed": include_robustness,
        "baseline_results": baseline.to_dict(orient="records"),
        "comparison": comparison.to_dict(orient="records"),
    }
    write_metadata_json(root / f"manifest_{cutoff}.json", manifest)
    return manifest


def build_applied_tariff_landmarks(config: PipelineConfig) -> dict[str, Any]:
    """Audit aggregate applied rates with and without rate provision 79.

    The paper defines the applied tariff as recorded duties divided by import
    value across the rate provisions and separately notes that Census omits
    calculated duties for provision 79.  The all-provision result is the
    canonical empirical object.  Dropping provision 79 is reported only as a
    sensitivity, never as a silent replacement for the baseline.
    """
    rate_glob = str(
        config.processed_trade_dir
        / "fk2025"
        / "rate_provision"
        / "year=2025"
        / "month=*"
        / "part.parquet"
    ).replace("\\", "/")
    con = duckdb.connect()
    try:
        frame = con.execute(
            f"""
            WITH expanded AS (
                SELECT strftime(
                           make_date(
                               cast(year AS BIGINT),
                               cast(month AS BIGINT),
                               1
                           ),
                           '%Y-%m'
                       ) AS period,
                       CASE
                           WHEN partner_code = '5700' THEN 'china'
                           WHEN partner_code IN ('1220', '2010') THEN 'canada_mexico'
                           ELSE 'row'
                       END AS partner_group,
                       rate_prov,
                       con_val_mo,
                       cal_dut_mo
                FROM read_parquet('{rate_glob}', hive_partitioning=false)
            ),
            labelled AS (
                SELECT * FROM expanded
                UNION ALL
                SELECT period, 'all' AS partner_group, rate_prov,
                       con_val_mo, cal_dut_mo
                FROM expanded
            )
            SELECT period,
                   partner_group,
                   sum(cal_dut_mo) / nullif(sum(con_val_mo), 0)
                       AS applied_tariff_all_provisions,
                   sum(cal_dut_mo) FILTER (WHERE rate_prov <> '79')
                       / nullif(sum(con_val_mo) FILTER (WHERE rate_prov <> '79'), 0)
                       AS applied_tariff_excluding_provision79,
                   sum(con_val_mo) FILTER (WHERE rate_prov = '79')
                       / nullif(sum(con_val_mo), 0)
                       AS provision79_value_share,
                   sum(con_val_mo) FILTER (WHERE cal_dut_mo IS NULL)
                       / nullif(sum(con_val_mo), 0)
                       AS missing_duty_value_share,
                   sum(con_val_mo) AS import_value
            FROM labelled
            GROUP BY period, partner_group
            ORDER BY period, partner_group
            """
        ).fetchdf()
    finally:
        con.close()

    root = config.processed_trade_dir / "regressions" / "fk2025" / "descriptive"
    parquet_path = root / "applied_tariff_landmarks.parquet"
    csv_path = root / "applied_tariff_landmarks.csv"
    write_parquet(frame, parquet_path, overwrite=True)
    frame.to_csv(csv_path, index=False)
    december = frame.loc[frame["period"].eq("2025-12")].copy()
    reported = {"all": 0.096, "china": 0.317, "canada_mexico": 0.039}
    comparisons = []
    for group, target in reported.items():
        row = december.loc[december["partner_group"].eq(group)].iloc[0]
        comparisons.append(
            {
                "partner_group": group,
                "paper_reported_rate": target,
                "all_provision_rate": float(row["applied_tariff_all_provisions"]),
                "excluding_provision79_rate": float(
                    row["applied_tariff_excluding_provision79"]
                ),
                "all_provision_absolute_difference": abs(
                    float(row["applied_tariff_all_provisions"]) - target
                ),
                "excluding_provision79_absolute_difference": abs(
                    float(row["applied_tariff_excluding_provision79"]) - target
                ),
                "provision79_value_share": float(row["provision79_value_share"]),
            }
        )
    manifest = {
        "version": VERSION,
        "status": "diagnostic_complete",
        "canonical_convention": "all_rate_provisions_in_import_value_denominator",
        "provision79_treatment": (
            "retain import value; sum only recorded calculated duties; "
            "do not impute missing duties"
        ),
        "exclusion_sensitivity_is_baseline": False,
        "paper_landmarks": comparisons,
        "parquet": _relative(config, parquet_path),
        "csv": _relative(config, csv_path),
    }
    write_metadata_json(root / "applied_tariff_landmarks_manifest.json", manifest)
    return manifest


def _plot_panel(config: PipelineConfig, coefficients: pd.DataFrame, *, panel: str) -> list[Path]:
    if panel == "figure4a":
        series = [("trade_war_2018", "all", "2018–19 Tariffs", "#d97706"), ("tariffs_2025", "all", "2025 Tariffs", "#2563eb")]
    else:
        series = [("tariffs_2025", "china", "China", "#dc2626"), ("tariffs_2025", "row", "Rest of World", "#1d4ed8")]
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=False)
    for axis, outcome in zip(axes.flat, OUTCOMES):
        for episode, group, label, color in series:
            line = coefficients.loc[
                coefficients["episode"].eq(episode)
                & coefficients["group"].eq(group)
                & coefficients["outcome"].eq(outcome)
            ].sort_values("horizon")
            if line.empty:
                continue
            x = line["horizon"].to_numpy(float)
            axis.fill_between(
                x,
                line["conf_low"].to_numpy(float),
                line["conf_high"].to_numpy(float),
                color=color,
                alpha=0.15,
                linewidth=0,
            )
            axis.plot(x, line["estimate"], color=color, marker="o", markersize=2.5, label=label)
        axis.axhline(0, color="0.35", linewidth=0.7)
        axis.axvline(0, color="0.55", linewidth=0.7, linestyle="--")
        axis.set_title(OUTCOME_LABELS[outcome])
        axis.set_xlabel("Months since targeted")
        axis.set_ylabel("Percent")
        axis.grid(alpha=0.2)
    axes[0, 0].legend(frameon=False)
    fig.tight_layout()
    root = config.repo_root / "figs" / "extension_2025"
    root.mkdir(parents=True, exist_ok=True)
    outputs = []
    for suffix in (".pdf", ".png"):
        path = root / f"fk2025_{panel}{suffix}"
        fig.savefig(path, dpi=220 if suffix == ".png" else None)
        outputs.append(path)
    plt.close(fig)
    return outputs


def _plot_extended_figure4a(
    config: PipelineConfig,
    coefficients: pd.DataFrame,
    unavailable: dict[str, list[int]],
) -> list[Path]:
    series = [
        (
            "trade_war_2018",
            "2018–19 tariffs (through +24)",
            "#d97706",
        ),
        (
            "tariffs_2025",
            "2025 tariffs (right-censored)",
            "#2563eb",
        ),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=True)
    for axis, outcome in zip(axes.flat, OUTCOMES):
        for episode, label, color in series:
            line = coefficients.loc[
                coefficients["episode"].eq(episode)
                & coefficients["outcome"].eq(outcome)
            ].sort_values("horizon")
            x = line["horizon"].to_numpy(float)
            axis.fill_between(
                x,
                line["conf_low"].to_numpy(float),
                line["conf_high"].to_numpy(float),
                color=color,
                alpha=0.15,
                linewidth=0,
            )
            axis.plot(
                x,
                line["estimate"],
                color=color,
                marker="o",
                markersize=2.5,
                label=label,
            )
        axis.axhline(0, color="0.35", linewidth=0.7)
        axis.axvline(
            0,
            color="0.55",
            linewidth=0.7,
            linestyle="--",
        )
        axis.set_xlim(-6.5, 24.5)
        axis.set_title(OUTCOME_LABELS[outcome])
        axis.set_xlabel("Months since targeted")
        axis.set_ylabel("Percent")
        axis.grid(alpha=0.2)
    axes[0, 0].legend(frameon=False, fontsize=8)
    missing_2025 = unavailable.get("tariffs_2025", [])
    if missing_2025:
        first_missing = min(missing_2025)
        fig.text(
            0.5,
            0.015,
            (
                "The 2025 series stops before requested horizon "
                f"+{max(missing_2025)}: local data end in 2025-12 "
                f"and support is unavailable from +{first_missing}."
            ),
            ha="center",
            fontsize=8,
        )
    fig.tight_layout(rect=(0, 0.035, 1, 1))
    root = config.repo_root / "figs" / "extension_2025"
    root.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []
    for suffix in (".pdf", ".png"):
        path = root / f"fk2025_figure4a_extended{suffix}"
        fig.savefig(path, dpi=220 if suffix == ".png" else None)
        outputs.append(path)
    plt.close(fig)
    return outputs


def finalize_extended_event(
    config: PipelineConfig,
) -> dict[str, Any]:
    frames: list[pd.DataFrame] = []
    unavailable: dict[str, list[int]] = {}
    records: list[dict[str, Any]] = []
    for spec in extended_event_grid():
        source_hash = _extended_event_source_hash(
            config,
            spec.episode,
        )
        valid, reason = _valid_extended_curve(
            config,
            spec,
            source_hash,
        )
        if not valid:
            raise RuntimeError(
                f"Cannot finalize {spec.fit_id}: {reason}"
            )
        coefficients_path, _, manifest_path = _extended_curve_paths(
            config,
            spec,
        )
        frame = pd.read_parquet(coefficients_path)
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
        frames.append(frame)
        missing = [
            int(row["horizon"])
            for row in manifest["unavailable_horizons"]
        ]
        unavailable.setdefault(spec.episode, [])
        unavailable[spec.episode] = sorted(
            set(unavailable[spec.episode]) | set(missing)
        )
        records.append(
            {
                "fit_id": spec.fit_id,
                "completed_horizons": int(len(frame)),
                "last_completed_horizon": int(frame["horizon"].max()),
                "unavailable_horizons": missing,
                "manifest": _relative(config, manifest_path),
            }
        )
    coefficients = pd.concat(frames, ignore_index=True)
    root = (
        config.processed_trade_dir
        / "regressions"
        / "fk2025"
        / "event_horizon_extension"
    )
    coefficient_path = root / "event_coefficients.parquet"
    write_parquet(coefficients, coefficient_path, overwrite=True)
    figures = _plot_extended_figure4a(
        config,
        coefficients,
        unavailable,
    )
    manifest = {
        "version": VERSION,
        "status": "complete_with_disclosed_right_censoring",
        "requested_horizons": {
            episode: [
                -definition["pre"],
                definition["requested_post"],
            ]
            for episode, definition in
            EXTENDED_EVENT_EPISODES.items()
        },
        "source_end_periods": {
            episode: definition["sample_end"]
            for episode, definition in
            EXTENDED_EVENT_EPISODES.items()
        },
        "curves": records,
        "unavailable_horizons": unavailable,
        "coefficient_path": _relative(config, coefficient_path),
        "figures": [_relative(config, path) for path in figures],
    }
    write_metadata_json(root / "manifest.json", manifest)
    return manifest


def preflight(config: PipelineConfig) -> dict[str, Any]:
    trade_manifest = config.processed_trade_dir / "fk2025" / "trade_manifest.json"
    statutory_manifest = config.processed_tariff_dir / "fk2025" / "policy_extension_manifest.json"
    trade = json.loads(trade_manifest.read_text(encoding="utf-8")) if trade_manifest.exists() else {"status": "missing"}
    statutory = json.loads(statutory_manifest.read_text(encoding="utf-8")) if statutory_manifest.exists() else {"status": "missing"}
    result = {
        "version": VERSION,
        "trade_status": trade.get("status"),
        "applied_tariff_event_ready": trade.get("status") == "passed",
        "statutory_instrument_status": statutory.get("status"),
        "quarterly_iv_ready": trade.get("status") == "passed" and statutory.get("status", "").startswith("passed"),
        "expected_event_curves": len(event_grid()),
        "expected_event_horizon_fits": sum(len(spec.horizons) for spec in event_grid()),
        "superseded_method": "common_February_event_and_distributed_lag_statutory_shock",
    }
    root = config.processed_trade_dir / "regressions" / "fk2025"
    write_metadata_json(root / "preflight.json", result)
    return result


def run_fits(
    config: PipelineConfig,
    specs: Iterable[EventCurveSpec],
    *,
    resume: bool = True,
) -> dict[str, Any]:
    gate = preflight(config)
    if not gate["applied_tariff_event_ready"]:
        raise RuntimeError("FK-2025 event study requires the validated consumption/applied-tariff panel")
    selected = list(specs)
    records = []
    source_cache: dict[str, pd.DataFrame] = {}
    for spec in selected:
        source_hash = _event_source_hash(config, spec.episode)
        valid, _ = _valid_curve(config, spec, source_hash)
        if resume and valid:
            records.append({"fit_id": spec.fit_id, "status": "resumed"})
            continue
        if spec.episode not in source_cache:
            # Keep only one episode in memory. Outcomes within that episode
            # reuse the same projected source frame.
            source_cache.clear()
            source_cache[spec.episode] = load_event_source(config, spec.episode)
        records.append(
            fit_event_curve(
                config,
                spec,
                resume=resume,
                raw_source=source_cache[spec.episode],
            )
        )
    return {"version": VERSION, "fits": records}


def finalize(config: PipelineConfig) -> dict[str, Any]:
    frames = []
    invalid = []
    for spec in event_grid():
        valid, reason = _valid_curve(config, spec, _event_source_hash(config, spec.episode))
        if valid:
            frames.append(pd.read_parquet(_curve_paths(config, spec)[0]))
        else:
            invalid.append({"fit_id": spec.fit_id, "reason": reason})
    if invalid:
        raise RuntimeError(f"Cannot finalize: {len(invalid)} event curves are invalid")
    coefficients = pd.concat(frames, ignore_index=True)
    root = config.processed_trade_dir / "regressions" / "fk2025"
    coefficients_path = root / "event_coefficients.parquet"
    write_parquet(coefficients, coefficients_path, overwrite=True)
    figures = _plot_panel(config, coefficients, panel="figure4a")
    figures += _plot_panel(config, coefficients, panel="figure4b")
    landmarks = build_applied_tariff_landmarks(config)
    paper = run_quarterly_analysis(
        config, cutoff="2025-11", instrument_scope="paper_coverage"
    )
    december = run_quarterly_analysis(
        config, cutoff="2025-12", instrument_scope="paper_coverage"
    )
    deterministic = run_quarterly_analysis(
        config,
        cutoff="2025-11",
        include_robustness=False,
        instrument_scope="deterministic",
    )
    quarterly_passed = paper["status"] == "passed"
    manifest = {
        "version": VERSION,
        "status": (
            "complete_all_registered_gates_passed"
            if quarterly_passed
            else "complete_with_failed_quarterly_iv_gate"
        ),
        "event_study_status": "complete",
        "quarterly_iv_paper_gate": paper["status"],
        "event_curves": len(event_grid()),
        "event_horizon_fits": sum(len(spec.horizons) for spec in event_grid()),
        "event_coefficients": _relative(config, coefficients_path),
        "figures": [_relative(config, path) for path in figures],
        "applied_tariff_landmarks": landmarks,
        "paper_cutoff_quarterly": paper,
        "full_december_robustness": december,
        "deterministic_scope_robustness": deterministic,
        "methodology_target": "Fajgelbaum and Khandelwal, Tariffs in 2025, March 2026",
    }
    write_metadata_json(root / "manifest.json", manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--run-event", action="store_true")
    parser.add_argument("--run-event-extension", action="store_true")
    parser.add_argument("--run-iv", action="store_true")
    parser.add_argument("--run-cumulative-lp-iv", action="store_true")
    parser.add_argument(
        "--build-cumulative-lp-panels",
        action="store_true",
    )
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--finalize-event-extension", action="store_true")
    parser.add_argument(
        "--finalize-cumulative-lp-iv",
        action="store_true",
    )
    parser.add_argument("--episode", choices=(*EPISODES, "all"), default="all")
    parser.add_argument("--group", choices=(*GROUPS, "all-groups"), default="all-groups")
    parser.add_argument("--outcome", choices=(*OUTCOMES, "all"), default="all")
    parser.add_argument("--only-fit")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument(
        "--iv-cutoff",
        choices=("paper", "december", "both"),
        default="both",
    )
    parser.add_argument("--iv-baseline-only", action="store_true")
    parser.add_argument(
        "--iv-instrument-scope",
        choices=("paper_coverage", "deterministic", "both"),
        default="paper_coverage",
    )
    args = parser.parse_args(argv)
    config = PipelineConfig.default()
    if args.preflight_only or not (
        args.run_event
        or args.run_event_extension
        or args.run_iv
        or args.run_cumulative_lp_iv
        or args.build_cumulative_lp_panels
        or args.finalize_only
        or args.finalize_event_extension
        or args.finalize_cumulative_lp_iv
    ):
        print(json.dumps(preflight(config), indent=2))
        return 0
    if args.run_event:
        selected = event_grid()
        selected = [
            spec for spec in selected
            if (args.episode == "all" or spec.episode == args.episode)
            and (args.group == "all-groups" or spec.group == args.group)
            and (args.outcome == "all" or spec.outcome == args.outcome)
            and (args.only_fit is None or spec.fit_id == args.only_fit)
        ]
        print(json.dumps(run_fits(config, selected, resume=not args.no_resume), indent=2))
    if args.run_event_extension:
        selected_extended = [
            spec
            for spec in extended_event_grid()
            if (
                args.episode == "all"
                or spec.episode == args.episode
            )
            and (
                args.outcome == "all"
                or spec.outcome == args.outcome
            )
            and (
                args.only_fit is None
                or spec.fit_id == args.only_fit
            )
        ]
        print(
            json.dumps(
                run_extended_event_fits(
                    config,
                    selected_extended,
                    resume=not args.no_resume,
                ),
                indent=2,
            )
        )
    if args.run_iv:
        iv_results = {}
        scopes = (
            ("paper_coverage", "deterministic")
            if args.iv_instrument_scope == "both"
            else (args.iv_instrument_scope,)
        )
        for scope in scopes:
            if args.iv_cutoff in ("paper", "both"):
                iv_results[f"paper_cutoff_{scope}"] = run_quarterly_analysis(
                    config,
                    cutoff="2025-11",
                    include_robustness=not args.iv_baseline_only,
                    instrument_scope=scope,
                )
            if args.iv_cutoff in ("december", "both"):
                iv_results[f"full_december_{scope}"] = run_quarterly_analysis(
                    config,
                    cutoff="2025-12",
                    include_robustness=not args.iv_baseline_only,
                    instrument_scope=scope,
                )
        print(json.dumps(iv_results, indent=2, default=str))
    if (
        args.build_cumulative_lp_panels
        or args.run_cumulative_lp_iv
        or args.finalize_cumulative_lp_iv
    ):
        from .cumulative_lp_iv import (
            build_source_panels as build_cumulative_panels,
            finalize as finalize_cumulative_lp,
            grid as cumulative_grid,
            run_fits as run_cumulative_fits,
        )

        cumulative_episodes = (
            tuple(EPISODES)
            if args.episode == "all"
            else (args.episode,)
        )
        if args.build_cumulative_lp_panels:
            print(
                json.dumps(
                    build_cumulative_panels(
                        config,
                        cumulative_episodes,
                    ),
                    indent=2,
                )
            )
        if args.run_cumulative_lp_iv:
            selected_cumulative = [
                spec
                for spec in cumulative_grid(cumulative_episodes)
                if (
                    args.only_fit is None
                    or spec.fit_id == args.only_fit
                )
            ]
            print(
                json.dumps(
                    run_cumulative_fits(
                        config,
                        selected_cumulative,
                        resume=not args.no_resume,
                    ),
                    indent=2,
                )
            )
        if args.finalize_cumulative_lp_iv:
            print(
                json.dumps(
                    finalize_cumulative_lp(config),
                    indent=2,
                    default=str,
                )
            )
    if args.finalize_only:
        print(json.dumps(finalize(config), indent=2, default=str))
    if args.finalize_event_extension:
        print(
            json.dumps(
                finalize_extended_event(config),
                indent=2,
                default=str,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
