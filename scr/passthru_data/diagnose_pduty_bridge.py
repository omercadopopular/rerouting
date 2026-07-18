"""Diagnose and re-estimate the duty-inclusive-price bridge with realized duty.

The paper's package defines ``m_pduty`` as ``(value + duty) / quantity``.
This diagnostic keeps the package treatment design fixed but reconstructs the
outcome from the archive-native Census ``cal_dut_mo`` field.  It deliberately
does not alter the independent statutory-policy reconstruction.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import argparse
import gc
import hashlib
import json
import uuid

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .bridge_diagnostics import ci_overlap, curve_metrics
from .bridge_runner import estimator_fingerprint
from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
from .trade_regressions import (
    _prepare_dynamic,
    _prepare_event_study,
    _run_dynamic_one,
    _run_event_study_one,
)


VERSION = "pduty_realized_duty_diagnostic_v1"
SPECS = ("event", "dynamic")


def duty_inclusive_unit_value(
    trade_value: pd.Series,
    quantity: pd.Series,
    calculated_duty: pd.Series,
) -> pd.Series:
    """Return ``(value + calculated duty) / quantity`` without zero filling."""
    value = pd.to_numeric(trade_value, errors="coerce")
    qty = pd.to_numeric(quantity, errors="coerce")
    duty = pd.to_numeric(calculated_duty, errors="coerce")
    valid = value.gt(0) & qty.gt(0) & duty.notna() & duty.ge(0)
    return ((value + duty) / qty).where(valid)


def _root(config: PipelineConfig) -> Path:
    path = (
        config.verification_dir
        / "trade_regressions"
        / "package_benchmark_v5"
        / "common_sample_v3"
        / "pduty_diagnosis"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _sql_path(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _source_paths(config: PipelineConfig) -> tuple[Path, str, Path]:
    benchmark = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    package = benchmark / "common_sample_v3" / "package_common_sample_anchor.parquet"
    extension_glob = _sql_path(
        config.analysis_dir / "extension_v2" / "flow=imports" / "year=*" / "month=*" / "part.parquet"
    )
    extension_manifest = config.verification_dir / "extension_v2" / "extension_build_manifest.json"
    if not package.exists():
        raise FileNotFoundError(package)
    if not extension_manifest.exists():
        raise FileNotFoundError(extension_manifest)
    return package, extension_glob, extension_manifest


def _build_actual_duty_panel(config: PipelineConfig, *, overwrite: bool) -> tuple[Path, dict[str, Any]]:
    package, extension_glob, extension_manifest = _source_paths(config)
    destination = _root(config) / "raw_outcomes_realized_duty_package_policy.parquet"
    manifest_path = _root(config) / "raw_outcomes_realized_duty_manifest.json"
    source_signature = hashlib.sha256(
        (sha256_file(package) + sha256_file(extension_manifest) + VERSION).encode("utf-8")
    ).hexdigest()
    if destination.exists() and manifest_path.exists() and not overwrite:
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if prior.get("source_signature") == source_signature and prior.get("status") == "complete":
            return destination, prior

    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    temporary.unlink(missing_ok=True)
    package_sql = _sql_path(package)
    query = f"""
        SELECT
            p.id, p.cty_code, p.cty_name, p.hs10, p.hs8, p.hs6, p.hs4, p.hs2,
            p.year, p.month, p.mdate, p.m_effective_mdate2, p.m_stattariff2,
            p.m_status2, p.m_ess, p.naics_str,
            CASE WHEN p.m_val > 0 AND r.trade_value > 0
                 THEN r.trade_value / 1000000.0 END AS m_val,
            CASE WHEN p.m_q1 > 0 AND r.quantity > 0
                 THEN r.quantity / 1000000.0 END AS m_q1,
            CASE WHEN p.m_p > 0 AND r.trade_value > 0 AND r.quantity > 0
                 THEN r.trade_value::DOUBLE / r.quantity END AS m_p,
            CASE WHEN p.m_pduty > 0 AND r.trade_value > 0 AND r.quantity > 0
                      AND r.cal_dut_mo IS NOT NULL AND r.cal_dut_mo >= 0
                 THEN (r.trade_value::DOUBLE + r.cal_dut_mo) / r.quantity END AS m_pduty,
            CASE WHEN p.m_pduty > 0 AND r.trade_value > 0 AND r.quantity > 0
                      AND p.m_stattariff2 IS NOT NULL
                 THEN (r.trade_value::DOUBLE / r.quantity) * (1 + p.m_stattariff2)
                 END AS m_pduty_statutory_diagnostic,
            r.cal_dut_mo AS raw_calculated_duty,
            r.dut_val_mo AS raw_dutiable_value
        FROM read_parquet('{package_sql}') p
        INNER JOIN read_parquet('{extension_glob}', hive_partitioning=false) r
          ON p.cty_code = try_cast(r.partner_code AS BIGINT)
         AND p.hs10 = r.hs10
         AND p.year = r.year
         AND p.month = r.month
    """
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(
            f"COPY ({query}) TO '{_sql_path(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row_count, valid_pduty, distinct_keys = con.execute(
            f"""
            SELECT count(*), count(m_pduty),
                   count(DISTINCT (cty_code, hs10, year, month))
            FROM read_parquet('{_sql_path(temporary)}')
            """
        ).fetchone()
        package_rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{package_sql}')"
        ).fetchone()[0]
        compression = {
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT compression FROM parquet_metadata('{_sql_path(temporary)}')"
            ).fetchall()
        }
        if int(row_count) != int(package_rows) or compression != {"ZSTD"}:
            raise RuntimeError(
                f"Invalid realized-duty panel: rows={row_count}/{package_rows}, compression={compression}"
            )
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        con.close()

    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_mode": "raw_outcomes_realized_duty_package_policy",
        "package_design_path": _relative(config, package),
        "package_design_sha256": sha256_file(package),
        "extension_manifest_path": _relative(config, extension_manifest),
        "extension_manifest_sha256": sha256_file(extension_manifest),
        "source_signature": source_signature,
        "output_path": _relative(config, destination),
        "output_sha256": sha256_file(destination),
        "rows": int(row_count),
        "distinct_keys": int(distinct_keys),
        "valid_realized_duty_outcomes": int(valid_pduty),
        "outcome_formula": "(trade_value + cal_dut_mo) / quantity",
        "dut_val_mo_role": "dutiable_value_not_duty_collected",
        "policy_source": "package treatment and statutory regressor only",
        "independent_policy_semantics_changed": False,
        "status": "complete",
    }
    write_metadata_json(manifest_path, manifest)
    return destination, manifest


def _fit_manifest_valid(directory: Path, panel_hash: str, estimator_hash: str) -> bool:
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        coefficients = read_table(directory / "coefficients.parquet")
        horizon = "event_time" if "event_time" in coefficients else "horizon"
        return (
            manifest.get("version") == VERSION
            and manifest.get("panel_sha256") == panel_hash
            and manifest.get("estimator_fingerprint") == estimator_hash
            and manifest.get("status") == "complete"
            and len(coefficients) == 13
            and coefficients[horizon].nunique() == 13
        )
    except Exception:
        return False


def _run_fit(
    config: PipelineConfig,
    panel_path: Path,
    spec: str,
    *,
    resume: bool,
) -> Path:
    directory = _root(config) / "fits" / spec
    directory.mkdir(parents=True, exist_ok=True)
    panel_hash = sha256_file(panel_path)
    estimator_hash = estimator_fingerprint()
    if resume and _fit_manifest_valid(directory, panel_hash, estimator_hash):
        return directory / "coefficients.parquet"
    frame = read_table(panel_path)
    prepared = _prepare_event_study("imports", frame) if spec == "event" else _prepare_dynamic("imports", frame)
    marker = _root(config) / "current_fit.json"
    write_metadata_json(
        marker,
        {
            "version": VERSION,
            "fit_id": f"raw_outcomes_realized_duty_package_policy|{spec}|pduty",
            "rows": int(len(prepared)),
            "estimated_memory_bytes": int(len(prepared) * 16 * 8),
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    try:
        result = (
            _run_event_study_one(
                config,
                "imports",
                "pduty",
                prepared,
                "raw_outcomes_realized_duty_package_policy",
                _relative(config, panel_path),
            )
            if spec == "event"
            else _run_dynamic_one(
                config,
                "imports",
                "pduty",
                prepared,
                "raw_outcomes_realized_duty_package_policy",
                _relative(config, panel_path),
            )
        )
        coefficient_path = directory / "coefficients.parquet"
        write_parquet(result.frame, coefficient_path, overwrite=True)
        write_parquet(
            pd.DataFrame(
                [
                    {
                        "spec": spec,
                        "outcome": "pduty",
                        "nobs": int(result.nobs),
                        "panel_sha256": panel_hash,
                    }
                ]
            ),
            directory / "sample_audit.parquet",
            overwrite=True,
        )
        write_metadata_json(
            directory / "manifest.json",
            {
                "version": VERSION,
                "fit_id": f"raw_outcomes_realized_duty_package_policy|{spec}|pduty",
                "panel_path": _relative(config, panel_path),
                "panel_sha256": panel_hash,
                "estimator_fingerprint": estimator_hash,
                "nobs": int(result.nobs),
                "horizons": 13,
                "outcome_formula": "(trade_value + cal_dut_mo) / quantity",
                "status": "complete",
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )
        if not _fit_manifest_valid(directory, panel_hash, estimator_hash):
            raise RuntimeError(f"Realized-duty checkpoint validation failed: {spec}")
        marker.unlink(missing_ok=True)
        return coefficient_path
    except Exception as exc:
        write_metadata_json(
            directory / "failure.json",
            {
                "version": VERSION,
                "spec": spec,
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "failed_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )
        raise
    finally:
        del prepared, frame
        gc.collect()


def _formula_audits(config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    package, extension_glob, _ = _source_paths(config)
    package_sql = _sql_path(package)
    con = duckdb.connect(database=":memory:")
    try:
        joined = f"""
            FROM read_parquet('{package_sql}') p
            INNER JOIN read_parquet('{extension_glob}', hive_partitioning=false) r
              ON p.cty_code = try_cast(r.partner_code AS BIGINT)
             AND p.hs10 = r.hs10 AND p.year = r.year AND p.month = r.month
            WHERE p.m_p > 0 AND p.m_pduty > 0 AND r.trade_value > 0
              AND r.quantity > 0 AND r.cal_dut_mo IS NOT NULL
              AND r.cal_dut_mo >= 0 AND p.m_stattariff2 IS NOT NULL
        """
        formula = con.execute(
            f"""
            WITH j AS (
                SELECT p.m_p, p.m_pduty, p.m_stattariff2,
                       r.trade_value::DOUBLE / r.quantity AS raw_p,
                       (r.trade_value::DOUBLE + r.cal_dut_mo) / r.quantity AS actual_pduty,
                       (r.trade_value::DOUBLE / r.quantity) * (1 + p.m_stattariff2) AS statutory_pduty,
                       r.cal_dut_mo::DOUBLE / r.trade_value AS raw_calculated_rate
                {joined}
            ), stacked AS (
                SELECT 'realized_calculated_duty' AS construction,
                       ln(m_pduty) AS package_log, ln(actual_pduty) AS raw_log FROM j
                UNION ALL
                SELECT 'statutory_rate_multiplier', ln(m_pduty), ln(statutory_pduty) FROM j
            )
            SELECT construction, count(*) AS rows,
                   corr(package_log, raw_log) AS log_correlation,
                   avg(abs(package_log - raw_log)) * 100 AS mean_abs_log_gap,
                   quantile_cont(abs(package_log - raw_log) * 100, 0.5) AS p50_abs_log_gap,
                   quantile_cont(abs(package_log - raw_log) * 100, 0.9) AS p90_abs_log_gap,
                   quantile_cont(abs(package_log - raw_log) * 100, 0.99) AS p99_abs_log_gap
            FROM stacked GROUP BY construction ORDER BY construction
            """
        ).fetchdf()
        horizon = con.execute(
            f"""
            WITH p0 AS (
                SELECT p.*,
                       max(p.m_status2) OVER (PARTITION BY p.id) AS esstatus
                FROM read_parquet('{package_sql}') p
            ), j AS (
                SELECT greatest(-6, least(6, date_diff('month', p.m_effective_mdate2, p.mdate))) AS horizon,
                       100 * (ln(1 + r.cal_dut_mo::DOUBLE / r.trade_value)
                              - ln(1 + p.m_stattariff2)) AS actual_minus_statutory,
                       100 * (ln(p.m_pduty / p.m_p)
                              - ln(1 + p.m_stattariff2)) AS package_applied_minus_statutory,
                       100 * (ln(p.m_pduty / p.m_p)
                              - ln(1 + r.cal_dut_mo::DOUBLE / r.trade_value)) AS package_minus_raw_duty_factor
                FROM p0 p
                INNER JOIN read_parquet('{extension_glob}', hive_partitioning=false) r
                  ON p.cty_code = try_cast(r.partner_code AS BIGINT)
                 AND p.hs10 = r.hs10 AND p.year = r.year AND p.month = r.month
                WHERE p.esstatus = 2 AND p.m_effective_mdate2 IS NOT NULL
                  AND p.m_p > 0 AND p.m_pduty > 0 AND p.m_stattariff2 IS NOT NULL
                  AND r.trade_value > 0 AND r.quantity > 0
                  AND r.cal_dut_mo IS NOT NULL AND r.cal_dut_mo >= 0
            )
            SELECT horizon, count(*) AS rows,
                   avg(actual_minus_statutory) AS mean_actual_minus_statutory,
                   avg(package_applied_minus_statutory) AS mean_package_applied_minus_statutory,
                   avg(package_minus_raw_duty_factor) AS mean_package_minus_raw_duty_factor,
                   quantile_cont(actual_minus_statutory, 0.1) AS p10_actual_minus_statutory,
                   quantile_cont(actual_minus_statutory, 0.5) AS p50_actual_minus_statutory,
                   quantile_cont(actual_minus_statutory, 0.9) AS p90_actual_minus_statutory
            FROM j GROUP BY horizon ORDER BY horizon
            """
        ).fetchdf()
    finally:
        con.close()
    return formula, horizon


def _rename_horizon(frame: pd.DataFrame) -> pd.DataFrame:
    if "event_time" in frame.columns:
        return frame.rename(columns={"event_time": "horizon"})
    return frame.copy()


def _finalize(config: PipelineConfig, fits: dict[str, Path]) -> dict[str, Any]:
    root = _root(config)
    benchmark = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    bridge = benchmark / "common_sample_v3" / "bridge_resumable"
    comparison = read_table(benchmark / "package_pdf_comparison.parquet")
    metrics_rows: list[dict[str, Any]] = []
    point_rows: list[pd.DataFrame] = []
    component_rows: list[pd.DataFrame] = []
    plotted: dict[str, dict[str, pd.DataFrame]] = {}
    for spec in SPECS:
        package = _rename_horizon(
            read_table(bridge / "package_common_sample_anchor" / spec / "pduty" / "coefficients.parquet")
        )
        current = _rename_horizon(
            read_table(bridge / "raw_outcomes_package_policy" / spec / "pduty" / "coefficients.parquet")
        )
        actual = _rename_horizon(read_table(fits[spec]))
        package_price = _rename_horizon(
            read_table(bridge / "package_common_sample_anchor" / spec / "p" / "coefficients.parquet")
        )
        raw_price = _rename_horizon(
            read_table(bridge / "raw_outcomes_package_policy" / spec / "p" / "coefficients.parquet")
        )
        plotted[spec] = {"package": package, "current": current, "actual": actual}
        for label, raw in (
            ("statutory_multiplier", current),
            ("realized_calculated_duty", actual),
        ):
            merged = package.merge(raw, on="horizon", suffixes=("_package", "_raw"), validate="one_to_one")
            metrics_rows.append(
                {
                    "spec": spec,
                    "raw_pduty_construction": label,
                    **curve_metrics(merged, exclude_baseline=False),
                }
            )
            point = merged[
                [
                    "horizon",
                    "estimate_package",
                    "std_error_package",
                    "conf_low_package",
                    "conf_high_package",
                    "estimate_raw",
                    "std_error_raw",
                    "conf_low_raw",
                    "conf_high_raw",
                ]
            ].copy()
            point.insert(0, "raw_pduty_construction", label)
            point.insert(0, "spec", spec)
            point["difference"] = point["estimate_package"] - point["estimate_raw"]
            point_rows.append(point)
        components = (
            package[["horizon", "estimate", "conf_low", "conf_high"]]
            .rename(
                columns={
                    "estimate": "package_pduty",
                    "conf_low": "package_pduty_conf_low",
                    "conf_high": "package_pduty_conf_high",
                }
            )
            .merge(
                actual[["horizon", "estimate", "conf_low", "conf_high"]].rename(
                    columns={
                        "estimate": "raw_actual_pduty",
                        "conf_low": "raw_actual_pduty_conf_low",
                        "conf_high": "raw_actual_pduty_conf_high",
                    }
                ),
                on="horizon",
                validate="one_to_one",
            )
            .merge(
                package_price[["horizon", "estimate"]].rename(columns={"estimate": "package_pre_duty_price"}),
                on="horizon",
                validate="one_to_one",
            )
            .merge(
                raw_price[["horizon", "estimate"]].rename(columns={"estimate": "raw_pre_duty_price"}),
                on="horizon",
                validate="one_to_one",
            )
        )
        components.insert(0, "spec", spec)
        components["pduty_gap"] = components["package_pduty"] - components["raw_actual_pduty"]
        components["pre_duty_price_gap"] = (
            components["package_pre_duty_price"] - components["raw_pre_duty_price"]
        )
        components["realized_duty_factor_gap"] = (
            (components["package_pduty"] - components["package_pre_duty_price"])
            - (components["raw_actual_pduty"] - components["raw_pre_duty_price"])
        )
        components["decomposition_residual"] = (
            components["pduty_gap"]
            - components["pre_duty_price_gap"]
            - components["realized_duty_factor_gap"]
        )
        components["ci_overlap"] = [
            ci_overlap(
                row.package_pduty_conf_low,
                row.package_pduty_conf_high,
                row.raw_actual_pduty_conf_low,
                row.raw_actual_pduty_conf_high,
                baseline=bool(row.horizon == -6),
            )
            for row in components.itertuples()
        ]
        component_rows.append(components)
    metrics = pd.DataFrame(metrics_rows)
    points = pd.concat(point_rows, ignore_index=True)
    components = pd.concat(component_rows, ignore_index=True)
    write_parquet(metrics, root / "pduty_curve_metrics.parquet", overwrite=True)
    metrics.to_csv(root / "pduty_curve_metrics.csv", index=False)
    write_parquet(points, root / "pduty_curve_comparison.parquet", overwrite=True)
    write_parquet(components, root / "pduty_curve_component_decomposition.parquet", overwrite=True)
    components.to_csv(root / "pduty_curve_component_decomposition.csv", index=False)

    formula, horizon = _formula_audits(config)
    write_parquet(formula, root / "pduty_formula_audit.parquet", overwrite=True)
    formula.to_csv(root / "pduty_formula_audit.csv", index=False)
    write_parquet(horizon, root / "pduty_tariff_component_by_horizon.parquet", overwrite=True)
    horizon.to_csv(root / "pduty_tariff_component_by_horizon.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.7), sharex=True)
    for axis, spec in zip(axes, SPECS):
        reference = comparison.loc[
            comparison["spec"].eq(spec) & comparison["outcome"].eq("pduty")
        ].sort_values("horizon")
        axis.plot(reference["horizon"], reference["reference_value"], "o-", color="black", label="Paper PDF reference")
        axis.plot(reference["horizon"], reference["estimate"], "--", color="#3165a8", label="Package-only replication")
        axis.plot(plotted[spec]["package"]["horizon"], plotted[spec]["package"]["estimate"], ":", color="#2b8c6b", label="Package common sample")
        axis.plot(plotted[spec]["current"]["horizon"], plotted[spec]["current"]["estimate"], "s-", color="#d35f00", label="Raw × statutory rate (old)")
        axis.plot(plotted[spec]["actual"]["horizon"], plotted[spec]["actual"]["estimate"], "D-", color="#7b3294", label="Raw + calculated duty (corrected)")
        axis.axhline(0, color="0.25", linewidth=0.7)
        axis.axvline(0, color="0.55", linewidth=0.8, linestyle="--")
        axis.grid(axis="y", color="0.9", linewidth=0.6)
        axis.set_title("Figure 2 event" if spec == "event" else "Figure 4a dynamic")
        axis.set_xlabel("Event horizon (months)")
        axis.set_ylabel("Duty-inclusive price coefficient (log points)")
        axis.set_xticks(range(-6, 7, 2))
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.98), fontsize=8.5)
    fig.suptitle("Duty-inclusive price: statutory multiplier versus realized calculated duty", y=1.06, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.84))
    figure = root / "pduty_event_study_zoom.png"
    fig.savefig(figure, dpi=220, bbox_inches="tight")
    fig.savefig(figure.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)

    report = [
        "# Duty-inclusive-price bridge diagnosis",
        "",
        "The paper package defines `m_pduty` as `(value + duty) / quantity`. The prior raw bridge instead multiplied unit value by the package statutory rate. Census `dut_val_mo` is dutiable value; `cal_dut_mo` is calculated duty and is the relevant numerator component.",
        "",
        "## Row-level formula audit",
        "",
        formula.to_markdown(index=False),
        "",
        "## Event-study metrics",
        "",
        metrics.to_markdown(index=False),
        "",
        "## Coefficient-gap decomposition",
        "",
        "The duty-inclusive gap is decomposed exactly into the pre-duty-price gap and the realized-duty-factor gap on the common estimator sample.",
        "",
        components.to_markdown(index=False),
        "",
        "## Treated-product tariff component by horizon",
        "",
        horizon.to_markdown(index=False),
        "",
        "This diagnostic holds package treatment timing and policy regressors fixed. It does not validate or change the independent legal-policy reconstruction.",
    ]
    (root / "pduty_diagnosis_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    status = "complete"
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "outcome_formula": "(trade_value + cal_dut_mo) / quantity",
        "metrics_path": _relative(config, root / "pduty_curve_metrics.parquet"),
        "pointwise_comparison_path": _relative(config, root / "pduty_curve_comparison.parquet"),
        "component_decomposition_path": _relative(config, root / "pduty_curve_component_decomposition.parquet"),
        "formula_audit_path": _relative(config, root / "pduty_formula_audit.parquet"),
        "horizon_audit_path": _relative(config, root / "pduty_tariff_component_by_horizon.parquet"),
        "figure_png": _relative(config, figure),
        "figure_pdf": _relative(config, figure.with_suffix(".pdf")),
        "report_path": _relative(config, root / "pduty_diagnosis_report.md"),
        "independent_policy_semantics_changed": False,
    }
    write_metadata_json(root / "pduty_diagnosis_manifest.json", manifest)
    return manifest


def run_pduty_diagnosis(
    config: PipelineConfig,
    *,
    specs: Iterable[str] = SPECS,
    overwrite_panel: bool = False,
    resume: bool = True,
) -> dict[str, Any]:
    requested = tuple(specs)
    invalid = set(requested) - set(SPECS)
    if invalid:
        raise ValueError(f"Unknown specifications: {sorted(invalid)}")
    panel, panel_manifest = _build_actual_duty_panel(config, overwrite=overwrite_panel)
    fit_paths = {spec: _run_fit(config, panel, spec, resume=resume) for spec in requested}
    if set(fit_paths) != set(SPECS):
        return {
            "version": VERSION,
            "status": "partial",
            "panel_manifest": panel_manifest,
            "completed_specs": sorted(fit_paths),
        }
    return _finalize(config, fit_paths)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", choices=("event", "dynamic", "all"), default="all")
    parser.add_argument("--overwrite-panel", action="store_true")
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args()
    specs = SPECS if args.spec == "all" else (args.spec,)
    print(
        run_pduty_diagnosis(
            PipelineConfig.default(),
            specs=specs,
            overwrite_panel=args.overwrite_panel,
            resume=not args.no_resume,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
