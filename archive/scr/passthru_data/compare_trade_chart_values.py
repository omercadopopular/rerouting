"""Extract reference chart values from package PDFs and compare to generated tables."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
try:
    from pypdf import PdfReader
    from pypdf.generic import ContentStream
except ModuleNotFoundError:
    PdfReader = None
    ContentStream = None

CURRENT_DIR = Path(__file__).resolve().parent
SCR_DIR = CURRENT_DIR.parent
if str(SCR_DIR) not in sys.path:
    sys.path.insert(0, str(SCR_DIR))

from passthru_data.config import PipelineConfig
from passthru_data.io_utils import ensure_dir

EVENT_TITLES = {
    "Log Value": "val",
    "Log Quantity": "q1",
    "Log Unit Value": "p",
    "Log Duty-Inclusive Unit Value": "pduty",
}

DYNAMIC_TITLES = EVENT_TITLES.copy()

EVENT_REF_FIGURES = {
    "imports": "fig_02.pdf",
    "exports": "fig_03.pdf",
}

RGB_EVENT_MARKER = (1.0, 0.0, 0.0)
RGB_EVENT_CI = (0.37647, 0.37647, 0.37647)
RGB_EVENT_ZERO = (0.33725, 0.70588, 0.91373)
RGB_DYNAMIC_SERIES = (0.10196, 0.27843, 0.43529)
RGB_DYNAMIC_CI = (0.11765, 0.17647, 0.32549)
RGB_DYNAMIC_ZERO = (0.6902, 0.6902, 0.6902)


@dataclass(frozen=True)
class TextItem:
    text: str
    x: float
    y: float
    font_size: float


@dataclass(frozen=True)
class PathItem:
    stroke: tuple[float, ...] | None
    fill: tuple[float, ...] | None
    width: tuple[float, ...] | None
    paint: str
    ops: list[tuple[str, list[float]]]


def _extract_text_items(page) -> list[TextItem]:
    items: list[TextItem] = []

    def visitor_text(text, cm, tm, font_dict, font_size) -> None:  # type: ignore[no-untyped-def]
        if text.strip():
            items.append(TextItem(text.strip(), float(tm[4]), float(tm[5]), float(font_size)))

    page.extract_text(visitor_text=visitor_text)
    return items


def _extract_page_paths(reader: PdfReader, page) -> list[PathItem]:
    if PdfReader is None or ContentStream is None:
        raise RuntimeError("PDF extraction blocked: pypdf is not installed locally")
    content = ContentStream(page.get_contents(), reader)
    state: dict[str, tuple[float, ...] | None] = {"RG": None, "rg": None, "w": None}
    path_ops: list[tuple[str, list[float]]] = []
    paths: list[PathItem] = []
    for operands, operator in content.operations:
        op = operator.decode("latin1")
        if op in {"RG", "rg", "w"}:
            state[op] = tuple(float(x) for x in operands)
            continue
        if op in {"m", "l", "c", "re", "h"}:
            path_ops.append((op, [float(x) for x in operands]))
            continue
        if op in {"S", "f", "B", "b", "s", "n", "f*", "B*", "b*"}:
            if path_ops:
                paths.append(
                    PathItem(
                        stroke=state["RG"],
                        fill=state["rg"],
                        width=state["w"],
                        paint=op,
                        ops=path_ops.copy(),
                    )
                )
            path_ops = []
    return paths


def _title_positions(items: list[TextItem], titles: dict[str, str]) -> pd.DataFrame:
    rows = []
    for item in items:
        if item.text in titles:
            rows.append({"title": item.text, "outcome": titles[item.text], "x": item.x, "y": item.y})
    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("No subplot titles found in reference figure.")
    result["col"] = np.where(result["x"] < result["x"].median(), "left", "right")
    result["row"] = np.where(result["y"] < result["y"].median(), "bottom", "top")
    return result


def _subplot_frame_from_titles(titles: pd.DataFrame, outcome: str) -> dict[str, str]:
    row = titles.loc[titles["outcome"] == outcome]
    if row.empty:
        raise ValueError(f"Missing title for outcome {outcome}.")
    return {"row": str(row.iloc[0]["row"]), "col": str(row.iloc[0]["col"])}


def _largest_gap_midpoint(values: pd.Series) -> float:
    unique = sorted(float(v) for v in values.dropna().unique())
    if len(unique) < 2:
        raise ValueError("Need at least two distinct values to compute a quadrant split.")
    gaps = [(unique[idx + 1] - unique[idx], idx) for idx in range(len(unique) - 1)]
    _, gap_idx = max(gaps, key=lambda item: item[0])
    return (unique[gap_idx] + unique[gap_idx + 1]) / 2.0


def _assign_quadrant(df: pd.DataFrame, x_split: float, y_split: float) -> pd.DataFrame:
    out = df.copy()
    out["col"] = np.where(out["x"] < x_split, "left", "right")
    out["row"] = np.where(out["y"] < y_split, "bottom", "top")
    return out


def _circle_center(path: PathItem) -> tuple[float, float] | None:
    if len(path.ops) != 5 or not all(op in {"m", "c"} for op, _ in path.ops):
        return None
    xs: list[float] = []
    ys: list[float] = []
    for _, vals in path.ops:
        xs.extend(vals[0::2])
        ys.extend(vals[1::2])
    return ((min(xs) + max(xs)) / 2.0, (min(ys) + max(ys)) / 2.0)


def _line_segment(path: PathItem) -> tuple[float, float, float, float] | None:
    if len(path.ops) != 2 or path.ops[0][0] != "m" or path.ops[1][0] != "l":
        return None
    x0, y0 = path.ops[0][1]
    x1, y1 = path.ops[1][1]
    return (x0, y0, x1, y1)


def _polyline_points(path: PathItem) -> list[tuple[float, float]] | None:
    if len(path.ops) != 13 or path.ops[0][0] != "m" or not all(op == "l" for op, _ in path.ops[1:]):
        return None
    points = [(path.ops[0][1][0], path.ops[0][1][1])]
    points.extend((vals[0], vals[1]) for _, vals in path.ops[1:])
    return points


def _y_axis_slope(y_labels: pd.DataFrame) -> float:
    if len(y_labels) < 2:
        raise ValueError("Need at least two y-axis labels to recover scale.")
    coeff = np.polyfit(y_labels["y"].to_numpy(), y_labels["value"].to_numpy(), 1)
    return float(coeff[0])


def _numeric_y_labels(text_df: pd.DataFrame, subplot: dict[str, str], x_left: float, point_ys: list[float]) -> pd.DataFrame:
    """Select numeric labels geometrically inside the subplot y-span."""
    y_min = min(point_ys) - 15.0
    y_max = max(point_ys) + 15.0
    candidates = text_df.loc[
        (text_df["row"] == subplot["row"])
        & (text_df["col"] == subplot["col"])
        & (text_df["x"] < float(x_left) - 40.0)
        & (text_df["y"] >= y_min)
        & (text_df["y"] <= y_max)
    ].copy()
    rows = []
    for _, item in candidates.iterrows():
        try:
            rows.append({"value": float(item["text"]), "y": float(item["y"])})
        except (TypeError, ValueError):
            continue
    labels = pd.DataFrame(rows).drop_duplicates()
    if len(labels) < 2:
        raise ValueError("Could not identify at least two geometric y-axis tick labels")
    return labels.sort_values("y").reset_index(drop=True)


def _axis_fit(y_labels: pd.DataFrame) -> tuple[float, float, float, float]:
    """Return slope, intercept, R2, and maximum tick residual."""
    x = y_labels["y"].to_numpy(dtype=float)
    y = y_labels["value"].to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    fitted = slope * x + intercept
    residual = y - fitted
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 if ss_tot == 0 else 1.0 - float((residual ** 2).sum()) / ss_tot
    return float(slope), float(intercept), float(r2), float(np.max(np.abs(residual)))

def _interp_y(points: list[tuple[float, float]], x_value: float) -> float:
    points = sorted(points)
    xs = np.array([point[0] for point in points], dtype=float)
    ys = np.array([point[1] for point in points], dtype=float)
    return float(np.interp(x_value, xs, ys))


def _segment_midpoint(segment: tuple[float, float, float, float]) -> tuple[float, float]:
    x0, y0, x1, y1 = segment
    return ((x0 + x1) / 2.0, (y0 + y1) / 2.0)


def _band_values_from_segments(
    segments: list[tuple[float, float, float, float]],
    x_targets: list[float],
) -> list[float]:
    endpoints = []
    for x0, y0, x1, y1 in segments:
        endpoints.append((x0, y0))
        endpoints.append((x1, y1))
    band_df = pd.DataFrame(endpoints, columns=["x", "y"]).groupby("x", as_index=False)["y"].mean().sort_values("x")
    xs = band_df["x"].to_numpy(dtype=float)
    ys = band_df["y"].to_numpy(dtype=float)
    return list(np.interp(np.array(x_targets, dtype=float), xs, ys))


def _extract_event_reference(config: PipelineConfig, flow: str) -> pd.DataFrame:
    fig_path = config.fajgelbaum_root / "results" / "main" / EVENT_REF_FIGURES[flow]
    reader = PdfReader(str(fig_path))
    page = reader.pages[0]
    text_items = _extract_text_items(page)
    paths = _extract_page_paths(reader, page)
    titles = _title_positions(text_items, EVENT_TITLES)

    circles = []
    for path in paths:
        if path.stroke == RGB_EVENT_MARKER:
            center = _circle_center(path)
            if center:
                circles.append({"x": center[0], "y": center[1]})
    circles_raw = pd.DataFrame(circles)
    x_split = _largest_gap_midpoint(circles_raw["x"])
    y_split = _largest_gap_midpoint(circles_raw["y"])
    circles_df = _assign_quadrant(circles_raw, x_split, y_split)
    titles = _assign_quadrant(titles.drop(columns=["col", "row"], errors="ignore"), x_split, y_split)

    zero_rows = []
    for path in paths:
        if path.stroke == RGB_EVENT_ZERO:
            seg = _line_segment(path)
            if seg and abs(seg[1] - seg[3]) < 1e-6:
                x0, y0, x1, _ = seg
                zero_rows.append({"x": (x0 + x1) / 2.0, "y": y0})
    zero_df = _assign_quadrant(pd.DataFrame(zero_rows), x_split, y_split)

    ci_rows = []
    for path in paths:
        if path.stroke == RGB_EVENT_CI:
            seg = _line_segment(path)
            if seg and abs(seg[0] - seg[2]) < 1e-6:
                x0, y0, _, y1 = seg
                y_low = min(y0, y1)
                y_high = max(y0, y1)
                ci_rows.append({"x": x0, "y": (y_low + y_high) / 2.0, "y_low": y_low, "y_high": y_high})
    ci_df = _assign_quadrant(pd.DataFrame(ci_rows), x_split, y_split)

    text_df = _assign_quadrant(pd.DataFrame([item.__dict__ for item in text_items]), x_split, y_split)
    rows: list[dict[str, Any]] = []
    for outcome in EVENT_TITLES.values():
        subplot = _subplot_frame_from_titles(titles, outcome)
        pts = circles_df.loc[(circles_df["row"] == subplot["row"]) & (circles_df["col"] == subplot["col"])].sort_values("x").reset_index(drop=True)
        if len(pts) != 13:
            raise ValueError(f"Expected 13 event-study points for {flow} {outcome}, found {len(pts)}.")
        cis = ci_df.loc[(ci_df["row"] == subplot["row"]) & (ci_df["col"] == subplot["col"])].sort_values("x").reset_index(drop=True)
        if len(cis) != 13:
            raise ValueError(f"Expected 13 event-study CI spikes for {flow} {outcome}, found {len(cis)}.")
        y_labels = _numeric_y_labels(
            text_df, subplot, float(pts["x"].min()), pts["y"].astype(float).tolist()
        )
        slope, intercept, axis_r2, axis_residual = _axis_fit(y_labels)
        if axis_r2 < 0.999999 or axis_residual > 0.01:
            raise ValueError(f"Invalid event PDF axis calibration: R2={axis_r2}, residual={axis_residual}")
        zero_y = float(zero_df.loc[(zero_df["row"] == subplot["row"]) & (zero_df["col"] == subplot["col"]), "y"].mode().iloc[0])
        for idx, point in pts.iterrows():
            ci_row = cis.iloc[idx]
            rows.append(
                {
                    "flow": flow,
                    "spec": "event",
                    "outcome": outcome,
                    "horizon": idx - 6,
                    "reference_value": slope * (float(point["y"]) - zero_y),
                    "reference_conf_low": slope * (float(ci_row["y_low"]) - zero_y),
                    "reference_conf_high": slope * (float(ci_row["y_high"]) - zero_y),
                    "reference_source": str(fig_path),
                }
            )
    return pd.DataFrame(rows)


def _extract_dynamic_reference_imports(config: PipelineConfig) -> pd.DataFrame:
    fig_path = config.fajgelbaum_root / "results" / "main" / "fig_04a.pdf"
    reader = PdfReader(str(fig_path))
    page = reader.pages[0]
    text_items = _extract_text_items(page)
    paths = _extract_page_paths(reader, page)
    titles = _title_positions(text_items, DYNAMIC_TITLES)

    polylines = []
    for path in paths:
        if path.stroke == RGB_DYNAMIC_SERIES:
            points = _polyline_points(path)
            if points:
                x_mean = float(np.mean([x for x, _ in points]))
                y_mean = float(np.mean([y for _, y in points]))
                polylines.append({"points": points, "x": x_mean, "y": y_mean})
    line_raw = pd.DataFrame(polylines)
    x_split = _largest_gap_midpoint(line_raw["x"])
    y_split = _largest_gap_midpoint(line_raw["y"])
    line_df = _assign_quadrant(line_raw, x_split, y_split)
    titles = _assign_quadrant(titles.drop(columns=["col", "row"], errors="ignore"), x_split, y_split)

    zero_rows = []
    for path in paths:
        if path.stroke == RGB_DYNAMIC_ZERO:
            seg = _line_segment(path)
            if seg and abs(seg[1] - seg[3]) < 1e-6:
                x0, y0, x1, _ = seg
                zero_rows.append({"x": (x0 + x1) / 2.0, "y": y0})
    zero_df = _assign_quadrant(pd.DataFrame(zero_rows), x_split, y_split)
    text_df = _assign_quadrant(pd.DataFrame([item.__dict__ for item in text_items]), x_split, y_split)

    band_segments = []
    for path in paths:
        if path.stroke == RGB_DYNAMIC_CI:
            seg = _line_segment(path)
            if seg:
                midpoint = _segment_midpoint(seg)
                band_segments.append({"segment": seg, "x": midpoint[0], "y": midpoint[1]})
    band_df = _assign_quadrant(pd.DataFrame(band_segments), x_split, y_split)

    rows: list[dict[str, Any]] = []
    for outcome in DYNAMIC_TITLES.values():
        subplot = _subplot_frame_from_titles(titles, outcome)
        line_row = line_df.loc[(line_df["row"] == subplot["row"]) & (line_df["col"] == subplot["col"])]
        if len(line_row) != 1:
            raise ValueError(f"Expected one dynamic polyline for imports {outcome}, found {len(line_row)}.")
        points = sorted(line_row.iloc[0]["points"])
        y_labels = _numeric_y_labels(
            text_df, subplot, min(x for x, _ in points), [y for _, y in points]
        )
        slope, intercept, axis_r2, axis_residual = _axis_fit(y_labels)
        if axis_r2 < 0.999999 or axis_residual > 0.01:
            raise ValueError(f"Invalid dynamic PDF axis calibration: R2={axis_r2}, residual={axis_residual}")
        zero_y = float(zero_df.loc[(zero_df["row"] == subplot["row"]) & (zero_df["col"] == subplot["col"]), "y"].mode().iloc[0])
        x_targets = [x for x, _ in points]
        subplot_segments = band_df.loc[(band_df["row"] == subplot["row"]) & (band_df["col"] == subplot["col"])].copy()
        upper_segments: list[tuple[float, float, float, float]] = []
        lower_segments: list[tuple[float, float, float, float]] = []
        for _, seg_row in subplot_segments.iterrows():
            segment = tuple(seg_row["segment"])
            mid_x, mid_y = _segment_midpoint(segment)
            center_y = _interp_y(points, mid_x)
            if mid_y > center_y:
                upper_segments.append(segment)
            else:
                lower_segments.append(segment)
        if not upper_segments or not lower_segments:
            raise ValueError(f"Could not classify dynamic confidence bands for imports {outcome}.")
        upper_values = _band_values_from_segments(upper_segments, x_targets)
        lower_values = _band_values_from_segments(lower_segments, x_targets)
        for idx, (x, y) in enumerate(points):
            conf_low = slope * (float(lower_values[idx]) - zero_y)
            conf_high = slope * (float(upper_values[idx]) - zero_y)
            rows.append(
                {
                    "flow": "imports",
                    "spec": "dynamic",
                    "outcome": outcome,
                    "horizon": idx - 6,
                    "reference_value": slope * (float(y) - zero_y),
                    "reference_conf_low": conf_low,
                    "reference_conf_high": conf_high,
                    "reference_source": str(fig_path),
                }
            )
    return pd.DataFrame(rows)


def _load_generated_coefficients(config: PipelineConfig) -> pd.DataFrame:
    frames = []
    for flow in ("imports", "exports"):
        event_path = config.analysis_dir / "trade_regressions" / "tables" / f"{flow}_event_study_coefficients.csv"
        dynamic_path = config.analysis_dir / "trade_regressions" / "tables" / f"{flow}_dynamic_coefficients.csv"
        if event_path.exists():
            df = pd.read_csv(event_path)
            frames.append(df.rename(columns={"event_time": "horizon"})[["flow", "spec", "outcome", "horizon", "estimate", "conf_low", "conf_high", "std_error"]])
        if dynamic_path.exists():
            df = pd.read_csv(dynamic_path)
            frames.append(df[["flow", "spec", "outcome", "horizon", "estimate", "conf_low", "conf_high", "std_error"]])
    if not frames:
        raise FileNotFoundError("No generated coefficient tables found.")
    return pd.concat(frames, ignore_index=True)


def compare_trade_chart_values(config: PipelineConfig) -> dict[str, Any]:
    ensure_dir(config.verification_dir)
    reference = pd.concat(
        [
            _extract_event_reference(config, "imports"),
            _extract_event_reference(config, "exports"),
            _extract_dynamic_reference_imports(config),
        ],
        ignore_index=True,
    )
    generated = _load_generated_coefficients(config)
    merged = reference.merge(generated, on=["flow", "spec", "outcome", "horizon"], how="left", validate="one_to_one")
    merged["difference"] = merged["estimate"] - merged["reference_value"]
    merged["abs_difference"] = merged["difference"].abs()

    comparison_path = config.verification_dir / "trade_chart_value_comparison.csv"
    merged.sort_values(["spec", "flow", "outcome", "horizon"]).to_csv(comparison_path, index=False)

    summary = (
        merged.groupby(["spec", "flow", "outcome"], dropna=False)
        .agg(
            n_points=("horizon", "size"),
            mean_abs_diff=("abs_difference", "mean"),
            median_abs_diff=("abs_difference", "median"),
            max_abs_diff=("abs_difference", "max"),
        )
        .reset_index()
    )
    summary_path = config.verification_dir / "trade_chart_value_comparison_summary.csv"
    summary.to_csv(summary_path, index=False)

    report_lines = [
        "# Trade Chart Value Comparison",
        "",
        "Reference chart values were extracted directly from the vector paths in the local package PDFs.",
        "",
        f"- Detailed comparison: `{comparison_path}`",
        f"- Summary table: `{summary_path}`",
        "",
        "## Summary",
        "",
    ]
    for _, row in summary.iterrows():
        report_lines.append(
            f"- `{row['spec']}` `{row['flow']}` `{row['outcome']}`: "
            f"mean abs diff=`{row['mean_abs_diff']:.3f}`, "
            f"median abs diff=`{row['median_abs_diff']:.3f}`, "
            f"max abs diff=`{row['max_abs_diff']:.3f}`."
        )
    report_lines.extend(
        [
            "",
            "## Scope",
            "",
            "- Event-study comparisons cover imports (`fig_02.pdf`) and exports (`fig_03.pdf`).",
            "- Dynamic comparisons currently cover imports (`fig_04a.pdf`).",
            "- The local replication bundle does not include `fig_04b.pdf`, so export dynamic values are not yet included in this direct package-PDF extraction.",
        ]
    )
    report_path = config.verification_dir / "trade_chart_value_comparison.md"
    report_path.write_text("\n".join(report_lines).rstrip() + "\n", encoding="utf-8")

    return {
        "comparison_csv": str(comparison_path),
        "summary_csv": str(summary_path),
        "report_md": str(report_path),
        "rows": int(len(merged)),
    }


def plot_import_overlay_charts(config: PipelineConfig) -> dict[str, Any]:
    comparison_path = config.verification_dir / "trade_chart_value_comparison.csv"
    if not comparison_path.exists():
        compare_trade_chart_values(config)
    df = pd.read_csv(comparison_path)
    imports = df.loc[df["flow"] == "imports"].copy()
    chart_dir = config.analysis_dir / "trade_regressions" / "charts"
    ensure_dir(chart_dir)
    outcome_titles = {
        "val": "Log Value",
        "q1": "Log Quantity",
        "p": "Log Unit Value",
        "pduty": "Log Duty-Inclusive Unit Value",
    }
    outputs: dict[str, Any] = {}
    for spec_name, xlabel in (("event", "Months Relative to Tariff Enactment"), ("dynamic", "Months Relative to Tariff Increase")):
        subset = imports.loc[imports["spec"] == spec_name].copy()
        fig, axes = plt.subplots(2, 2, figsize=(11, 8.5), constrained_layout=True)
        axes = axes.ravel()
        handles = None
        labels = None
        for idx, outcome in enumerate(["val", "q1", "p", "pduty"]):
            ax = axes[idx]
            panel = subset.loc[subset["outcome"] == outcome].sort_values("horizon")
            x = panel["horizon"].to_numpy(dtype=float)
            ref_line, = ax.plot(x, panel["reference_value"], color="navy", linewidth=2, marker="o", label="Original")
            ax.fill_between(x, panel["reference_conf_low"], panel["reference_conf_high"], color="navy", alpha=0.15)
            rep_line, = ax.plot(x, panel["estimate"], color="darkorange", linewidth=2, marker="s", label="Replication")
            ax.fill_between(x, panel["conf_low"], panel["conf_high"], color="darkorange", alpha=0.18)
            ax.axhline(0.0, color="0.7", linestyle="--", linewidth=1)
            ax.set_title(outcome_titles[outcome])
            ax.set_xlabel(xlabel)
            ax.set_ylabel("Percent")
            ax.set_xticks(list(range(-6, 7)))
            ax.set_xticklabels(["-6", "-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5", "6+"])
            handles = [ref_line, rep_line]
            labels = ["Original", "Replication"]
        if handles is not None and labels is not None:
            fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False)
        fig.suptitle(f"Imports {'Event Study' if spec_name == 'event' else 'Dynamic Lead-Lag'}: Original vs Replication")
        png_path = chart_dir / f"imports_{spec_name}_original_vs_replication.png"
        pdf_path = chart_dir / f"imports_{spec_name}_original_vs_replication.pdf"
        fig.savefig(png_path, dpi=220)
        fig.savefig(pdf_path)
        plt.close(fig)
        outputs[spec_name] = {"png": str(png_path), "pdf": str(pdf_path)}
    overlay_csv = config.verification_dir / "trade_chart_imports_overlay_data.csv"
    imports.sort_values(["spec", "outcome", "horizon"]).to_csv(overlay_csv, index=False)
    outputs["data_csv"] = str(overlay_csv)
    return outputs


if __name__ == "__main__":
    cfg = PipelineConfig.default()
    cfg.ensure_directories()
    result = compare_trade_chart_values(cfg)
    overlays = plot_import_overlay_charts(cfg)
    print(result)
    print(overlays)
