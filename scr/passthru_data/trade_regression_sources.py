"""Source audit for trade regression replication."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import read_table, write_metadata_json
from .trade_regression_common import WORKHORSE_SPECS, workhorse_output_path, write_markdown_report


def _list_files(path: Path) -> list[str]:
    if not path.exists():
        return []
    return sorted(str(item) for item in path.rglob("*") if item.is_file())


def run_trade_regression_source_audit(config: PipelineConfig) -> dict[str, Any]:
    policy_files = _list_files(config.manual_input_dir / "policy")
    trade_manual_files = _list_files(config.manual_input_dir / "trade")
    concordance_manual_files = _list_files(config.manual_input_dir / "concordances")

    naics_candidates = [
        path
        for path in policy_files + trade_manual_files + concordance_manual_files
        if "naics" in path.lower() or "crosswalk" in path.lower()
    ]

    flows = [config.trade_flow] if config.trade_flow else list(WORKHORSE_SPECS)
    local_panel_status: dict[str, Any] = {}
    all_local_ready = True
    for flow in flows:
        spec = WORKHORSE_SPECS[flow]
        panel_path = config.analysis_dir / f"{spec['basename']}.parquet"
        status: dict[str, Any] = {"path": str(panel_path), "exists": panel_path.exists()}
        workhorse_path = workhorse_output_path(config, flow)
        status["workhorse_path"] = str(workhorse_path)
        status["workhorse_exists"] = workhorse_path.exists()
        if panel_path.exists():
            columns = read_table(panel_path).columns.tolist()
            missing = [column for column in spec["required_columns"] if column not in columns]
            status["missing_required_columns"] = missing
            status["column_count"] = len(columns)
            if missing and workhorse_path.exists():
                workhorse_columns = list(pq.read_schema(workhorse_path).names)
                workhorse_missing = [column for column in spec["required_columns"] if column not in workhorse_columns]
                status["workhorse_missing_required_columns"] = workhorse_missing
                status["is_regression_ready"] = not workhorse_missing
                if workhorse_missing:
                    all_local_ready = False
            else:
                status["is_regression_ready"] = not missing
                if missing:
                    all_local_ready = False
        else:
            status["missing_required_columns"] = spec["required_columns"]
            status["is_regression_ready"] = False
            all_local_ready = False
        local_panel_status[flow] = status

    regression_ready_from_raw = all_local_ready
    fallback_required = not regression_ready_from_raw
    missing_capabilities = []
    if not all_local_ready:
        missing_capabilities.append("regression_ready_local_workhorse_panels")

    audit = {
        "status": "ready_from_raw" if regression_ready_from_raw else "reference_fallback_required",
        "regression_ready_from_raw": regression_ready_from_raw,
        "fallback_required": fallback_required,
        "missing_capabilities": missing_capabilities,
        "manual_policy_files": policy_files,
        "manual_trade_files": trade_manual_files,
        "manual_concordance_files": concordance_manual_files,
        "naics_mapping_candidates": naics_candidates,
        "local_panel_status": local_panel_status,
        "note": (
            "Raw-only mode is enabled. Regression readiness is determined by whether the local analysis "
            "panels contain all required workhorse columns after raw-data construction."
        ),
    }

    json_path = config.verification_dir / "trade_regression_source_audit.json"
    md_path = config.verification_dir / "trade_regression_source_audit.md"
    write_metadata_json(json_path, audit)
    lines = [
        "# Trade Regression Source Audit",
        "",
        f"- Status: `{audit['status']}`",
        f"- Regression-ready from raw: `{audit['regression_ready_from_raw']}`",
        f"- Fallback required: `{audit['fallback_required']}`",
        f"- Missing capabilities: {', '.join(missing_capabilities) if missing_capabilities else 'none'}",
        "",
        "## Local Inputs",
        "",
        f"- Manual policy files: `{len(policy_files)}`",
        f"- Manual trade files: `{len(trade_manual_files)}`",
        f"- Manual concordance files: `{len(concordance_manual_files)}`",
        f"- NAICS mapping candidates: `{len(naics_candidates)}`",
        "",
        "## Local Panel Status",
        "",
    ]
    for flow, status in local_panel_status.items():
        lines.append(f"- `{flow}`: ready=`{status['is_regression_ready']}`, missing={status['missing_required_columns']}")
    lines.extend(["", audit["note"]])
    write_markdown_report(md_path, lines)
    return {"audit_json": str(json_path), "audit_markdown": str(md_path), **audit}
