"""Audit raw fixed-width quantity tokens without coercing blanks to zero.

The archive-native extension parser intentionally converts numeric fields to
nullable numbers.  This companion audit inspects the original fixed-width
quantity substrings first, so a source blank, an explicit zero, and a malformed
or suppressed token cannot be confused after parsing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import re
import zipfile

import pandas as pd

from .build_trade_extension import _archive_path, _repo_relative
from .config import PipelineConfig
from .download_trade import FLOW_SPECS, _resolve_member_name
from .io_utils import iter_months, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_quantity_token_audit_v1"
_NUMBER_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?$")


def _classify(token: str) -> str:
    value = token.strip()
    if not value:
        return "blank"
    # Census fixed-width fields may contain commas, but punctuation or
    # suppression markers must not silently become zero.
    compact = value.replace(",", "")
    if not _NUMBER_RE.fullmatch(compact):
        return "malformed_or_suppressed"
    try:
        number = float(compact)
    except ValueError:
        return "malformed_or_suppressed"
    if number == 0:
        return "explicit_zero"
    if number > 0:
        return "positive"
    return "negative"


def _classify_bytes(token: bytes) -> str:
    """Fast path for the archive scan; preserve the same classifications."""
    value = token.strip()
    if not value:
        return "blank"
    compact = value.replace(b",", b"")
    if compact.startswith((b"+", b"-")):
        digits = compact[1:]
        sign = compact[:1]
    else:
        digits = compact
        sign = b"+"
    if digits.count(b".") > 1 or not digits.replace(b".", b"").isdigit():
        # Scientific notation and other unusual fields go through the exact
        # string classifier instead of being silently coerced.
        return _classify(value.decode("latin1", errors="replace"))
    if b"." in digits:
        try:
            number = float(compact)
        except ValueError:
            return "malformed_or_suppressed"
        if number == 0:
            return "explicit_zero"
        return "positive" if number > 0 else "negative"
    if not digits:
        return "malformed_or_suppressed"
    if all(byte == 48 for byte in digits):
        return "explicit_zero"
    return "negative" if sign == b"-" else "positive"


def _audit_archive(config: PipelineConfig, flow: str, period: str, archive: Path) -> dict[str, Any]:
    spec = FLOW_SPECS[flow]
    q_start, q_end = spec["detail_colspecs"][4]
    y_start, y_end = spec["detail_colspecs"][2]
    m_start, m_end = spec["detail_colspecs"][3]
    duty_specs = spec["detail_colspecs"][6:8] if flow == "imports" else []
    counts = {name: 0 for name in ("blank", "explicit_zero", "positive", "negative", "malformed_or_suppressed")}
    duty_counts = {"dut_val_mo_nonblank": 0, "cal_dut_mo_nonblank": 0, "dut_val_mo_malformed": 0, "cal_dut_mo_malformed": 0}
    period_mismatch = 0
    source_rows = 0
    malformed_period_rows = 0
    member = None
    with zipfile.ZipFile(archive) as zf:
        member = _resolve_member_name(zf, spec["detail_member"])
        with zf.open(member) as handle:
            carry = b""
            while True:
                block = handle.read(8 * 1024 * 1024)
                if not block:
                    lines = [carry] if carry else []
                else:
                    pieces = (carry + block).split(b"\n")
                    carry = pieces.pop()
                    lines = pieces
                for raw_line in lines:
                    if not raw_line.strip():
                        continue
                    source_rows += 1
                    line = raw_line.rstrip(b"\r")
                    counts[_classify_bytes(line[q_start:q_end])] += 1
                    year_token = line[y_start:y_end].strip()
                    month_token = line[m_start:m_end].strip()
                    try:
                        if int(year_token) != int(period[:4]) or int(month_token) != int(period[5:7]):
                            period_mismatch += 1
                    except ValueError:
                        malformed_period_rows += 1
                    for (duty_start, duty_end), name in zip(duty_specs, ("dut_val_mo", "cal_dut_mo")):
                        duty_token = line[duty_start:duty_end].strip()
                        if duty_token:
                            duty_counts[f"{name}_nonblank"] += 1
                            if _classify_bytes(duty_token) == "malformed_or_suppressed":
                                duty_counts[f"{name}_malformed"] += 1
                if not block:
                    break
    digest = sha256_file(archive)
    return {
        "flow": flow,
        "period": period,
        "archive": _repo_relative(config, archive),
        "archive_sha256": digest,
        "detail_member": member,
        "source_rows": source_rows,
        "quantity_blank_rows": counts["blank"],
        "quantity_explicit_zero_rows": counts["explicit_zero"],
        "quantity_positive_rows": counts["positive"],
        "quantity_negative_rows": counts["negative"],
        "quantity_malformed_or_suppressed_rows": counts["malformed_or_suppressed"],
        "period_mismatch_rows": period_mismatch,
        "malformed_period_rows": malformed_period_rows,
        **duty_counts,
        "status": "passed" if period_mismatch == 0 and malformed_period_rows == 0 else "failed_period_validation",
    }


def audit_quantity_tokens(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12") -> dict[str, Any]:
    out = config.verification_dir / "extension_v3"
    out.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    for flow in ("imports", "exports"):
        for period in iter_months(start_period, end_period):
            archive = _archive_path(config, flow, period)
            if not archive.exists():
                missing.append({"flow": flow, "period": period, "archive": _repo_relative(config, archive)})
                continue
            try:
                rows.append(_audit_archive(config, flow, period, archive))
            except Exception as exc:
                rows.append({"flow": flow, "period": period, "archive": _repo_relative(config, archive), "status": "failed", "error_type": type(exc).__name__, "error_message": str(exc)})
    frame = pd.DataFrame(rows)
    write_parquet(frame, out / "extension_quantity_token_audit.parquet", overwrite=True)
    summary_cols = ["flow", "period", "source_rows", "quantity_blank_rows", "quantity_explicit_zero_rows", "quantity_positive_rows", "quantity_negative_rows", "quantity_malformed_or_suppressed_rows", "period_mismatch_rows", "malformed_period_rows", "dut_val_mo_nonblank", "cal_dut_mo_nonblank", "status"]
    frame[[c for c in summary_cols if c in frame.columns]].to_csv(out / "extension_quantity_token_summary.csv", index=False)
    passed = bool(not missing and not frame.empty and (frame["status"] == "passed").all())
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(frame)),
        "missing_archives": missing,
        "archive_count_expected": 312,
        "archive_count_observed": int(len(frame)),
        "quantity_token_gate": "passed" if passed else "failed_or_incomplete",
        "source_blank_quantity_rows": int(frame.get("quantity_blank_rows", pd.Series(dtype="int64")).sum()) if not frame.empty else None,
        "source_explicit_zero_rows": int(frame.get("quantity_explicit_zero_rows", pd.Series(dtype="int64")).sum()) if not frame.empty else None,
        "source_positive_quantity_rows": int(frame.get("quantity_positive_rows", pd.Series(dtype="int64")).sum()) if not frame.empty else None,
        "source_malformed_or_suppressed_rows": int(frame.get("quantity_malformed_or_suppressed_rows", pd.Series(dtype="int64")).sum()) if not frame.empty else None,
        "source_duty_fields_audited": True,
        "period_validation": "passed" if passed else "failed_or_incomplete",
    }
    write_metadata_json(out / "extension_quantity_token_manifest.json", manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2013-01")
    parser.add_argument("--end", default="2025-12")
    args = parser.parse_args(argv)
    print(audit_quantity_tokens(PipelineConfig.default(), start_period=args.start, end_period=args.end))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
