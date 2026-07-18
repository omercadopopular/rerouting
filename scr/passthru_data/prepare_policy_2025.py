"""Prepare (but never estimate) the independent 2025 policy layer."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json
import re

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json


VERSION = "policy_2025_preflight_v1"
LEDGER_FIELDS = [
    "action_id", "legal_authority", "policy_family", "partner_scope",
    "hts_vintage", "hts_code", "legal_effective_date", "suspension_end_date",
    "additional_statutory_rate", "stacking_rule", "exclusion_status",
    "exclusion_start_date", "exclusion_end_date", "official_source_path",
    "official_source_url", "source_page_or_row", "source_sha256",
    "extraction_method", "confidence_status", "review_status",
]


def _policy_inventory_candidate(path: Path, policy_root: Path) -> tuple[bool, str | None]:
    """Keep only reviewed policy-source candidates, never browser/cache data."""
    parts = {part.lower() for part in path.relative_to(policy_root).parts}
    name = path.name.lower()
    forbidden_tokens = ("_tmp", "profile", "cache", "cookie", "selenium", "chrome", "edge", "browser")
    if any(token in part for part in parts for token in forbidden_tokens) or any(token in name for token in forbidden_tokens):
        return False, "temporary_or_credential_artifact"
    if path.suffix.lower() not in {".pdf", ".zip", ".json", ".csv", ".parquet", ".txt", ".html"}:
        return False, "unsupported_source_suffix"
    # The annual tariff archives and the root tariff metadata are the only
    # currently identifiable canonical local policy sources.  Other files need
    # explicit review before they enter the release inventory.
    relative = path.relative_to(policy_root).as_posix()
    if relative == "tariff_annual.json" or re.fullmatch(r"annual/tariff_data_\d{4}\.zip", relative):
        return True, None
    return False, "unreviewed_local_policy_file"


def prepare_policy_2025(config: PipelineConfig) -> dict[str, Any]:
    out = config.verification_dir / "policy_2025_preflight"
    out.mkdir(parents=True, exist_ok=True)
    policy_root = config.raw_dir / "policy"
    candidates: list[dict[str, Any]] = []
    excluded: dict[str, int] = {}
    if policy_root.exists():
        for path in policy_root.rglob("*"):
            if not path.is_file():
                continue
            keep, reason = _policy_inventory_candidate(path, policy_root)
            if not keep:
                excluded[reason or "excluded"] = excluded.get(reason or "excluded", 0) + 1
                continue
            record = {"path": path.resolve().relative_to(config.repo_root.resolve()).as_posix(), "bytes": int(path.stat().st_size)}
            # Stream hashes for manageable local source files.  Large files are
            # inventory evidence but remain explicitly unverified here.
            if path.stat().st_size <= 250_000_000:
                record["sha256"] = sha256_file(path)
                record["hash_status"] = "complete"
            else:
                record["sha256"] = None
                record["hash_status"] = "deferred_large_source"
            candidates.append(record)
    missing = [
        {"requirement": "official_2025_China_IEEPA_text_and_HTS_scope", "status": "missing_or_unverified"},
        {"requirement": "official_2025_Section_232_201_exclusions_and_stacking", "status": "missing_or_unverified"},
        {"requirement": "reviewed_product_date_rate_ledger_through_2025_12", "status": "not_constructed"},
    ]
    schema = {
        "version": VERSION,
        "ledger_name": "public_policy_ledger_2025",
        "fields": LEDGER_FIELDS,
        "required_nonnull_for_release": [field for field in LEDGER_FIELDS if field not in {"suspension_end_date", "exclusion_start_date", "exclusion_end_date", "official_source_url"}],
        "calendars": {"legal_effective": "date-level statutory calendar", "paper_compatible": "nearest-month calendar derived only after legal calendar validation"},
        "unresolved_policy_values_must_remain_null": True,
    }
    write_metadata_json(out / "policy_2025_ledger_schema.json", schema)
    write_metadata_json(out / "policy_2025_local_source_inventory.json", {
        "version": VERSION,
        "root": policy_root.resolve().relative_to(config.repo_root.resolve()).as_posix() if policy_root.exists() else None,
        "inventory_scope": "reviewed_local_policy_candidates_only",
        "files": candidates,
        "excluded_file_count": int(sum(excluded.values())),
        "excluded_by_reason": excluded,
        "credentials_and_browser_artifacts_excluded": True,
    })
    write_metadata_json(out / "policy_2025_missing_sources.json", {"version": VERSION, "missing": missing, "status": "blocked_missing_sources"})
    prereg = "# 2025 policy/event-study preregistration (not estimated)\n\n"
    prereg += "The February 2025 event remains blocked until the versioned legal ledger passes its own product/date/rate verification gate. No unresolved rate, scope, exclusion, stacking rule, or date is imputed as zero.\n\n"
    prereg += "- Candidate event dates: derived from the verified ledger, not hardcoded.\n- Calendars: legal-effective and paper-compatible nearest-month calendars are separate.\n- Outcomes: nominal value, quantity, pre-duty unit value, and duty-inclusive unit value only after tariff validation.\n- Fixed effects/clusters: specified after the validated treatment design; right-tail horizons beyond the December 2025 data cutoff are unavailable, not imputed.\n- Section 301, Section 201, Section 232, and other overlapping actions are retained as separate policy families with explicit stacking.\n\nStatus: **blocked_missing_sources**; no 2025 event regression was run.\n"
    (out / "policy_2025_event_preregistration.md").write_text(prereg, encoding="utf-8")
    manifest = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "schema_path": (out / "policy_2025_ledger_schema.json").resolve().relative_to(config.repo_root.resolve()).as_posix(), "source_inventory_path": (out / "policy_2025_local_source_inventory.json").resolve().relative_to(config.repo_root.resolve()).as_posix(), "missing_sources_path": (out / "policy_2025_missing_sources.json").resolve().relative_to(config.repo_root.resolve()).as_posix(), "independent_policy_gate": "failed", "event_2025_ready": False, "status": "preflight_only"}
    write_metadata_json(out / "policy_2025_preflight_manifest.json", manifest)
    return manifest


def main() -> int:
    print(prepare_policy_2025(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
