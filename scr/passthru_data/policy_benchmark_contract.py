"""Machine-readable rules for like-for-like historical policy comparisons."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "specifications"
    / "historical_policy_benchmark_contract.json"
)


def load_contract(path: Path | None = None) -> dict[str, Any]:
    """Load the committed benchmark contract without consulting generated data."""

    contract_path = path or CONTRACT_PATH
    with contract_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def target_contract(target: str, path: Path | None = None) -> dict[str, Any]:
    contract = load_contract(path)
    try:
        return contract["targets"][target]
    except KeyError as exc:
        raise KeyError(f"Unknown historical policy benchmark target: {target}") from exc


def comparison_eligibility(
    *,
    target: str,
    reconstructed_calendar: str,
    reconstructed_policy_variables: set[str] | list[str] | tuple[str, ...],
) -> tuple[bool, str | None]:
    """Return whether a reconstruction is eligible for a published comparison.

    The legal-calendar object is intentionally allowed to exist, but it is not
    eligible for a published comparison when the original target uses the
    nearest-full-month convention.
    """

    spec = target_contract(target)
    expected_calendar = spec["paper_comparison_eligible_calendar"]
    expected_variables = set(spec.get("policy_variables", []))
    actual_variables = set(reconstructed_policy_variables)
    if reconstructed_calendar != expected_calendar:
        return False, f"calendar mismatch: expected {expected_calendar}, got {reconstructed_calendar}"
    if not expected_variables.issubset(actual_variables):
        missing = sorted(expected_variables - actual_variables)
        return False, f"policy-variable mismatch: missing {missing}"
    return True, None


def assert_published_comparison_eligible(
    *,
    target: str,
    reconstructed_calendar: str,
    reconstructed_policy_variables: set[str] | list[str] | tuple[str, ...],
) -> None:
    eligible, reason = comparison_eligibility(
        target=target,
        reconstructed_calendar=reconstructed_calendar,
        reconstructed_policy_variables=reconstructed_policy_variables,
    )
    if not eligible:
        raise ValueError(f"Published comparison is not eligible for {target}: {reason}")
