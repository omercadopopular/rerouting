"""Parse observable 2025 partner reciprocal-tariff schedules from local HTS PDFs.

The official HTS revisions contain both the broad ten-percent reciprocal rate
and the partner-specific Chapter 99 headings.  This module deliberately returns
only schedules that are a deterministic function of country, date, and the
ordinary tariff line.  Canada, Mexico, and partner arrangements whose rate
depends on an MFN floor or a separate product agreement are flagged for
exclusion from the flat-rate cross-partner specification.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import unicodedata
from pathlib import Path
from typing import Iterable

import pandas as pd


GENERAL_START = pd.Timestamp("2025-04-05")
AUGUST_SCHEDULE_START = pd.Timestamp("2025-08-07")
CHINA_RECIPROCAL_CODES = frozenset({"5660", "5700", "5820"})
GENERAL_EXEMPT_CODES = frozenset({"1220", "2010", "2390", "4621", "4622", "5790"})

# These partners have entry-certification, additional bilateral actions, or
# total-duty-floor arrangements during 2025.  Their legal schedules are useful
# diagnostics, but are not silently treated as simple additive rates.
NON_FLAT_PARTNER_CODES = frozenset(
    {
        "1220",  # Canada: USMCA certification and energy/potash variants
        "2010",  # Mexico: USMCA certification and potash variant
        "3510",  # Brazil: separate 40 percent IEEPA action and exceptions
        "5330",  # India: separate Russian-oil action and exceptions
        "5800",  # South Korea: 15 percent total-duty floor and aircraft rules
        "5880",  # Japan: 15 percent total-duty floor and aircraft rules
    }
)

EU_PARTNER_CODES = frozenset(
    {
        "4010", "4050", "4099", "4190", "4210", "4231", "4239", "4279",
        "4280", "4330", "4351", "4359", "4370", "4470", "4490", "4510",
        "4550", "4700", "4710", "4730", "4759", "4791", "4792", "4840",
        "4850", "4870", "4910",
    }
)

SPECIAL_ALIASES: dict[str, tuple[str, ...]] = {
    "3720": ("falkland islands",),
    "4039": ("norway",),
    "4120": ("united kingdom",),
    "4411": ("liechtenstein",),
    "4793": ("bosnia and herzegovina",),
    "4794": ("north macedonia",),
    "5460": ("myanmar", "burma"),
    "5650": ("philippines",),
    "5660": ("macau",),
    "5800": ("south korea",),
    "7480": ("cote d ivoire",),
    "7630": ("republic of the congo", "congo brazzaville"),
    "7660": ("democratic republic of the congo", "congo kinshasa"),
}


def normalize_country_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    text = text.lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _pdftotext() -> Path:
    located = shutil.which("pdftotext")
    candidates = (
        Path(located) if located else None,
        Path(r"C:\Program Files\Git\mingw64\bin\pdftotext.exe"),
        Path(r"C:\Program Files\Git\usr\bin\pdftotext.exe"),
    )
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    raise FileNotFoundError("pdftotext is required to parse local HTS revisions")


def pdf_text(path: Path, first_page: int = 3300, last_page: int = 4000) -> str:
    result = subprocess.run(
        [
            str(_pdftotext()),
            "-f",
            str(first_page),
            "-l",
            str(last_page),
            "-layout",
            str(path),
            "-",
        ],
        check=True,
        capture_output=True,
    )
    return result.stdout.decode("utf-8", errors="replace").replace("\r", "")


def _heading_blocks(text: str) -> Iterable[tuple[str, str]]:
    matches = list(re.finditer(r"(?m)^9903\.\d{2}\.\d{2}\s+1/", text))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        yield match.group().split()[0], text[match.start():end]


def parse_flat_partner_headings(pdf_path: Path) -> pd.DataFrame:
    """Return active simple-additive reciprocal headings in an HTS revision."""

    rows: list[dict[str, object]] = []
    for heading, block in _heading_blocks(pdf_text(pdf_path)):
        chapter, number = heading.rsplit(".", 1)
        suffix = int(number)
        in_reciprocal_family = (
            heading == "9903.01.25"
            or (chapter == "9903.01" and 43 <= suffix <= 76)
            or (chapter == "9903.02" and 2 <= suffix <= 71)
        )
        if not in_reciprocal_family:
            continue
        flat = re.sub(r"\s+", " ", block)
        lowered = flat.lower()
        if "provision terminated" in lowered or "provision suspended" in lowered:
            continue
        rate_matches = re.findall(
            r"subheading\s*\+\s*(\d+(?:\.\d+)?)%", flat, flags=re.I
        )
        if not rate_matches:
            continue
        if heading == "9903.01.25":
            country_text = "ALL_OTHER_PARTNERS"
        else:
            country_match = re.search(
                r"articles\s+the\s+product\s+of\s+(.+?)(?:,\s*)?as\s+provided",
                flat,
                flags=re.I,
            )
            if not country_match:
                continue
            country_text = country_match.group(1)
        rows.append(
            {
                "heading": heading,
                "country_text": country_text,
                "additional_rate": float(rate_matches[-1]) / 100.0,
            }
        )
    return pd.DataFrame(rows).drop_duplicates()


def parse_april_country_rates(pdf_path: Path) -> pd.DataFrame:
    """Parse the one-day April 9 Annex-I schedule retained in the HTS note."""

    text = pdf_text(pdf_path)
    start = re.search(
        r"\(xiv\).*?Heading 9903\.01\.25 shall not apply", text, flags=re.I | re.S
    )
    if not start:
        return pd.DataFrame(columns=["country_text", "additional_rate"])
    end = re.search(r"\n\s*\(xv\)", text[start.end():], flags=re.I)
    body = text[start.end(): start.end() + end.start()] if end else text[start.end():]
    rows = [
        {
            "country_text": match.group(1).strip(),
            "additional_rate": float(match.group(2)) / 100.0,
        }
        for match in re.finditer(
            r"\(\d+\)\s+(.+?),\s+subject to an additional duty of\s+(\d+(?:\.\d+)?)%",
            body,
            flags=re.I,
        )
    ]
    return pd.DataFrame(rows).drop_duplicates()


def resolve_country_text(
    country_text: str,
    partners: pd.DataFrame,
) -> tuple[str, ...]:
    """Resolve an official country phrase to Census partner codes."""

    normalized = normalize_country_name(country_text)
    if normalized == "all other partners":
        return ()
    if "european union" in normalized:
        return tuple(sorted(EU_PARTNER_CODES))
    if "china" in normalized and ("hong kong" in normalized or "macau" in normalized):
        return tuple(sorted(CHINA_RECIPROCAL_CODES))

    matches: set[str] = set()
    for record in partners[["partner_code", "partner_name"]].drop_duplicates().itertuples(index=False):
        code = str(record.partner_code).zfill(4)
        aliases = SPECIAL_ALIASES.get(
            code, (normalize_country_name(record.partner_name),)
        )
        for alias in aliases:
            if re.search(rf"\b{re.escape(alias)}\b", normalized):
                matches.add(code)
                break
    return tuple(sorted(matches))


def parsed_release_schedule(
    pdf_path: Path,
    partners: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve the active flat-rate headings in one official revision."""

    parsed = parse_flat_partner_headings(pdf_path)
    rows: list[dict[str, object]] = []
    for record in parsed.itertuples(index=False):
        if record.country_text == "ALL_OTHER_PARTNERS":
            continue
        codes = resolve_country_text(str(record.country_text), partners)
        for code in codes:
            rows.append(
                {
                    "partner_code": code,
                    "heading": record.heading,
                    "country_text": record.country_text,
                    "additional_rate": float(record.additional_rate),
                }
            )
    return pd.DataFrame(rows).drop_duplicates()
