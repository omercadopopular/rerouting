"""Heuristic extractor: HTS PDF text -> row CSV for one or many releases.

Prototype fallback for releases without machine-readable exports.
"""

from __future__ import annotations

from pathlib import Path
import argparse
import re

import pandas as pd


CODE_RE = re.compile(r"^\s*(\d{4}\.\d{2}\.\d{2}(?:\.\d{2})?)\s*$")
RATE_RE = re.compile(
    r"(Free(?:\s*\([^)]+\))?|[0-9]+(?:\.[0-9]+)?\s*%|[0-9]+(?:\.[0-9]+)?\s*¢/[A-Za-z]+|[0-9]+(?:\.[0-9]+)?\s*\$/[A-Za-z]+)",
    re.I,
)
IGNORE_LINE_RE = re.compile(
    r"^(Rates of Duty|Unit|of|Quantity|Article Description|Stat\.|Suf-|Heading/|Subheading|General|Special|[12]\s*$|Harmonized Tariff Schedule)",
    re.I,
)


def _clean_line(text: str) -> str:
    return " ".join(str(text or "").strip().split())


def extract_pdf_rows(pdf_path: Path) -> pd.DataFrame:
    try:
        import fitz
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("PyMuPDF is required: pip install pymupdf") from exc

    doc = fitz.open(pdf_path)
    rows: list[dict[str, object]] = []

    for page_idx in range(doc.page_count):
        lines = [_clean_line(line) for line in doc.load_page(page_idx).get_text("text").splitlines()]
        lines = [line for line in lines if line]

        desc_buffer: list[str] = []
        for i, line in enumerate(lines):
            code_match = CODE_RE.match(line)
            if code_match:
                code = code_match.group(1)
                # Build description from recent non-header lines.
                desc_candidates = [x for x in desc_buffer[-8:] if not IGNORE_LINE_RE.search(x)]
                description_blob = " ".join(desc_candidates).strip()

                # Capture nearby rate tokens after the code row.
                rate_tokens: list[str] = []
                for j in range(i + 1, min(i + 10, len(lines))):
                    nxt = lines[j]
                    if CODE_RE.match(nxt):
                        break
                    found = RATE_RE.findall(nxt)
                    for token in found:
                        token = _clean_line(token)
                        if token and token not in rate_tokens:
                            rate_tokens.append(token)
                    if len(rate_tokens) >= 2:
                        break

                # Keep short context for manual QA.
                context = " | ".join(lines[max(0, i - 3) : min(len(lines), i + 6)])
                rows.append(
                    {
                        "pdf_file": pdf_path.name,
                        "page": page_idx + 1,
                        "hs_code": code,
                        "hs_digits": code.replace(".", ""),
                        "description_blob": description_blob,
                        "rate_token_1": rate_tokens[0] if len(rate_tokens) >= 1 else pd.NA,
                        "rate_token_2": rate_tokens[1] if len(rate_tokens) >= 2 else pd.NA,
                        "context_excerpt": context,
                    }
                )
                continue

            # Maintain description buffer with meaningful lines.
            if not IGNORE_LINE_RE.search(line):
                desc_buffer.append(line)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["code_level"] = out["hs_digits"].astype("string").str.len()
    return out


def _default_output_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "data" / "staging" / "passthru_data" / "policy" / "pdf_extract"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _run_single(pdf_path: Path, out_path: Path) -> dict[str, object]:
    frame = extract_pdf_rows(pdf_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_path, index=False)
    return {
        "release_name": pdf_path.stem,
        "pdf_path": str(pdf_path),
        "output_path": str(out_path),
        "rows": int(len(frame)),
        "status": "ok",
    }


def _run_batch(
    start_year: int,
    end_year: int,
    pdf_dir: Path,
    out_dir: Path,
    fallback_only: bool,
) -> dict[str, object]:
    root = Path(__file__).resolve().parents[2]
    catalog_path = root / "data" / "reference" / "passthru_data" / "policy_release_catalog.csv"
    if not catalog_path.exists():
        raise FileNotFoundError(f"Missing release catalog: {catalog_path}")
    catalog = pd.read_csv(catalog_path)
    catalog["year"] = pd.to_numeric(catalog["year"], errors="coerce").astype("Int64")
    catalog = catalog[catalog["year"].between(start_year, end_year)].copy()

    fallback_releases: set[str] = set()
    if fallback_only:
        status_path = root / "data" / "verification" / "passthru_data" / "policy_machine_vs_pdf_status_2017_2019.csv"
        if status_path.exists():
            status = pd.read_csv(status_path)
            fallback = status[~status["has_machine_readable_local"].astype(bool)]
            fallback_releases = set(fallback["release_name"].astype("string").dropna().astype(str).tolist())

    records: list[dict[str, object]] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    for _, row in catalog.sort_values(["year", "release_name"]).iterrows():
        release = str(row.get("release_name") or "").strip()
        if not release:
            continue
        if fallback_only and release not in fallback_releases:
            records.append({"release_name": release, "status": "skipped_machine_available", "rows": 0})
            continue
        pdf_path = pdf_dir / f"{release}.pdf"
        if not pdf_path.exists():
            records.append({"release_name": release, "status": "missing_pdf", "rows": 0})
            continue
        out_path = out_dir / f"{release}_extracted_rows.csv"
        try:
            result = _run_single(pdf_path, out_path)
            result["year"] = int(row["year"]) if pd.notna(row["year"]) else pd.NA
            records.append(result)
        except Exception as exc:
            records.append(
                {
                    "release_name": release,
                    "year": int(row["year"]) if pd.notna(row["year"]) else pd.NA,
                    "status": f"failed_{type(exc).__name__}",
                    "rows": 0,
                    "error": str(exc),
                }
            )
    frame = pd.DataFrame(records)
    manifest = out_dir / f"pdf_extract_manifest_{start_year}_{end_year}.csv"
    frame.to_csv(manifest, index=False)
    return {
        "manifest_path": str(manifest),
        "attempted_releases": int(len(frame)),
        "ok_releases": int((frame["status"] == "ok").sum()) if not frame.empty else 0,
        "missing_pdf_releases": int((frame["status"] == "missing_pdf").sum()) if not frame.empty else 0,
        "failed_releases": int(frame["status"].astype("string").str.startswith("failed_").sum()) if not frame.empty else 0,
        "total_rows_extracted": int(frame.loc[frame["status"] == "ok", "rows"].sum()) if not frame.empty else 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract HTS rows heuristically from one PDF or batch of releases.")
    parser.add_argument("--pdf", help="Path to one HTS release PDF.")
    parser.add_argument("--out", help="Output CSV path. Defaults under data/staging/passthru_data/policy/pdf_extract/")
    parser.add_argument("--batch", action="store_true", help="Run batch extraction over release catalog.")
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=2019)
    parser.add_argument("--fallback-only", action="store_true", help="Batch mode: extract only releases without local machine-readable files.")
    parser.add_argument("--pdf-dir", default="", help="Directory containing release PDFs. Defaults to raw/passthru_data/policy/archive/pdf.")
    parser.add_argument("--out-dir", default="", help="Batch output directory for extracted CSV files.")
    args = parser.parse_args()

    if args.batch:
        root = Path(__file__).resolve().parents[2]
        pdf_dir = Path(args.pdf_dir).resolve() if args.pdf_dir else root / "data" / "raw" / "passthru_data" / "policy" / "archive" / "pdf"
        out_dir = Path(args.out_dir).resolve() if args.out_dir else _default_output_dir()
        result = _run_batch(
            start_year=args.start_year,
            end_year=args.end_year,
            pdf_dir=pdf_dir,
            out_dir=out_dir,
            fallback_only=bool(args.fallback_only),
        )
        for key, value in result.items():
            print(f"{key}: {value}")
        return 0

    if not args.pdf:
        raise ValueError("--pdf is required unless --batch is used.")
    pdf_path = Path(args.pdf).resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    out_path = Path(args.out).resolve() if args.out else _default_output_dir() / f"{pdf_path.stem}_extracted_rows.csv"

    result = _run_single(pdf_path, out_path)
    frame = pd.read_csv(out_path)
    print(f"rows: {result['rows']}")
    print(f"output: {result['output_path']}")
    if len(frame):
        print("code_level_counts:")
        print(frame["code_level"].value_counts().sort_index().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
