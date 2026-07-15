"""Build monthly HTS schedules from annual baselines and revision files."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import re
import zipfile

import pandas as pd

from .config import PipelineConfig
from .io_utils import ensure_dir, normalize_hs_code, read_table, write_data_dictionary, write_metadata_json, write_parquet


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _annual_files(config: PipelineConfig) -> list[Path]:
    annual_dir = ensure_dir(config.raw_dir / "policy" / "annual")
    return sorted(annual_dir.glob("tariff_data_*.zip"))


def _archive_machine_readable_files(config: PipelineConfig) -> list[Path]:
    archive_data_dir = ensure_dir(config.raw_dir / "policy" / "archive" / "data")
    return sorted(path for path in archive_data_dir.iterdir() if path.is_file() and path.suffix.lower() in {".csv", ".xls", ".xlsx", ".json"})


def _archive_pdf_files(config: PipelineConfig) -> list[Path]:
    archive_pdf_dir = ensure_dir(config.raw_dir / "policy" / "archive" / "pdf")
    return sorted(path for path in archive_pdf_dir.iterdir() if path.is_file() and path.suffix.lower() == ".pdf")


def _required_pdf_release_names(config: PipelineConfig) -> set[str]:
    path = config.verification_dir / "policy_source_downloads.json"
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    attempts = payload.get("download_attempts") or []
    names: set[str] = set()
    for attempt in attempts:
        if attempt.get("source_type") != "archive_pdf_fallback":
            continue
        name = str(attempt.get("release_name") or "").strip()
        if name:
            names.add(name)
    return names


def _extract_year_from_name(name: str) -> int | None:
    match = re.search(r"(19|20)\d{2}", name)
    return int(match.group(0)) if match else None


def _annual_frame(zip_path: Path) -> pd.DataFrame:
    year = _extract_year_from_name(zip_path.name)
    if year is None:
        return pd.DataFrame()
    with zipfile.ZipFile(zip_path) as handle:
        txt_candidates = [name for name in handle.namelist() if name.lower().endswith(".txt")]
        if not txt_candidates:
            return pd.DataFrame()
        txt_name = sorted(txt_candidates)[0]
        with handle.open(txt_name) as raw:
            df = pd.read_csv(raw, dtype=str, engine="python", sep=None, on_bad_lines="skip", encoding="latin1")

    hs_col = None
    for candidate in ("hts8", "hts", "hts_number", "hts_num", "htsno"):
        if candidate in df.columns:
            hs_col = candidate
            break
    if hs_col is None:
        for column in df.columns:
            if str(column).strip().lower().startswith("hts"):
                hs_col = column
                break
    if hs_col is None:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "hs8": df[hs_col].map(lambda value: normalize_hs_code(value, 8)),
            "mfn_text_rate": df.get("mfn_text_rate"),
            "mfn_ad_val_rate": _to_numeric(df.get("mfn_ad_val_rate", pd.Series(index=df.index, dtype="float64"))),
            "additional_duty": df.get("additional_duty"),
            "effective_start": _to_datetime(df.get("begin_effect_date", pd.Series(index=df.index, dtype="object"))),
            "effective_end": _to_datetime(df.get("end_effective_date", pd.Series(index=df.index, dtype="object"))),
        }
    )
    out["effective_start"] = out["effective_start"].fillna(pd.Timestamp(year=year, month=1, day=1))
    out["effective_end"] = out["effective_end"].fillna(pd.Timestamp(year=year, month=12, day=31))
    out["source_type"] = "annual_zip"
    out["release_name"] = f"tariff_data_{year}"
    out["source_priority"] = 1
    out = out.dropna(subset=["hs8"]).drop_duplicates()
    return out


def _release_metadata(config: PipelineConfig) -> pd.DataFrame:
    path = config.reference_dir / "policy_release_catalog.csv"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    keep_columns = [column for column in ["release_name", "machine_readable_stem", "machine_readable_stem_core", "release_start_date", "release_end_date", "release_date"] if column in frame.columns]
    return frame[keep_columns].drop_duplicates()


def _archive_frame(path: Path, release_meta: pd.DataFrame) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, dtype=str, low_memory=False)
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            if "data" in payload and isinstance(payload["data"], list):
                df = pd.DataFrame(payload["data"])
            else:
                df = pd.DataFrame(payload)
        else:
            df = pd.DataFrame(payload)
    elif suffix in {".xls", ".xlsx"}:
        df = pd.read_excel(path, dtype=str)
    else:
        return pd.DataFrame()

    stem = path.stem
    stem_candidates: set[str] = {stem}
    if "_" in stem and stem.rsplit("_", 1)[-1] in {"csv", "json", "xls", "xlsx"}:
        stem_candidates.add(stem.rsplit("_", 1)[0])
    stem_candidates |= {f"{candidate}_data" for candidate in list(stem_candidates)}
    if "machine_readable_stem_core" in release_meta.columns:
        meta_row = release_meta.loc[
            release_meta["machine_readable_stem"].isin(stem_candidates)
            | release_meta["machine_readable_stem_core"].isin(stem_candidates)
        ]
    else:
        meta_row = release_meta.loc[release_meta["machine_readable_stem"].isin(stem_candidates)]
    release_name = meta_row["release_name"].iloc[0] if not meta_row.empty else stem
    release_start = pd.to_datetime(meta_row["release_start_date"].iloc[0], errors="coerce") if not meta_row.empty and "release_start_date" in meta_row.columns else pd.NaT
    release_end = pd.to_datetime(meta_row["release_end_date"].iloc[0], errors="coerce") if not meta_row.empty and "release_end_date" in meta_row.columns else pd.NaT
    release_date = pd.to_datetime(meta_row["release_date"].iloc[0], errors="coerce") if not meta_row.empty and "release_date" in meta_row.columns else pd.NaT

    hs_source = None
    if "hts8" in df.columns:
        hs_source = df["hts8"].map(lambda value: normalize_hs_code(value, 8))
    elif "HTS Number" in df.columns:
        hs_source = df["HTS Number"].map(lambda value: re.sub(r"\D", "", str(value)) if value is not None else None).map(lambda value: normalize_hs_code(value, 8))
    elif "hts10" in df.columns:
        hs_source = df["hts10"].map(lambda value: normalize_hs_code(value, 10)).str.slice(0, 8)
    else:
        return pd.DataFrame()

    mfn_text = None
    if "mfn_text_rate" in df.columns:
        mfn_text = df["mfn_text_rate"]
    elif "General Rate of Duty" in df.columns:
        mfn_text = df["General Rate of Duty"]

    mfn_ad_val = None
    if "mfn_ad_val_rate" in df.columns:
        mfn_ad_val = _to_numeric(df["mfn_ad_val_rate"])
    elif "mfn_ave" in df.columns:
        mfn_ad_val = _to_numeric(df["mfn_ave"])
    else:
        mfn_ad_val = pd.Series(index=df.index, dtype="float64")

    additional_duty = None
    if "additional_duty" in df.columns:
        additional_duty = df["additional_duty"]
    elif "Additional Duties" in df.columns:
        additional_duty = df["Additional Duties"]

    effective_start = _to_datetime(df["begin_effect_date"]) if "begin_effect_date" in df.columns else pd.Series(pd.NaT, index=df.index)
    effective_end = _to_datetime(df["end_effective_date"]) if "end_effective_date" in df.columns else pd.Series(pd.NaT, index=df.index)
    effective_start = effective_start.fillna(release_start).fillna(release_date)
    effective_end = effective_end.fillna(release_end).fillna(pd.Timestamp(year=_extract_year_from_name(path.name) or 2050, month=12, day=31))

    out = pd.DataFrame(
        {
            "hs8": hs_source,
            "mfn_text_rate": mfn_text,
            "mfn_ad_val_rate": mfn_ad_val,
            "additional_duty": additional_duty,
            "effective_start": effective_start,
            "effective_end": effective_end,
        }
    )
    out["source_type"] = f"archive_{suffix.lstrip('.')}"
    out["release_name"] = release_name
    out["source_priority"] = 2
    out = out.dropna(subset=["hs8"]).drop_duplicates()
    return out


def _parse_pdf_rate_token(value: str) -> str | None:
    token = (value or "").strip()
    if not token:
        return None
    first = token.split()[0]
    if first.lower().startswith("free"):
        return "Free"
    return first


def _archive_pdf_frame(path: Path, release_meta: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    release_name = path.stem
    meta_row = release_meta.loc[release_meta["release_name"] == release_name]
    if meta_row.empty:
        return pd.DataFrame()

    try:
        import pdfplumber
    except Exception:
        return pd.DataFrame()

    release_start = pd.to_datetime(meta_row["release_start_date"].iloc[0], errors="coerce") if "release_start_date" in meta_row.columns else pd.NaT
    release_end = pd.to_datetime(meta_row["release_end_date"].iloc[0], errors="coerce") if "release_end_date" in meta_row.columns else pd.NaT
    release_date = pd.to_datetime(meta_row["release_date"].iloc[0], errors="coerce") if "release_date" in meta_row.columns else pd.NaT

    cache_dir = ensure_dir(config.staging_dir / "policy" / "pdf_parsed")
    cache_path = cache_dir / f"{release_name}.parquet"
    if cache_path.exists() and not config.overwrite:
        cached = pd.read_parquet(cache_path)
        if not cached.empty:
            return cached

    line_re = re.compile(r"^(?P<hs4>\d{4})\.(?P<hs2>\d{2})\.(?P<hs2b>\d{2})\s+(?P<suf>\d{2})\b(?P<rest>.*)$")
    rows: list[dict[str, Any]] = []

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text:
                continue
            for raw_line in text.splitlines():
                line = raw_line.strip()
                match = line_re.match(line)
                if not match:
                    continue
                hs10 = f"{match.group('hs4')}{match.group('hs2')}{match.group('hs2b')}{match.group('suf')}"
                rest = match.group("rest").strip()
                dot_ix = rest.find("........")
                tail = rest[dot_ix + 8 :].strip() if dot_ix >= 0 else rest
                mfn_text_rate = _parse_pdf_rate_token(tail)
                rows.append(
                    {
                        "hs8": hs10[:8],
                        "mfn_text_rate": mfn_text_rate,
                        "mfn_ad_val_rate": pd.NA,
                        "additional_duty": pd.NA,
                        "effective_start": release_start if pd.notna(release_start) else release_date,
                        "effective_end": release_end if pd.notna(release_end) else pd.Timestamp(year=_extract_year_from_name(path.name) or 2050, month=12, day=31),
                        "source_type": "archive_pdf_parsed",
                        "release_name": release_name,
                        "source_priority": 2,
                    }
                )

    out = pd.DataFrame(rows).dropna(subset=["hs8"]).drop_duplicates()
    if not out.empty:
        out.to_parquet(cache_path, index=False)
    return out


def _expand_to_months(frame: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    if frame.empty:
        return frame

    start_period = pd.Period(config.start_period, freq="M")
    end_period = pd.Period(config.end_period, freq="M")
    rows: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        if pd.isna(row.effective_start) or pd.isna(row.effective_end):
            continue
        row_start = max(pd.Period(row.effective_start, freq="M"), start_period)
        row_end = min(pd.Period(row.effective_end, freq="M"), end_period)
        if row_start > row_end:
            continue
        for period in pd.period_range(row_start, row_end, freq="M"):
            rows.append(
                {
                    "period": str(period),
                    "hs8": row.hs8,
                    "mfn_text_rate": row.mfn_text_rate,
                    "mfn_ad_val_rate": row.mfn_ad_val_rate,
                    "additional_duty": row.additional_duty,
                    "effective_start": row.effective_start,
                    "effective_end": row.effective_end,
                    "source_type": row.source_type,
                    "release_name": row.release_name,
                    "source_priority": row.source_priority,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["period_obj"] = pd.PeriodIndex(out["period"], freq="M")
    out = out.sort_values(
        ["hs8", "period_obj", "source_priority", "effective_start"],
        ascending=[True, True, False, False],
    ).drop_duplicates(["hs8", "period"], keep="first")
    out["year"] = out["period_obj"].dt.year.astype(int)
    out["month"] = out["period_obj"].dt.month.astype(int)
    out = out.drop(columns=["period_obj"]).reset_index(drop=True)
    return out


def _forward_fill_hs8_rates(monthly_hs8: pd.DataFrame) -> pd.DataFrame:
    """Carry forward last observed ad-valorem rate within HS8 when current month is missing.

    HTS revisions can provide sparse row coverage or omit explicit ad-valorem fields.
    In those cases, the prior month's ad-valorem rate should remain in force unless a
    new numeric rate is explicitly observed.
    """
    if monthly_hs8.empty or "mfn_ad_val_rate" not in monthly_hs8.columns:
        return monthly_hs8

    out = monthly_hs8.copy()
    out["mfn_ad_val_rate"] = _to_numeric(out["mfn_ad_val_rate"])
    out = out.sort_values(["hs8", "year", "month"]).reset_index(drop=True)
    out["mfn_ad_val_rate_observed"] = out["mfn_ad_val_rate"]
    out["mfn_ad_val_rate"] = out.groupby("hs8", sort=False)["mfn_ad_val_rate"].ffill()
    out["mfn_ad_val_rate_ffilled"] = out["mfn_ad_val_rate_observed"].isna() & out["mfn_ad_val_rate"].notna()
    return out


def _hs10_reference(config: PipelineConfig) -> pd.DataFrame:
    reference_path = config.reference_dir / "hs10_codes.parquet"
    if reference_path.exists():
        codes = read_table(reference_path)[["hs10"]].dropna().drop_duplicates()
    else:
        imports_path = config.analysis_dir / "m_flow_hs10_fm_new.parquet"
        exports_path = config.analysis_dir / "x_flow_hs10_fm_new.parquet"
        imports = read_table(imports_path)[["hs10"]]
        exports = read_table(exports_path)[["hs10"]]
        codes = pd.concat([imports, exports], ignore_index=True).dropna().drop_duplicates()
    codes["hs10"] = codes["hs10"].map(lambda value: normalize_hs_code(value, 10))
    codes = codes.dropna().drop_duplicates()
    codes["hs8"] = codes["hs10"].str.slice(0, 8)
    return codes


def run_hts_monthly_schedule_build(config: PipelineConfig) -> dict[str, Any]:
    """Build monthly HTS schedules at HS8 and HS10 level."""
    annual_frames = [_annual_frame(path) for path in _annual_files(config)]
    annual_data = pd.concat([frame for frame in annual_frames if not frame.empty], ignore_index=True) if annual_frames else pd.DataFrame()

    meta = _release_metadata(config)
    archive_frames = [_archive_frame(path, meta) for path in _archive_machine_readable_files(config)]
    archive_data = pd.concat([frame for frame in archive_frames if not frame.empty], ignore_index=True) if archive_frames else pd.DataFrame()

    required_pdf_releases = _required_pdf_release_names(config)
    pdf_candidates = [path for path in _archive_pdf_files(config) if path.stem in required_pdf_releases] if required_pdf_releases else []
    pdf_frames = [_archive_pdf_frame(path, meta, config) for path in pdf_candidates]
    pdf_data = pd.concat([frame for frame in pdf_frames if not frame.empty], ignore_index=True) if pdf_frames else pd.DataFrame()

    combined_parts = [frame for frame in (annual_data, archive_data, pdf_data) if not frame.empty]
    combined = pd.concat(combined_parts, ignore_index=True) if combined_parts else pd.DataFrame()
    monthly_hs8 = _expand_to_months(combined, config)
    monthly_hs8 = _forward_fill_hs8_rates(monthly_hs8)

    hs10_ref = _hs10_reference(config)
    monthly_hs10 = monthly_hs8.merge(hs10_ref, on="hs8", how="inner") if not monthly_hs8.empty else pd.DataFrame(columns=["hs10"])
    if not monthly_hs10.empty:
        monthly_hs10 = monthly_hs10[
            [
                "period",
                "year",
                "month",
                "hs10",
                "hs8",
                "mfn_text_rate",
                "mfn_ad_val_rate",
                "additional_duty",
                "source_type",
                "release_name",
                "effective_start",
                "effective_end",
            ]
        ].sort_values(["hs10", "year", "month"]).reset_index(drop=True)

    hs8_path = config.reference_dir / "hts_monthly_hs8_schedule.parquet"
    hs10_path = config.reference_dir / "hts_monthly_hs10_schedule.parquet"
    write_parquet(monthly_hs8, hs8_path, overwrite=True)
    write_parquet(monthly_hs10, hs10_path, overwrite=True)
    write_data_dictionary(monthly_hs10, config.reference_dir / "hts_monthly_hs10_schedule.dictionary.json", key_columns=["hs10", "year", "month"])
    metadata = {
        "annual_file_count": len(_annual_files(config)),
        "archive_machine_readable_file_count": len(_archive_machine_readable_files(config)),
        "input_rows_annual": int(len(annual_data)),
        "input_rows_archive": int(len(archive_data)),
        "monthly_hs8_rows": int(len(monthly_hs8)),
        "monthly_hs10_rows": int(len(monthly_hs10)),
        "monthly_hs8_mfn_ad_val_rate_ffilled_rows": int(monthly_hs8["mfn_ad_val_rate_ffilled"].sum()) if "mfn_ad_val_rate_ffilled" in monthly_hs8.columns else 0,
        "period_start": config.start_period,
        "period_end": config.end_period,
        "hs8_output": str(hs8_path),
        "hs10_output": str(hs10_path),
    }
    write_metadata_json(config.reference_dir / "hts_monthly_schedule.metadata.json", metadata)
    return metadata
