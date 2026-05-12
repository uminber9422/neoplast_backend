"""CSV / XLSX ingestion: read, normalize, dedup, persist.

Robust to messy real-world spreadsheets: tries multiple encodings, ignores
malformed rows (logs them), preserves unmapped columns in `raw_data`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from email_validator import EmailNotValidError, validate_email
from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.logging_setup import get_logger
from backend.models.prospect import Prospect
from backend.models.upload import UploadHistory
from backend.pipeline import llm_extractor
from backend.utils.columns import map_columns
from backend.utils.phone import normalize_phone
from backend.utils.quality import compute_quality_score

log = get_logger(__name__)


@dataclass
class IngestResult:
    filename: str
    total: int
    new: int
    duplicates: int
    skipped: int
    errors: list[str]


def _read_dataframe(path: Path) -> pd.DataFrame:
    """Read a CSV or XLSX file with encoding fallback. Treats all values as strings."""
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    # CSV — try utf-8, fallback to cp1252 (Windows Excel default)
    for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path.name} with any common encoding.")


def _sample_rows_for_llm(df: pd.DataFrame, *, max_rows: int = 3) -> list[dict]:
    """Pick the first few non-empty rows to send to the LLM column detector.

    We strip None / blank cells per row so the model isn't distracted by them,
    and cap each value at 200 chars to keep the prompt small. Limited to a few
    rows total — the task is column identification, not data extraction.
    """
    sample: list[dict] = []
    for _, row in df.head(max_rows * 3).iterrows():
        cleaned: dict[str, str] = {}
        for col, value in row.items():
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            cleaned[str(col)] = text[:200]
        if cleaned:
            sample.append(cleaned)
        if len(sample) >= max_rows:
            break
    return sample


def _normalize_email(raw: str | None) -> str | None:
    if not raw:
        return None
    candidate = raw.strip().lower()
    if not candidate:
        return None
    try:
        # Don't actually do DNS lookup — we'll do that in the validation step.
        validated = validate_email(candidate, check_deliverability=False)
        return validated.normalized
    except EmailNotValidError:
        return None


_CANONICAL_INGEST_FIELDS: tuple[str, ...] = (
    "name",
    "email",
    "phone",
    "company_name",
    "address",
    "city",
    "state",
    "pincode",
    "country",
    "website_csv",
    "notes",
    "fax",
)


def _normalize_website(raw: str | None) -> str | None:
    """Light normalization for CSV-supplied website URLs.

    Strips whitespace, trims trailing slash. Doesn't add scheme — that's the
    scraper's job. Returns None if empty.
    """
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    # Drop trailing slash for stable storage; keep scheme as-is
    if text.endswith("/") and len(text) > 1:
        text = text[:-1]
    return text


def _row_to_prospect_dict(
    row: pd.Series,
    column_map: dict[str, str],
    source_file: str,
) -> dict[str, Any] | None:
    """Convert a raw row to a normalized prospect dict, or None if unusable."""
    record: dict[str, Any] = {f: None for f in _CANONICAL_INGEST_FIELDS}
    raw_data: dict[str, Any] = {}

    # Track first/last name separately and combine.
    first_name: str | None = None
    last_name: str | None = None

    for original, canonical in column_map.items():
        value = row.get(original)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        # First/last name handling
        normalized_header = original.lower().replace(" ", "").replace("_", "")
        if normalized_header == "firstname":
            first_name = text
            continue
        if normalized_header == "lastname":
            last_name = text
            continue
        record[canonical] = text

    # Combine first/last if name not already populated
    if not record.get("name"):
        if first_name and last_name:
            record["name"] = f"{first_name} {last_name}"
        elif first_name:
            record["name"] = first_name
        elif last_name:
            record["name"] = last_name

    # Preserve unmapped columns in raw_data
    for col, value in row.items():
        if col in column_map:
            continue
        if value is None:
            continue
        text = str(value).strip()
        if text:
            raw_data[str(col)] = text

    record["raw_data"] = raw_data
    record["source_file"] = source_file

    # Email is required (primary key for dedup)
    record["email"] = _normalize_email(record["email"])
    if not record["email"]:
        return None

    record["phone"] = normalize_phone(record["phone"])
    record["website_csv"] = _normalize_website(record["website_csv"])
    record["data_quality_score"] = compute_quality_score(record)
    return record


def ingest_file(
    db: Session,
    file_path: Path,
    *,
    uploaded_by: str | None = None,
) -> IngestResult:
    """Ingest a single CSV/XLSX file. Idempotent: existing emails are kept untouched.

    Returns an IngestResult and writes an UploadHistory row.
    """
    settings = get_settings()
    log.info("ingest_start", file=str(file_path), uploaded_by=uploaded_by)

    if not file_path.exists():
        raise FileNotFoundError(file_path)
    if file_path.stat().st_size > settings.upload_max_bytes:
        raise ValueError(
            f"File exceeds {settings.UPLOAD_MAX_SIZE_MB} MB limit "
            f"({file_path.stat().st_size} bytes)"
        )

    df = _read_dataframe(file_path)
    headers = list(df.columns)
    column_map = map_columns(headers)
    if "email" not in column_map.values():
        # Fuzzy mapper missed the email column — try the LLM fallback before
        # giving up. detect_columns() returns {} when no API key is set, so the
        # mock-mode behavior is identical to the old "raise immediately" path.
        log.info(
            "ingest_llm_column_fallback_invoked",
            file=file_path.name,
            headers=headers[:20],
            fuzzy_mapped=sorted(set(column_map.values())),
        )
        sample_rows = _sample_rows_for_llm(df)
        llm_map = llm_extractor.detect_columns(headers, sample_rows)
        # LLM mapping overrides fuzzy on conflicting headers — fuzzy already
        # missed enough that we're calling LLM, so the model gets the benefit
        # of the doubt for headers it covered.
        column_map = {**column_map, **llm_map}
        if "email" not in column_map.values():
            raise ValueError(
                f"No email column found in {file_path.name} (after LLM fallback). "
                f"Headers: {headers[:10]}"
            )

    total = len(df)
    new_count = 0
    dup_count = 0
    skipped_count = 0
    errors: list[str] = []
    batch: list[Prospect] = []

    # Pre-fetch existing emails in this file's batch to avoid per-row queries.
    candidate_emails: set[str] = set()
    for _, row in df.iterrows():
        email = _normalize_email(row.get(_first_match(column_map, "email")))
        if email:
            candidate_emails.add(email)
    existing_emails: set[str] = set()
    if candidate_emails:
        rows = db.query(Prospect.email).filter(Prospect.email.in_(candidate_emails)).all()
        existing_emails = {r[0] for r in rows}

    seen_in_file: set[str] = set()
    now = datetime.now(timezone.utc)

    for idx, row in df.iterrows():
        try:
            record = _row_to_prospect_dict(row, column_map, file_path.name)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"row {idx}: {exc}")
            skipped_count += 1
            continue
        if record is None:
            skipped_count += 1
            continue
        email = record["email"]
        if email in seen_in_file:
            dup_count += 1
            continue
        seen_in_file.add(email)
        if email in existing_emails:
            dup_count += 1
            continue

        batch.append(
            Prospect(
                **{k: v for k, v in record.items() if k not in {"raw_data", "source_file"}},
                raw_data=record["raw_data"],
                source_file=record["source_file"],
                created_at=now,
                updated_at=now,
            )
        )
        new_count += 1

        if len(batch) >= settings.INGEST_BATCH_SIZE:
            db.bulk_save_objects(batch)
            db.commit()
            batch.clear()

    if batch:
        db.bulk_save_objects(batch)
        db.commit()

    history = UploadHistory(
        filename=file_path.name,
        total_records=total,
        new_records=new_count,
        duplicate_records=dup_count,
        skipped_records=skipped_count,
        uploaded_by=uploaded_by,
    )
    db.add(history)
    db.commit()

    log.info(
        "ingest_complete",
        file=file_path.name,
        total=total,
        new=new_count,
        duplicates=dup_count,
        skipped=skipped_count,
        errors=len(errors),
    )

    return IngestResult(
        filename=file_path.name,
        total=total,
        new=new_count,
        duplicates=dup_count,
        skipped=skipped_count,
        errors=errors[:100],  # cap to keep response/log size bounded
    )


def _first_match(column_map: dict[str, str], canonical: str) -> str:
    """Return the original header matched to `canonical`."""
    for original, c in column_map.items():
        if c == canonical:
            return original
    return ""
