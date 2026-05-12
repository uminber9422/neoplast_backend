"""Export filtered prospect rows to CSV or XLSX."""

from __future__ import annotations

import io
from collections.abc import Iterable, Sequence

import pandas as pd

EXPORT_COLUMNS: Sequence[str] = (
    "name",
    "email",
    "phone",
    "fax",
    "company_name",
    "address",
    "city",
    "state",
    "pincode",
    "country",
    "detected_country_code",
    "search_locale",
    "website_csv",
    "notes",
    "industry",
    "sub_category",
    "company_size",
    "company_website",
    "company_linkedin",
    "person_linkedin",
    "company_description",
    "email_status",
    "email_activity",
    "email_activity_score",
    "industry_confidence",
    "relevance_score",
    "data_quality_score",
    "source_file",
    "created_at",
    "enriched_at",
)


def to_dataframe(rows: Iterable[dict]) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=list(EXPORT_COLUMNS))


def export_csv(rows: Iterable[dict]) -> bytes:
    df = to_dataframe(rows)
    return df.to_csv(index=False).encode("utf-8-sig")  # BOM helps Excel detect UTF-8


def export_xlsx(rows: Iterable[dict]) -> bytes:
    df = to_dataframe(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="prospects", index=False)
    return buf.getvalue()
