"""Pipeline — ingest + mock validators + mock LLM extraction."""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from backend.models.prospect import Prospect
from backend.pipeline import email_validator, llm_extractor, web_enricher
from backend.pipeline.ingest import ingest_file


def _write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_ingest_basic(db: Session, tmp_path: Path):
    csv_path = tmp_path / "leads.csv"
    _write_csv(
        csv_path,
        ["Name", "Email", "Phone", "Company", "City", "State"],
        [
            {
                "Name": "Raj Patel",
                "Email": "raj@example.in",
                "Phone": "9876543210",
                "Company": "ABC Plastics",
                "City": "Pune",
                "State": "Maharashtra",
            },
            {
                "Name": "Neha Shah",
                "Email": "neha@x.com",
                "Phone": "9123456789",
                "Company": "XYZ Polymers",
                "City": "Mumbai",
                "State": "Maharashtra",
            },
        ],
    )

    result = ingest_file(db, csv_path, uploaded_by="tester")
    assert result.total == 2
    assert result.new == 2
    assert result.duplicates == 0

    prospects = db.query(Prospect).filter(Prospect.email.in_(["raj@example.in", "neha@x.com"])).all()
    assert len(prospects) == 2
    raj = next(p for p in prospects if p.email == "raj@example.in")
    assert raj.phone == "+919876543210"
    assert raj.company_name == "ABC Plastics"
    assert raj.data_quality_score is not None and raj.data_quality_score > 0.8


def test_ingest_dedup_within_file(db: Session, tmp_path: Path):
    csv_path = tmp_path / "dup.csv"
    _write_csv(
        csv_path,
        ["Email", "Name"],
        [
            {"Email": "dup@x.com", "Name": "Dup1"},
            {"Email": "dup@x.com", "Name": "Dup2"},
            {"Email": "DUP@X.COM", "Name": "Dup3"},
        ],
    )
    result = ingest_file(db, csv_path)
    assert result.new == 1
    assert result.duplicates == 2


def test_ingest_skips_invalid_email(db: Session, tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    _write_csv(
        csv_path,
        ["Email", "Name"],
        [
            {"Email": "", "Name": "Empty"},
            {"Email": "not-an-email", "Name": "Bad"},
            {"Email": "ok@x.com", "Name": "Good"},
        ],
    )
    result = ingest_file(db, csv_path)
    assert result.new == 1
    assert result.skipped == 2


def test_ingest_rejects_csv_without_email_column(db: Session, tmp_path: Path):
    csv_path = tmp_path / "noemail.csv"
    _write_csv(csv_path, ["Name", "Phone"], [{"Name": "x", "Phone": "1"}])
    with pytest.raises(ValueError, match="email column"):
        ingest_file(db, csv_path)


def test_email_validator_mock_mode_is_deterministic():
    a = asyncio.run(email_validator.validate_emails(["alice@x.com", "bob@y.com"]))
    b = asyncio.run(email_validator.validate_emails(["alice@x.com", "bob@y.com"]))
    assert a["alice@x.com"].status == b["alice@x.com"].status
    assert a["alice@x.com"].status in {"valid", "catch-all", "invalid"}


def test_web_enricher_mock_returns_results():
    out = asyncio.run(
        web_enricher.enrich_batch(
            [{"email": "x@y.com", "name": "X", "company_name": "Foo Co", "city": "Pune"}]
        )
    )
    assert "x@y.com" in out
    assert out["x@y.com"].company_results
    assert out["x@y.com"].person_results


def test_llm_extractor_mock_picks_taxonomy_industry():
    snippets = {
        "company_results": [
            {"title": "Foo", "link": "https://www.foo.in", "snippet": "Foo Co manufactures plastics."},
            {"title": "LinkedIn", "link": "https://www.linkedin.com/company/foo", "snippet": "Foo on LinkedIn"},
        ],
        "person_results": [
            {"title": "Person", "link": "https://www.linkedin.com/in/person", "snippet": "..."},
        ],
    }
    taxonomy = ["Plastics & Polymers", "Packaging", "Unknown"]
    extracted = asyncio.run(
        llm_extractor.extract_fields(
            {"name": "X", "company_name": "Foo Co", "city": "Pune", "state": "MH"},
            snippets,
            taxonomy,
        )
    )
    assert extracted.industry in taxonomy
    assert 0.0 <= extracted.industry_confidence <= 1.0
    assert extracted.company_linkedin == "https://www.linkedin.com/company/foo"
    assert extracted.person_linkedin == "https://www.linkedin.com/in/person"
