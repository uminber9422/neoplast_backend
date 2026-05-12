"""Unit tests for utility modules."""

from __future__ import annotations

from pathlib import Path

from backend.utils.columns import map_columns
from backend.utils.filenames import (
    has_allowed_extension,
    is_within_directory,
    sanitize_filename,
)
from backend.utils.phone import normalize_phone
from backend.utils.quality import compute_quality_score


def test_phone_normalize_indian_10_digit():
    assert normalize_phone("9876543210") == "+919876543210"


def test_phone_normalize_with_country_code():
    assert normalize_phone("+91 98765-43210") == "+919876543210"


def test_phone_normalize_strips_leading_zero():
    assert normalize_phone("09876543210") == "+919876543210"


def test_phone_normalize_returns_none_for_garbage():
    assert normalize_phone("---") is None
    assert normalize_phone("") is None
    assert normalize_phone(None) is None


def test_quality_score_full_record():
    score = compute_quality_score(
        {
            "name": "Raj",
            "email": "raj@x.com",
            "phone": "+91...",
            "company_name": "X",
            "city": "Pune",
            "state": "MH",
            "pincode": "411001",
        }
    )
    assert score == 1.0


def test_quality_score_email_only():
    score = compute_quality_score({"email": "x@x.com"})
    assert 0.25 < score < 0.4  # weight 0.30


def test_columns_maps_synonyms():
    headers = ["Full Name", "Email Address", "Mobile Number", "Company"]
    mapping = map_columns(headers)
    assert mapping["Full Name"] == "name"
    assert mapping["Email Address"] == "email"
    assert mapping["Mobile Number"] == "phone"
    assert mapping["Company"] == "company_name"


def test_columns_ignores_unknown():
    mapping = map_columns(["foobar", "Email"])
    assert "foobar" not in mapping
    assert mapping["Email"] == "email"


def test_sanitize_filename_strips_path_traversal():
    out = sanitize_filename("../../etc/passwd")
    assert "/" not in out and "\\" not in out
    assert ".." not in out


def test_sanitize_filename_keeps_extension():
    out = sanitize_filename("Contacts Q1.csv")
    assert out.endswith(".csv")


def test_sanitize_filename_handles_empty():
    assert sanitize_filename("").startswith("upload_")


def test_has_allowed_extension():
    assert has_allowed_extension("data.csv")
    assert has_allowed_extension("data.XLSX")
    assert not has_allowed_extension("data.exe")
    assert not has_allowed_extension("data")


def test_is_within_directory(tmp_path: Path):
    parent = tmp_path / "uploads"
    parent.mkdir()
    inside = parent / "x.csv"
    outside = tmp_path / "x.csv"
    assert is_within_directory(inside, parent)
    assert not is_within_directory(outside, parent)
