"""Tests for the LLM-powered CSV column detection fallback.

Two surfaces:
    1. ``llm_extractor.detect_columns`` — mock behavior + JSON coercion
    2. ``ingest.ingest_file`` — fallback only fires when fuzzy match misses email
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.orm import Session

from backend.models.prospect import Prospect
from backend.pipeline import ingest, llm_extractor
from backend.pipeline.ingest import ingest_file


def _write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# detect_columns — mock mode + JSON coercion
# ---------------------------------------------------------------------------


def test_detect_columns_returns_empty_in_mock_mode(monkeypatch: pytest.MonkeyPatch):
    """No API key → returns empty dict (caller falls back to fuzzy as-is).

    We force-clear the API key in case the test environment has one set in .env;
    otherwise the function would actually hit OpenAI and return a real mapping.
    """
    from backend.config import get_settings
    settings = get_settings()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "", raising=False)
    out = llm_extractor.detect_columns(
        ["Mailto", "Person", "Org"],
        [{"Mailto": "x@y.com", "Person": "X", "Org": "Y"}],
    )
    assert out == {}


def test_detect_columns_empty_headers_returns_empty():
    out = llm_extractor.detect_columns([], [])
    assert out == {}


def test_coerce_filters_unknown_canonical_fields():
    """LLM hallucinates a non-canonical field → it gets dropped."""
    raw = {
        "EMAIL": "email",
        "PERSON": "name",
        "WEIRD": "linkedin_url",  # not a canonical field
    }
    headers = ["EMAIL", "PERSON", "WEIRD", "OTHER"]
    out = llm_extractor._coerce_column_mapping(raw, headers)
    assert out == {"EMAIL": "email", "PERSON": "name"}


def test_coerce_filters_unknown_headers():
    """LLM invents a header that isn't in the file → it gets dropped."""
    raw = {"EMAIL": "email", "GHOST_HEADER": "name"}
    headers = ["EMAIL", "PERSON"]
    out = llm_extractor._coerce_column_mapping(raw, headers)
    assert out == {"EMAIL": "email"}


def test_coerce_filters_non_string_values():
    raw = {"EMAIL": "email", "ID": 42, "TS": None}
    headers = ["EMAIL", "ID", "TS"]
    out = llm_extractor._coerce_column_mapping(raw, headers)
    assert out == {"EMAIL": "email"}


def test_coerce_handles_non_dict_input():
    """LLM returns a list or string instead of a dict → empty mapping, no crash."""
    assert llm_extractor._coerce_column_mapping([], ["A"]) == {}
    assert llm_extractor._coerce_column_mapping("oops", ["A"]) == {}


def test_coerce_dedupes_repeated_header_keys():
    """Defensive: if the LLM somehow emits the same header twice, first wins."""
    raw = {"EMAIL": "email"}  # dicts can't really repeat but exercise the guard
    headers = ["EMAIL"]
    assert llm_extractor._coerce_column_mapping(raw, headers) == {"EMAIL": "email"}


def test_detect_columns_with_mocked_openai(monkeypatch: pytest.MonkeyPatch):
    """End-to-end test with the OpenAI call stubbed — verifies prompt → response → coerce flow."""

    captured: dict[str, Any] = {}

    def fake_call(api_key, model, system_prompt, user_prompt):
        captured["system"] = system_prompt
        captured["user"] = user_prompt
        return {
            "MAILTO": "email",
            "PERSON_IN_CHARGE": "name",
            "BIZ": "company_name",
            "LOC": "city",
            "BOGUS": "totally_not_a_real_field",
        }

    monkeypatch.setattr(llm_extractor, "_call_openai_sync", fake_call)
    # Pretend we have an API key
    from backend.config import get_settings
    settings = get_settings()
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-test-fake-key", raising=False)
    get_settings.cache_clear()
    monkeypatch.setattr(
        "backend.pipeline.llm_extractor.get_settings",
        lambda: type(settings)(SECRET_KEY=settings.SECRET_KEY, OPENAI_API_KEY="sk-test"),
    )

    headers = ["MAILTO", "PERSON_IN_CHARGE", "BIZ", "LOC", "BOGUS"]
    out = llm_extractor.detect_columns(headers, [{"MAILTO": "x@y.com"}])
    # Bogus mapping was filtered out
    assert out == {
        "MAILTO": "email",
        "PERSON_IN_CHARGE": "name",
        "BIZ": "company_name",
        "LOC": "city",
    }
    # Prompt should mention canonical fields and include the headers
    assert "MAILTO" in captured["user"]
    assert "email" in captured["user"]


# ---------------------------------------------------------------------------
# Ingest LLM fallback — fires only when fuzzy misses email
# ---------------------------------------------------------------------------


def test_ingest_does_not_invoke_llm_for_clean_csv(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Normal CSV with recognized headers should NOT trigger detect_columns."""
    calls: list[Any] = []

    def spy(headers, sample_rows):
        calls.append((headers, sample_rows))
        return {}

    monkeypatch.setattr(llm_extractor, "detect_columns", spy)
    # Also patch the symbol that ingest imports
    monkeypatch.setattr(ingest.llm_extractor, "detect_columns", spy)

    csv_path = tmp_path / "clean.csv"
    _write_csv(
        csv_path,
        ["Email", "Name", "Company"],
        [{"Email": "a@b.com", "Name": "Alice", "Company": "Acme"}],
    )
    result = ingest_file(db, csv_path)
    assert result.new == 1
    assert calls == [], "detect_columns should not be called when fuzzy finds email"


def test_ingest_invokes_llm_when_fuzzy_misses_email(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Garbled CSV — fuzzy can't find email → LLM fallback fires and rescues the ingest."""
    calls: list[tuple[list[str], list[dict]]] = []

    def llm_returns_email_mapping(headers, sample_rows):
        calls.append((headers, sample_rows))
        return {
            "mailto_field": "email",
            "person_in_charge": "name",
            "biz_outfit": "company_name",
        }

    monkeypatch.setattr(ingest.llm_extractor, "detect_columns", llm_returns_email_mapping)

    csv_path = tmp_path / "garbled.csv"
    _write_csv(
        csv_path,
        ["mailto_field", "person_in_charge", "biz_outfit"],
        [
            {
                "mailto_field": "weird@example.dz",
                "person_in_charge": "Karim Belaid",
                "biz_outfit": "GROUPE RYMM",
            },
        ],
    )
    result = ingest_file(db, csv_path)
    assert result.new == 1
    assert len(calls) == 1, "LLM fallback should fire exactly once"
    # Sample rows should include the cells from row 1
    headers_sent, sample_sent = calls[0]
    assert "mailto_field" in headers_sent
    assert sample_sent[0]["mailto_field"] == "weird@example.dz"

    p = db.query(Prospect).filter(Prospect.email == "weird@example.dz").one()
    assert p.name == "Karim Belaid"
    assert p.company_name == "GROUPE RYMM"


def test_ingest_still_raises_when_llm_also_misses_email(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """LLM returns a mapping but email isn't in it → still raises, with updated message."""
    monkeypatch.setattr(ingest.llm_extractor, "detect_columns", lambda h, s: {"phone": "phone"})

    csv_path = tmp_path / "noemail.csv"
    _write_csv(csv_path, ["phone", "weird"], [{"phone": "9876543210", "weird": "x"}])
    with pytest.raises(ValueError, match=r"No email column found .* \(after LLM fallback\)"):
        ingest_file(db, csv_path)


def test_ingest_still_raises_when_llm_returns_empty(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Mock-mode behavior: LLM returns {} → fall through to the same error as before."""
    monkeypatch.setattr(ingest.llm_extractor, "detect_columns", lambda h, s: {})

    csv_path = tmp_path / "noemail.csv"
    _write_csv(csv_path, ["weirdcol", "another"], [{"weirdcol": "x", "another": "y"}])
    with pytest.raises(ValueError, match="No email column found"):
        ingest_file(db, csv_path)


def test_ingest_llm_mapping_merges_with_fuzzy_partial(
    db: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Fuzzy got 'name' from a recognized header but missed email; LLM fills email.
    Both mappings should compose — name from fuzzy, email from LLM."""

    def llm_fills_email(headers, sample_rows):
        # LLM only returns the email mapping; the rest came from fuzzy
        return {"contact_addr": "email"}

    monkeypatch.setattr(ingest.llm_extractor, "detect_columns", llm_fills_email)

    csv_path = tmp_path / "partial.csv"
    _write_csv(
        csv_path,
        ["Name", "contact_addr", "Company"],  # Name + Company recognized by fuzzy
        [{"Name": "Alice", "contact_addr": "alice@a.com", "Company": "Acme"}],
    )
    result = ingest_file(db, csv_path)
    assert result.new == 1
    p = db.query(Prospect).filter(Prospect.email == "alice@a.com").one()
    assert p.name == "Alice"            # fuzzy mapping survived
    assert p.company_name == "Acme"     # fuzzy mapping survived
    assert p.email == "alice@a.com"     # LLM fallback supplied this


def test_sample_rows_helper_strips_blanks_and_truncates():
    """White-box check on the sample-row builder."""
    import pandas as pd
    df = pd.DataFrame(
        [
            {"a": "  ", "b": "hello", "c": "x" * 300},
            {"a": "world", "b": "", "c": "short"},
            {"a": "", "b": "", "c": ""},  # all blank — skipped
            {"a": "third", "b": "row", "c": "ok"},
        ]
    )
    sample = ingest._sample_rows_for_llm(df, max_rows=2)
    assert len(sample) == 2
    assert sample[0] == {"b": "hello", "c": "x" * 200}  # blank 'a' stripped, c truncated
    assert sample[1] == {"a": "world", "c": "short"}     # blank 'b' stripped
