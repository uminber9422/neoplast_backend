"""Data quality scoring for ingested prospect rows."""

from __future__ import annotations

# Weighted: email is required so its weight reflects baseline rather than bonus.
# Sum of weights = 1.0
FIELD_WEIGHTS: dict[str, float] = {
    "name": 0.15,
    "email": 0.30,
    "phone": 0.10,
    "company_name": 0.20,
    "city": 0.10,
    "state": 0.10,
    "pincode": 0.05,
}


def compute_quality_score(row: dict) -> float:
    """0.0–1.0 completeness score based on populated key fields."""
    score = 0.0
    for field, weight in FIELD_WEIGHTS.items():
        value = row.get(field)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        score += weight
    return round(score, 3)
