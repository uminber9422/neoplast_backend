"""Default industry taxonomy and accessors.

The list is editable via the Settings page; this module is the seed and
the resolver when no DB-stored taxonomy exists yet.
"""

from __future__ import annotations

import json

from sqlalchemy.orm import Session

from backend.models.setting import Setting

DEFAULT_TAXONOMY: list[str] = [
    "Plastics & Polymers",
    "Packaging",
    "Automotive & Auto Components",
    "Pharma & Healthcare",
    "Construction & Building Materials",
    "Food & Beverage Processing",
    "Agriculture & Agri-Processing",
    "Textiles & Garments",
    "Chemicals & Petrochemicals",
    "FMCG",
    "Pipes & Fittings",
    "Electrical & Electronics",
    "Infrastructure & Real Estate",
    "Trading & Distribution",
    "Government / PSU",
    "Rubber & Composites",
    "Recycling & Waste Management",
    "Other Manufacturing",
    "Services / Non-Manufacturing",
    "Unknown",
]

TAXONOMY_KEY = "industry_taxonomy"


def get_taxonomy(db: Session) -> list[str]:
    """Return the active taxonomy (DB override → default)."""
    row = db.query(Setting).filter(Setting.key == TAXONOMY_KEY).one_or_none()
    if row and row.value:
        try:
            data = json.loads(row.value)
            if isinstance(data, list) and all(isinstance(s, str) for s in data):
                return data
        except (json.JSONDecodeError, TypeError):
            pass
    return list(DEFAULT_TAXONOMY)


def set_taxonomy(db: Session, categories: list[str]) -> list[str]:
    """Persist a new taxonomy. Always appends 'Unknown' if missing."""
    cleaned = [c.strip() for c in categories if c and c.strip()]
    if "Unknown" not in cleaned:
        cleaned.append("Unknown")
    row = db.query(Setting).filter(Setting.key == TAXONOMY_KEY).one_or_none()
    payload = json.dumps(cleaned)
    if row is None:
        db.add(Setting(key=TAXONOMY_KEY, value=payload))
    else:
        row.value = payload
    db.commit()
    return cleaned
