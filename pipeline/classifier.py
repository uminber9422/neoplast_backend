"""Industry classifier — currently a thin wrapper around LLM extraction.

Kept as its own module so we can later swap in a rules-based or ML classifier
without touching the orchestrator.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from backend.models.prospect import Prospect
from backend.utils.industry_taxonomy import get_taxonomy


def reclassify_unknowns(db: Session, taxonomy: list[str] | None = None) -> int:
    """Force-reset Prospect.industry to 'Unknown' when its current value
    isn't in the active taxonomy. Useful after taxonomy edits.
    """
    active = taxonomy or get_taxonomy(db)
    active_set = set(active)
    rows = (
        db.query(Prospect)
        .filter(Prospect.industry.isnot(None))
        .filter(~Prospect.industry.in_(active_set))
        .all()
    )
    count = 0
    for row in rows:
        row.industry = "Unknown"
        count += 1
    if count:
        db.commit()
    return count
