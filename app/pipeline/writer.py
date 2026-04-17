"""
Pipeline writer: NormalizedDraw dataclass and save_draw helper.

NormalizedDraw is the canonical intermediate representation produced by every
official scraper and consumed by the persistence layer.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import Draw, Game


@dataclass
class NormalizedDraw:
    game_slug: str
    draw_date: date
    draw_type: str = "main"
    main_numbers: List[int] = field(default_factory=list)
    bonus_number: Optional[str] = None
    multiplier: Optional[str] = None
    source_url: Optional[str] = None
    source_provider: Optional[str] = None
    confidence_score: float = 0.0
    verification_status: str = "unverified"


def save_draw(db: Session, normalized: NormalizedDraw) -> str:
    """
    Upsert a NormalizedDraw into the draws table.

    Returns:
        "created"  — a new row was inserted.
        "updated"  — an existing row was updated.
        "skipped"  — game not found in the games table.
    """
    game: Optional[Game] = db.execute(
        select(Game).where(Game.slug == normalized.game_slug)
    ).scalar_one_or_none()

    if game is None:
        return "skipped"

    existing: Optional[Draw] = db.execute(
        select(Draw).where(
            Draw.game_id == game.id,
            Draw.draw_date == normalized.draw_date,
            Draw.draw_type == normalized.draw_type,
        )
    ).scalar_one_or_none()

    if existing:
        existing.main_numbers = normalized.main_numbers
        existing.bonus_number = normalized.bonus_number
        existing.multiplier = normalized.multiplier
        existing.source_url = normalized.source_url
        _set_optional(existing, "source_provider", normalized.source_provider)
        _set_optional(existing, "confidence_score", normalized.confidence_score)
        _set_optional(existing, "verification_status", normalized.verification_status)
        _set_optional(existing, "needs_review", False)
        db.commit()
        return "updated"

    row = Draw(
        game_id=game.id,
        draw_date=normalized.draw_date,
        draw_type=normalized.draw_type,
        main_numbers=normalized.main_numbers,
        bonus_number=normalized.bonus_number,
        multiplier=normalized.multiplier,
        source_url=normalized.source_url,
    )
    _set_optional(row, "source_provider", normalized.source_provider)
    _set_optional(row, "confidence_score", normalized.confidence_score)
    _set_optional(row, "verification_status", normalized.verification_status)
    _set_optional(row, "needs_review", False)
    db.add(row)
    db.commit()
    return "created"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _set_optional(obj, attr: str, value) -> None:
    """Set an attribute only when the mapped model exposes that column."""
    try:
        setattr(obj, attr, value)
    except AttributeError:
        pass
