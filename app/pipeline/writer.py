"""
Pipeline writer: NormalizedDraw dataclass and save_draw helper.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import Draw, Game

logger = logging.getLogger(__name__)


@dataclass
class NormalizedDraw:
    game_slug: str
    draw_date: date
    draw_type: str
    main_numbers: List[int]
    bonus_number: Optional[str] = None
    multiplier: Optional[str] = None
    jackpot: Optional[str] = None
    jackpot_change: Optional[str] = None
    cash_payout: Optional[str] = None
    next_draw_text: Optional[str] = None
    next_draw_timezone: Optional[str] = None
    next_draw_relative: Optional[str] = None
    secondary_draws: Optional[Any] = None
    source_url: Optional[str] = None
    source_provider: str = "unknown"
    notes: Optional[str] = None
    raw_payload: Optional[Any] = None
    confidence_score: Optional[float] = None
    verification_status: str = "pending"
    needs_review: bool = False


def save_draw(db: Session, normalized: NormalizedDraw) -> str:
    """Upsert a NormalizedDraw into the database.

    Returns:
        'created'  - a new row was inserted.
        'updated'  - an existing row was updated.
        'skipped'  - game not found in the games table.
    """
    game: Optional[Game] = db.execute(
        select(Game).where(Game.slug == normalized.game_slug.lower())
    ).scalar_one_or_none()

    if game is None:
        logger.warning("save_draw: game not found for slug '%s' — skipping", normalized.game_slug)
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
        existing.jackpot = normalized.jackpot
        existing.jackpot_change = normalized.jackpot_change
        existing.cash_payout = normalized.cash_payout
        existing.next_draw_text = normalized.next_draw_text
        existing.next_draw_timezone = normalized.next_draw_timezone
        existing.next_draw_relative = normalized.next_draw_relative
        existing.secondary_draws = normalized.secondary_draws
        existing.source_url = normalized.source_url
        existing.source_provider = normalized.source_provider
        existing.notes = normalized.notes
        existing.raw_payload = normalized.raw_payload
        existing.confidence_score = normalized.confidence_score
        existing.verification_status = normalized.verification_status
        existing.needs_review = normalized.needs_review
        db.commit()
        return "updated"

    draw = Draw(
        game_id=game.id,
        draw_date=normalized.draw_date,
        draw_type=normalized.draw_type,
        main_numbers=normalized.main_numbers,
        bonus_number=normalized.bonus_number,
        multiplier=normalized.multiplier,
        jackpot=normalized.jackpot,
        jackpot_change=normalized.jackpot_change,
        cash_payout=normalized.cash_payout,
        next_draw_text=normalized.next_draw_text,
        next_draw_timezone=normalized.next_draw_timezone,
        next_draw_relative=normalized.next_draw_relative,
        secondary_draws=normalized.secondary_draws,
        source_url=normalized.source_url,
        source_provider=normalized.source_provider,
        notes=normalized.notes,
        raw_payload=normalized.raw_payload,
        confidence_score=normalized.confidence_score,
        verification_status=normalized.verification_status,
        needs_review=normalized.needs_review,
    )
    db.add(draw)
    db.commit()
    return "created"
