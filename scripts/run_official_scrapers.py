#!/usr/bin/env python3
"""
Runner para scrapers de fuentes oficiales.

Uso:
  python scripts/run_official_scrapers.py                    # todos los juegos registrados
  python scripts/run_official_scrapers.py --game powerball   # un juego específico
  python scripts/run_official_scrapers.py --dry-run          # sin escribir a DB
  python scripts/run_official_scrapers.py --shadow           # compara contra DB sin escribir
"""

import argparse
import sys
from datetime import date

# ---------------------------------------------------------------------------
# Paths – ensure the repo root is on sys.path when the script is executed
# directly (e.g. python scripts/run_official_scrapers.py)
# ---------------------------------------------------------------------------
import os

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from scrapers.registry import SCRAPER_REGISTRY, get_scraper  # noqa: E402
from app.pipeline.writer import NormalizedDraw, save_draw  # noqa: E402


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Runner para scrapers de fuentes oficiales (NY Open Data)."
    )
    parser.add_argument(
        "--game",
        metavar="SLUG",
        default=None,
        help="Juego específico a scrapear (ej: powerball, mega-millions). "
             "Si se omite, se procesan todos los juegos del registro.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Imprime los NormalizedDraw sin escribir nada en la base de datos.",
    )
    parser.add_argument(
        "--shadow",
        action="store_true",
        help="Compara el resultado del scraper oficial contra el último draw "
             "en la DB e imprime diferencias, sin modificar nada.",
    )
    return parser.parse_args()


def _shadow_compare(game_slug: str, scraped: NormalizedDraw) -> None:
    """
    Carga el último draw guardado en DB para *game_slug* y lo compara con
    *scraped*.  Imprime las diferencias encontradas (si hay alguna).
    """
    from sqlalchemy import select
    from app.database import SessionLocal
    from app.models import Draw, Game

    db = SessionLocal()
    try:
        game = db.execute(
            select(Game).where(Game.slug == game_slug)
        ).scalar_one_or_none()

        if game is None:
            print(f"  [shadow] WARN: juego '{game_slug}' no encontrado en games.")
            return

        latest_draw: Draw | None = db.execute(
            select(Draw)
            .where(Draw.game_id == game.id, Draw.draw_type == scraped.draw_type)
            .order_by(Draw.draw_date.desc())
        ).scalar_one_or_none()

        if latest_draw is None:
            print(f"  [shadow] INFO: no hay draws en DB para '{game_slug}'.")
            return

        db_date: date = latest_draw.draw_date
        sc_date: date = scraped.draw_date

        if db_date != sc_date:
            print(f"  [shadow] DIFF draw_date: DB={db_date}  scraper={sc_date}")
            return  # fechas distintas → no comparar números

        diffs = []

        db_nums = latest_draw.main_numbers or []
        if list(db_nums) != scraped.main_numbers:
            diffs.append(f"main_numbers: DB={db_nums}  scraper={scraped.main_numbers}")

        if latest_draw.bonus_number != scraped.bonus_number:
            diffs.append(
                f"bonus_number: DB={latest_draw.bonus_number}  scraper={scraped.bonus_number}"
            )

        if latest_draw.multiplier != scraped.multiplier:
            diffs.append(
                f"multiplier: DB={latest_draw.multiplier}  scraper={scraped.multiplier}"
            )

        if diffs:
            print(f"  [shadow] DIFFS para {game_slug} ({sc_date}):")
            for d in diffs:
                print(f"    {d}")
        else:
            print(f"  [shadow] OK — {game_slug} ({sc_date}) coincide con DB.")
    finally:
        db.close()


def _run_scraper(game_slug: str, dry_run: bool, shadow: bool) -> str:
    """
    Ejecuta el scraper para *game_slug* y actúa según el modo seleccionado.

    Returns:
        "ok", "error", or "skipped"
    """
    scraper = get_scraper(game_slug)
    if scraper is None:
        print(f"[{game_slug}] ERROR: no existe scraper registrado.")
        return "skipped"

    try:
        normalized: NormalizedDraw = scraper.scrape_latest()
    except Exception as exc:  # noqa: BLE001
        print(f"[{game_slug}] ERROR al scrapear: {exc}")
        return "error"

    print(
        f"[{game_slug}] {normalized.draw_date}  "
        f"main={normalized.main_numbers}  "
        f"bonus={normalized.bonus_number}  "
        f"multiplier={normalized.multiplier}  "
        f"confidence={normalized.confidence_score}"
    )

    if dry_run:
        print(f"  [dry-run] sin escritura en DB.")
        return "ok"

    if shadow:
        _shadow_compare(game_slug, normalized)
        return "ok"

    # Modo normal: guardar en DB
    from app.database import SessionLocal

    db = SessionLocal()
    try:
        status = save_draw(db, normalized)
        print(f"  [DB] {status}")
    except Exception as exc:  # noqa: BLE001
        print(f"  [DB] ERROR al guardar: {exc}")
        return "error"
    finally:
        db.close()

    return "ok"


def main() -> None:
    args = _parse_args()

    if args.game:
        slugs = [args.game]
    else:
        slugs = list(SCRAPER_REGISTRY.keys())

    results = {"ok": 0, "error": 0, "skipped": 0}

    print("=" * 70)
    print("RUNNER — scrapers de fuentes oficiales")
    print(f"  Modo: {'dry-run' if args.dry_run else 'shadow' if args.shadow else 'normal'}")
    print(f"  Juegos: {', '.join(slugs)}")
    print("=" * 70)

    for slug in slugs:
        status = _run_scraper(slug, dry_run=args.dry_run, shadow=args.shadow)
        results[status] = results.get(status, 0) + 1

    print()
    print("=" * 70)
    print("RESUMEN")
    print(f"  OK:      {results['ok']}")
    print(f"  Errors:  {results['error']}")
    print(f"  Skipped: {results['skipped']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
