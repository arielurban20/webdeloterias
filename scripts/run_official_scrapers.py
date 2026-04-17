#!/usr/bin/env python3
"""
Runner for official lottery scrapers.

Usage:
  python scripts/run_official_scrapers.py                     # write all to DB
  python scripts/run_official_scrapers.py --state fl          # one state, write to DB
  python scripts/run_official_scrapers.py --game powerball    # one game, write to DB
  python scripts/run_official_scrapers.py --dry-run           # print only, no DB
  python scripts/run_official_scrapers.py --shadow            # compare vs DB, no writes
  python scripts/run_official_scrapers.py --state ny --shadow # state in shadow mode
"""
import argparse
import logging
import sys
from app.database import SessionLocal
from app.models import Draw, Game
from app.pipeline.writer import NormalizedDraw, save_draw
from scrapers.registry import SCRAPER_REGISTRY, get_scraper, get_scraper_for_state

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("run_official_scrapers")


def compare_draw(db, normalized: NormalizedDraw) -> dict:
    """Compare normalized draw against latest DB draw. Returns diff dict."""
    from sqlalchemy import select, desc
    game = db.execute(select(Game).where(Game.slug == normalized.game_slug)).scalar_one_or_none()
    if not game:
        return {"status": "game_not_found", "game_slug": normalized.game_slug}
    existing = db.execute(
        select(Draw)
        .where(Draw.game_id == game.id, Draw.draw_date == normalized.draw_date, Draw.draw_type == normalized.draw_type)
    ).scalar_one_or_none()
    if not existing:
        latest = db.execute(
            select(Draw).where(Draw.game_id == game.id).order_by(desc(Draw.draw_date), desc(Draw.id)).limit(1)
        ).scalar_one_or_none()
        return {
            "status": "not_in_db",
            "game_slug": normalized.game_slug,
            "official_date": str(normalized.draw_date),
            "latest_db_date": str(latest.draw_date) if latest else None,
            "official_numbers": normalized.main_numbers,
        }
    diff = {}
    if existing.main_numbers != normalized.main_numbers:
        diff["main_numbers"] = {"db": existing.main_numbers, "official": normalized.main_numbers}
    if existing.bonus_number != normalized.bonus_number:
        diff["bonus_number"] = {"db": existing.bonus_number, "official": normalized.bonus_number}
    return {
        "status": "match" if not diff else "mismatch",
        "game_slug": normalized.game_slug,
        "draw_date": str(normalized.draw_date),
        "diff": diff,
    }


def run(args):
    created = updated = skipped = errors = 0
    draws_to_process = []

    if args.state:
        scraper = get_scraper_for_state(args.state)
        if not scraper:
            logger.error("No scraper found for state: %s", args.state)
            sys.exit(1)
        logger.info("Scraping state: %s", args.state)
        draws_to_process = scraper.scrape_all_latest()

    elif args.game:
        scraper = get_scraper(args.game)
        if not scraper:
            logger.error("No scraper found for game: %s", args.game)
            sys.exit(1)
        logger.info("Scraping game: %s", args.game)
        if hasattr(scraper, "scrape_latest"):
            result = scraper.scrape_latest()
            draws_to_process = [result] if result else []
        else:
            draws_to_process = scraper.scrape_all_latest()

    else:
        for slug, cls in SCRAPER_REGISTRY.items():
            try:
                scraper = cls()
                if hasattr(scraper, "scrape_all_latest"):
                    draws_to_process.extend(scraper.scrape_all_latest())
                elif hasattr(scraper, "scrape_latest"):
                    r = scraper.scrape_latest()
                    if r:
                        draws_to_process.append(r)
            except Exception as e:
                logger.error("Error scraping %s: %s", slug, e)
                errors += 1

    if args.dry_run:
        for nd in draws_to_process:
            print(f"  DRY-RUN: {nd.game_slug} | {nd.draw_date} | {nd.draw_type} | {nd.main_numbers} | bonus={nd.bonus_number}")
        print(f"\nDRY-RUN total: {len(draws_to_process)} draws")
        return

    db = SessionLocal()
    try:
        if args.shadow:
            print("\n--- SHADOW MODE: comparing official vs DB ---")
            for nd in draws_to_process:
                result = compare_draw(db, nd)
                status = result.get("status")
                if status == "match":
                    print(f"  MATCH    {nd.game_slug} {nd.draw_date}")
                elif status == "mismatch":
                    print(f"  MISMATCH {nd.game_slug} {nd.draw_date}: {result['diff']}")
                elif status == "not_in_db":
                    print(f"  NEW      {nd.game_slug} {nd.draw_date} (not in DB yet) official={nd.main_numbers}")
                else:
                    print(f"  UNKNOWN  {nd.game_slug}: {result}")
        else:
            for nd in draws_to_process:
                try:
                    action = save_draw(db, nd)
                    if action == "created":
                        created += 1
                    elif action == "updated":
                        updated += 1
                    else:
                        skipped += 1
                    logger.info("%s %s %s %s -> %s", action.upper(), nd.game_slug, nd.draw_date, nd.draw_type, nd.main_numbers)
                except Exception as e:
                    logger.error("save_draw failed for %s: %s", nd.game_slug, e)
                    errors += 1
    finally:
        db.close()

    print(f"\nSUMMARY: created={created} updated={updated} skipped={skipped} errors={errors}")


def main():
    parser = argparse.ArgumentParser(description="Run official lottery scrapers")
    parser.add_argument("--state", help="State slug (e.g. fl, ny, tx, ca)")
    parser.add_argument("--game", help="Game slug (e.g. powerball, mega-millions)")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    parser.add_argument("--shadow", action="store_true", help="Compare vs DB without writing")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
