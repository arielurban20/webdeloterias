#!/usr/bin/env python3
"""Master runner para VPS — ejecutar desde crontab.

Uso:
  python scripts/run_all_scrapers.py              # produccion: escribe a DB
  python scripts/run_all_scrapers.py --dry-run    # sin escribir a DB
"""
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.pipeline.writer import save_draw
from scrapers.registry import SCRAPER_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("run_all_scrapers")


def main():
    parser = argparse.ArgumentParser(description="Master scraper runner for VPS cron")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    args = parser.parse_args()

    total_created = total_updated = total_skipped = total_errors = 0
    all_draws = []

    for slug, cls in SCRAPER_REGISTRY.items():
        try:
            scraper = cls()
            if hasattr(scraper, "scrape_all_latest"):
                draws = scraper.scrape_all_latest()
            elif hasattr(scraper, "scrape_latest"):
                d = scraper.scrape_latest()
                draws = [d] if d else []
            else:
                draws = []
            logger.info("Scraped %s: %d draws", slug, len(draws))
            all_draws.extend(draws)
        except Exception as e:
            logger.error("Scraper %s failed: %s", slug, e)
            total_errors += 1

    if args.dry_run:
        for nd in all_draws:
            print(f"  DRY-RUN: {nd.game_slug} | {nd.draw_date} | {nd.draw_type} | {nd.main_numbers}")
        print(f"\nDRY-RUN: {len(all_draws)} draws found, nothing written")
        return

    db = SessionLocal()
    try:
        for nd in all_draws:
            try:
                action = save_draw(db, nd)
                if action == "created":
                    total_created += 1
                elif action == "updated":
                    total_updated += 1
                else:
                    total_skipped += 1
            except Exception as e:
                logger.error("save_draw error for %s %s: %s", nd.game_slug, nd.draw_date, e)
                total_errors += 1
    finally:
        db.close()

    logger.info(
        "DONE — created=%d updated=%d skipped=%d errors=%d",
        total_created, total_updated, total_skipped, total_errors
    )
    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
