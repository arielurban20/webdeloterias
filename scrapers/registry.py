"""
Scraper registry: maps game_slug -> scraper class.
"""

from scrapers.multistate.powerball import PowerballScraper
from scrapers.multistate.megamillions import MegaMillionsScraper

SCRAPER_REGISTRY = {
    "powerball": PowerballScraper,
    "mega-millions": MegaMillionsScraper,
}


def get_scraper(game_slug: str):
    """Return an instance of the scraper for the given game_slug, or None."""
    scraper_class = SCRAPER_REGISTRY.get(game_slug)
    if scraper_class:
        return scraper_class()
    return None
