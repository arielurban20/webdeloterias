"""Scraper registry: maps game_slug or state_slug -> scraper class."""

from scrapers.multistate.powerball import PowerballScraper
from scrapers.multistate.megamillions import MegaMillionsScraper
from scrapers.states.ny import NYLotteryScraper
from scrapers.states.fl import FLLotteryScraper
from scrapers.states.tx import TXLotteryScraper
from scrapers.states.ca import CALotteryScraper

SCRAPER_REGISTRY = {
    # Multi-state games
    "powerball": PowerballScraper,
    "mega-millions": MegaMillionsScraper,
    # States
    "ny": NYLotteryScraper,
    "fl": FLLotteryScraper,
    "tx": TXLotteryScraper,
    "ca": CALotteryScraper,
}


def get_scraper(game_slug: str):
    """Return an instance of the scraper for the given game_slug, or None."""
    cls = SCRAPER_REGISTRY.get(game_slug)
    return cls() if cls else None


def get_scraper_for_state(state_slug: str):
    """Return an instance of the state scraper for the given state_slug, or None."""
    cls = SCRAPER_REGISTRY.get(state_slug.lower())
    return cls() if cls else None
