from scrapers.multistate.powerball import PowerballScraper
from scrapers.multistate.megamillions import MegaMillionsScraper

# Registry: game_slug -> scraper class
SCRAPER_REGISTRY = {
    "powerball": PowerballScraper,
    "mega-millions": MegaMillionsScraper,
}


def get_scraper(game_slug: str):
    """Retorna una instancia del scraper para el game_slug dado, o None si no existe."""
    scraper_class = SCRAPER_REGISTRY.get(game_slug)
    if scraper_class:
        return scraper_class()
    return None
