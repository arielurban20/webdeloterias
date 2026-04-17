"""Mega Millions multi-state lottery scraper."""
import logging
from typing import List

from app.pipeline.base_scraper import BaseScraper
from app.pipeline.writer import NormalizedDraw

logger = logging.getLogger(__name__)


class MegaMillionsScraper(BaseScraper):
    source_name = "Mega Millions (Official)"
    state_slug = "multistate"

    def fetch(self, game_slug: str) -> str:
        return "{}"

    def parse(self, raw: str) -> List[NormalizedDraw]:
        return []

    def scrape_latest(self) -> None:
        return None

    def scrape_all_latest(self) -> List[NormalizedDraw]:
        return []
