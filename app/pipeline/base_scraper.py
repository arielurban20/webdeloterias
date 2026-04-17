from abc import ABC, abstractmethod
from typing import List

from app.pipeline.writer import NormalizedDraw


class BaseScraper(ABC):
    """Clase base para todos los scrapers de fuentes oficiales."""

    source_name: str = "unknown"
    state_slug: str = ""

    @abstractmethod
    def fetch(self, game_slug: str) -> str:
        """Obtiene el HTML o JSON crudo de la fuente oficial."""
        ...

    @abstractmethod
    def parse(self, raw: str) -> List[NormalizedDraw]:
        """Parsea el contenido crudo y retorna sorteos normalizados."""
        ...

    def scrape(self, game_slug: str) -> List[NormalizedDraw]:
        """Método principal: fetch + parse."""
        raw = self.fetch(game_slug)
        return self.parse(raw)
