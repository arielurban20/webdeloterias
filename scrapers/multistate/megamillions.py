"""
Scraper oficial para Mega Millions usando NY Open Data.

Endpoint: https://data.ny.gov/resource/5xaw-6ayf.json
Documentación: https://dev.socrata.com/foundry/data.ny.gov/5xaw-6ayf
"""

from datetime import datetime
from typing import List

import requests

from app.pipeline.writer import NormalizedDraw

_SOURCE_URL = "https://data.ny.gov/resource/5xaw-6ayf.json"


class MegaMillionsScraper:
    """Obtiene resultados de Mega Millions desde NY Open Data (fuente oficial)."""

    game_slug = "mega-millions"
    source_url = _SOURCE_URL
    source_provider = "NY Open Data (Official)"

    def scrape(self, limit: int = 5) -> List[NormalizedDraw]:
        """
        Descarga los últimos ``limit`` sorteos y retorna una lista de
        NormalizedDraw ordenada del más reciente al más antiguo.
        """
        params = {
            "$order": "draw_date DESC",
            "$limit": limit,
        }
        response = requests.get(self.source_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return [self._parse(row) for row in data]

    def scrape_latest(self) -> NormalizedDraw:
        """Retorna solo el sorteo más reciente."""
        results = self.scrape(limit=1)
        if not results:
            raise ValueError(f"No draws returned from {self.source_url}")
        return results[0]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _parse(self, row: dict) -> NormalizedDraw:
        draw_date = datetime.fromisoformat(row["draw_date"]).date()

        main_numbers = [
            int(n)
            for n in row["winning_numbers"].split()
        ][:5]

        bonus_number = row.get("mega_ball")

        multiplier_raw = row.get("multiplier")
        multiplier = f"X{multiplier_raw}" if multiplier_raw else None

        return NormalizedDraw(
            game_slug=self.game_slug,
            draw_date=draw_date,
            draw_type="main",
            main_numbers=main_numbers,
            bonus_number=bonus_number,
            multiplier=multiplier,
            source_url=self.source_url,
            source_provider=self.source_provider,
            confidence_score=99.0,
            verification_status="verified",
        )
