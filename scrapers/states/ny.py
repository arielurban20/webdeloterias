"""New York State Lottery scraper — NY Open Data (data.ny.gov)."""
import logging
from datetime import datetime
from typing import List, Optional
import httpx
from app.pipeline.base_scraper import BaseScraper
from app.pipeline.writer import NormalizedDraw

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LotteryBot/1.0)"}
TIMEOUT = 30


class NYLotteryScraper(BaseScraper):
    source_name = "NY Open Data (Official)"
    state_slug = "ny"

    ENDPOINTS = {
        "numbers-ny": "https://data.ny.gov/resource/dg63-4siq.json",
        "win-4-ny": "https://data.ny.gov/resource/hsys-3def.json",
        "take-5-ny": "https://data.ny.gov/resource/dg63-4siq.json",
        "lotto-ny": "https://data.ny.gov/resource/6nbc-h7bj.json",
        "pick-10-ny": "https://data.ny.gov/resource/bycu-cw7c.json",
    }

    def fetch(self, game_slug: str) -> str:
        url = self.ENDPOINTS.get(game_slug, "")
        if not url:
            return "[]"
        params = {"$order": "draw_date DESC", "$limit": "5"}
        with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            return r.text

    def parse(self, raw: str) -> List[NormalizedDraw]:
        import json
        return json.loads(raw)

    def _parse_date(self, s: str):
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    def _parse_numbers(self, s: str) -> List[int]:
        parts = []
        for x in s.split():
            try:
                parts.append(int(x))
            except ValueError:
                pass
        return parts

    def _make(self, game_slug: str, draw_date, draw_type: str, numbers: List[int], bonus: Optional[str] = None, source_url: str = "") -> NormalizedDraw:
        return NormalizedDraw(
            game_slug=game_slug,
            draw_date=draw_date,
            draw_type=draw_type,
            main_numbers=numbers,
            bonus_number=bonus,
            source_url=source_url,
            source_provider=self.source_name,
            confidence_score=99.0,
            verification_status="verified",
        )

    def scrape_numbers(self) -> List[NormalizedDraw]:
        results = []
        try:
            url = self.ENDPOINTS["numbers-ny"]
            params = {"$order": "draw_date DESC", "$limit": "5"}
            with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
                data = client.get(url, params=params).json()
            for row in data:
                d = self._parse_date(row.get("draw_date", ""))
                if not d:
                    continue
                midday = row.get("midday_winning_numbers") or row.get("midday_numbers")
                evening = row.get("winning_numbers") or row.get("evening_winning_numbers")
                if midday:
                    results.append(self._make("numbers-ny", d, "midday", self._parse_numbers(midday), source_url=url))
                if evening:
                    results.append(self._make("numbers-ny", d, "evening", self._parse_numbers(evening), source_url=url))
        except Exception as e:
            logger.error("NY Numbers scrape failed: %s", e)
        return results

    def scrape_win4(self) -> List[NormalizedDraw]:
        results = []
        try:
            url = self.ENDPOINTS["win-4-ny"]
            params = {"$order": "draw_date DESC", "$limit": "5"}
            with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
                data = client.get(url, params=params).json()
            for row in data:
                d = self._parse_date(row.get("draw_date", ""))
                if not d:
                    continue
                midday = row.get("midday_winning_numbers") or row.get("midday_numbers")
                evening = row.get("winning_numbers") or row.get("evening_winning_numbers")
                if midday:
                    results.append(self._make("win-4-ny", d, "midday", self._parse_numbers(midday), source_url=url))
                if evening:
                    results.append(self._make("win-4-ny", d, "evening", self._parse_numbers(evening), source_url=url))
        except Exception as e:
            logger.error("NY Win4 scrape failed: %s", e)
        return results

    def scrape_take5(self) -> List[NormalizedDraw]:
        results = []
        try:
            url = self.ENDPOINTS["take-5-ny"]
            params = {"$order": "draw_date DESC", "$limit": "5"}
            with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
                data = client.get(url, params=params).json()
            for row in data:
                d = self._parse_date(row.get("draw_date", ""))
                nums_raw = row.get("winning_numbers") or row.get("take5_winning_numbers")
                if not d or not nums_raw:
                    continue
                nums = self._parse_numbers(nums_raw)
                if len(nums) == 5:
                    results.append(self._make("take-5-ny", d, "main", nums, source_url=url))
        except Exception as e:
            logger.error("NY Take5 scrape failed: %s", e)
        return results

    def scrape_lotto(self) -> List[NormalizedDraw]:
        results = []
        try:
            url = self.ENDPOINTS["lotto-ny"]
            params = {"$order": "draw_date DESC", "$limit": "5"}
            with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
                data = client.get(url, params=params).json()
            for row in data:
                d = self._parse_date(row.get("draw_date", ""))
                nums_raw = row.get("winning_numbers")
                bonus_raw = row.get("bonus")
                if not d or not nums_raw:
                    continue
                nums = self._parse_numbers(nums_raw)
                results.append(self._make("lotto-ny", d, "main", nums, bonus=bonus_raw, source_url=url))
        except Exception as e:
            logger.error("NY Lotto scrape failed: %s", e)
        return results

    def scrape_all_latest(self) -> List[NormalizedDraw]:
        results = []
        for fn in [self.scrape_numbers, self.scrape_win4, self.scrape_lotto]:
            results.extend(fn())
        return results
