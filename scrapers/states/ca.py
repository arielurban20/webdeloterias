"""California Lottery scraper — calottery.com official JSON API."""
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional
import httpx
from app.pipeline.base_scraper import BaseScraper
from app.pipeline.writer import NormalizedDraw

logger = logging.getLogger(__name__)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LotteryBot/1.0)"}
TIMEOUT = 30

GAMES = {
    "fantasy-5-ca": {"id": 19, "draw_type": "main"},
    "lotto-ca": {"id": 17, "draw_type": "main"},
    "daily-3-ca-midday": {"id": 9, "slug": "daily-3-ca", "draw_type": "midday"},
    "daily-3-ca-evening": {"id": 10, "slug": "daily-3-ca", "draw_type": "evening"},
    "daily-4-ca": {"id": 14, "draw_type": "main"},
}


class CALotteryScraper(BaseScraper):
    source_name = "California Lottery (Official)"
    state_slug = "ca"

    def fetch(self, game_slug: str) -> str:
        cfg = GAMES.get(game_slug)
        if not cfg:
            return "{}"
        game_id = cfg["id"]
        url = f"https://www.calottery.com/api/DrawGameApi/DrawGamePastDrawResults/{game_id}/1/5"
        with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
            r = client.get(url)
            r.raise_for_status()
            return r.text

    def parse(self, raw: str) -> List[NormalizedDraw]:
        return []

    def _parse_ms_date(self, s: str):
        m = re.search(r"/Date\((\d+)\)/", s)
        if m:
            ts = int(m.group(1)) / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).date()
        return None

    def _parse_numbers(self, s: str) -> List[int]:
        parts = []
        for x in s.split():
            try:
                parts.append(int(x))
            except ValueError:
                pass
        return parts

    def scrape_game(self, internal_key: str) -> List[NormalizedDraw]:
        results = []
        try:
            cfg = GAMES.get(internal_key)
            if not cfg:
                return results
            game_slug = cfg.get("slug", internal_key)
            draw_type = cfg.get("draw_type", "main")
            game_id = cfg["id"]
            url = f"https://www.calottery.com/api/DrawGameApi/DrawGamePastDrawResults/{game_id}/1/5"
            with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
                data = client.get(url).json()
            rows = data.get("DrawGamePastDrawResults") or []
            for row in rows:
                d = self._parse_ms_date(row.get("DrawDate", ""))
                nums_raw = row.get("WinningNumbers") or row.get("Numbers", "")
                jackpot = row.get("Jackpot") or row.get("JackpotAmount")
                if not d or not nums_raw:
                    continue
                nums = self._parse_numbers(nums_raw)
                results.append(NormalizedDraw(
                    game_slug=game_slug,
                    draw_date=d,
                    draw_type=draw_type,
                    main_numbers=nums,
                    jackpot=str(jackpot) if jackpot else None,
                    source_url=url,
                    source_provider=self.source_name,
                    confidence_score=97.0,
                    verification_status="verified",
                ))
        except Exception as e:
            logger.error("CA %s scrape failed: %s", internal_key, e)
        return results

    def scrape_all_latest(self) -> List[NormalizedDraw]:
        results = []
        for key in GAMES:
            results.extend(self.scrape_game(key))
        return results
