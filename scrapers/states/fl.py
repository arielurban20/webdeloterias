"""Florida Lottery scraper — flalottery.com official JSON API."""
import logging
from datetime import datetime
from typing import List
import httpx
from app.pipeline.base_scraper import BaseScraper
from app.pipeline.writer import NormalizedDraw

logger = logging.getLogger(__name__)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LotteryBot/1.0)"}
TIMEOUT = 30
BASE_URL = "https://www.flalottery.com/exptkt/winningNumbers.do"

GAMES = {
    "pick-2-fl": "PICK2",
    "pick-3-fl": "PICK3",
    "pick-4-fl": "PICK4",
    "pick-5-fl": "PICK5",
    "lotto-fl": "FL_LOTTO",
    "fantasy-5-fl": "FANTASY5",
}


class FLLotteryScraper(BaseScraper):
    source_name = "Florida Lottery (Official)"
    state_slug = "fl"

    def fetch(self, game_slug: str) -> str:
        abbr = GAMES.get(game_slug)
        if not abbr:
            return "{}"
        params = {"gameNameAbr": abbr, "drawDate": "", "numDraws": "5", "isJson": "true"}
        with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
            r = client.get(BASE_URL, params=params)
            r.raise_for_status()
            return r.text

    def parse(self, raw: str) -> List[NormalizedDraw]:
        import json
        return json.loads(raw)

    def _parse_date(self, s: str):
        try:
            return datetime.strptime(s, "%m/%d/%Y").date()
        except Exception:
            return None

    def _parse_numbers(self, s: str) -> List[int]:
        parts = []
        for x in s.replace("-", " ").split():
            try:
                parts.append(int(x))
            except ValueError:
                pass
        return parts

    def scrape_game(self, game_slug: str) -> List[NormalizedDraw]:
        results = []
        try:
            raw = self.fetch(game_slug)
            import json
            data = json.loads(raw)
            draws = data.get("drawResults") or data.get("winningNumbersList") or []
            abbr = GAMES.get(game_slug, "")
            source_url = f"{BASE_URL}?gameNameAbr={abbr}&isJson=true"
            for row in draws:
                date_str = row.get("drawDate") or row.get("date", "")
                nums_str = row.get("winningNumbers") or row.get("numbers", "")
                jackpot = row.get("jackpotPrize") or row.get("jackpot")
                d = self._parse_date(date_str)
                if not d or not nums_str:
                    continue
                nums = self._parse_numbers(nums_str)
                results.append(NormalizedDraw(
                    game_slug=game_slug,
                    draw_date=d,
                    draw_type="main",
                    main_numbers=nums,
                    jackpot=jackpot,
                    source_url=source_url,
                    source_provider=self.source_name,
                    confidence_score=97.0,
                    verification_status="verified",
                ))
        except Exception as e:
            logger.error("FL %s scrape failed: %s", game_slug, e)
        return results

    def scrape_all_latest(self) -> List[NormalizedDraw]:
        results = []
        for game_slug in GAMES:
            results.extend(self.scrape_game(game_slug))
        return results
