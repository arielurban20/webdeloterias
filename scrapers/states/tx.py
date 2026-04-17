"""Texas Lottery scraper — txlottery.org official CSV files."""
import csv
import io
import logging
from datetime import date
from typing import List
import httpx
from app.pipeline.base_scraper import BaseScraper
from app.pipeline.writer import NormalizedDraw

logger = logging.getLogger(__name__)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LotteryBot/1.0)"}
TIMEOUT = 30

CSV_URLS = {
    "pick-3-tx": "https://www.txlottery.org/export/sites/lottery/Games/Pick_3/Winning_Numbers/pick3.csv",
    "daily-4-tx": "https://www.txlottery.org/export/sites/lottery/Games/Daily_4/Winning_Numbers/daily4.csv",
}


class TXLotteryScraper(BaseScraper):
    source_name = "Texas Lottery (Official)"
    state_slug = "tx"

    def fetch(self, game_slug: str) -> str:
        url = CSV_URLS.get(game_slug, "")
        if not url:
            return ""
        with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
            r = client.get(url)
            r.raise_for_status()
            return r.text

    def parse(self, raw: str) -> List[NormalizedDraw]:
        return []

    def _parse_csv(self, game_slug: str, raw: str) -> List[NormalizedDraw]:
        results = []
        url = CSV_URLS.get(game_slug, "")
        reader = csv.DictReader(io.StringIO(raw))
        rows = list(reader)
        for row in rows[:5]:
            try:
                month = int(row.get("Month", 0))
                day_val = int(row.get("Day", 0))
                year = int(row.get("Year", 0))
                if not (month and day_val and year):
                    continue
                d = date(year, month, day_val)
                nums = []
                for k in ["Num1", "Num2", "Num3", "Num4", "Num5"]:
                    v = row.get(k)
                    if v and v.strip().isdigit():
                        nums.append(int(v.strip()))
                ampm = (row.get("AM/PM") or row.get("Day/Night") or "").strip().upper()
                if ampm in ("AM", "DAY"):
                    draw_type = "day"
                elif ampm in ("PM", "NIGHT", "EVE", "EVENING"):
                    draw_type = "night"
                else:
                    draw_type = "main"
                results.append(NormalizedDraw(
                    game_slug=game_slug,
                    draw_date=d,
                    draw_type=draw_type,
                    main_numbers=nums,
                    source_url=url,
                    source_provider=self.source_name,
                    confidence_score=95.0,
                    verification_status="verified",
                ))
            except Exception as e:
                logger.warning("TX CSV row parse error: %s", e)
        return results

    def scrape_game(self, game_slug: str) -> List[NormalizedDraw]:
        try:
            raw = self.fetch(game_slug)
            return self._parse_csv(game_slug, raw)
        except Exception as e:
            logger.error("TX %s scrape failed: %s", game_slug, e)
            return []

    def scrape_all_latest(self) -> List[NormalizedDraw]:
        results = []
        for game_slug in CSV_URLS:
            results.extend(self.scrape_game(game_slug))
        return results
