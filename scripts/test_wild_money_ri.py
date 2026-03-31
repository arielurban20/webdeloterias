import re
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright
from sqlalchemy import select, text

from app.database import SessionLocal
from app.models import Draw, Game
from app.utils.game_normalizer import (
    split_main_and_bonus,
    GAME_RULES_BY_FINAL_SLUG,
    GAME_RULES_BY_CANONICAL_SLUG,
)


STATE_SLUG = "ri"
STATE_URL = "https://www.lotterypost.com/results/ri"
FINAL_SLUG = "wild-money-ri"
CANONICAL_SLUG = "wild-money"
CANONICAL_NAME = "Wild Money"


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_date(text: str):
    try:
        return datetime.strptime(clean(text), "%A, %B %d, %Y").date()
    except Exception:
        return None


def get_or_create_game_in_db(
    final_slug: str,
    canonical_name: str,
):
    db = SessionLocal()
    try:
        game = db.execute(
            select(Game).where(Game.slug == final_slug.lower())
        ).scalar_one_or_none()

        if game:
            changed = False
            if game.name != canonical_name:
                game.name = canonical_name
                changed = True

            if changed:
                db.commit()
                db.refresh(game)

            return game, False

        game = Game(
            name=canonical_name,
            slug=final_slug.lower(),
            is_active=True,
        )
        db.add(game)
        db.commit()
        db.refresh(game)
        return game, True
    finally:
        db.close()


def save_draw(
    game: Game,
    draw_date,
    draw_type: str,
    main_numbers: list[int],
    bonus_number,
    source_url: str,
    raw_payload: dict,
    notes: str,
):
    db = SessionLocal()
    try:
        existing = db.execute(
            select(Draw).where(
                Draw.game_id == game.id,
                Draw.draw_date == draw_date,
                Draw.draw_type == draw_type,
            )
        ).scalar_one_or_none()

        if existing:
            existing.main_numbers = main_numbers
            existing.bonus_number = bonus_number
            existing.source_url = source_url
            existing.notes = notes

            if hasattr(existing, "raw_payload"):
                existing.raw_payload = raw_payload
            if hasattr(existing, "source_provider"):
                existing.source_provider = "Lottery Post"
            if hasattr(existing, "verification_status"):
                existing.verification_status = "verified"
            if hasattr(existing, "confidence_score"):
                existing.confidence_score = 90
            if hasattr(existing, "needs_review"):
                existing.needs_review = False

            db.commit()
            return "updated"

        row = Draw(
            game_id=game.id,
            draw_date=draw_date,
            draw_type=draw_type,
            draw_time=None,
            main_numbers=main_numbers,
            bonus_number=bonus_number,
            multiplier=None,
            jackpot=None,
            cash_payout=None,
            secondary_draws=None,
            notes=notes,
            source_url=source_url,
        )

        if hasattr(row, "raw_payload"):
            row.raw_payload = raw_payload
        if hasattr(row, "source_provider"):
            row.source_provider = "Lottery Post"
        if hasattr(row, "verification_status"):
            row.verification_status = "verified"
        if hasattr(row, "confidence_score"):
            row.confidence_score = 90
        if hasattr(row, "needs_review"):
            row.needs_review = False

        db.add(row)
        db.commit()
        return "created"
    finally:
        db.close()


def validate_entry(
    final_slug: str,
    canonical_slug: str,
    main_numbers: list[int],
    bonus_number=None,
) -> bool:
    if not main_numbers:
        return False

    rule = GAME_RULES_BY_FINAL_SLUG.get(final_slug)
    if not rule:
        rule = GAME_RULES_BY_CANONICAL_SLUG.get(canonical_slug)

    if not rule:
        return False

    expected_main = rule.get("main", 0)
    expected_bonus = rule.get("bonus", 0)

    if len(main_numbers) != expected_main:
        return False

    has_bonus = bonus_number is not None and str(bonus_number).strip() != ""

    if expected_bonus > 0 and not has_bonus:
        return False

    if expected_bonus == 0 and has_bonus:
        return False

    return True


def extract_date_from_section(section):
    time_loc = section.locator("time")
    for i in range(time_loc.count()):
        try:
            txt = clean(time_loc.nth(i).inner_text())
            dt = parse_date(txt)
            if dt:
                return dt
        except Exception:
            pass

    text_block = clean(section.inner_text())
    m = re.search(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}",
        text_block,
        re.IGNORECASE,
    )
    if m:
        try:
            return datetime.strptime(clean(m.group(0)), "%A, %B %d, %Y").date()
        except Exception:
            return None

    return None


def extract_main_numbers(section) -> list[int]:
    nums = []

    selectors = [
        "ul.resultsnums li",
        "div.resultsnumsrow ul.resultsnums li",
        "div.resultsnumsrow li",
    ]

    for selector in selectors:
        loc = section.locator(selector)
        if loc.count() == 0:
            continue

        temp = []
        for i in range(loc.count()):
            try:
                txt = clean(loc.nth(i).inner_text())
                if re.fullmatch(r"\d+", txt):
                    temp.append(int(txt))
            except Exception:
                pass

        if temp:
            nums = temp
            break

    if nums:
        return nums[:5]

    text_block = clean(section.inner_text())
    m = re.search(
        r"Wild Money.*?Friday,\s+March\s+\d{1,2},\s+\d{4}(.*?)(?:Next Drawing:|Next Jackpot:|Prizes/Odds|Speak)",
        text_block,
        re.IGNORECASE,
    )
    if m:
        found = re.findall(r"\b\d+\b", m.group(1))
        vals = [int(x) for x in found]
        if len(vals) >= 5:
            return vals[:5]

    return []


def extract_bonus_from_section(section) -> Optional[str]:
    selectors = [
        "ul.gold li",
        "ul.gold",
        "[class*='extra'] li",
        "[class*='extra']",
        "[class*='gold'] li",
        "[class*='gold']",
    ]

    for selector in selectors:
        loc = section.locator(selector)
        for i in range(loc.count()):
            try:
                txt = clean(loc.nth(i).inner_text())
                if re.fullmatch(r"\d{1,2}", txt):
                    return txt
            except Exception:
                pass

    text_block = clean(section.inner_text())

    patterns = [
        r"Extra[:\s]+(\d{1,2})",
        r"Extra Ball[:\s]+(\d{1,2})",
        r"Bonus[:\s]+(\d{1,2})",
        r"Bonus Ball[:\s]+(\d{1,2})",
    ]

    for pattern in patterns:
        m = re.search(pattern, text_block, re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return None


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
        page = browser.new_page()
        page.goto(STATE_URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(5000)

        sections = page.locator("section")
        found = False

        for i in range(sections.count()):
            section = sections.nth(i)

            try:
                section_text = clean(section.inner_text())
            except Exception:
                continue

            if "Wild Money" not in section_text:
                continue

            found = True
            draw_date = extract_date_from_section(section)
            raw_numbers = extract_main_numbers(section)
            bonus_number = extract_bonus_from_section(section)

            print("\n" + "=" * 80)
            print("TEST WILD MONEY RI")
            print("DRAW DATE:", draw_date)
            print("RAW NUMBERS:", raw_numbers)
            print("BONUS:", bonus_number)
            print("=" * 80)

            parts = split_main_and_bonus(
                game_slug=FINAL_SLUG,
                raw_numbers=raw_numbers,
                bonus_number=bonus_number,
                multiplier=None,
            )

            main_numbers = parts["main_numbers"]
            final_bonus_number = parts["bonus_number"]

            print("MAIN NUMBERS:", main_numbers)
            print("FINAL BONUS:", final_bonus_number)

            ok = validate_entry(
                final_slug=FINAL_SLUG,
                canonical_slug=CANONICAL_SLUG,
                main_numbers=main_numbers,
                bonus_number=final_bonus_number,
            )

            print("VALID:", ok)

            if not draw_date:
                print("ERROR: no se pudo sacar la fecha")
                break

            if not ok:
                print("ERROR: el sorteo sigue inválido")
                break

            game, created_game = get_or_create_game_in_db(
                final_slug=FINAL_SLUG,
                canonical_name=CANONICAL_NAME,
            )

            action = save_draw(
                game=game,
                draw_date=draw_date,
                draw_type="main",
                main_numbers=main_numbers,
                bonus_number=final_bonus_number,
                source_url=STATE_URL,
                raw_payload={
                    "state_slug": STATE_SLUG,
                    "title": "Wild Money",
                    "final_slug": FINAL_SLUG,
                    "raw_numbers": raw_numbers,
                    "main_numbers": main_numbers,
                    "bonus_number": final_bonus_number,
                },
                notes="Wild Money RI test script",
            )

            print("DB ACTION:", action)
            if created_game:
                print("CREATED GAME:", game.slug)

            break

        if not found:
            print("No encontré la sección de Wild Money.")

        browser.close()


if __name__ == "__main__":
    main()