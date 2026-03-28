import json
import os
import re
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright
from sqlalchemy import select, text

from app.database import SessionLocal
from app.models import Draw, Game
from app.utils.game_normalizer import (
    canonical_game_info,
    split_main_and_bonus,
    GAME_RULES_BY_FINAL_SLUG,
    GAME_RULES_BY_CANONICAL_SLUG,
)


MULTI_STATE_FINAL_SLUGS = {
    "powerball",
    "powerball-double-play",
    "mega-millions",
    "millionaire-for-life",
    "lotto-america",
    "2by2",
}

DRAW_TYPE_KEYWORDS = {
    "midday": ["midday"],
    "evening": ["evening"],
    "afternoon": ["afternoon"],
    "morning": ["morning"],
    "day": [" día", " dia", "(día)", "(dia)", " day"],
    "night": [" noche", " night"],
}

DEBUG_SLUGS = {
    "mega-millions",
    "powerball",
    "powerball-double-play",
    "pega-2-pr",
    "pega-3-pr",
    "pega-4-pr",
    "pick-3-co",
    "pick-4-co",
    "pick-3-ar",
    "pick-4-ar",
    "bonus-match-5-md",
    "big-sky-bonus-mt",
    "wild-money-ri",
    "texas-two-step-tx",
}


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_date(text: str):
    try:
        return datetime.strptime(clean(text), "%A, %B %d, %Y").date()
    except Exception:
        return None


def get_states():
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, name, slug, source_url
                FROM states
                WHERE is_active = true
                ORDER BY name
                """
            )
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_games():
    db = SessionLocal()
    try:
        games = db.execute(select(Game).where(Game.is_active == True)).scalars().all()
        return {g.slug.lower(): g for g in games}
    finally:
        db.close()


def _extract_numbers_from_node(node) -> list[int]:
    nums = []

    li_items = node.locator("li")
    if li_items.count() > 0:
        for j in range(li_items.count()):
            try:
                txt = clean(li_items.nth(j).inner_text())
                if re.fullmatch(r"\d+", txt):
                    nums.append(int(txt))
            except Exception:
                pass
        return nums

    try:
        text_block = clean(node.inner_text())
        found = re.findall(r"\b\d+\b", text_block)
        nums = [int(x) for x in found]
    except Exception:
        nums = []

    return nums


def detect_draw_type_from_text(text: str) -> str:
    txt = clean(text).lower()

    for draw_type, keywords in DRAW_TYPE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in txt:
                return draw_type

    return "main"


def _looks_like_draw_block_text(txt: str) -> bool:
    if not txt:
        return False

    has_date = bool(
        re.search(
            r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}",
            txt,
            re.IGNORECASE,
        )
    )
    has_draw_label = bool(
        re.search(
            r"(midday|evening|afternoon|morning|día|dia|noche|night|day)",
            txt,
            re.IGNORECASE,
        )
    )
    has_ui_signal = (
        "Next Drawing:" in txt
        or "Prizes/Odds" in txt
        or "Speak" in txt
        or "Past Results" in txt
    )
    has_number = bool(re.search(r"\b\d{1,5}\b", txt))

    return has_number and (has_date or has_draw_label or has_ui_signal)


def _dedupe_blocks(blocks):
    deduped = []
    seen = set()

    for block in blocks:
        try:
            key = clean(block.inner_text())[:500]
        except Exception:
            key = None

        if not key:
            continue

        if key in seen:
            continue

        seen.add(key)
        deduped.append(block)

    return deduped


def detect_draw_blocks(section):
    blocks = []

    marker_loc = section.locator("h3, h4, strong, b, .title, .gameTitle")
    marker_count = marker_loc.count()

    for i in range(marker_count):
        marker = marker_loc.nth(i)
        try:
            marker_text = clean(marker.inner_text())
        except Exception:
            continue

        if not marker_text:
            continue

        if not re.search(
            r"(midday|evening|afternoon|morning|día|dia|noche|night|day)",
            marker_text,
            re.IGNORECASE,
        ):
            continue

        try:
            ancestor = marker.locator(
                "xpath=ancestor::div[.//time or .//li or .//*[contains(normalize-space(.), 'Prizes/Odds')] or .//*[contains(normalize-space(.), 'Speak')]][1]"
            )
            if ancestor.count() > 0:
                txt = clean(ancestor.first.inner_text())
                if _looks_like_draw_block_text(txt):
                    blocks.append(ancestor.first)
        except Exception:
            pass

    candidate_selectors = [
        ":scope > div",
        "div.resultdraw",
        "div.resultsdraw",
        "div.drawrow",
        "div.resultrow",
        "div.resultblock",
        "div.panel",
        "article",
    ]

    for selector in candidate_selectors:
        try:
            loc = section.locator(selector)
            if loc.count() == 0:
                continue

            temp = []
            for i in range(loc.count()):
                node = loc.nth(i)
                try:
                    txt = clean(node.inner_text())
                except Exception:
                    txt = ""

                if not _looks_like_draw_block_text(txt):
                    continue

                temp.append(node)

            temp = _dedupe_blocks(temp)

            if temp:
                blocks.extend(temp)
                if selector != ":scope > div" or len(temp) >= 2:
                    break
        except Exception:
            pass

    blocks = _dedupe_blocks(blocks)

    if not blocks:
        return [section]

    return blocks


def extract_title_from_block(block, fallback_title: str) -> str:
    title_selectors = ["h2", "h3", "h4", "strong", ".title", ".gameTitle"]

    for selector in title_selectors:
        loc = block.locator(selector)
        if loc.count() > 0:
            for i in range(loc.count()):
                try:
                    txt = clean(loc.nth(i).inner_text())
                    if txt and len(txt) <= 120:
                        return txt
                except Exception:
                    pass

    block_text = clean(block.inner_text())

    patterns = [
        r"([A-Za-zÁÉÍÓÚáéíóú0-9\-\+ ]+\b(?:Midday|Evening|Afternoon|Morning|Día|Dia|Noche|Night|Day))",
    ]

    for pattern in patterns:
        m = re.search(pattern, block_text, re.IGNORECASE)
        if m:
            return clean(m.group(1))

    return fallback_title


def extract_date_from_block(block):
    time_loc = block.locator("time")
    if time_loc.count() > 0:
        for i in range(time_loc.count()):
            try:
                txt = clean(time_loc.nth(i).inner_text())
                dt = parse_date(txt)
                if dt:
                    return dt
            except Exception:
                pass

    text_block = clean(block.inner_text())
    m = re.search(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})",
        text_block,
        re.IGNORECASE,
    )
    if m:
        try:
            return datetime.strptime(clean(m.group(0)), "%A, %B %d, %Y").date()
        except Exception:
            return None

    return None


def extract_primary_number_list(section, parser_type: str = "standard", final_slug: str = "") -> list[int]:
    if parser_type in {"many-numbers", "2by2"} or final_slug in {
        "2by2",
        "pick-10-ny",
        "quick-draw-in",
        "daily-keno-wa",
        "keno-mi",
        "all-or-nothing-tx",
        "all-or-nothing-wi",
    }:
        all_nums = []
        loc = section.locator("ul.resultsnums")
        for i in range(loc.count()):
            nums = _extract_numbers_from_node(loc.nth(i))
            if nums:
                all_nums.extend(nums)

        if all_nums:
            return all_nums

    candidates = [
        "ul.resultsnums",
        "div.resultsnumsrow ul.resultsnums",
        "div.resultsnumsrow",
    ]

    for selector in candidates:
        loc = section.locator(selector)
        if loc.count() == 0:
            continue

        # importante: juntar todos los bloques, no devolver solo el primero
        all_nums = []
        for i in range(loc.count()):
            nums = _extract_numbers_from_node(loc.nth(i))
            if nums:
                all_nums.extend(nums)

        if all_nums:
            return all_nums

    return []


def _has_class_hint(node) -> bool:
    try:
        own_class = (node.get_attribute("class") or "").lower()
    except Exception:
        own_class = ""

    try:
        parent = node.locator("xpath=..")
        parent_class = ((parent.get_attribute("class") or "").lower() if parent.count() > 0 else "")
    except Exception:
        parent_class = ""

    combined = f"{own_class} {parent_class}"
    return any(hint in combined for hint in ["num", "ball", "result", "draw", "card"])


def extract_numbers_for_block(
    block,
    parser_type: str = "standard",
    final_slug: str = "",
    canonical_slug: str = "",
) -> list[int]:
    nums = extract_primary_number_list(
        block,
        parser_type=parser_type,
        final_slug=final_slug,
    )
    if nums:
        return nums

    selector_plan = [
        ("ul.resultsnums li", False),
        ("[class*='resultsnums'] li", False),
        ("[class*='num'] li", False),
        ("[class*='ball']", False),
        ("[class*='number']", False),
        ("[class*='num']", False),
        ("li", False),
        ("span", True),
        ("div", True),
        ("td", True),
    ]

    collected = []

    for selector, require_hint in selector_plan:
        try:
            loc = block.locator(selector)
            count = loc.count()
        except Exception:
            count = 0

        current = []
        for i in range(count):
            node = loc.nth(i)
            try:
                txt = clean(node.inner_text())
            except Exception:
                continue

            if not txt or not re.fullmatch(r"\d{1,5}", txt):
                continue

            if require_hint and not _has_class_hint(node):
                continue

            current.append(int(txt))

        if current:
            collected = current
            break

    if collected:
        normalized = []
        for n in collected:
            if not normalized or normalized[-1] != n:
                normalized.append(n)

        rule = GAME_RULES_BY_FINAL_SLUG.get(final_slug)
        if not rule:
            rule = GAME_RULES_BY_CANONICAL_SLUG.get(canonical_slug)

        if rule:
            expected_main = rule.get("main", 0)
            expected_bonus = rule.get("bonus", 0)
            bonus_mode = rule.get("bonus_mode")
            expected_total = expected_main + (0 if bonus_mode == "optional" else expected_bonus)
            if expected_total > 0 and len(normalized) >= expected_total:
                return normalized[:expected_total]

        return normalized

    try:
        block_text = clean(block.inner_text())
    except Exception:
        block_text = ""

    if not block_text:
        return []

    block_text = re.sub(r"Next Drawing:.*", "", block_text, flags=re.IGNORECASE)
    block_text = re.sub(r"Prizes/Odds", "", block_text, flags=re.IGNORECASE)
    block_text = re.sub(r"Speak", "", block_text, flags=re.IGNORECASE)
    block_text = re.sub(r"Past Results.*", "", block_text, flags=re.IGNORECASE)
    block_text = re.sub(r"Calendar.*", "", block_text, flags=re.IGNORECASE)
    block_text = re.sub(r"More.*", "", block_text, flags=re.IGNORECASE)

    found = re.findall(r"\b\d{1,5}\b", block_text)
    nums = [int(x) for x in found]

    if not nums:
        return []

    rule = GAME_RULES_BY_FINAL_SLUG.get(final_slug)
    if not rule:
        rule = GAME_RULES_BY_CANONICAL_SLUG.get(canonical_slug)

    if rule:
        expected_main = rule.get("main", 0)
        expected_bonus = rule.get("bonus", 0)
        bonus_mode = rule.get("bonus_mode")
        expected_total = expected_main + (0 if bonus_mode == "optional" else expected_bonus)
        if expected_total > 0 and len(nums) >= expected_total:
            return nums[:expected_total]

    return nums


def _extract_bonus_from_text(text_value: str) -> Optional[str]:
    if not text_value:
        return None

    bonus_patterns = [
        r"Cash Ball[:\s]+(\d{1,2})",
        r"Star Ball[:\s]+(\d{1,2})",
        r"Powerball[:\s]+(\d{1,2})",
        r"Mega Ball[:\s]+(\d{1,2})",
        r"Mega Number[:\s]+(\d{1,2})",
        r"Millionaire Ball[:\s]+(\d{1,2})",
        r"Bullseye[:\s]+(\d{1,2})",
        r"Fireball[:\s]+(\d{1,2})",
        r"Wild Ball[:\s]+(\d{1,2})",
        r"Wild Money[:\s]+(\d{1,2})",
        r"Superball[:\s]+(\d{1,2})",
        r"Kicker[:\s]+(\d+)",
        r"Cash Ball 225[:\s]+(\d{1,2})",
        r"Bonus Ball[:\s]+(\d{1,2})",
        r"Bonus Number[:\s]+(\d{1,2})",
        r"Bonus[:\s]+(\d{1,2})",
        r"Extra Ball[:\s]+(\d{1,2})",
        r"Extra Number[:\s]+(\d{1,2})",
        r"Extra[:\s]+(\d{1,2})",
        r"Bolo Cash[:\s]+(\d{1,2})",
        r"Megaball[:\s]+(\d{1,2})",
    ]

    for pattern in bonus_patterns:
        m = re.search(pattern, text_value, re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return None


def extract_text_extras(section) -> dict:
    def safe_text(locator) -> str:
        try:
            if locator.count() > 0:
                texts = []
                for i in range(locator.count()):
                    try:
                        txt = clean(locator.nth(i).inner_text())
                        if txt:
                            texts.append(txt)
                    except Exception:
                        pass
                return " | ".join(texts)
        except Exception:
            pass
        return ""

    full_text = clean(section.inner_text())

    extra_chunks = [
        safe_text(section.locator(".jackpot")),
        safe_text(section.locator(".nextdraw")),
        safe_text(section.locator(".nextdrawing")),
        safe_text(section.locator(".resultsnext")),
        safe_text(section.locator(".drawinfo")),
        safe_text(section.locator(".gameinfo")),
        safe_text(section.locator(".prize")),
        safe_text(section.locator(".panel")),
        safe_text(section.locator("[class*='bonus']")),
        safe_text(section.locator("[class*='ball']")),
        safe_text(section.locator("[class*='extra']")),
        safe_text(section.locator("ul.gold li")),
        safe_text(section.locator("ul.gold")),
        safe_text(section.locator("p")),
        safe_text(section.locator("small")),
        safe_text(section.locator("strong")),
        safe_text(section.locator("span")),
        safe_text(section.locator("td")),
    ]

    combined_text = " | ".join([full_text] + [x for x in extra_chunks if x])
    combined_text = clean(combined_text)

    data = {
        "bonus_number": None,
        "multiplier": None,
        "jackpot": None,
        "jackpot_change": None,
        "next_draw_text": None,
        "next_draw_timezone": None,
        "next_draw_relative": None,
        "full_text": combined_text,
    }

    data["bonus_number"] = _extract_bonus_from_text(combined_text)

    if not data["bonus_number"]:
        try:
            gold_loc = section.locator("ul.gold li, ul.gold")
            for i in range(gold_loc.count()):
                txt = clean(gold_loc.nth(i).inner_text())
                if re.fullmatch(r"\d{1,2}", txt):
                    data["bonus_number"] = txt
                    break
        except Exception:
            pass

    mult_patterns = [
        r"All Star Bonus[:\s]+([Xx]?\d+)",
        r"Power Play[:\s]+([Xx]?\d+)",
        r"Megaplier[:\s]+([Xx]?\d+)",
        r"Multiplier[:\s]+([Xx]?\d+)",
        r"Plus[:\s]+([Xx]?\d+)",
        r"Multiplicador[:\s]+([Xx]?\d+)",
        r"XTRA[:\s]+([Xx]?\d+)",
    ]

    jackpot_patterns = [
        r"Next Jackpot[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Estimated Jackpot[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Estimated Grand Prize[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Grand Prize[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Top Prize[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Cash Value[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Annuity[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    jackpot_change_patterns = [
        r"Change from last[:\s]+([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot Change[:\s]+([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Change[:\s]+([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    next_draw_patterns = [
        r"Next Drawing[:\s]+([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing[:\s]+([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing[:\s]+([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4})",
        r"Next Draw[:\s]+([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Draw[:\s]+([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
    ]

    timezone_patterns = [
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+Time\s*\(GMT[^\)]*\))",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+Time\s*\(UTC[^\)]*\))",
        r"\b(Eastern Time|Central Time|Mountain Time|Pacific Time|Atlantic Standard Time)\b",
        r"\b(ET|CT|MT|PT)\b",
    ]

    relative_patterns = [
        r"(\d+\s+(?:minute|minutes|hour|hours|day|days)\s+from\s+now)",
        r"(in\s+\d+\s*(?:m|h|d))",
        r"(in\s+\d+\s+(?:minutes|hours|days))",
    ]

    for pattern in mult_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            value = m.group(1).strip().upper()
            data["multiplier"] = value if value.startswith("X") else f"X{value}"
            break

    for pattern in jackpot_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["jackpot"] = m.group(1).strip()
            break

    for pattern in jackpot_change_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["jackpot_change"] = m.group(1).strip()
            break

    for pattern in next_draw_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["next_draw_text"] = clean(m.group(1))
            break

    for pattern in timezone_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["next_draw_timezone"] = clean(m.group(1))
            break

    for pattern in relative_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["next_draw_relative"] = clean(m.group(1))
            break

    return data


def extract_page_level_extras(full_page_text: str, title: str) -> dict:
    page_text = clean(full_page_text)
    title_clean = clean(title)

    data = {
        "jackpot": None,
        "jackpot_change": None,
        "next_draw_text": None,
        "next_draw_timezone": None,
        "next_draw_relative": None,
        "bonus_number": None,
    }

    known_titles = [
        "Powerball Double Play",
        "Powerball",
        "Mega Millions",
        "Millionaire for Life",
        "2by2",
        "Lotto America",
        "Cowboy Draw",
        "Cash Pop",
        "Fantasy 5",
        "Pick 2",
        "Pick 3",
        "Pick 4",
        "Pick 5",
        "Take 5",
        "Numbers",
        "Win 4",
        "Lotto",
        "Daily 3",
        "Daily 4",
        "Daily Derby",
        "SuperLotto Plus",
        "Jackpot Triple Play",
        "DC-3",
        "DC-4",
        "DC-5",
        "Pega 2",
        "Pega 3",
        "Pega 4",
        "Revancha",
        "Loto Cash",
        "Loteria Tradicional",
        "Bonus Match 5",
        "Wild Money",
        "Big Sky Bonus",
        "Texas Two Step",
    ]

    matches = list(re.finditer(re.escape(title_clean), page_text, re.IGNORECASE))
    if not matches:
        return data

    best_chunk = None

    for match in matches:
        start = match.start()
        tail = page_text[start:start + 2200]

        looks_like_game_block = (
            re.search(
                r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}",
                tail,
                re.IGNORECASE,
            )
            or "Prizes/Odds" in tail
            or "Speak" in tail
            or "Past Results" in tail
            or "Next Drawing:" in tail
            or "Bonus Ball" in tail
            or "Bonus Number" in tail
            or "Extra Ball" in tail
            or "Extra:" in tail
        )

        if not looks_like_game_block:
            continue

        next_positions = []
        for other_title in known_titles:
            other_title_clean = clean(other_title)
            if other_title_clean.lower() == title_clean.lower():
                continue

            other_match = re.search(re.escape(other_title_clean), tail, re.IGNORECASE)
            if other_match and other_match.start() > 0:
                next_positions.append(other_match.start())

        end = min(next_positions) if next_positions else min(len(tail), 2200)
        chunk = clean(tail[:end])

        if (
            "Next Drawing:" in chunk
            or "Next Jackpot:" in chunk
            or "Past Results" in chunk
            or "Bonus Ball" in chunk
            or "Bonus Number" in chunk
            or "Extra Ball" in chunk
            or "Extra:" in chunk
        ):
            best_chunk = chunk
            break

        if best_chunk is None:
            best_chunk = chunk

    if not best_chunk:
        return data

    chunk = best_chunk
    data["bonus_number"] = _extract_bonus_from_text(chunk)

    next_draw_patterns = [
        r"Next Drawing:\s*([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing:\s*([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing:\s*([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4})",
    ]

    jackpot_patterns = [
        r"Next Jackpot:\s*(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Estimated Jackpot:\s*(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot:\s*(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    jackpot_change_patterns = [
        r"Change from last:\s*([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot Change:\s*([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    timezone_patterns = [
        r"(Eastern Time\s*\(GMT[^\)]*\))",
        r"(Central Time\s*\(GMT[^\)]*\))",
        r"(Mountain Time\s*\(GMT[^\)]*\))",
        r"(Pacific Time\s*\(GMT[^\)]*\))",
        r"(Atlantic Standard Time\s*\(GMT[^\)]*\))",
        r"\b(Eastern Time|Central Time|Mountain Time|Pacific Time|Atlantic Standard Time)\b",
    ]

    relative_patterns = [
        r"(\d+\s+(?:minute|minutes|hour|hours|day|days)\s+from\s+now)",
        r"(in\s+\d+\s+(?:minutes|hours|days))",
    ]

    for pattern in next_draw_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["next_draw_text"] = clean(m.group(1))
            break

    for pattern in jackpot_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["jackpot"] = clean(m.group(1))
            break

    for pattern in jackpot_change_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["jackpot_change"] = clean(m.group(1))
            break

    for pattern in timezone_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["next_draw_timezone"] = clean(m.group(1))
            break

    for pattern in relative_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["next_draw_relative"] = clean(m.group(1))
            break

    return data


def validate_entry(
    final_slug: str,
    canonical_slug: str,
    main_numbers: list[int],
    bonus_number=None,
) -> bool:
    if final_slug == "poker-lotto-mi" or canonical_slug == "poker-lotto":
        return False

    if not main_numbers:
        return False

    rule = GAME_RULES_BY_FINAL_SLUG.get(final_slug)

    if not rule:
        rule = GAME_RULES_BY_CANONICAL_SLUG.get(canonical_slug)

    if not rule:
        return len(main_numbers) > 0

    expected_main = rule.get("main", 0)
    expected_bonus = rule.get("bonus", 0)
    bonus_mode = rule.get("bonus_mode")

    if len(main_numbers) != expected_main:
        return False

    has_bonus = bonus_number is not None and str(bonus_number).strip() != ""

    if bonus_mode == "optional":
        return True

    if expected_bonus > 0 and not has_bonus:
        return False

    if expected_bonus == 0 and has_bonus:
        return False

    if final_slug == "myday-ne":
        if len(main_numbers) != 3:
            return False

        month, day, year = main_numbers

        if not (1 <= month <= 12):
            return False

        if not (0 <= year <= 99):
            return False

        days_in_month = {
            1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
        }

        if not (1 <= day <= days_in_month[month]):
            return False

    return True


def save_draw(
    game: Game,
    draw_date,
    draw_type: str,
    main_numbers: list[int],
    bonus_number,
    multiplier,
    jackpot,
    jackpot_change,
    next_draw_text,
    next_draw_timezone,
    next_draw_relative,
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
            existing.multiplier = multiplier
            existing.jackpot = jackpot
            existing.source_url = source_url
            existing.notes = notes

            if hasattr(existing, "jackpot_change"):
                existing.jackpot_change = jackpot_change
            if hasattr(existing, "next_draw_text"):
                existing.next_draw_text = next_draw_text
            if hasattr(existing, "next_draw_timezone"):
                existing.next_draw_timezone = next_draw_timezone
            if hasattr(existing, "next_draw_relative"):
                existing.next_draw_relative = next_draw_relative

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
            multiplier=multiplier,
            jackpot=jackpot,
            cash_payout=None,
            secondary_draws=None,
            notes=notes,
            source_url=source_url,
        )

        if hasattr(row, "jackpot_change"):
            row.jackpot_change = jackpot_change
        if hasattr(row, "next_draw_text"):
            row.next_draw_text = next_draw_text
        if hasattr(row, "next_draw_timezone"):
            row.next_draw_timezone = next_draw_timezone
        if hasattr(row, "next_draw_relative"):
            row.next_draw_relative = next_draw_relative

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


def get_or_create_game_in_db(
    final_slug: str,
    canonical_name: str,
    games_by_slug: dict[str, Game],
):
    game = games_by_slug.get(final_slug.lower())
    if game:
        return game, False

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

            games_by_slug[game.slug.lower()] = game
            return game, False

        game = Game(
            name=canonical_name,
            slug=final_slug.lower(),
            is_active=True,
        )
        db.add(game)
        db.commit()
        db.refresh(game)

        games_by_slug[game.slug.lower()] = game
        return game, True
    finally:
        db.close()


def scrape_state(page, state: dict, games_by_slug: dict[str, Game]):
    page.goto(state["source_url"], wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)
    full_page_text = clean(page.locator("body").inner_text())

    sections = page.locator("section")
    results = []

    for i in range(sections.count()):
        section = sections.nth(i)

        if section.locator("h2").count() == 0:
            continue

        try:
            section_title = clean(section.locator("h2").first.inner_text())
        except Exception:
            continue

        if not section_title:
            continue

        block_nodes = detect_draw_blocks(section)

        for block in block_nodes:
            try:
                block_title = extract_title_from_block(block, section_title)
                draw_date = extract_date_from_block(block)

                if not draw_date and section.locator("time").count() > 0:
                    try:
                        draw_date = parse_date(section.locator("time").first.inner_text())
                    except Exception:
                        draw_date = None

                if not draw_date:
                    continue

                info = canonical_game_info(block_title, state_code=state["slug"])

                canonical_name = info["canonical_name"]
                canonical_slug = info["canonical_slug"]
                final_slug = info["final_slug"]
                parser_type = info["parser_type"]

                if final_slug == "poker-lotto-mi" or canonical_slug == "poker-lotto":
                    continue

                block_text = clean(block.inner_text())
                inferred_draw_type = detect_draw_type_from_text(block_title + " " + block_text)
                draw_type = inferred_draw_type if inferred_draw_type else info["draw_type"]

                raw_numbers = extract_numbers_for_block(
                    block,
                    parser_type=parser_type,
                    final_slug=final_slug,
                    canonical_slug=canonical_slug,
                )

                extras = extract_text_extras(block)
                page_extras = extract_page_level_extras(full_page_text, block_title)

                if final_slug in DEBUG_SLUGS:
                    print("\nDEBUG EXTRAS")
                    print("STATE:", state["slug"])
                    print("TITLE:", block_title)
                    print("FINAL SLUG:", final_slug)
                    print("DRAW TYPE:", draw_type)
                    print("RAW NUMBERS:", raw_numbers)
                    print("SECTION BONUS:", extras.get("bonus_number"))
                    print("PAGE BONUS:", page_extras.get("bonus_number"))
                    print("PAGE EXTRAS:", page_extras)

                bonus_number = extras["bonus_number"] or page_extras.get("bonus_number")
                multiplier = extras["multiplier"]
                debug_text = extras["full_text"]

                jackpot = extras["jackpot"] or page_extras["jackpot"]
                jackpot_change = extras["jackpot_change"] or page_extras["jackpot_change"]
                next_draw_text = extras["next_draw_text"] or page_extras["next_draw_text"]
                next_draw_timezone = extras["next_draw_timezone"] or page_extras["next_draw_timezone"]
                next_draw_relative = extras["next_draw_relative"] or page_extras["next_draw_relative"]

                parts = split_main_and_bonus(
                    game_slug=final_slug,
                    raw_numbers=raw_numbers,
                    bonus_number=bonus_number,
                    multiplier=multiplier,
                )

                main_numbers = parts["main_numbers"]
                final_bonus_number = parts["bonus_number"]
                final_multiplier = parts["multiplier"]

                payload = {
                    "title": block_title,
                    "section_title": section_title,
                    "state_slug": state["slug"],
                    "canonical_name": canonical_name,
                    "canonical_slug": canonical_slug,
                    "final_slug": final_slug,
                    "draw_type": draw_type,
                    "parser_type": parser_type,
                    "raw_numbers": raw_numbers,
                    "main_numbers": main_numbers,
                    "bonus_number": final_bonus_number,
                    "multiplier": final_multiplier,
                    "jackpot": jackpot,
                    "jackpot_change": jackpot_change,
                    "next_draw_text": next_draw_text,
                    "next_draw_timezone": next_draw_timezone,
                    "next_draw_relative": next_draw_relative,
                    "debug_text": debug_text[:2000],
                }

                if not validate_entry(
                    final_slug=final_slug,
                    canonical_slug=canonical_slug,
                    main_numbers=main_numbers,
                    bonus_number=final_bonus_number,
                ):
                    results.append({
                        "status": "invalid",
                        "title": block_title,
                        "resolved_slug": final_slug,
                        "draw_date": str(draw_date),
                        "draw_type": draw_type,
                        "payload": payload,
                    })
                    continue

                try:
                    game, created_game = get_or_create_game_in_db(
                        final_slug=final_slug,
                        canonical_name=canonical_name,
                        games_by_slug=games_by_slug,
                    )
                except Exception as e:
                    results.append({
                        "status": "unmatched",
                        "title": block_title,
                        "resolved_slug": final_slug,
                        "draw_date": str(draw_date),
                        "draw_type": draw_type,
                        "error": str(e),
                        "payload": payload,
                    })
                    continue

                notes_parts = [
                    "Scraped from Lottery Post DOM sections v6 multi-draw",
                    f"original_title={block_title}",
                    f"section_title={section_title}",
                    f"parser_type={parser_type}",
                    f"canonical_slug={canonical_slug}",
                    f"draw_type={draw_type}",
                ]

                if parser_type in {"special", "many-numbers", "2by2"}:
                    notes_parts.append(f"raw_numbers={raw_numbers}")

                final_notes = " | ".join(notes_parts)

                action = save_draw(
                    game=game,
                    draw_date=draw_date,
                    draw_type=draw_type,
                    main_numbers=main_numbers,
                    bonus_number=final_bonus_number,
                    multiplier=final_multiplier,
                    jackpot=jackpot,
                    jackpot_change=jackpot_change,
                    next_draw_text=next_draw_text,
                    next_draw_timezone=next_draw_timezone,
                    next_draw_relative=next_draw_relative,
                    source_url=state["source_url"],
                    raw_payload=payload,
                    notes=final_notes,
                )

                row = {
                    "status": action,
                    "title": block_title,
                    "resolved_slug": game.slug,
                    "draw_date": str(draw_date),
                    "draw_type": draw_type,
                    "main_numbers": main_numbers,
                    "bonus_number": final_bonus_number,
                    "multiplier": final_multiplier,
                    "jackpot": jackpot,
                    "jackpot_change": jackpot_change,
                    "next_draw_text": next_draw_text,
                    "next_draw_timezone": next_draw_timezone,
                    "next_draw_relative": next_draw_relative,
                }

                if created_game:
                    row["created_game"] = True

                results.append(row)

            except Exception as e:
                results.append({
                    "status": "error",
                    "title": section_title,
                    "resolved_slug": None,
                    "draw_date": None,
                    "error": str(e),
                })

    return results


def main():
    states = get_states()
    games_by_slug = get_games()

    created = 0
    updated = 0
    unmatched = 0
    invalid = 0
    created_games = 0
    report = []

    with sync_playwright() as p:
        is_ci = os.getenv("CI", "").lower() == "true"

        if is_ci:
            browser = p.chromium.launch(headless=True)
        else:
            browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)

        page = browser.new_page()

        for state in states:
            print("\n" + "=" * 90)
            print(f"STATE: {state['name']} ({state['slug']})")
            print(f"URL: {state['source_url']}")
            print("=" * 90)

            try:
                rows = scrape_state(page, state, games_by_slug)
            except Exception as e:
                print(f"ERROR: {e}")
                report.append({
                    "state": state["slug"],
                    "state_name": state["name"],
                    "error": str(e),
                    "rows": [],
                })
                continue

            for row in rows:
                status = row["status"]

                if row.get("created_game"):
                    created_games += 1
                    print(f"CREATED GAME ON THE FLY: {row['resolved_slug']}")

                if status == "created":
                    created += 1
                    print(f"CREATED: {row['title']} -> {row['resolved_slug']} -> {row['main_numbers']}")
                elif status == "updated":
                    updated += 1
                    print(f"UPDATED: {row['title']} -> {row['resolved_slug']} -> {row['main_numbers']}")
                elif status == "unmatched":
                    unmatched += 1
                    print(f"UNMATCHED: {row['title']} -> {row['resolved_slug']}")
                elif status == "invalid":
                    invalid += 1
                    print(f"INVALID: {row['title']} -> {row['resolved_slug']}")
                elif status == "error":
                    print(f"ERROR ROW: {row.get('title')} -> {row.get('error')}")

            report.append({
                "state": state["slug"],
                "state_name": state["name"],
                "rows": rows,
            })

        browser.close()

    with open("all_states_dom_report_v6.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\nSUMMARY")
    print("=" * 90)
    print(f"Created games: {created_games}")
    print(f"Created draws: {created}")
    print(f"Updated draws: {updated}")
    print(f"Unmatched: {unmatched}")
    print(f"Invalid: {invalid}")
    print("Report saved: all_states_dom_report_v6.json")


if __name__ == "__main__":
    main()