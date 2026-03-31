import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright
from sqlalchemy import text

from app.database import SessionLocal
from app.utils.game_normalizer import canonical_game_info


OUTPUT_DIR = Path("public/lottery-logos")
MANIFEST_PATH = Path("public/lottery-logos-manifest.json")

MULTI_STATE_SLUGS = {
    "powerball",
    "powerball-double-play",
    "mega-millions",
    "millionaire-for-life",
    "lotto-america",
    "2by2",
}


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)


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


def file_ext_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in [".png", ".jpg", ".jpeg", ".svg", ".webp"]:
        if path.endswith(ext):
            return ext.replace(".", "")
    return "png"


def safe_filename(slug: str, ext: str) -> str:
    ext = ext.lower().strip(".")
    if ext not in {"png", "jpg", "jpeg", "svg", "webp"}:
        ext = "png"
    return f"{slug}.{ext}"


def is_good_logo_url(url: str) -> bool:
    low = url.lower()
    bad_parts = [
        "banner",
        "adservice",
        "doubleclick",
        "googlesyndication",
        "analytics",
        "facebook",
        "twitter",
        "avatar",
        "favicon",
        "icon",
        "sprite",
        "blank",
        "pixel",
    ]
    if any(x in low for x in bad_parts):
        return False

    good_parts = [
        "logo",
        "lotto",
        "pick",
        "cash",
        "powerball",
        "mega",
        "millionaire",
        "numbers",
        "fantasy",
        "draw",
        "game",
        "keno",
        "daily",
        "play",
        "wild",
        "bonus",
        "match",
        "money",
        "revancha",
        "pega",
        "loto",
        "megabucks",
        "badger",
        "super",
        "allornothing",
        "all-or-nothing",
        "2by2",
    ]
    return any(x in low for x in good_parts)


def extract_game_blocks(page):
    blocks = []

    sections = page.locator("section")
    for i in range(sections.count()):
        section = sections.nth(i)

        try:
            title = clean(section.locator("h2").first.inner_text()) if section.locator("h2").count() else ""
        except Exception:
            title = ""

        if not title:
            continue

        text_value = ""
        try:
            text_value = clean(section.inner_text())
        except Exception:
            pass

        if not text_value:
            continue

        if not (
            "Prizes/Odds" in text_value
            or "Speak" in text_value
            or "Next Drawing:" in text_value
            or re.search(
                r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}",
                text_value,
                re.IGNORECASE,
            )
        ):
            continue

        blocks.append(section)

    return blocks


def extract_title_from_block(block):
    selectors = ["h3", "h4", "strong", "b", "h2", ".title", ".gameTitle"]
    for selector in selectors:
        loc = block.locator(selector)
        if loc.count() > 0:
            for i in range(loc.count()):
                try:
                    txt = clean(loc.nth(i).inner_text())
                    if 1 < len(txt) <= 120:
                        return txt
                except Exception:
                    pass

    try:
        txt = clean(block.inner_text())
    except Exception:
        txt = ""

    if txt:
        first_line = txt.split("Next Drawing:")[0]
        first_line = clean(first_line)
        if first_line:
            return first_line[:120]

    return ""


def collect_candidate_image_urls(page, block, base_url: str):
    found = []

    selectors = [
        "img",
        "picture img",
        "source",
    ]

    for selector in selectors:
        loc = block.locator(selector)
        for i in range(loc.count()):
            node = loc.nth(i)

            attrs = []
            for attr_name in ["src", "data-src", "data-lazy-src", "srcset", "data-original"]:
                try:
                    value = node.get_attribute(attr_name)
                except Exception:
                    value = None

                if value:
                    attrs.append(value)

            for raw in attrs:
                parts = [x.strip() for x in raw.split(",") if x.strip()]
                for part in parts:
                    url_part = part.split(" ")[0].strip()
                    if not url_part:
                        continue
                    full = urljoin(base_url, url_part)
                    if full not in found:
                        found.append(full)

    # fallback más agresivo: buscar en html del bloque
    try:
        html = block.inner_html()
    except Exception:
        html = ""

    if html:
        for match in re.findall(r"""(?:src|data-src|data-lazy-src)=["']([^"']+)["']""", html, re.IGNORECASE):
            full = urljoin(base_url, match.strip())
            if full not in found:
                found.append(full)

    return found


def choose_best_logo_url(urls):
    if not urls:
        return None

    scored = []
    for url in urls:
        score = 0
        low = url.lower()

        if is_good_logo_url(url):
            score += 10

        for token in [
            "powerball",
            "mega",
            "pick",
            "cash",
            "lotto",
            "logo",
            "fantasy",
            "wild",
            "money",
            "bonus",
            "match",
            "revancha",
            "pega",
            "loto",
            "daily",
            "numbers",
            "megabucks",
            "badger",
            "super",
            "2by2",
        ]:
            if token in low:
                score += 2

        if low.endswith(".svg"):
            score += 1
        if low.endswith(".png"):
            score += 2
        if "logo" in low:
            score += 4
        if "results" in low:
            score -= 1
        if "banner" in low:
            score -= 10
        if "ad" in low:
            score -= 8

        scored.append((score, url))

    scored.sort(reverse=True, key=lambda x: x[0])
    return scored[0][1]


def save_image_from_page_context(page, image_url: str, filepath: Path):
    js = """
    async (imageUrl) => {
      const resp = await fetch(imageUrl, { credentials: 'include' });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} for ${imageUrl}`);
      }
      const arrayBuffer = await resp.arrayBuffer();
      const bytes = Array.from(new Uint8Array(arrayBuffer));
      return bytes;
    }
    """
    bytes_list = page.evaluate(js, image_url)
    filepath.write_bytes(bytes(bytes_list))


def build_manifest():
    ensure_dirs()

    manifest = {
        "by_final_slug": {},
        "by_canonical_slug": {},
        "meta": {
            "total_files": 0,
        },
    }

    states = get_states()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            slow_mo=20,
        )

        page = browser.new_page(
            viewport={"width": 1440, "height": 2200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )

        total_downloaded = 0
        total_mapped = 0

        for state in states:
            print("\n" + "=" * 90)
            print(f"STATE: {state['name']} ({state['slug']})")
            print(f"URL: {state['source_url']}")
            print("=" * 90)

            try:
                page.goto(state["source_url"], wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(5000)
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
            except Exception as e:
                print(f"ERROR OPENING PAGE: {e}")
                continue

            try:
                page.mouse.wheel(0, 2400)
                page.wait_for_timeout(1000)
                page.mouse.wheel(0, -2400)
                page.wait_for_timeout(1000)
            except Exception:
                pass

            blocks = extract_game_blocks(page)
            seen_state_slugs = set()

            if not blocks:
                print("NO GAME BLOCKS FOUND")
                continue

            for block in blocks:
                game_title = extract_title_from_block(block)
                if not game_title:
                    continue

                info = canonical_game_info(game_title, state_code=state["slug"])
                canonical_slug = info["canonical_slug"]
                final_slug = info["final_slug"]

                if final_slug == "poker-lotto-mi" or canonical_slug == "poker-lotto":
                    continue

                if final_slug in seen_state_slugs:
                    continue

                seen_state_slugs.add(final_slug)

                candidate_urls = collect_candidate_image_urls(page, block, state["source_url"])
                best_url = choose_best_logo_url(candidate_urls)

                if not best_url:
                    print(f"NO IMAGE: {game_title} -> {final_slug}")
                    continue

                target_slug = canonical_slug if canonical_slug in MULTI_STATE_SLUGS else final_slug
                ext = file_ext_from_url(best_url)
                filename = safe_filename(target_slug, ext)
                filepath = OUTPUT_DIR / filename
                public_path = f"/lottery-logos/{filename}"

                if not filepath.exists():
                    try:
                        save_image_from_page_context(page, best_url, filepath)
                        total_downloaded += 1
                        print(f"DOWNLOADED: {game_title} -> {filename}")
                    except Exception as e:
                        print(f"DOWNLOAD ERROR: {game_title} -> {best_url} -> {e}")
                        continue
                else:
                    print(f"EXISTS: {game_title} -> {filename}")

                if canonical_slug in MULTI_STATE_SLUGS:
                    manifest["by_canonical_slug"][canonical_slug] = public_path
                else:
                    manifest["by_final_slug"][final_slug] = public_path
                    manifest["by_canonical_slug"].setdefault(canonical_slug, public_path)

                total_mapped += 1

        browser.close()

    manifest["meta"]["total_files"] = len([p for p in OUTPUT_DIR.glob("*") if p.is_file()])

    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("\nDONE")
    print(f"Downloaded files: {total_downloaded}")
    print(f"Mapped entries: {total_mapped}")
    print(f"Manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    build_manifest()