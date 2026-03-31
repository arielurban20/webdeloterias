import json
import os
import re
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from app.database import SessionLocal
from app.utils.game_normalizer import canonical_game_info


OUTPUT_DIR = Path("public/lottery-logos")
MANIFEST_PATH = Path("public/lottery-logos-manifest.json")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

MULTI_STATE_SLUGS = {
    "powerball",
    "powerball-double-play",
    "mega-millions",
    "millionaire-for-life",
    "lotto-america",
    "2by2",
}

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def clean(text_value: str) -> str:
    return re.sub(r"\s+", " ", text_value or "").strip()


def safe_filename_from_slug(slug: str, ext: str) -> str:
    ext = ext.lower().strip(".")
    if ext not in {"png", "jpg", "jpeg", "svg", "webp"}:
        ext = "png"
    return f"{slug}.{ext}"


def file_ext_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower()

    for ext in [".png", ".jpg", ".jpeg", ".svg", ".webp"]:
        if path.endswith(ext):
            return ext.replace(".", "")

    return "png"


def ensure_output():
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


def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=45)
    resp.raise_for_status()
    return resp.text


def find_game_cards(soup: BeautifulSoup):
    cards = []

    # Lottery Post suele tener muchos layouts distintos.
    # Probamos varios patrones razonables.
    for section in soup.select("section"):
        h2 = section.select_one("h2")
        if not h2:
            continue

        section_title = clean(h2.get_text(" ", strip=True))
        if not section_title:
            continue

        results_links = section.select("a[href]")
        if not results_links:
            continue

        for a in results_links:
            href = a.get("href", "").strip()
            text_value = clean(a.get_text(" ", strip=True))

            if not href:
                continue

            if not re.search(r"(results|past results|calendar|more|tickets)", text_value, re.IGNORECASE):
                continue

            cards.append({
                "section_title": section_title,
                "link_text": text_value,
                "href": href,
                "node": a,
            })

    return cards


def guess_game_title_from_card(card: dict) -> str:
    node = card["node"]

    # buscar título cercano
    candidates = []

    # ancestros cercanos
    current = node.parent
    steps = 0
    while current and steps < 4:
        for tag_name in ["h3", "h4", "strong", "b"]:
            for found in current.find_all(tag_name):
                txt = clean(found.get_text(" ", strip=True))
                if txt:
                    candidates.append(txt)
        current = current.parent
        steps += 1

    # fallback: section title
    if not candidates:
        section_title = clean(card.get("section_title", ""))
        if section_title:
            candidates.append(section_title)

    # fallback: link text
    if not candidates:
        link_text = clean(card.get("link_text", ""))
        if link_text:
            candidates.append(link_text)

    # escoger el más corto y razonable
    for txt in candidates:
        if 2 <= len(txt) <= 120:
            return txt

    return clean(card.get("link_text", "")) or clean(card.get("section_title", ""))


def find_logo_url_near_card(base_url: str, card: dict) -> str | None:
    node = card["node"]

    # subir hasta un contenedor razonable
    container = node
    steps = 0
    while getattr(container, "parent", None) and steps < 5:
        container = container.parent
        steps += 1

        if not hasattr(container, "find_all"):
            continue

        imgs = container.find_all(["img", "source"])
        for img in imgs:
            src = (
                img.get("src")
                or img.get("data-src")
                or img.get("data-lazy-src")
                or img.get("srcset")
            )

            if not src:
                continue

            src = src.split(" ")[0].strip()
            if not src:
                continue

            if any(x in src.lower() for x in ["logo", "lotto", "pick", "powerball", "mega", "cash", "draw", "game"]):
                return urljoin(base_url, src)

        # si hay cualquier imagen, como fallback usa la primera
        if imgs:
            img = imgs[0]
            src = (
                img.get("src")
                or img.get("data-src")
                or img.get("data-lazy-src")
                or img.get("srcset")
            )
            if src:
                src = src.split(" ")[0].strip()
                if src:
                    return urljoin(base_url, src)

    return None


def download_file(url: str, path: Path):
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    path.write_bytes(resp.content)


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def build_logo_library():
    ensure_output()

    manifest = {
        "by_final_slug": {},
        "by_canonical_slug": {},
        "meta": {
            "generated_at": None,
            "total_files": 0,
        },
    }

    downloaded_hashes = {}
    states = get_states()
    total_downloaded = 0
    total_mapped = 0

    for state in states:
        state_slug = state["slug"]
        state_name = state["name"]
        url = state["source_url"]

        print("\n" + "=" * 80)
        print(f"STATE: {state_name} ({state_slug})")
        print(f"URL: {url}")
        print("=" * 80)

        try:
            html = fetch_html(url)
        except Exception as e:
            print(f"ERROR FETCHING STATE PAGE: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        cards = find_game_cards(soup)

        seen_slugs = set()

        for card in cards:
            game_title = guess_game_title_from_card(card)
            if not game_title:
                continue

            info = canonical_game_info(game_title, state_code=state_slug)
            canonical_slug = info["canonical_slug"]
            final_slug = info["final_slug"]

            if final_slug in seen_slugs:
                continue

            seen_slugs.add(final_slug)

            logo_url = find_logo_url_near_card(url, card)
            if not logo_url:
                print(f"NO LOGO FOUND: {game_title} -> {final_slug}")
                continue

            target_slug = canonical_slug if canonical_slug in MULTI_STATE_SLUGS else final_slug
            ext = file_ext_from_url(logo_url)
            filename = safe_filename_from_slug(target_slug, ext)
            filepath = OUTPUT_DIR / filename

            try:
                if not filepath.exists():
                    download_file(logo_url, filepath)
                    file_hash = hash_file(filepath)
                    downloaded_hashes[file_hash] = filename
                    total_downloaded += 1
                    print(f"DOWNLOADED: {game_title} -> {filename}")
                else:
                    file_hash = hash_file(filepath)
                    downloaded_hashes[file_hash] = filename
                    print(f"EXISTS: {game_title} -> {filename}")
            except Exception as e:
                print(f"DOWNLOAD ERROR: {game_title} -> {logo_url} -> {e}")
                continue

            public_path = f"/lottery-logos/{filename}"

            if canonical_slug in MULTI_STATE_SLUGS:
                manifest["by_canonical_slug"][canonical_slug] = public_path
            else:
                manifest["by_final_slug"][final_slug] = public_path
                manifest["by_canonical_slug"].setdefault(canonical_slug, public_path)

            total_mapped += 1

    manifest["meta"]["generated_at"] = datetime.utcnow().isoformat() + "Z"
    manifest["meta"]["total_files"] = len(list(OUTPUT_DIR.glob("*.*")))

    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("\nDONE")
    print(f"Downloaded files: {total_downloaded}")
    print(f"Mapped entries: {total_mapped}")
    print(f"Manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    from datetime import datetime
    build_logo_library()