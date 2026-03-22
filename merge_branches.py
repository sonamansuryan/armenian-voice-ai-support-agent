import argparse
import json
import logging
import sys
import time
import re
from pathlib import Path
from collections import Counter

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("merge_branches")

# ---------------------------------------------------------------------------
# rate.am config
# ---------------------------------------------------------------------------
RATE_AM_BANKS = {
    "Ameriabank":  "ameriabank",
    "Ardshinbank": "ardshinbank",
    "Inecobank":   "inecobank",
}
BASE_URL = "https://www.rate.am/hy/bank"

ARM_RE = re.compile(r"[\u0530-\u058F\uFB13-\uFB17]")
ADDR_RE = re.compile(
    r"Երևան|Կոտայք|Արարատ|Արմավիր|Արագածոտն|Գեղարքունիք|"
    r"Լոռի|Շիրակ|Սյունիք|Վայոց|Տավուշ|"
    r"փող|պող|հրապ|փ\.|պ\.|հ\.|քաղ"
)
PHONE_RE = re.compile(r"\+?374|\(374")


def is_armenian(text: str, min_chars: int = 10) -> bool:
    return len(ARM_RE.findall(text)) >= min_chars


def fetch_html(url: str, session: requests.Session, timeout: int = 30) -> str | None:
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return resp.text
    except requests.RequestException as exc:
        logger.error("GET %s failed: %s", url, exc)
        return None


def parse_branches(html: str, bank_name: str, source_url: str) -> list[dict]:
    """
    Parse rate.am page → ONE record per branch.

    rate.am renders each branch as a card with a <a href="tel:..."> link.
    Strategy: anchor on tel: links, walk up DOM to find the smallest
    container that holds Armenian text + phone (≤2000 chars).
    This produces exactly one clean record per branch.
    """
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.select("script, style, nav, header, footer, img, svg"):
        tag.decompose()

    seen: set[str] = set()
    cards: list[str] = []

    # Strategy 1: tel: anchor walk-up — one card per branch.
    # Walk UP until the container includes "Աշխատանքային ժամեր"
    # (working hours marker) — this ensures hours are included.
    HOURS_MARKER = "Աշխատանքային"
    tel_links = soup.select('a[href^="tel:"]')
    for link in tel_links:
        node = link.parent
        best: str | None = None
        for _ in range(10):
            if node is None:
                break
            text = re.sub(r"\s+", " ", node.get_text(separator=" ")).strip()
            if not is_armenian(text, 8) or len(text) > 3000:
                node = node.parent
                continue
            if HOURS_MARKER in text and len(text) >= 40:
                best = text
                break  # found container with hours — stop here
            if len(text) >= 40 and best is None:
                best = text  # fallback: at least has address
            node = node.parent

        if best and best not in seen:
            seen.add(best)
            cards.append(best)

    # Remove supersets — keep the most granular card per branch
    if cards:
        cards = [t for t in cards
                 if not any(t != o and t in o for o in cards)]
        logger.info("[%s] tel-anchor → %d branch records", bank_name, len(cards))
        return [
            {"bank": bank_name, "section": "branches", "url": source_url, "text": t}
            for t in cards
        ]

    # Strategy 2 (fallback): div scan with address + phone patterns
    logger.warning("[%s] No tel: links — falling back to div scan", bank_name)
    for el in soup.find_all(["div", "li", "article"]):
        t = re.sub(r"\s+", " ", el.get_text(separator=" ")).strip()
        if (is_armenian(t, 8) and 40 <= len(t) <= 800
                and bool(ADDR_RE.search(t)) and bool(PHONE_RE.search(t))
                and t not in seen):
            seen.add(t)
            cards.append(t)

    cards = [t for t in cards if not any(t != o and t in o for o in cards)]
    logger.info("[%s] div-scan fallback → %d branch records", bank_name, len(cards))
    return [
        {"bank": bank_name, "section": "branches", "url": source_url, "text": t}
        for t in cards
    ]


def scrape_all_branches(delay: float = 1.5) -> list[dict]:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "hy-AM,hy;q=0.9,en-US;q=0.5,en;q=0.3",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.rate.am/hy/",
    })

    all_records: list[dict] = []
    for bank_name, slug in RATE_AM_BANKS.items():
        url = f"{BASE_URL}/{slug}"
        logger.info("Scraping %s from %s …", bank_name, url)
        time.sleep(delay)
        html = fetch_html(url, session)
        if not html:
            logger.warning("Skipping %s — fetch failed.", bank_name)
            continue
        records = parse_branches(html, bank_name, url)
        all_records.extend(records)

    return all_records


def merge(original_path: str, new_branches: list[dict], output_path: str) -> None:
    """
    Load original JSON, remove old branch entries, insert new ones, save.
    Credits and deposits are untouched.
    """
    orig = Path(original_path)
    if not orig.exists():
        logger.error("Input file not found: %s", original_path)
        sys.exit(1)

    with orig.open(encoding="utf-8") as f:
        data: list[dict] = json.load(f)

    # Split
    non_branch = [r for r in data if r["section"] != "branches"]
    old_branches = [r for r in data if r["section"] == "branches"]

    old_counts = Counter(r["bank"] for r in old_branches)
    new_counts = Counter(r["bank"] for r in new_branches)

    logger.info("Old branches: %s", dict(old_counts))
    logger.info("New branches: %s", dict(new_counts))

    # Warn if a bank got no new data (keep old in that case)
    final_branches = list(new_branches)
    for bank in RATE_AM_BANKS:
        if new_counts.get(bank, 0) == 0:
            old_for_bank = [r for r in old_branches if r["bank"] == bank]
            logger.warning(
                "[%s] rate.am returned 0 records — keeping %d old entries.",
                bank, len(old_for_bank),
            )
            final_branches.extend(old_for_bank)

    merged = non_branch + final_branches

    # Sort: keep original order logic (bank, section)
    section_order = {"credits": 0, "deposits": 1, "branches": 2}
    merged.sort(key=lambda r: (r["bank"], section_order.get(r["section"], 9)))

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Saved %d records → %s  (credits: %d, deposits: %d, branches: %d)",
        len(merged),
        out.resolve(),
        sum(1 for r in merged if r["section"] == "credits"),
        sum(1 for r in merged if r["section"] == "deposits"),
        sum(1 for r in merged if r["section"] == "branches"),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Merge rate.am branches into bank_data JSON")
    p.add_argument("--input",  default="bank_data_final.json", help="Source JSON file")
    p.add_argument("--output", default="bank_data_final.json", help="Output JSON file (can be same as input)")
    p.add_argument("--delay",  type=float, default=1.5, help="Delay between requests (seconds)")
    p.add_argument("--dry-run", action="store_true", help="Parse and print stats only, don't write")
    args = p.parse_args()

    new_branches = scrape_all_branches(delay=args.delay)
    print(f"\n✓ Scraped {len(new_branches)} branch records from rate.am")

    if args.dry_run:
        for r in new_branches[:5]:
            print(f"  [{r['bank']}] {r['text'][:120]}")
        print("  (dry-run — nothing written)")
        return

    merge(args.input, new_branches, args.output)
    print(f"\n✓ Done. Output: {args.output}")


if __name__ == "__main__":
    main()