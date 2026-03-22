"""
Pipeline orchestrator.

Usage:
    python pipeline.py                          # run all banks
    python pipeline.py --banks ameriabank       # single bank
    python pipeline.py --sections credits       # single section
    python pipeline.py --output results.json    # custom output path
    python pipeline.py --workers 4              # parallel workers
"""

from __future__ import annotations

import re
import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from fix_bank_data import clean_text

from scrapers import SCRAPER_REGISTRY, BaseBankScraper, BankRecord

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Post-processing helpers
# ---------------------------------------------------------------------------

def deduplicate(records: list[BankRecord]) -> list[BankRecord]:
    """
    Remove duplicates based on a cleaned hash of the text content.
    Records from different (bank, section) pairs are always kept.
    """
    unique = []
    seen_hashes: set[int] = set()

    for r in records:
        normalized = re.sub(r'\s+', '', r.text)  # ← renamed
        key = f"{r.bank}::{r.section}::{normalized}"

        text_hash = hash(key)

        if text_hash not in seen_hashes:
            seen_hashes.add(text_hash)
            unique.append(r)

    return unique


def filter_short(records: list[BankRecord], min_chars: int = 30) -> list[BankRecord]:
    return [r for r in records if len(r.text) >= min_chars]


def apply_section_filter(
    records: list[BankRecord], sections: list[str]
) -> list[BankRecord]:
    if not sections:
        return records
    return [r for r in records if r.section in sections]


def truncate_text(records: list[BankRecord], max_chars: int = 8000) -> list[BankRecord]:
    """
    Cap each record's text at a section-specific character limit.
    Truncation always happens at a sentence boundary (։ or \\n) so the
    last sentence is never left half-cut.

    Limits (generous — with leaf-only extraction records are much smaller now):
      - branches:  40 000
      - deposits:  20 000
      - credits:   15 000
    """
    section_limits = {
        "branches": 40_000,
        "deposits": 20_000,
        "credits":  15_000,
    }
    result = []
    for r in records:
        limit = section_limits.get(r.section, max_chars)
        if len(r.text) <= limit:
            result.append(r)
            continue

        truncated = r.text[:limit]

        # Try to cut at the last Armenian full-stop (։) within the limit
        arm_stop = truncated.rfind("։")
        newline   = truncated.rfind("\n")
        period    = truncated.rfind(".")

        # Pick the latest clean boundary that is at least 50 % into the text
        boundary = max(
            (pos for pos in [arm_stop, newline, period] if pos > limit // 2),
            default=-1,
        )
        if boundary > 0:
            truncated = truncated[:boundary + 1]

        result.append(BankRecord(
            bank=r.bank, section=r.section,
            url=r.url, text=truncated, metadata=r.metadata
        ))
        logger.debug(
            "[truncate] %s/%s %d→%d chars", r.bank, r.section,
            len(r.text), len(truncated)
        )
    return result


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_scraper(slug: str, scraper_cls: type[BaseBankScraper]) -> list[BankRecord]:
    start = time.perf_counter()
    scraper = scraper_cls()
    records = scraper.scrape_all()

    cleaned_records = []
    for r in records:
        r.text = clean_text(r.text)  # Սա պետք է թարմացնի օբյեկտը
        if len(r.text) >= 15:  # MIN_TEXT_LENGTH
            cleaned_records.append(r)

    elapsed = time.perf_counter() - start
    logger.info("✓ %-20s %3d records in %.1fs", slug, len(cleaned_records), elapsed)
    return cleaned_records


def run_pipeline(
    bank_slugs: list[str] | None = None,
    sections: list[str] | None = None,
    workers: int = 3,
    output_path: str = "bank_data_clean.json",
    min_chars: int = 30,
    max_chars: int = 8000,
) -> list[dict]:
    """
    Main entry point.

    Args:
        bank_slugs: Which banks to scrape. None → all registered banks.
        sections:   Which sections to keep. None → all three.
        workers:    Thread-pool size (be polite — keep low).
        output_path: Where to write JSON output.
        min_chars:   Minimum text length to keep a record.
        max_chars:   Maximum text length per record (truncated at newline boundary).
    """
    slugs_to_run = bank_slugs or list(SCRAPER_REGISTRY.keys())
    unknown = [s for s in slugs_to_run if s not in SCRAPER_REGISTRY]
    if unknown:
        raise ValueError(f"Unknown bank(s): {unknown}. "
                         f"Available: {list(SCRAPER_REGISTRY)}")

    all_records: list[BankRecord] = []

    logger.info("Starting pipeline — banks: %s | workers: %d", slugs_to_run, workers)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(run_scraper, slug, SCRAPER_REGISTRY[slug]): slug
            for slug in slugs_to_run
        }
        for future in as_completed(futures):
            slug = futures[future]
            try:
                all_records.extend(future.result())
            except Exception as exc:
                logger.error("Scraper %s raised: %s", slug, exc)

    # Post-process
    before = len(all_records)
    all_records = filter_short(all_records, min_chars)
    all_records = deduplicate(all_records)
    all_records = truncate_text(all_records, max_chars)
    if sections:
        all_records = apply_section_filter(all_records, sections)
    after = len(all_records)

    logger.info("Post-processing: %d → %d records (filtered %d)", before, after, before - after)

    # Summary by bank + section
    from collections import Counter
    breakdown = Counter((r.bank, r.section) for r in all_records)
    for (bank, section), count in sorted(breakdown.items()):
        logger.info("  %-20s %-10s %3d records", bank, section, count)

    output = [r.to_dict() for r in all_records]

    # Write output
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved %d records → %s", len(output), out.resolve())

    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Armenian Bank Data Scraper Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available banks: {', '.join(SCRAPER_REGISTRY)}",
    )
    p.add_argument(
        "--banks", nargs="+", metavar="BANK",
        help="Bank slugs to scrape (default: all)"
    )
    p.add_argument(
        "--sections", nargs="+",
        choices=["credits", "deposits", "branches"],
        help="Sections to include (default: all)"
    )
    p.add_argument(
        "--workers", type=int, default=3,
        help="Parallel workers (default: 3)"
    )
    p.add_argument(
        "--output", default="bank_data_clean.json",
        help="Output JSON file path (default: bank_data_clean.json)"
    )
    p.add_argument(
        "--min-chars", type=int, default=30,
        help="Minimum text length to keep a record (default: 30)"
    )
    p.add_argument(
        "--list-banks", action="store_true",
        help="List registered banks and exit"
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    if args.list_banks:
        print("Registered banks:")
        for slug, cls in SCRAPER_REGISTRY.items():
            print(f"  {slug:20s} → {cls.__name__}")
        return

    run_pipeline(
        bank_slugs=args.banks,
        sections=args.sections,
        workers=args.workers,
        output_path=args.output,
        min_chars=args.min_chars,
    )


if __name__ == "__main__":
    main()