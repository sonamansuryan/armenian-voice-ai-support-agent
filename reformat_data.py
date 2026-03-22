"""
reformat_data.py

Rewrites raw scraped bank data into clean, natural Armenian text
suitable for a voice AI agent using GPT-4o.

Usage:
    python reformat_data.py
    python reformat_data.py --input bank_data_final.json --output bank_data_clean.json
    python reformat_data.py --dry-run        # preview first 3 records only
    python reformat_data.py --workers 5      # parallel API calls (faster)

Cost estimate: ~$0.50 for 167 records
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("reformat")

# ---------------------------------------------------------------------------
# Prompts per section
# ---------------------------------------------------------------------------

SECTION_PROMPTS = {
    "credits": """Դու հայերեն խմբագիր ես։ Քեզ տրվում է հայկական բանկի վարկային ծառայության վերաբերյալ հում տեքստ, որը scrape-ված է կայքից։
Տեքստը կարող է պարունակել աղբ, կտրված նախադասություններ, թվանշաններ առանց կոնտեքստի, UI տարրեր։

Քո խնդիրն է վերաձևել տեքստը հետևյալ կանոններով.
1. Գրիր բնական, ամբողջական հայերեն նախադասություններով։
2. Պահիր բոլոր կոնկրետ թվային տվյալները — գումարներ, տոկոսներ, ժամկետներ։
3. Հեռացրու UI աղբը (կոճակների անուններ, մենյուի տարրեր, HTML արտեֆակտներ)։
4. Կառուցի տեքստը հետևյալ հերթականությամբ եթե տեղեկատվությունը հասանելի է.
   - Վարկի տեսակ և նպատակ
   - Վարկի գումար (min-max, արժույթ)
   - Մարման ժամկետ
   - Տոկոսադրույք (անվանական և փաստացի)
   - Անհրաժեշտ փաստաթղթեր կամ պայմաններ
5. Մի հորինիր տվյալներ — օգտագործիր ՄԻԱՅՆ տրված տեքստի տեղեկատվությունը։
6. Պատասխանիր ՄԻԱՅՆ վերաձևված տեքստով — ոչ մի բացատրություն կամ մեկնաբանություն։""",

    "deposits": """Դու հայերեն խմբագիր ես։ Քեզ տրվում է հայկական բանկի ավանդային ծառայության վերաբերյալ հում տեքստ, որը scrape-ված է կայքից։
Տեքստը կարող է պարունակել աղբ, կտրված նախադասություններ, թվանշաններ առանց կոնտեքստի, UI տարրեր։

Քո խնդիրն է վերաձևել տեքստը հետևյալ կանոններով.
1. Գրիր բնական, ամբողջական հայերեն նախադասություններով։
2. Պահիր բոլոր կոնկրետ թվային տվյալները — տոկոսներ, ժամկետներ, նվազագույն գումարներ։
3. Հեռացրու UI աղբը։
4. Կառուցի տեքստը հետևյալ հերթականությամբ եթե տեղեկատվությունը հասանելի է.
   - Ավանդի տեսակ և նկարագրություն
   - Արժույթ (դրամ, դոլար, եվրո)
   - Տոկոսադրույք ըստ արժույթի և ժամկետի
   - Նվազագույն գումար
   - Ժամկետ
   - Հատուկ պայմաններ (վաղաժամ փակման տույժ և այլն)
5. Մի հորինիր տվյալներ։
6. Պատասխանիր ՄԻԱՅՆ վերաձևված տեքստով։""",

    "branches": """Դու հայերեն խմբագիր ես։ Քեզ տրվում է հայկական բանկի մասնաճյուղի վերաբերյալ հում տեքստ, որը scrape-ված է կայքից։
Տեքստը կարող է պարունակել մի քանի մասնաճյուղի տեղեկատվություն։

Քո խնդիրն է վերաձևել տեքստը հետևյալ կանոններով.
1. Գրիր բնական, ամբողջական հայերեն նախադասություններով։
2. Յուրաքանչյուր մասնաճյուղի համար պահիր.
   - Մասնաճյուղի անվանումը կամ թաղամասը
   - Ամբողջական հասցեն (փողոց, շենք)
   - Հեռախոսահամարը
   - Աշխատանքային ժամերը ըստ օրերի
3. Հեռացրու UI աղբը, կրկնությունները։
4. Կրճատումները գրիր լրիվ ձևով (փ. → փողոց, պող. → պողոտա, խճ. → խճուղի)։
5. Մի հորինիր տվյալներ։
6. Պատասխանիր ՄԻԱՅՆ վերաձևված տեքստով։""",
}


# ---------------------------------------------------------------------------
# Reformat single record
# ---------------------------------------------------------------------------

def reformat_record(
    record: dict,
    client: OpenAI,
    model: str = "gpt-4o",
    max_retries: int = 3,
) -> dict:
    section = record.get("section", "credits")
    bank = record.get("bank", "")
    text = record.get("text", "")

    system_prompt = SECTION_PROMPTS.get(section, SECTION_PROMPTS["credits"])
    user_prompt = f"Բանկ: {bank}\nՍեկցիա: {section}\n\nՀԱՐԿԱՎՈՐ ՎԵՐԱՁԵՎԵԼ.\n{text}"

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=1000,
                temperature=0.1,
            )
            clean_text = response.choices[0].message.content.strip()

            return {
                **record,
                "text": clean_text,
                "original_text": text,
            }

        except Exception as e:
            logger.warning("Attempt %d failed for %s/%s: %s", attempt + 1, bank, section, e)
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)

    logger.error("All retries failed for %s/%s — keeping original", bank, section)
    return record


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def reformat_all(
    input_path: str = "bank_data_final.json",
    output_path: str = "bank_data_clean.json",
    model: str = "gpt-4o",
    workers: int = 5,
    dry_run: bool = False,
) -> None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY not set in .env")

    client = OpenAI(api_key=api_key)

    data = json.loads(Path(input_path).read_text(encoding="utf-8"))
    logger.info("Loaded %d records from %s", len(data), input_path)

    if dry_run:
        data = data[:3]
        logger.info("Dry run — processing first 3 records only")

    # Estimate cost
    total_chars = sum(len(r.get("text", "")) for r in data)
    est_tokens = total_chars / 3
    est_cost = (est_tokens / 1_000_000) * 2.5  # GPT-4o input ~$2.50/1M tokens
    logger.info(
        "Estimated cost: ~$%.3f for %d records (%d tokens)",
        est_cost, len(data), int(est_tokens),
    )

    results = [None] * len(data)
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(reformat_record, record, client, model): i
            for i, record in enumerate(data)
        }
        completed = 0
        for future in as_completed(futures):
            i = futures[future]
            record = data[i]
            try:
                results[i] = future.result()
                completed += 1
                if completed % 10 == 0 or completed == len(data):
                    logger.info(
                        "Progress: %d/%d (%.0f%%)",
                        completed, len(data), 100 * completed / len(data),
                    )
            except Exception as e:
                logger.error("Record %d failed: %s", i, e)
                results[i] = record
                failed += 1

    # Remove original_text from final output (keep it clean)
    final = []
    for r in results:
        if r:
            r_clean = {k: v for k, v in r.items() if k != "original_text"}
            final.append(r_clean)

    Path(output_path).write_text(
        json.dumps(final, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info("Done — %d records saved to %s", len(final), output_path)
    if failed:
        logger.warning("%d records kept original due to API errors", failed)

    if dry_run:
        print("\n=== DRY RUN PREVIEW ===\n")
        for r in results[:3]:
            if r:
                print(f"Bank: {r['bank']} | Section: {r['section']}")
                print(f"URL: {r['url']}")
                print(f"Text:\n{r['text'][:400]}")
                print("-" * 60)


# ---------------------------------------------------------------------------
# Re-index into ChromaDB
# ---------------------------------------------------------------------------

def reindex(clean_path: str = "bank_data_clean.json") -> None:
    logger.info("Re-indexing %s into ChromaDB...", clean_path)
    try:
        from rag.indexer import BankIndexer
        indexer = BankIndexer()
        indexer.index(clean_path)
        logger.info("Re-indexing complete.")
    except Exception as e:
        logger.error("Re-indexing failed: %s", e)
        logger.info(
            "Run manually: python -c \"from rag.indexer import BankIndexer; "
            "BankIndexer().index('%s')\"", clean_path
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reformat raw bank data into clean Armenian text using GPT-4o"
    )
    parser.add_argument("--input", default="bank_data_final.json")
    parser.add_argument("--output", default="bank_data_clean.json")
    parser.add_argument("--model", default="gpt-4o", help="OpenAI model (default: gpt-4o)")
    parser.add_argument("--workers", type=int, default=5, help="Parallel API calls")
    parser.add_argument("--dry-run", action="store_true", help="Process first 3 records only")
    parser.add_argument("--no-reindex", action="store_true", help="Skip ChromaDB re-indexing")
    args = parser.parse_args()

    reformat_all(
        input_path=args.input,
        output_path=args.output,
        model=args.model,
        workers=args.workers,
        dry_run=args.dry_run,
    )

    if not args.dry_run and not args.no_reindex:
        reindex(args.output)
        logger.info(
            "All done. Update BANK_DATA_PATH=%s in .env to use the new data.",
            args.output,
        )


if __name__ == "__main__":
    main()
