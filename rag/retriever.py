"""
rag/retriever.py

Retrieves relevant bank knowledge chunks from ChromaDB.
Used by the LiveKit agent at query time.

Usage (standalone test):
    python -m rag.retriever "Ամերիաբանկի վարկի տոկոսադրույքը"
    python -m rag.retriever "Ինեկոբանկ մասնաճյուղ Կոմիտաս" --bank Inecobank
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass

import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

COLLECTION_NAME = "bank_knowledge"
DEFAULT_N_RESULTS = 5

# Words that appear in virtually every branch record and carry no
# location-specific signal.  Excluded from the keyword scoring so that
# generic terms don't inflate scores for unrelated branches.
_BRANCH_NOISE: set[str] = {
    "մասնաճյուղ", "մասնաճյուղը", "մասնաճյուղի", "մասնաճյուղեր",
    "գտնվում", "հասցեում", "հասցե", "հեռախոսահամարն", "հեռախոս",
    "աշխատանքային", "ժամերն", "ժամը", "երկուշաբթիից", "ուրբաթ",
    "շաբաթ", "կիրակի", "տոնական", "օրերին", "փակ", "բաց",
    "փողոց", "շենք", "մուտք", "քաղաքում", "մարզում", "թաղամասում",
    "արդշինբանկի", "ամերիաբանկի", "ինեկոբանկի",
    "արդշինբանկ", "ամերիաբանկ", "ինեկոբանկ",
    "ardshinbank", "ameriabank", "inecobank",
    "branch", "կա",
}


@dataclass
class RetrievedChunk:
    text:     str
    bank:     str
    section:  str
    url:      str
    distance: float   # lower = more similar (cosine)

    def __str__(self) -> str:
        return (
            f"[{self.bank} / {self.section}]\n"
            f"{self.text}\n"
            f"(source: {self.url})"
        )


# Latin → Armenian transliteration for city names that Whisper may output in Latin
_LATIN_TO_ARM: dict[str, str] = {
    "gyumri": "Գյումրի", "giumri": "Գյումրի", "kumayri": "Գյումրի",
    "vanadzor": "Վանաձոր", "yerevan": "Երևան", "erevan": "Երևան",
    "abovyan": "Աբովյան", "hrazdan": "Հրազդան", "kapan": "Կապան",
    "goris": "Գորիս", "sevan": "Սևան", "armavir": "Արմավիր",
    "artashat": "Արտաշատ", "ashtarak": "Աշտարակ", "ijevan": "Իջևան",
    "dilijan": "Դիլիջան", "stepanavan": "Ստեփանավան",
    "arabkir": "Արաբկիր", "komitas": "Կոմիտաս", "kentron": "Կենտրոն",
    "shengavit": "Շենգավիթ", "malatia": "Մալաթիա", "ajapnyak": "Աջափնյակ",
    "nor nork": "Նոր Նորք", "davtashen": "Դավթաշեն", "avan": "Ավան",
}


def _arm_stem(word: str) -> str:
    """
    Strip common Armenian case suffixes to get a searchable stem.
    Handles: ում, ից, ով, ին, ի, ու, ն, ը, ն suffixes.
    e.g. «Գյումրիում» → «Գյումրի»
         «Կոմիտասում» → «Կոմիտաս»
         «Երևանից»    → «Երևան»
    Falls back to first (len-2) chars if no suffix matched.
    """
    SUFFIXES = ["ների", "ներից", "ներով", "ներում", "ներին",
                "ում", "ից", "ով", "ին", "ի", "ու", "ն", "ը"]
    w = word
    for suffix in SUFFIXES:
        if w.endswith(suffix) and len(w) - len(suffix) >= 3:
            return w[: -len(suffix)]
    return w[:max(4, len(w) - 2)]


class BankRetriever:
    """
    Wraps ChromaDB collection for semantic search over bank knowledge.

    Args:
        db_path:    Path to ChromaDB persistent storage directory.
        n_results:  Number of chunks to return per query.
    """

    def __init__(self, db_path: str = "chroma_db", n_results: int = DEFAULT_N_RESULTS):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY not set. Add it to .env file.")

        self.n_results = n_results
        self._client = chromadb.PersistentClient(path=db_path)
        self._ef = embedding_functions.OpenAIEmbeddingFunction(
            api_key=api_key,
            model_name="text-embedding-3-small",
        )
        self._collection = self._client.get_collection(
            name=COLLECTION_NAME,
            embedding_function=self._ef,
        )
        logger.info(
            "BankRetriever ready — %d chunks in collection",
            self._collection.count(),
        )

    def query(
        self,
        question: str,
        bank:    str | None = None,
        section: str | None = None,
        n_results: int | None = None,
    ) -> list[RetrievedChunk]:
        """
        Retrieves relevant chunks.

        For 'branches' section: bypasses semantic search entirely and uses
        location-keyword scoring instead.  Branch records are structurally
        identical (same template), so their embeddings are nearly
        indistinguishable and cosine similarity cannot surface the right city.

        For other sections: standard semantic search via ChromaDB.
        """
        n = n_results or self.n_results

        if section == "branches":
            return self._keyword_search_branches(question, bank=bank, top_n=n)

        # ── Semantic search for credits / deposits ──────────────────────────
        where: dict | None = None
        if bank and section:
            where = {"$and": [{"bank": {"$eq": bank}}, {"section": {"$eq": section}}]}
        elif bank:
            where = {"bank": {"$eq": bank}}
        elif section:
            where = {"section": {"$eq": section}}

        kwargs: dict = {
            "query_texts": [question],
            "n_results":   n,
            "include":     ["documents", "metadatas", "distances"],
        }
        if where:
            kwargs["where"] = where

        try:
            results = self._collection.query(**kwargs)
        except Exception as exc:
            logger.error("ChromaDB query failed: %s", exc)
            return []

        chunks = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            chunks.append(RetrievedChunk(
                text=doc,
                bank=meta.get("bank", ""),
                section=meta.get("section", ""),
                url=meta.get("url", ""),
                distance=dist,
            ))
        return chunks

    # ── Branch keyword search ───────────────────────────────────────────────

    def _keyword_search_branches(
        self,
        question: str,
        bank: str | None,
        top_n: int,
    ) -> list[RetrievedChunk]:
        """
        Fetch ALL branch chunks for the given bank from ChromaDB (no embedding
        query), then rank them by location-keyword overlap with the question.

        Scoring:
          +2  exact word match  (e.g. «Գյումրի» in chunk text)
          +1  stem  match       (e.g. «Գյում»   in chunk text)

        Only location-specific words (after stripping noise) are scored,
        so generic words like «մասնաճյուղ» do not inflate scores for
        every branch equally.
        """
        # Fetch all branch chunks from ChromaDB (metadata only + documents)
        where: dict
        if bank:
            where = {"$and": [{"bank": {"$eq": bank}}, {"section": {"$eq": "branches"}}]}
        else:
            where = {"section": {"$eq": "branches"}}

        try:
            raw = self._collection.get(
                where=where,
                include=["documents", "metadatas"],
            )
        except Exception as exc:
            logger.error("ChromaDB get failed for branches: %s", exc)
            return []

        if not raw["ids"]:
            logger.info("No branch chunks found for bank=%s", bank)
            return []

        # Build RetrievedChunk list (distance=0.0 placeholder)
        all_chunks = [
            RetrievedChunk(
                text=doc,
                bank=meta.get("bank", ""),
                section=meta.get("section", ""),
                url=meta.get("url", ""),
                distance=0.0,
            )
            for doc, meta in zip(raw["documents"], raw["metadatas"])
        ]

        logger.info(
            "Branch keyword search: %d candidates for bank=%s", len(all_chunks), bank
        )

        # Extract location words (strip noise + bank names)
        raw_words = [
            w.strip("։,.?!՞՛ ")
            for w in question.split()
            if len(w.strip("։,.?!՞՛ ")) >= 3
        ]
        # Transliterate any Latin city names Whisper may have output
        raw_words = [
            _LATIN_TO_ARM.get(w.lower(), w) for w in raw_words
        ]
        location_words = [
            w for w in raw_words if w.lower() not in _BRANCH_NOISE
        ]

        stems = [_arm_stem(w) for w in location_words]

        logger.info("Branch location_words=%s  stems=%s", location_words, stems)

        def score(chunk: RetrievedChunk) -> int:
            t = chunk.text.lower()
            s = sum(2 for w in location_words if w.lower() in t)
            s += sum(1 for stem in stems if stem.lower() in t)
            return s

        scored = sorted(
            ((score(c), i, c) for i, c in enumerate(all_chunks)),
            key=lambda x: (-x[0], x[1]),
        )

        results = [c for _, _, c in scored[:top_n]]

        if results:
            logger.info(
                "Branch top result (score=%d): %s...",
                scored[0][0],
                results[0].text[:70],
            )

        return results

    def format_context(self, chunks: list[RetrievedChunk]) -> str:
        """
        Format retrieved chunks into a single context string for the LLM prompt.
        """
        if not chunks:
            return ""

        parts = []
        for i, chunk in enumerate(chunks, 1):
            parts.append(
                f"[{i}] {chunk.bank} — {chunk.section}\n"
                f"{chunk.text}"
            )

        return "\n\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# CLI — quick test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    p = argparse.ArgumentParser(description="Test RAG retrieval")
    p.add_argument("question", help="Question to search for")
    p.add_argument("--db",      default="chroma_db",  help="ChromaDB path")
    p.add_argument("--bank",    default=None,          help="Filter by bank name")
    p.add_argument("--section", default=None,          help="Filter by section")
    p.add_argument("--n",       type=int, default=3,   help="Number of results")
    args = p.parse_args()

    retriever = BankRetriever(db_path=args.db, n_results=args.n)
    chunks = retriever.query(args.question, bank=args.bank, section=args.section)

    print(f"\n{'='*60}")
    print(f"Query: {args.question}")
    print(f"Results: {len(chunks)}")
    print('='*60)
    for chunk in chunks:
        print(f"\n{chunk}")
        print(f"Distance: {chunk.distance:.4f}")