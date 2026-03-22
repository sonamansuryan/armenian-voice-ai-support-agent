from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("rag.indexer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
COLLECTION_NAME = "bank_knowledge"
MAX_CHUNK_CHARS = 1500   # split records longer than this
CHUNK_OVERLAP   = 100    # overlap between chunks to preserve context


def chunk_text(text: str, max_chars: int = MAX_CHUNK_CHARS, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """
    Split long text into overlapping chunks at sentence boundaries.
    Short texts are returned as-is (single chunk).
    """
    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        if end >= len(text):
            chunks.append(text[start:])
            break

        # Try to cut at Armenian sentence boundary (։) or newline
        for boundary in ["։", "\n", "."]:
            pos = text.rfind(boundary, start, end)
            if pos > start + max_chars // 2:
                end = pos + 1
                break

        chunks.append(text[start:end])
        start = end - overlap

    return [c.strip() for c in chunks if c.strip()]


def build_index(data_path: str, db_path: str) -> None:
    """
    Load JSON, chunk, embed, and store in ChromaDB.
    Re-running this will reset the collection (idempotent).
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not set. Add it to .env file.")
        sys.exit(1)

    # Load data
    with open(data_path, encoding="utf-8") as f:
        records: list[dict] = json.load(f)
    logger.info("Loaded %d records from %s", len(records), data_path)

    # Setup ChromaDB
    client = chromadb.PersistentClient(path=db_path)

    # Delete existing collection if present (fresh index)
    try:
        client.delete_collection(COLLECTION_NAME)
        logger.info("Deleted existing collection '%s'", COLLECTION_NAME)
    except Exception:
        pass

    openai_ef = embedding_functions.OpenAIEmbeddingFunction(
        api_key=api_key,
        model_name="text-embedding-3-small",
    )
    collection = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=openai_ef,
        metadata={"hnsw:space": "cosine"},
    )

    # Chunk, build metadata, and add to collection
    ids, texts, metadatas = [], [], []
    doc_id = 0

    for record in records:
        bank    = record["bank"]
        section = record["section"]
        url     = record["url"]
        text    = record["text"]

        chunks = chunk_text(text)
        for i, chunk in enumerate(chunks):
            ids.append(f"doc_{doc_id}")
            texts.append(chunk)
            metadatas.append({
                "bank":    bank,
                "section": section,
                "url":     url,
                "chunk":   i,
            })
            doc_id += 1

    logger.info("Total chunks to embed: %d", len(ids))

    # Batch upsert (ChromaDB handles batching internally)
    BATCH = 100
    for i in range(0, len(ids), BATCH):
        collection.add(
            ids=ids[i:i+BATCH],
            documents=texts[i:i+BATCH],
            metadatas=metadatas[i:i+BATCH],
        )
        logger.info("Indexed %d / %d chunks", min(i + BATCH, len(ids)), len(ids))

    logger.info("✓ Index built — %d chunks in collection '%s' at '%s'",
                len(ids), COLLECTION_NAME, db_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build ChromaDB index from bank data")
    p.add_argument("--data", default="bank_data_clean.json", help="Path to JSON data file")
    p.add_argument("--db",   default="chroma_db",            help="ChromaDB storage directory")
    args = p.parse_args()

    build_index(args.data, args.db)