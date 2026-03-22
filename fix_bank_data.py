import json
import re
import unicodedata
from typing import Optional


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# Minimum character length to keep a cleaned record.
MIN_TEXT_LENGTH = 15

# Armenian Unicode range: \u0531-\u058F  (also includes U+FB13–U+FB17 ligatures)
ARM_CHAR = r"[\u0531-\u058F\uFB13-\uFB17]"

# ── UI / Navigation noise ────────────────────────────────────
# Exact Armenian UI tokens that appear as standalone words/buttons.
# Add more as you discover them.
UI_TOKENS = [
    "Մուտք",
    "Գրանցվել",
    "FAQ",
    "Քարտեզ",
    "Դիմել",
    "Դիմե՛ք",          # imperative form
    "Ձևակերպել",
    "ձեվակերպել",       # common lowercase misspelling
    "ՁԵՎԱԿԵՐՊԵԼ",
    "ՁԵՎԱԿԵՐՊԵ՛Լ",
    "մանրամասներ",      # "details" link
    "Մանրամասներ",
    "ԹՈՂՆԵԼ ԿԱՐԾԻՔ",    # "Leave a review" button
    "Թողնել կարծիք",
    "Մաքրել հաշվիչը",   # "Reset calculator" button
]

# Compiled pattern: each token as a standalone unit (not mid-word)
_ui_tokens_pattern = re.compile(
    r"(?<!\w)(" + "|".join(re.escape(t) for t in UI_TOKENS) + r")(?!\w)"
)

# ── Calculator widget UI fragments ───────────────────────────
# These are labels from the interactive JS loan-calculator widget
# that get scraped as text. They carry zero informational value.
CALCULATOR_LABELS = [
    r"Վարկի հաշվիչ",
    r"Արժույթ\s+[֏$€]?",
    r"Գումար",
    r"Ժամկետ\s*\(ամիս\)",
    r"\d+[\.,]\d+\s*%\s*Տարեկան տոկոսադրույք",   # computed result row
    r"\d[\d\s]+[֏]\s*Ամսական մարում",             # computed result row
]

_calc_pattern = re.compile(
    r"(?:" + "|".join(CALCULATOR_LABELS) + r")",
    re.UNICODE,
)

# ── Timestamp footer ─────────────────────────────────────────
_timestamp_pattern = re.compile(
    r"Թարմացված է\s+\d{1,2}\.\d{2}\.\d{4}\s+\d{1,2}:\d{2}",
    re.UNICODE,
)

# ── Copyright / legal footer noise ───────────────────────────
_copyright_pattern = re.compile(
    r"©\s*\d{4}[^։\n]{0,120}(?:պաշտպանված|reserved|rights)[^։\n]{0,60}[։.]?",
    re.UNICODE | re.IGNORECASE,
)

# ── Phone / support channel fragments ───────────────────────
_phone_pattern = re.compile(
    r"\b\d{3}\s+\d{3}\s+\d{3}\b"            # 010 510 510 style
    r"|\b24/7\b"                              # 24/7 badge
    r"|\+374[\s\-\d]{7,}",                   # international Armenian numbers
    re.UNICODE,
)

# ── Marketing / promotional phrases ─────────────────────────
# Phrases that are purely promotional and contain no financial data.
MARKETING_PHRASES = [
    r"Ֆինասնավորման նոր մշակույթ Հայաստանում[\.։]?",
    r"sprintonline\.am կայքը բազմալիք կենտրոնացած հարթակ է[^։.]*[։.]?",
    r"Ինչո՞ւ Ինեկոբանկը",
    r"Արագ և հասանելի",       # section header with no data
    r"Առանց թղթաբանության",   # section header (content kept separately below)
    # "What is sprint?" marketing header
    r"Ի՞նչ է sprint[^Ա-֏]*",
    r"Ի՞նչ\s+է\s+sprint[-\u2013]?ը[^Ա-֏]*",
]

_marketing_pattern = re.compile(
    r"(?:" + "|".join(MARKETING_PHRASES) + r")",
    re.UNICODE,
)

# ── Tab / section nav labels with no body content ────────────
# E.g.  "Վարկի Մասին   Սակագներ եվ պայմաններ   Լրացուցիչ Փաստաթղթեր"
_section_nav_pattern = re.compile(
    r"(Վարկի Մասին|Սակագներ\s+[Ee]?[Vv]?\s*[Ee]?[Uu]?\s*պայմաններ"
    r"|Սակագներ\s+եվ\s+պայմաններ"
    r"|Լրացուցիչ\s+Փաստաթղթեր)",
    re.UNICODE | re.IGNORECASE,
)

# ── Duplicate sentence splitter ──────────────────────────────
# Split on Armenian sentence-ending punctuation and newlines.
_sent_split = re.compile(r"(?<=[։.!?])\s+|\n+")

# ── Orphan fragment pattern ───────────────────────────────────
# After calc-widget stripping, patterns like "17." or lone "%" or
# "am հartak..." survive as prefixes.  Clean them up.
_orphan_prefix = re.compile(
    r"(?:^|\s)(?:\d{1,2}\.?\d*\s*%?\s*[-–]?\s*\d*\.?\d*\s*%?)"  # dangling number/rate fragment
    r"(?=\s+[Տtտ]արekan|$)",                                      # only if followed by a label or EOL
    re.UNICODE,
)


# ─────────────────────────────────────────────────────────────
# STEP 1 – Unicode normalization
# ─────────────────────────────────────────────────────────────

def unicode_normalize(text: str) -> str:
    """
    Apply NFC normalization and strip invisible/zero-width Unicode characters.
    Ensures Armenian characters are in their canonical composed form.
    """
    # NFC: canonical decomposition then canonical composition
    text = unicodedata.normalize("NFC", text)
    # Remove zero-width spaces, joiners, and other invisible characters
    invisible = [
        "\u200b",  # ZERO WIDTH SPACE
        "\u200c",  # ZERO WIDTH NON-JOINER
        "\u200d",  # ZERO WIDTH JOINER
        "\u200e",  # LEFT-TO-RIGHT MARK
        "\u200f",  # RIGHT-TO-LEFT MARK
        "\ufeff",  # BOM / ZERO WIDTH NO-BREAK SPACE
        "\u00ad",  # SOFT HYPHEN
    ]
    for ch in invisible:
        text = text.replace(ch, "")
    return text


# ─────────────────────────────────────────────────────────────
# STEP 2 – Remove UI noise tokens
# ─────────────────────────────────────────────────────────────

def remove_ui_noise(text: str) -> str:
    """
    Strip navigation items, button labels, phone numbers, copyright footers,
    and support-channel references that are scraped from page chrome.
    """
    # Remove exact UI token words
    text = _ui_tokens_pattern.sub(" ", text)
    # Remove phone numbers and "24/7" badges
    text = _phone_pattern.sub(" ", text)
    # Remove section navigation tab labels
    text = _section_nav_pattern.sub(" ", text)
    # Remove copyright footer lines
    text = _copyright_pattern.sub(" ", text)
    return text


# ─────────────────────────────────────────────────────────────
# STEP 3 – Remove calculator widget fragments
# ─────────────────────────────────────────────────────────────

def remove_calculator_ui(text: str) -> str:
    """
    Remove fragments that originate from the interactive JS loan-calculator
    widget (labels, computed cells scraped as text).
    NOTE: The key financial rates (e.g. "16%-18% Տarakan անvanakan…")
    are preserved because they match a different pattern.
    """
    text = _calc_pattern.sub(" ", text)
    return text


# ─────────────────────────────────────────────────────────────
# STEP 4 – Remove timestamp footers
# ─────────────────────────────────────────────────────────────

def remove_timestamps(text: str) -> str:
    """Remove 'Թarmatvats e DD.MM.YYYY HH:MM' page-update stamps."""
    return _timestamp_pattern.sub(" ", text)


# ─────────────────────────────────────────────────────────────
# STEP 5 – Remove marketing / promotional phrases
# ─────────────────────────────────────────────────────────────

def remove_marketing(text: str) -> str:
    """
    Strip generic promotional copy and slogans.
    Operates at the phrase level, not on entire sentences, so that
    surrounding financial data in the same sentence is preserved.
    """
    text = _marketing_pattern.sub(" ", text)
    return text


# ─────────────────────────────────────────────────────────────
# STEP 6 – Normalize whitespace and punctuation
# ─────────────────────────────────────────────────────────────

def normalize_whitespace(text: str) -> str:
    """
    - Collapse runs of spaces/tabs to a single space.
    - Collapse 3+ consecutive newlines to 2 (paragraph boundary).
    - Remove space before Armenian sentence-ending punctuation (։).
    - Remove leading/trailing whitespace.
    """
    # Collapse horizontal whitespace
    text = re.sub(r"[ \t]+", " ", text)
    # Collapse vertical whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Remove space before Armenian full stop (։) and common punctuation
    text = re.sub(r"\s+([։.,:;!?»])", r"\1", text)
    # Strip
    text = text.strip()
    return text


# ─────────────────────────────────────────────────────────────
# STEP 7 – Deduplicate sentences within a single text block
# ─────────────────────────────────────────────────────────────

def _normalize_key(s: str) -> str:
    """Return a lowercase, whitespace-collapsed string for dedup comparison."""
    return re.sub(r"\s+", " ", s.strip().lower())


def deduplicate_sentences(text: str) -> str:
    """
    Split text into sentence-like chunks (on ։ . ! ? or newlines),
    then remove exact-duplicate chunks while preserving order.

    Two extra heuristics:
    - Chunks under 8 characters are skipped (orphan fragments).
    - Normalised key ignores capitalisation and whitespace variation.
    """
    # Split keeping delimiters so we can reassemble
    parts = re.split(r"(\s*\n+\s*|(?<=[։.!?])\s+)", text)
    seen: set = set()
    result_parts: list = []

    for part in parts:
        stripped = part.strip()
        if not stripped:
            result_parts.append(part)
            continue
        # Drop very short orphan fragments left by earlier cleaning steps
        if len(stripped) < 8:
            continue
        key = _normalize_key(stripped)
        if key not in seen:
            seen.add(key)
            result_parts.append(part)
        # else: duplicate — drop it

    return "".join(result_parts)


# ─────────────────────────────────────────────────────────────
# STEP 8 – Deduplicate paragraph-level blocks
# ─────────────────────────────────────────────────────────────

def deduplicate_paragraphs(text: str) -> str:
    """
    Split on blank-line paragraph boundaries and remove duplicate paragraphs.
    This catches larger copy-paste blocks that sentence-level dedup misses.
    """
    paragraphs = re.split(r"\n\s*\n", text)
    seen: set = set()
    unique: list = []

    for para in paragraphs:
        stripped = para.strip()
        if not stripped:
            continue
        key = _normalize_key(stripped)
        if key not in seen:
            seen.add(key)
            unique.append(stripped)

    return "\n\n".join(unique)


_404_PHRASES = [
    "Էջը գոյություն չունի",
    "Վերադառնալ գլխավոր էջ",
    "Page not found",
]


def is_404_page(text: str) -> bool:
    """Return True if text is a 404 / error page with no real content."""
    stripped = text.strip()
    if len(stripped) < 200:
        for phrase in _404_PHRASES:
            if phrase in stripped:
                return True
    return False


def _deduplicate_repeated_block(text: str) -> str:
    """
    Fix the Playwright double-render artifact: the scraper sometimes gets the
    page content twice in a row (because the SPA mounts twice).

    Inecobank's SPA repeats content inline (space-separated), so we use a
    char-offset probe rather than line-splitting.

    Strategy:
    1. Take a 120-char probe from the start of the text.
    2. Search for that probe starting from 25% into the text.
    3. If found, the text up to that position is the clean first copy.
    """
    n = len(text)
    if n < 200:
        return text

    # Use first 60 chars (after leading whitespace) as probe — shorter is safer
    # because Inecobank's SPA sometimes starts the second render mid-sentence
    probe = text.lstrip()[:60]
    if len(probe) < 30:
        return text

    # Search for the probe in the latter 75% of the text
    search_start = n // 4
    idx = text.find(probe, search_start)
    if idx > 0:
        return text[:idx].rstrip()

    # Fallback: line-based search (covers newline-separated repeats)
    lines = text.split("\n")
    ln = len(lines)
    if ln >= 6:
        for half in range(ln // 3, (2 * ln) // 3):
            anchor = "\n".join(lines[:half]).strip()
            if len(anchor) < 50:
                continue
            rest = "\n".join(lines[half:]).strip()
            if rest.startswith(anchor[:min(120, len(anchor))]):
                return anchor

    return text


def clean_text(raw: str) -> str:
    # Reject 404 / error pages immediately
    if is_404_page(raw):
        return ""

    text = unicode_normalize(raw)

    # Fix Playwright double-render before sentence-level dedup
    text = _deduplicate_repeated_block(text)

    text = remove_ui_noise(text)
    text = remove_calculator_ui(text)
    text = remove_timestamps(text)
    text = remove_marketing(text)
    text = normalize_whitespace(text)

    text = deduplicate_sentences(text)
    text = deduplicate_paragraphs(text)

    return normalize_whitespace(text)


def process_records(records: list[dict]) -> list[dict]:
    """
    Apply clean_text() to every record in the dataset.
    Records whose cleaned text falls below MIN_TEXT_LENGTH are dropped.

    Args:
        records: List of dicts with keys: bank, section, url, text

    Returns:
        List of dicts with the same structure, cleaned text, dropped if empty.
    """
    cleaned_records = []
    stats = {"total": len(records), "kept": 0, "dropped": 0}

    for record in records:
        raw_text = record.get("text", "")
        cleaned = clean_text(raw_text)

        if len(cleaned) < MIN_TEXT_LENGTH:
            stats["dropped"] += 1
            continue  # skip near-empty records

        cleaned_records.append({
            "bank":    record.get("bank", ""),
            "section": record.get("section", ""),
            "url":     record.get("url", ""),
            "text":    cleaned,
        })
        stats["kept"] += 1

    print(
        f"[pipeline] total={stats['total']}  "
        f"kept={stats['kept']}  "
        f"dropped={stats['dropped']}"
    )
    return cleaned_records