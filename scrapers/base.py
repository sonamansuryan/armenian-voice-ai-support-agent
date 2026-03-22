"""
Base scraper abstract class.
All bank scrapers must implement this interface.

Language policy (Requirement 9):
  - ALL extracted text MUST be in Armenian (Հայերեն).
  - Pages with no Armenian content are silently skipped.
  - If a page mixes languages, only Armenian-script nodes are kept.
  - Content is NEVER translated, summarised, or rewritten.
  - All HTTP responses are decoded as UTF-8.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Armenian Unicode block: U+0530 – U+058F  (Armenian script)
# A small helper compiled once at import time.
# ---------------------------------------------------------------------------
_ARMENIAN_RE = re.compile(r"[\u0530-\u058F\uFB13-\uFB17]")   # Armenian + MSCS ligatures
_MIN_ARMENIAN_CHARS = 10   # minimum Armenian chars to consider a text "Armenian"


def is_armenian_text(text: str) -> bool:
    """Return True if *text* contains at least _MIN_ARMENIAN_CHARS Armenian characters."""
    return len(_ARMENIAN_RE.findall(text)) >= _MIN_ARMENIAN_CHARS


@dataclass
class BankRecord:
    """Canonical data record returned by every scraper."""
    bank: str
    section: str          # "credits" | "deposits" | "branches"
    url: str
    text: str             # Original Armenian text, never translated
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "bank": self.bank,
            "section": self.section,
            "url": self.url,
            "text": self.text,
        }


class BaseBankScraper(ABC):
    """
    Abstract base for all bank scrapers.

    Language contract
    -----------------
    Every subclass MUST target the Armenian-language (``/hy/``) version of each
    page.  The shared helpers ``_get_armenian()``, ``_armenian_nodes()``, and
    ``_assert_armenian()`` enforce this at runtime so no English text can
    accidentally slip through.

    Subclasses must implement:
        - scrape_credits()
        - scrape_deposits()
        - scrape_branches()
    """

    BANK_NAME: str = "Unknown Bank"

    def __init__(self, session=None, timeout: int = 30, delay: float = 1.0):
        self.timeout = timeout
        self.delay = delay

        if session:
            self.session = session
            return

        # Use cloudscraper for ALL banks — it handles:
        #   - Cloudflare JS challenges (Inecobank 403)
        #   - Brotli decompression (Ardshinbank / Ameriabank garbled text)
        #   - Realistic browser TLS fingerprinting
        # Falls back to plain requests if not installed.
        try:
            import cloudscraper
            self.session = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
        except ImportError:
            import requests
            self.session = requests.Session()
            logger.warning(
                "cloudscraper not installed — some sites may fail. "
                "Run: pip install cloudscraper"
            )

        self.session.headers.update({
            "Accept-Language": "hy-AM,hy;q=0.9,en-US;q=0.5,en;q=0.3",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def scrape_credits(self) -> list[BankRecord]:
        """Return credit / loan product records in Armenian."""

    @abstractmethod
    def scrape_deposits(self) -> list[BankRecord]:
        """Return deposit product records in Armenian."""

    @abstractmethod
    def scrape_branches(self) -> list[BankRecord]:
        """Return branch location records in Armenian."""

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def scrape_all(self) -> list[BankRecord]:
        """Run all three scrapers and aggregate results."""
        results: list[BankRecord] = []
        for method_name, section in [
            ("scrape_credits", "credits"),
            ("scrape_deposits", "deposits"),
            ("scrape_branches", "branches"),
        ]:
            try:
                logger.info("[%s] Scraping %s …", self.BANK_NAME, section)
                records = getattr(self, method_name)()
                logger.info("[%s] %s → %d records", self.BANK_NAME, section, len(records))
                results.extend(records)
            except Exception as exc:
                logger.error("[%s] Failed to scrape %s: %s", self.BANK_NAME, section, exc)
        return results

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, **kwargs) -> Optional["requests.Response"]:
        """
        HTTP GET with polite delay, compression handling, and robust UTF-8 decoding.

        Encoding strategy:
          1. Try UTF-8 (correct for all Armenian bank sites).
          2. If that fails (UnicodeDecodeError), detect encoding from bytes via chardet.
          3. Attach the decoded text as resp._decoded_text for use by callers.

        Always use  resp.text  normally — this method patches resp.encoding = 'utf-8'
        so that resp.text returns the correctly decoded string.
        requests auto-decompresses gzip/deflate/br (stream=False is the default).
        """
        import time
        import requests
        time.sleep(self.delay)
        try:
            resp = self.session.get(url, timeout=self.timeout, **kwargs)
            resp.raise_for_status()

            # Skip non-HTML content (PDFs, images, JSON APIs …)
            ct = resp.headers.get("Content-Type", "")
            if ct and "html" not in ct and "text" not in ct:
                logger.info(
                    "[%s] Skipping non-HTML response at %s (%s)",
                    self.BANK_NAME, url, ct
                )
                return None

            # Detect and enforce correct encoding.
            # resp.content is already decompressed by requests.
            raw: bytes = resp.content
            try:
                raw.decode("utf-8")
                resp.encoding = "utf-8"
            except (UnicodeDecodeError, AttributeError):
                # Fall back: let chardet sniff the encoding
                try:
                    import chardet
                    detected = chardet.detect(raw).get("encoding") or "utf-8"
                except ImportError:
                    detected = "utf-8"
                resp.encoding = detected
                logger.debug(
                    "[%s] Non-UTF-8 response at %s, detected encoding: %s",
                    self.BANK_NAME, url, detected
                )

            return resp
        except requests.RequestException as exc:
            logger.warning("[%s] GET %s failed: %s", self.BANK_NAME, url, exc)
            return None

    # ------------------------------------------------------------------
    # Language-enforcement helpers  (Requirement 9)
    # ------------------------------------------------------------------

    @staticmethod
    def is_armenian_text(text: str) -> bool:
        """True if *text* contains enough Armenian script characters."""
        return is_armenian_text(text)

    def _armenian_nodes(self, soup: "BeautifulSoup") -> list["Tag"]:
        """
        Walk *soup* and return only leaf/block elements whose text content
        is predominantly Armenian.  Elements with no Armenian characters at
        all are dropped entirely — this prevents English-only blocks from
        entering the output even on mixed-language pages.
        """
        from bs4 import NavigableString, Tag
        results = []
        for el in soup.find_all(True):
            # Only consider block-ish elements (skip inline spans that are
            # children of blocks we will already capture)
            if el.name not in {
                "p", "li", "td", "th", "dt", "dd", "h1", "h2", "h3",
                "h4", "h5", "h6", "div", "section", "article", "address",
            }:
                continue
            text = el.get_text(separator=" ")
            if is_armenian_text(text):
                results.append(el)
        return results

    def _assert_armenian(self, text: str, url: str) -> bool:
        """
        Log a warning and return False if *text* contains no Armenian content.
        Subclasses use this to skip pages that are not in Armenian.
        """
        if not is_armenian_text(text):
            logger.info(
                "[%s] Skipping %s — no Armenian content detected.", self.BANK_NAME, url
            )
            return False
        return True

    def _extract_armenian_text(self, soup: "BeautifulSoup", url: str) -> Optional[str]:
        """
        High-level helper: strip noise tags, collect only Armenian-bearing
        nodes, join their raw text, and return it — or None if the page has
        no Armenian content.

        The text is returned VERBATIM (only whitespace-normalised) so the
        original Armenian wording is preserved exactly.
        """
        from bs4 import BeautifulSoup

        # Remove script / style / nav / header / footer noise first
        for tag in soup.select("script, style, nav, header, footer, [class*='menu']"):
            tag.decompose()

        # Gather Armenian-bearing nodes
        nodes = self._armenian_nodes(soup)
        if not nodes:
            logger.info("[%s] Skipping %s — no Armenian nodes found.", self.BANK_NAME, url)
            return None

        # Join raw text from each node, preserving original wording
        parts = []
        seen: set[str] = set()
        for node in nodes:
            raw = self._clean(node.get_text(separator=" "))
            if raw and raw not in seen and is_armenian_text(raw):
                seen.add(raw)
                parts.append(raw)

        return "\n".join(parts) if parts else None

    # ------------------------------------------------------------------
    # General helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(text: str) -> str:
        """Collapse whitespace; preserve all Unicode (Armenian) characters."""
        return re.sub(r"\s+", " ", text or "").strip()

    def _make_record(self, section: str, url: str, text: str, **meta) -> BankRecord:
        return BankRecord(
            bank=self.BANK_NAME,
            section=section,
            url=url,
            text=self._clean(text),
            metadata=meta,
        )