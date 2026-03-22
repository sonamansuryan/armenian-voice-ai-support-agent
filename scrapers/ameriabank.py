import logging
from bs4 import BeautifulSoup
from .playwright_base import PlaywrightBankScraper, BankRecord
from .base import is_armenian_text

logger = logging.getLogger(__name__)

# Ameriabank UI noise to exclude
_AMERIABANK_NOISE = {
    "ՀԱՅ", "ENG", "РУС",
    "Մուտք", "Գրանցվել",
    "Դառնալ հաճախորդ",
    "Ֆինանսական օգնական",
    "Ֆինանսական կրթություն",
    "Ներդնել ավանդ",
    "Դիմիր օնլայն",
}


class AmeriabankScraper(PlaywrightBankScraper):
    BANK_NAME = "Ameriabank"
    BASE_URL = "https://ameriabank.am"

    # ------------------------------------------------------------------ credits
    CREDIT_URLS = [
        ("/personal/loans/consumer-loans/consumer-loans",    "Վարկ"),
        ("/personal/loans/consumer-loans/consumer-finance",  "Ապառիկ"),
        ("/personal/loans/consumer-loans/credit-line",       "Վարկային գիծ"),
        ("/personal/loans/mortgage/primary-market",          "Հիփոթեք (կառուցվող)"),
        ("/personal/loans/mortgage/primary-market-loan",     "Հիփոթեք (կառուցվող 2)"),
        ("/personal/loans/mortgage/secondary-market",        "Հիփոթեք (երկրորդային)"),
        ("/personal/loans/mortgage/renovation-mortgage",     "Հիփոթեք (վերանորոգում)"),
        ("/personal/loans/mortgage/construction-mortgage",   "Հիփոթեք (կառուցապատում)"),
        ("/campaigns/mortgage-loan-for-diaspora",            "Հիփոթեք (սփյուռք)"),
        ("/personal/loans/car-loans",                        "Ավտոմեքենայի վարկ"),
        ("/personal/loans/other-loans/investment-loan",      "Ներդրումային վարկ"),
        ("/personal/loans/other-loans/overdraft",            "Օվերդրաֆտ"),
    ]

    def scrape_credits(self) -> list[BankRecord]:
        return self._scrape_urls_rendered(self.CREDIT_URLS, "credits")

    # ----------------------------------------------------------------- deposits
    DEPOSIT_URLS = [
        ("/personal/saving/deposits/ameria-deposit",       "Ameria ավանդ"),
        ("/personal/saving/deposits/cumulative-deposit",   "Կուտակային ավանդ"),
        ("/personal/saving/deposits/kids-deposit",         "Մանկական ավանդ"),
        ("/personal/accounts/accounts/saving-account",     "Խնայողական հաշիվ"),
        ("/personal/saving/deposits",                      "Ավանդներ (ընդհ.)"),
    ]

    def scrape_deposits(self) -> list[BankRecord]:
        return self._scrape_urls_rendered(self.DEPOSIT_URLS, "deposits")

    # ----------------------------------------------------------------- branches
    BRANCH_URL   = "/service-network"
    RATE_AM_URL  = "https://www.rate.am/hy/bank/ameriabank"

    def scrape_branches(self) -> list[BankRecord]:
        # Strategy 1 (PRIMARY): rate.am — ունի հասցե + հեռախոս + աշխ. ժամ
        records = self._scrape_branches_rate_am()
        if records:
            logger.info("[%s] %d branch records via rate.am", self.BANK_NAME, len(records))
            return records
        # Fallback: original JS extraction
        url = self.BASE_URL + self.BRANCH_URL
        return self._scrape_branches_ameriabank(url)

    def _scrape_branches_rate_am(self) -> list[BankRecord]:
        """
        Scrape from https://www.rate.am/hy/bank/ameriabank
        Page is Next.js SSR — all branch data is in raw HTML.
        Anchors on <a href="tel:..."> links, walks up DOM to find
        the branch card container with Armenian address + hours.
        """
        resp = self._get(self.RATE_AM_URL)
        if resp is None:
            logger.warning("[%s] rate.am fetch failed", self.BANK_NAME)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup.select("script,style,nav,header,footer"):
            tag.decompose()

        records: list[BankRecord] = []
        tel_links = soup.select('a[href^="tel:"]')
        seen: set[str] = set()

        for link in tel_links:
            node = link.parent
            for _ in range(6):
                if node is None:
                    break
                text = self._clean(node.get_text(separator=" "))
                if (is_armenian_text(text)
                        and len(text) >= 40
                        and len(text) <= 2000
                        and text not in seen):
                    seen.add(text)
                    records.append(
                        self._make_record("branches", self.RATE_AM_URL, text))
                    break
                node = node.parent

        if records:
            texts = [r.text for r in records]
            return [r for r in records
                    if not any(r.text != o and r.text in o for o in texts)]
        return records

    def _scrape_branches_ameriabank(self, url: str) -> list[BankRecord]:
        """
        Ameriabank's /service-network renders a map + list via React.
        We use Playwright to wait for content, then extract branch cards
        via JS evaluation looking for address-bearing elements.
        """
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            return []

        import time
        time.sleep(self.delay)
        records = []
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(
                    locale="hy-AM", java_script_enabled=True,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                )
                page = context.new_page()
                page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())

                try:
                    page.goto(url, wait_until="networkidle", timeout=self.ARMENIAN_WAIT_TIMEOUT)
                except PWTimeout:
                    pass

                # Wait for Armenian text
                try:
                    page.wait_for_function(
                        "() => (document.body.innerText.match(/[\u0530-\u058F]/g)||[]).length >= 20",
                        timeout=self.ARMENIAN_WAIT_TIMEOUT,
                    )
                except PWTimeout:
                    pass

                page.wait_for_timeout(2000)

                branch_texts = page.evaluate("""
                    () => {
                        const armRe = /[\u0530-\u058F]/;
                        const addrRe = /\u0567\u0580\u0587\u0561\u0576|\u0584\\.|\u0541|\u0531\u0562|\u057c|\u0533\u0575\u0578\u0582\u0574|\u054e\u056b\u0580\u0561\u056f|\u053e\u056b\u056f/;
                        const phoneRe = /\\+374|010|011|060|077/;

                        const seen = new Set();
                        const results = [];

                        // Try known Ameriabank branch list item selectors
                        const listSels = [
                            '[class*="branch"]', '[class*="Branch"]',
                            '[class*="office"]', '[class*="Office"]',
                            '[class*="network"]', '[class*="service"]',
                            '[class*="location"]', '[class*="Location"]',
                            '[class*="item"]', '[class*="card"]',
                            'li', '.row > div', '.col'
                        ];

                        let cards = [];
                        for (const sel of listSels) {
                            const found = Array.from(document.querySelectorAll(sel)).filter(el => {
                                const t = (el.innerText || '').trim();
                                return t.length >= 30 && t.length <= 1000
                                    && armRe.test(t)
                                    && (addrRe.test(t) || phoneRe.test(t));
                            });
                            if (found.length >= 3) {
                                cards = found;
                                break;
                            }
                        }

                        // Fallback: scan all divs
                        if (cards.length < 3) {
                            cards = Array.from(document.querySelectorAll('div, li, article')).filter(el => {
                                const t = (el.innerText || '').trim();
                                return t.length >= 40 && t.length <= 800
                                    && armRe.test(t)
                                    && (addrRe.test(t) || phoneRe.test(t));
                            });
                        }

                        // Keep leaf-most elements (not parent containers)
                        for (const el of cards) {
                            const t = (el.innerText || '').trim().replace(/\\s+/g, ' ');
                            if (!seen.has(t)) {
                                seen.add(t);
                                results.push(t);
                            }
                        }

                        // Remove supersets
                        return results.filter(t => !results.some(o => o !== t && t.length < o.length && o.includes(t.slice(0, 40))));
                    }
                """)

                browser.close()

            seen_texts: set = set()
            for text in (branch_texts or []):
                clean = self._clean(text)
                if clean and clean not in seen_texts and is_armenian_text(clean) and len(clean) >= 30:
                    seen_texts.add(clean)
                    records.append(self._make_record("branches", url, clean))

        except Exception as exc:
            logger.warning("[%s] Branch JS extraction failed: %s", self.BANK_NAME, exc)

        return records

    # ----------------------------------------------------------------- override
    def _scrape_urls_rendered(self, url_list: list[tuple], section: str) -> list[BankRecord]:
        """
        Override base implementation to apply Ameriabank-specific noise filtering.
        """
        records = []
        seen_texts: set[str] = set()

        for path, label in url_list:
            url = self.BASE_URL + path
            soup = self._get_rendered(url)
            if soup is None:
                continue

            text = self._extract_clean_armenian(soup, url)
            if text and text not in seen_texts:
                seen_texts.add(text)
                records.append(self._make_record(section, url, text))
            else:
                logger.info("[%s] No Armenian content at %s — skipped.", self.BANK_NAME, url)
        return records

    def _extract_clean_armenian(self, soup: BeautifulSoup, url: str) -> str | None:
        """
        Extract Armenian text with Ameriabank-specific noise filtering.
        Uses LEAF-ONLY extraction (p/li/td/h1-h6) to avoid parent-div
        duplicating its children's text.
        """
        # --- Step 1: remove ALL chrome/navigation noise ---
        for tag in soup.select(
            "script, style, nav, header, footer, "
            "[class*='menu'], [class*='footer'], [class*='cookie'], "
            "[class*='breadcrumb'], [class*='social'], [class*='chat'], "
            "[class*='modal'], [class*='banner'], [class*='popup'], "
            "[class*='language'], [class*='lang-switch'], "
            "[class*='header'], [class*='toolbar'], [class*='topbar'], "
            "[class*='sidebar'], [class*='nav']"
        ):
            tag.decompose()

        # --- Step 2: LEAF-ONLY content elements (no div/section/article) ---
        LEAF_TAGS = {"p", "li", "td", "th", "dt", "dd",
                     "h1", "h2", "h3", "h4", "h5", "h6", "address"}

        parts = []
        seen: set[str] = set()

        for el in soup.find_all(LEAF_TAGS):
            raw = self._clean(el.get_text(separator=" "))
            if not raw or raw in seen:
                continue
            if not is_armenian_text(raw):
                continue
            if raw in _AMERIABANK_NOISE:
                continue
            if len(raw) < 15 and any(noise in raw for noise in _AMERIABANK_NOISE):
                continue
            seen.add(raw)
            parts.append(raw)

        # --- Step 3: fallback to divs if nothing found ---
        if not parts:
            div_texts = []
            for el in soup.find_all(["div", "section", "article"]):
                raw = self._clean(el.get_text(separator=" "))
                if not raw or raw in seen or not is_armenian_text(raw):
                    continue
                if raw in _AMERIABANK_NOISE:
                    continue
                seen.add(raw)
                div_texts.append(raw)
            # Keep only granular items (remove supersets)
            parts = [t for t in div_texts
                     if not any(t != o and t in o for o in div_texts)]

        return "\n".join(parts) if parts else None