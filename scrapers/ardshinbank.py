import logging
import time as _time
from bs4 import BeautifulSoup
from .playwright_base import PlaywrightBankScraper, BankRecord
from .base import is_armenian_text

logger = logging.getLogger(__name__)

_NOISE = {
    "Նախկինում գործող պայմաններ (արխիվ)",
    "Վարկային արձակուրդ",
    "arrow_drop_down",
    "chevron_left",
    "chevron_right",
    "expand_more",
    "Ֆինանսական օգնական",
    "Դառնալ հաճախորդ",
    "Հանդիսանում եք Բանկի հաճախորդ",
    "Ստանալ վարկ",
}

# Patterns to intercept — broad first, then narrow
_BRANCH_API_PATTERNS = [
    # Ardshinbank-specific patterns (discovered from network log)
    "/api/filial", "/api/branch", "/api/atm",
    "filial", "filials", "atm-list", "atm_list",
    "branch-list", "branchlist", "branchList",
    # Generic
    "branch", "location", "office", "network",
    "map/data", "mapdata", "points",
]

_LIST_WORD = "\u0551\u0578\u0582\u0581\u0561\u056f"


class ArdshinbankScraper(PlaywrightBankScraper):
    BANK_NAME = "Ardshinbank"
    BASE_URL = "https://ardshinbank.am"

    CREDIT_URLS = [
        ("/for-you/loans-ardshinbank?lang=hy",                           "Consumer loan"),
        ("/for-you/loans-ardshinbank?lang=hy&sft=cash-secured-loans",    "Cash secured loan"),
        ("/for-you/loans-ardshinbank?lang=hy&sft=secured-by-real-estate","Real estate loan"),
        ("/for-you/loans-ardshinbank?lang=hy&sft=special-state-programs","State programs"),
        ("/for-you/loans-ardshinbank?lang=hy&sft=secured-by-gold",       "Gold secured loan"),
        ("/for-you/mortgage?lang=hy",                                    "Mortgage"),
        ("/for-you/vosku-gravov-sparoxakan-vark?lang=hy",                "Gold consumer loan"),
    ]

    def scrape_credits(self) -> list[BankRecord]:
        return self._scrape_with_extended_wait(self.CREDIT_URLS, "credits")

    DEPOSIT_URLS = [
        ("/for-you/avand?lang=hy",                                                                                          "Ժամկետային ավանդ (ընդհ.)"),
        ("/for-you/avand?sft=%D4%BA%D5%A1%D5%B4%D5%AF%D5%A5%D5%BF%D5%AB+%D5%BE%D5%A5%D6%80%D5%BB%D5%B8%D6%82%D5%B4",   "Ժամկետի վերջում վճարմամբ"),
        ("/for-you/avand?sft=%D4%B5%D5%BC%D5%A1%D5%B4%D5%BD%D5%B5%D5%A1+%D5%BE%D5%B3%D5%A1%D6%80%D5%B4%D5%A1%D5%B4%D5%A2", "Եռամսյա վճարմամբ"),
        ("/for-you/avand?sft=%D4%B1%D5%B4%D5%A5%D5%B6%D5%A1%D5%B4%D5%BD%D5%B5%D5%A1+%D5%BE%D5%B3%D5%A1%D6%80%D5%B4%D5%A1%D5%B4%D5%A2", "Ամենամսյա վճարմամբ"),
        ("/for-you/avand?sft=%D4%BA%D5%A1%D5%B4%D5%AF%D5%A5%D5%BF%D5%AB+%D5%BE%D5%A5%D6%80%D5%BB%D5%B8%D6%82%D5%B4%D5%9D+%D5%A1%D5%BE%D5%A5%D5%AC%D5%A1%D6%81%D5%B4%D5%A1%D5%B6+%D5%AB%D6%80%D5%A1%D5%BE%D5%B8%D6%82%D5%B6%D6%84%D5%B8%D5%BE", "Ավելացման իրավունքով"),
        ("/for-you/savings-account?lang=hy",                                                                                "Խնայողական հաշիվ"),
    ]

    # PDF-ից ստացված static տվյալ (04.02.2026) — scraper-ի fallback
    _PDF_DEPOSIT_DATA = """Արդշինբանկ Ժամկետային Ավանդներ «Ստանդարտ» - Տեղեկատվական ամփոփագիր (04.02.2026)

Բանկն ավանդ է ընդունում ՀՀ դրամով, ԱՄՆ դոլարով, եվրոյով և ՌԴ ռուբլիով:

Տարեկան անվանական տոկոսադրույքներ՝ ժամկետի վերջում վճարմամբ.
31-90 օր: AMD 5.75%, USD 0.25%, EUR -, RUB 3.0%
91-180 օր: AMD 6.75%, USD 1.25%, EUR 0.50%, RUB 4.0%
181-270 օր: AMD 7.50%, USD 2.50%, EUR 1.25%, RUB 5.0%
271-366 օր: AMD 7.75%, USD 3.00%, EUR 1.50%, RUB 5.0%
367-549 օր: AMD 8.25%, USD 3.50%, EUR 2.00%, RUB 6.0%
550-730 օր: AMD 9.00%, USD 3.75%, EUR 2.25%, RUB 5.75%

Տարեկան անվանական տոկոսադրույքներ՝ եռամսյա վճարմամբ.
91-180 օր: AMD 6.6%, USD 1.1%, EUR 0.4%, RUB 3.8%
181-270 օր: AMD 7.3%, USD 2.4%, EUR 1.2%, RUB 4.8%
271-366 օր: AMD 7.5%, USD 2.9%, EUR 1.4%, RUB 4.8%
367-549 օր: AMD 8.0%, USD 3.4%, EUR 1.9%, RUB 5.8%
550-730 օր: AMD 8.8%, USD 3.6%, EUR 2.1%, RUB 5.5%

Տարեկան անվանական տոկոսադրույքներ՝ ամենամսյա վճարմամբ.
31-90 օր: AMD 5.6%, USD 0.1%, EUR -, RUB 2.9%
91-180 օր: AMD 6.5%, USD 1.0%, EUR 0.3%, RUB 3.7%
181-270 օր: AMD 7.2%, USD 2.3%, EUR 1.1%, RUB 4.7%
271-366 օր: AMD 7.4%, USD 2.8%, EUR 1.3%, RUB 4.7%
367-549 օր: AMD 7.9%, USD 3.3%, EUR 1.8%, RUB 5.6%
550-730 օր: AMD 8.6%, USD 3.5%, EUR 2.0%, RUB 5.4%

Տարեկան անվանական տոկոսադրույքներ՝ ավելացման իրավունքով, տոկոսները ժամկետի վերջում.
31-90 օր: AMD 5.6%, USD 0.1%, EUR -, RUB 2.6%
91-180 օր: AMD 6.5%, USD 0.8%, EUR 0.2%, RUB 3.5%
181-270 օր: AMD 7.2%, USD 2.0%, EUR 1.0%, RUB 4.4%
271-366 օր: AMD 7.4%, USD 2.5%, EUR 1.2%, RUB 4.4%
367-549 օր: AMD 7.9%, RUB -
550-730 օր: AMD 8.6%, RUB -

Ավանդ ձևակերպելու համար անհրաժեշտ փաստաթղթեր.
- Անձը հաստատող փաստաթուղթ
- Հանրային ծառայության համարանիշ (ՀԾՀ) կամ ՀԾՀ չունենալու մասին տեղեկանք

Երաշխավորված ավանդի առավելագույն սահմանաչափեր.
- Միայն դրամային ավանդ՝ 16,000,000 ՀՀ դրամ
- Միայն արտարժութային ավանդ՝ 7,000,000 ՀՀ դրամ

Ժամկետից շուտ վերադարձնելու դեպքում՝ վերահաշվարկ կատարվում է բանկային հաշվի օրական մնացորդի վրա կիրառվող տարեկան տոկոսադրույքով:
Հաշվեգրված տոկոսներից պահվում են օրենսդրությամբ նախատեսված հարկերը:"""

    def scrape_deposits(self) -> list[BankRecord]:
        records = self._scrape_with_extended_wait(self.DEPOSIT_URLS, "deposits")
        # Միշտ ավելացնել PDF-ի static տվյալը որպես հիմք
        pdf_record = self._make_record(
            "deposits",
            "https://ardshinbank.am/for-you/avand?lang=hy",
            self._PDF_DEPOSIT_DATA
        )
        # Ավելացնել PDF record-ը սկզբում
        return [pdf_record] + records

    BRANCH_URL   = "/Information/branch-atm?lang=hy"
    RATE_AM_URL  = "https://www.rate.am/hy/bank/ardshinbank"

    def scrape_branches(self) -> list[BankRecord]:
        # Strategy 1 (PRIMARY): rate.am — ունի հասցե + հեռախոս + աշխ. ժամ
        records = self._scrape_branches_rate_am()
        if records:
            logger.info("[%s] %d branch records via rate.am", self.BANK_NAME, len(records))
            return records

        url = self.BASE_URL + self.BRANCH_URL

        # Strategy 2 (fallback): broad API interception
        records = self._scrape_branches_via_api(url)
        if records:
            logger.info("[%s] Got %d branch records via API intercept", self.BANK_NAME, len(records))
            return records

        # Strategy 3: JS list-view
        records = self._scrape_branches_js_list(url)
        if records:
            logger.info("[%s] Got %d branch records via JS list-view", self.BANK_NAME, len(records))
            return records

        # Strategy 4: full text from rendered page
        soup = self._get_rendered(url)
        if soup:
            text = self._extract_armenian_text_full(soup, url)
            if text:
                return [self._make_record("branches", url, text)]

        logger.warning("[%s] All branch strategies failed", self.BANK_NAME)
        return []

    # ------------------------------------------------------------------
    def _scrape_branches_rate_am(self) -> list[BankRecord]:
        """
        Scrape from https://www.rate.am/hy/bank/ardshinbank
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

        # Strategy A: anchor on tel: links
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

        # Strategy B: fallback — phone string search
        phone_patterns = ["+374 10", "Հեռ."]
        seen2: set[str] = set()
        candidates: list[str] = []
        for el in soup.find_all(["div", "li", "section", "article"]):
            raw = self._clean(el.get_text(separator=" "))
            if not any(p in raw for p in phone_patterns):
                continue
            if len(raw) < 30 or len(raw) > 2000:
                continue
            if not is_armenian_text(raw) or raw in seen2:
                continue
            seen2.add(raw)
            candidates.append(raw)

        if candidates:
            leaf = [t for t in candidates
                    if not any(t != o and o in t for o in candidates)]
            for text in (leaf if len(leaf) >= 2 else candidates):
                records.append(self._make_record("branches", self.RATE_AM_URL, text))

        return records

    # ------------------------------------------------------------------
    def _scrape_branches_via_api(self, url: str) -> list[BankRecord]:
        """
        Intercept all XHR/fetch responses.
        Ardshinbank's map loads from a separate API subdomain or path.
        We capture every JSON response and check if it contains branch data.
        """
        soup, intercepted = self._get_rendered_with_api_intercept(
            url,
            api_url_patterns=_BRANCH_API_PATTERNS,
            wait_ms=8000,
        )

        # Also log ALL intercepted URLs for debugging
        all_soup, all_intercepted = self._get_rendered_with_api_intercept(
            url,
            api_url_patterns=[""],   # empty string matches everything
            wait_ms=3000,
        )
        for item in all_intercepted:
            if item.get("data") and "html" not in item["url"]:
                logger.info("[%s] Network call: %s", self.BANK_NAME, item["url"])

        records = []
        for item in intercepted:
            if self._is_branch_json(item["data"]):
                text = self._parse_branch_json(item["data"])
                if text:
                    records.append(self._make_record("branches", item["url"], text))
        return records

    def _is_branch_json(self, data) -> bool:
        """Check if JSON data looks like a branch list (has addresses or coordinates)."""
        import json
        text = json.dumps(data) if not isinstance(data, str) else data
        # Branch data has Armenian chars OR lat/lng coordinates OR address fields
        import re
        has_armenian = bool(re.search(r'[\u0530-\u058F]', text))
        has_coords = bool(re.search(r'"lat":|"lng":|"latitude":|"longitude":', text))
        has_addr = bool(re.search(r'"address"|"addr"|"location"', text, re.I))
        # Must NOT be just page metadata
        is_page_meta = '"title"' in text and '"content"' in text and len(text) > 50000
        return (has_armenian or has_coords) and has_addr and not is_page_meta

    def _parse_branch_json(self, data) -> str | None:
        lines = []
        def extract(obj):
            if isinstance(obj, dict):
                for field in [
                    "address", "name", "title", "nameHy", "addressHy",
                    "address_hy", "name_hy", "branchName", "branchAddress",
                    "workingHours", "workHours", "phone", "phoneNumber",
                    "region", "city",
                ]:
                    val = obj.get(field, "")
                    if val and isinstance(val, str) and len(val.strip()) > 2:
                        lines.append(val.strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        extract(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item)
        extract(data)
        if not lines:
            return None
        arm = [ln for ln in lines if is_armenian_text(ln)]
        phone = [ln for ln in lines if "+374" in ln and ln not in arm]
        useful = arm + phone
        return "\n".join(useful) if useful else "\n".join(lines[:100])

    def _scrape_branches_js_list(self, url: str) -> list[BankRecord]:
        """
        Use Playwright JS evaluation to find and click list-view,
        then extract ONLY top-level branch cards (not their children).
        """
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            return []

        _time.sleep(self.delay)
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
                try:
                    page.wait_for_function(
                        "() => (document.body.innerText.match(/[\u0530-\u058F]/g)||[]).length >= 10",
                        timeout=8000
                    )
                except PWTimeout:
                    pass

                # Click list-view button
                list_word = _LIST_WORD
                try:
                    page.evaluate(f"""
                        () => {{
                            const word = '{list_word}';
                            const els = Array.from(document.querySelectorAll('button,a,span,li,div'));
                            const btn = els.find(e => {{
                                const t = e.textContent.trim();
                                return t === word || t.startsWith(word);
                            }});
                            if (btn) btn.click();
                        }}
                    """)
                    page.wait_for_timeout(3000)
                except Exception:
                    pass

                # Extract top-level branch cards via JS
                branch_texts = page.evaluate("""
                    () => {
                        const armRe = /[\u0530-\u058F]/;
                        const phoneRe = /\\+374|010|011|012|060|077|091|094|095|096/;
                        const addrRe = /\u0584\\.|\u0535\u0580\u0587\u0561\u0576|\u0548\u0582\u0580\u0562|\u0533\u0575\u0578\u0582\u0574|\\d{4}/;

                        const results = [];
                        const seen = new Set();

                        // Try known card selectors
                        const sels = [
                            '.branch-item', '.branch-card', '.office-item',
                            '[class*="branch-item"]', '[class*="branchItem"]',
                            '[class*="filial"]', '[class*="location-card"]',
                            'li.branch', 'li.office'
                        ];
                        let cards = [];
                        for (const s of sels) {
                            cards = Array.from(document.querySelectorAll(s));
                            if (cards.length > 2) break;
                        }

                        // Fallback: find divs with Armenian + phone/address
                        if (cards.length === 0) {
                            const all = Array.from(document.querySelectorAll('div,li,article'));
                            for (const el of all) {
                                const t = el.innerText || '';
                                if (t.length < 40 || t.length > 3000) continue;
                                if (!armRe.test(t)) continue;
                                if (!phoneRe.test(t) && !addrRe.test(t)) continue;
                                const pText = (el.parentElement && el.parentElement.innerText) || '';
                                if (pText.length > t.length * 1.5) cards.push(el);
                            }
                        }

                        for (const c of cards) {
                            const t = (c.innerText || '').trim().replace(/\\s+/g, ' ');
                            if (t.length >= 30 && !seen.has(t)) {
                                seen.add(t);
                                results.push(t);
                            }
                        }
                        return results;
                    }
                """)

                browser.close()

            for text in (branch_texts or []):
                if is_armenian_text(text):
                    records.append(self._make_record("branches", url, self._clean(text)))

        except Exception as exc:
            logger.warning("[%s] JS list-view failed: %s", self.BANK_NAME, exc)
        return records

    # ------------------------------------------------------------------
    def _scrape_with_extended_wait(self, url_list, section):
        """
        Ardshinbank's SPA renders content via React after initial load.
        This method uses Playwright with extended wait times and tries to
        click filter tabs (sft= params) to expose per-product content.
        Falls back to _scrape_with_tables for non-SPA pages.
        """
        records = []
        seen_texts: set = set()

        for path, label in url_list:
            url = self.BASE_URL + path
            soup = self._get_rendered_ardshinbank(url)
            if soup is None:
                continue

            recs = self._parse_tables(soup, section, url)
            if recs:
                for r in recs:
                    if r.text not in seen_texts:
                        seen_texts.add(r.text)
                        records.append(r)
            else:
                text = self._extract_armenian_text_full(soup, url)
                if text and text not in seen_texts and len(text) >= 50:
                    seen_texts.add(text)
                    records.append(self._make_record(section, url, text))
                else:
                    logger.info("[%s] No content at %s", self.BANK_NAME, url)

        return records

    def _get_rendered_ardshinbank(self, url: str):
        """
        Extended Playwright render for Ardshinbank:
        - Waits up to 25s for Armenian content (heavier SPA than Inecobank)
        - Scrolls to trigger lazy-loaded sections
        - Tries clicking any visible tab/filter button if content is thin
        """
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            return self._get_rendered(url)

        _time.sleep(self.delay)
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(
                    locale="hy-AM", java_script_enabled=True,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                )
                page = context.new_page()
                page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3,ico}", lambda r: r.abort())

                try:
                    page.goto(url, wait_until="networkidle", timeout=25_000)
                except PWTimeout:
                    pass

                # Wait for meaningful Armenian content (≥50 chars)
                try:
                    page.wait_for_function(
                        "() => (document.body.innerText.match(/[\u0530-\u058F]/g)||[]).length >= 50",
                        timeout=20_000
                    )
                except PWTimeout:
                    logger.warning("[%s] Armenian wait timeout at %s", self.BANK_NAME, url)

                page.wait_for_timeout(2000)

                # Scroll down to trigger lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                page.wait_for_timeout(1500)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)

                # If content is still thin, try clicking the first visible Armenian tab
                arm_count = page.evaluate(
                    "(document.body.innerText.match(/[\u0530-\u058F]/g)||[]).length"
                )
                if arm_count < 100:
                    page.evaluate("""
                        () => {
                            const armRe = /[\u0530-\u058F]/;
                            const tabs = Array.from(document.querySelectorAll(
                                'button, [role="tab"], [class*="tab"], [class*="filter"], a.nav-link'
                            )).filter(e => armRe.test(e.textContent || ''));
                            if (tabs.length > 0) tabs[0].click();
                        }
                    """)
                    page.wait_for_timeout(2000)

                html = page.content()
                browser.close()

            from bs4 import BeautifulSoup
            return BeautifulSoup(html, "lxml")
        except Exception as exc:
            logger.warning("[%s] Extended render failed for %s: %s", self.BANK_NAME, url, exc)
            return self._get_rendered(url)

    def _extract_armenian_text_full(self, soup, url):
        for tag in soup.select(
            "script,style,nav,header,footer,"
            "[class*='menu'],[class*='footer'],[class*='cookie'],"
            "[class*='modal'],[class*='banner'],[class*='popup'],"
            "[class*='header'],[class*='toolbar'],[class*='topbar'],"
            "[class*='sidebar'],[class*='nav']"
        ):
            tag.decompose()

        LEAF_TAGS = {"p", "li", "td", "th", "dt", "dd",
                     "h1", "h2", "h3", "h4", "h5", "h6", "address"}
        parts, seen = [], set()
        for el in soup.find_all(LEAF_TAGS):
            raw = self._clean(el.get_text(separator=" "))
            if not raw or raw in seen or not is_armenian_text(raw) or len(raw) < 15:
                continue
            seen.add(raw)
            parts.append(raw)

        # fallback to divs
        if not parts:
            div_texts = []
            for el in soup.find_all(["div", "section", "article"]):
                raw = self._clean(el.get_text(separator=" "))
                if not raw or raw in seen or not is_armenian_text(raw) or len(raw) < 15:
                    continue
                seen.add(raw)
                div_texts.append(raw)
            parts = [t for t in div_texts
                     if not any(t != o and t in o for o in div_texts)]

        return "\n".join(parts) if parts else None

    def _parse_tables(self, soup, section, url):
        records = []
        for table in soup.select("table"):
            lines = []
            for row in table.select("tr"):
                cells = [self._clean(c.get_text()) for c in row.select("td,th") if self._clean(c.get_text())]
                if not cells:
                    continue
                row_text = " | ".join(cells)
                if is_armenian_text(row_text) or (lines and cells):
                    lines.append(row_text)
            if lines:
                records.append(self._make_record(section, url, "\n".join(lines)))
        return records