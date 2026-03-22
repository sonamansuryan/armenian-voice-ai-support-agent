import logging
import time as _time
from bs4 import BeautifulSoup
from .playwright_base import PlaywrightBankScraper, BankRecord
from .base import is_armenian_text

logger = logging.getLogger(__name__)

_BRANCH_API_PATTERNS = [
    "branch", "Branch", "map-data", "location", "office",
    "atm", "ATM", "filial", "network", "geo", "marker",
    "/api/", "branches", "offices", "points",
]

_LIST_WORD = "\u0551\u0578\u0582\u0581\u0561\u056f"   # Ցուցակ


class InecobankScraper(PlaywrightBankScraper):
    BANK_NAME = "Inecobank"
    BASE_URL = "https://www.inecobank.am"

    CREDIT_URLS = [
        ("/hy/Individual/consumer-loans/1-click",                   "1 Click Loan"),
        ("/hy/Individual/consumer-loans/secured-consumer-loan/terms","Secured consumer loan terms"),
        ("/hy/Individual/consumer-loans/secured-consumer-loan",     "Secured loan general"),
        ("/hy/Individual/consumer-loans/gold-pledge-secured",       "Gold pledge loan"),
        ("/hy/Individual/consumer-loans/deposit-secured",           "Deposit secured loan"),
        ("/hy/Individual/consumer-loans/bond-secured",              "Bond secured loan"),
        ("/hy/Individual/consumer-loans/refinance",                 "Refinance"),
        ("/hy/Individual/mortgage-loans",                           "Mortgage"),
        ("/hy/Individual/car-loans",                                "Car loan primary"),
        ("/hy/Individual/secondarymarket-car-loans",                "Car loan secondary"),
        ("/hy/Individual/investment",                               "Investment loan"),
    ]

    def scrape_credits(self) -> list[BankRecord]:
        return self._scrape_urls(self.CREDIT_URLS, "credits")

    DEPOSIT_URLS = [
        ("/hy/Individual/deposits",                "Deposits general"),
        ("/hy/Individual/deposits/simple",         "Simple deposit"),
        ("/hy/Individual/deposits/simple/terms",   "Simple deposit terms"),
        ("/hy/Individual/deposits/accumulative",   "Accumulative deposit"),
        ("/hy/Individual/deposits/flexible",       "Flexible deposit"),
        ("/hy/Individual/deposits/flexible/terms", "Flexible deposit terms"),
    ]

    def scrape_deposits(self) -> list[BankRecord]:
        return self._scrape_urls(self.DEPOSIT_URLS, "deposits")

    RATE_AM_URL = "https://www.rate.am/hy/bank/inecobank"
    MAP_URL     = "/hy/map"

    def scrape_branches(self) -> list[BankRecord]:
        # Strategy 1 (PRIMARY): rate.am - static HTML, no Google Maps needed
        records = self._scrape_branches_rate_am()
        if records:
            logger.info("[%s] %d branch records via rate.am", self.BANK_NAME, len(records))
            return records

        # Strategy 2: intercept API calls on inecobank.am/hy/map
        url = self.BASE_URL + self.MAP_URL
        records = self._scrape_branches_via_api(url)
        if records:
            logger.info("[%s] %d branch records via API intercept", self.BANK_NAME, len(records))
            return records

        # Strategy 3: render page, click list-view
        records = self._scrape_branches_rendered_listview(url)
        if records:
            logger.info("[%s] %d branch records via rendered list-view", self.BANK_NAME, len(records))
            return records

        logger.warning("[%s] All branch strategies failed.", self.BANK_NAME)
        return []

    def _scrape_branches_rate_am(self) -> list[BankRecord]:
        """
        Scrape from https://www.rate.am/hy/bank/inecobank
        Page is Next.js SSR — all branch data is in raw HTML.
        CSS classes are hashed, so we anchor on tel: links:
        every branch has exactly one <a href="tel:37410510510">.
        We walk up to find the card container, then extract text.
        """
        resp = self._get(self.RATE_AM_URL)
        if resp is None:
            logger.warning("[%s] rate.am fetch failed", self.BANK_NAME)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup.select("script,style,nav,header,footer"):
            tag.decompose()

        records: list[BankRecord] = []

        # ---- Strategy A: anchor on tel: links -------------------------
        # Each Inecobank branch card has <a href="tel:37410510510">
        # Walk up the DOM to find the smallest container that has both
        # a phone link AND an Armenian address.
        tel_links = soup.select('a[href^="tel:"]')  # all phone anchors
        seen: set[str] = set()

        for link in tel_links:
            # Walk up max 6 levels to find the branch card container
            node = link.parent
            for _ in range(6):
                if node is None:
                    break
                text = self._clean(node.get_text(separator=" "))
                # A valid branch card: Armenian, has address, not too big
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
            logger.info("[%s] rate.am tel-anchor: %d branches",
                        self.BANK_NAME, len(records))
            # Remove any supersets (if we grabbed both child and parent)
            texts = [r.text for r in records]
            filtered = [r for r in records
                        if not any(r.text != o and r.text in o for o in texts)]
            return filtered

        # ---- Strategy B: phone string search --------------------------
        # Fallback: find all elements containing the Inecobank phone
        phone_str = "(374 10) 510 510"
        candidates: list[str] = []
        seen2: set[str] = set()
        for el in soup.find_all(["div", "li", "section", "article"]):
            raw = self._clean(el.get_text(separator=" "))
            if phone_str not in raw:
                continue
            if len(raw) < 30 or len(raw) > 2000:
                continue
            if not is_armenian_text(raw):
                continue
            if raw not in seen2:
                seen2.add(raw)
                candidates.append(raw)

        if candidates:
            leaf = [t for t in candidates
                    if not any(t != o and o in t for o in candidates)]
            final = leaf if len(leaf) >= 2 else candidates
            for text in final:
                records.append(
                    self._make_record("branches", self.RATE_AM_URL, text))
            logger.info("[%s] rate.am phone-search: %d branches",
                        self.BANK_NAME, len(records))

        return records

    # ------------------------------------------------------------------
    def _scrape_branches_via_api(self, url: str) -> list[BankRecord]:
        soup, intercepted = self._get_rendered_with_api_intercept(
            url, api_url_patterns=_BRANCH_API_PATTERNS, wait_ms=7000,
        )
        records = []
        for item in intercepted:
            text = self._parse_branch_json(item["data"])
            if text:
                records.append(self._make_record("branches", item["url"], text))
        return records

    def _parse_branch_json(self, data) -> str | None:
        lines = []
        def extract(obj):
            if isinstance(obj, dict):
                for field in ["address","name","title","nameHy","addressHy",
                               "address_hy","name_hy","branchName","branchAddress",
                               "workingHours","workHours","phone","phoneNumber","city"]:
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
        arm   = [ln for ln in lines if is_armenian_text(ln)]
        phone = [ln for ln in lines if "+374" in ln and ln not in arm]
        return "\n".join(arm + phone) if (arm or phone) else "\n".join(lines[:100])

    def _scrape_branches_rendered_listview(self, url: str) -> list[BankRecord]:

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
                page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3,ico}", lambda r: r.abort())

                # domcontentloaded — don't wait for Google Maps network idle
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                except PWTimeout:
                    logger.warning("[%s] goto timeout — continuing anyway", self.BANK_NAME)

                # Wait for Armenian text (up to 20s)
                try:
                    page.wait_for_function(
                        "() => (document.body.innerText.match(/[\u0530-\u058F]/g)||[]).length >= 20",
                        timeout=20_000
                    )
                except PWTimeout:
                    logger.warning("[%s] Armenian text wait timeout", self.BANK_NAME)

                # Give page a moment to finish rendering
                page.wait_for_timeout(2000)

                # Click "Ցuцak" list-view button
                list_word = _LIST_WORD
                clicked = page.evaluate(f"""
                    () => {{
                        const word = '{list_word}';
                        // Search all clickable elements for the list-view button
                        const candidates = Array.from(document.querySelectorAll(
                            'button, a, span, li, div, label'
                        ));
                        // Find element whose DIRECT text content matches
                        for (const el of candidates) {{
                            const direct = Array.from(el.childNodes)
                                .filter(n => n.nodeType === 3)
                                .map(n => n.textContent.trim())
                                .join('');
                            if (direct === word) {{ el.click(); return 'direct:' + el.tagName; }}
                        }}
                        // Fallback: any element containing the word
                        for (const el of candidates) {{
                            if (el.textContent.trim() === word) {{
                                el.click();
                                return 'full:' + el.tagName;
                            }}
                        }}
                        return null;
                    }}
                """)

                if clicked:
                    logger.info("[%s] Clicked list-view: %s", self.BANK_NAME, clicked)
                    # Wait specifically for branch address content to appear
                    # Inecobank branch cards contain "ք." (city abbrev) and phone numbers
                    try:
                        page.wait_for_function(
                            "() => document.body.innerText.includes('ք.')",
                            timeout=10_000
                        )
                        logger.info("[%s] Branch address content detected", self.BANK_NAME)
                    except PWTimeout:
                        logger.warning("[%s] Address content wait timeout — trying longer wait", self.BANK_NAME)
                        page.wait_for_timeout(6000)

                    # Extra scroll to trigger lazy-loaded list
                    page.evaluate("window.scrollTo(0, 300)")
                    page.wait_for_timeout(2000)
                else:
                    logger.warning("[%s] List-view button not found", self.BANK_NAME)
                    # Try clicking by partial text match (button label might have changed)
                    try:
                        page.get_by_text("Ցուցակ").first.click()
                        page.wait_for_timeout(6000)
                        logger.info("[%s] Clicked via get_by_text fallback", self.BANK_NAME)
                    except Exception:
                        page.wait_for_timeout(2000)

                # Get the full rendered HTML after click
                html = page.content()
                browser.close()

            # Parse with BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Remove ALL chrome/map noise
            for tag in soup.select(
                "script,style,nav,header,footer,"
                "[class*='map'],[id*='map'],canvas,"
                "[class*='header'],[class*='toolbar'],[class*='topbar'],"
                "[class*='cookie'],[class*='modal'],[class*='banner']"
            ):
                tag.decompose()

            # Keywords in real Inecobank branch entries
            addr_kw  = ["ք.", "Երևան", "Baghramyan",
                         "Komitas", "Mashtots", "Tigranyan", "Abovyan",
                         "Nor Nork", "Vazgen", "Isahakyan",
                         "Sayat-Nova", "Sevak", "Myasnikyan"]
            phone_kw = ["+374", "010 51", "010-51", "010510",
                         "(010)", "093", "094", "095", "098", "077"]

            # ---- Strategy A: known card CSS selectors ----
            card_selectors = [
                ".branch-item", ".branch-card", ".office-item",
                "[class*='branch-item']", "[class*='branchItem']",
                "[class*='filial']", "[class*='location-card']",
                "[class*='list-item']", "[class*='listItem']",
                "li.branch", "li.office",
            ]
            card_texts: list[str] = []
            for sel in card_selectors:
                cards = soup.select(sel)
                if len(cards) >= 2:
                    for c in cards:
                        t = self._clean(c.get_text(separator=" "))
                        if len(t) >= 30 and is_armenian_text(t):
                            card_texts.append(t)
                    if card_texts:
                        break

            if card_texts:
                seen_a: set[str] = set()
                for text in card_texts:
                    if text not in seen_a:
                        seen_a.add(text)
                        records.append(self._make_record("branches", url, text))
                logger.info("[%s] Strategy A: %d branch cards", self.BANK_NAME, len(records))

            else:
                # ---- Strategy B: BOTH addr + phone, keep leaf elements ----
                candidates: list[str] = []
                seen_b: set[str] = set()

                for el in soup.find_all(["div", "li", "article", "p"]):
                    raw = self._clean(el.get_text(separator=" "))
                    if len(raw) < 30 or len(raw) > 2000:
                        continue
                    if not is_armenian_text(raw):
                        continue
                    has_addr  = any(kw in raw for kw in addr_kw)
                    has_phone = any(kw in raw for kw in phone_kw)
                    if not (has_addr and has_phone):
                        continue
                    if raw not in seen_b:
                        seen_b.add(raw)
                        candidates.append(raw)

                if candidates:
                    leaf = [t for t in candidates
                            if not any(t != o and o in t for o in candidates)]
                    final = leaf if len(leaf) >= 2 else candidates
                    seen_c: set[str] = set()
                    for text in final:
                        if text not in seen_c:
                            seen_c.add(text)
                            records.append(self._make_record("branches", url, text))
                    logger.info("[%s] Strategy B: %d branches", self.BANK_NAME, len(records))

                else:
                    # ---- Strategy C: split biggest block by phone boundary ----
                    import re as _re
                    all_arm = []
                    for el in soup.find_all(["div", "li", "p"]):
                        raw = self._clean(el.get_text(separator=" "))
                        if len(raw) > 100 and is_armenian_text(raw):
                            all_arm.append(raw)
                    if all_arm:
                        big_text = max(all_arm, key=len)
                        parts = _re.split(r'(?=\+374|\(010\)|010[\s\-])', big_text)
                        for part in parts:
                            part = part.strip()
                            if len(part) >= 30 and is_armenian_text(part):
                                records.append(self._make_record("branches", url, part))
                        logger.info("[%s] Strategy C (split): %d parts", self.BANK_NAME, len(records))

        except Exception as exc:
            logger.warning("[%s] Rendered list-view failed: %s", self.BANK_NAME, exc)

        return records

    # ------------------------------------------------------------------
    def _scrape_urls(self, url_list, section):
        """
        Use Playwright (not plain HTTP) because Inecobank is a React SPA —
        plain requests returns an empty shell with no Armenian content.

        All structured records from a single URL are merged into one record
        to avoid oversplitting (e.g. a page with 30 table rows → 30 records).
        """
        records = []
        seen_urls: set[str] = set()
        seen_texts: set[str] = set()

        for path, label in url_list:
            url = self.BASE_URL + path
            if url in seen_urls:
                continue
            seen_urls.add(url)

            soup = self._get_rendered(url)
            if soup is None:
                continue

            # Try structured parsers — merge all results per URL into one record
            merged_parts = []
            for parse_fn in [self._parse_dl, self._parse_tables]:
                recs = parse_fn(soup, section, url)
                if recs:
                    for r in recs:
                        if r.text not in seen_texts:
                            merged_parts.append(r.text)

            if merged_parts:
                merged_text = "\n".join(merged_parts)
                if merged_text not in seen_texts:
                    seen_texts.add(merged_text)
                    records.append(self._make_record(section, url, merged_text))
            else:
                # Fallback: free-text extraction
                text = self._extract_clean_armenian(soup, url)
                if text and text not in seen_texts:
                    seen_texts.add(text)
                    records.append(self._make_record(section, url, text))
                elif not text:
                    logger.info("[%s] No Armenian content at %s", self.BANK_NAME, url)

        return records

    def _extract_clean_armenian(self, soup, url):
        for tag in soup.select("script,style,nav,header,footer,[class*='menu'],[class*='footer'],[class*='cookie'],[class*='modal'],[class*='banner'],[class*='sidebar']"):
            tag.decompose()
        parts, seen = [], set()
        for el in soup.find_all(True):
            if el.name not in {"p","li","td","th","dt","dd","h1","h2","h3","h4","h5","h6","div","section","article","address"}:
                continue
            raw = self._clean(el.get_text(separator=" "))
            if not raw or raw in seen or not is_armenian_text(raw):
                continue
            seen.add(raw)
            parts.append(raw)
        return "\n".join(parts) if parts else None

    def _parse_dl(self, soup, section, url):
        records = []
        for dl in soup.select("dl"):
            items = []
            for dt, dd in zip(dl.select("dt"), dl.select("dd")):
                key = self._clean(dt.get_text())
                val = self._clean(dd.get_text())
                if self.is_armenian_text(key):
                    items.append(f"{key}: {val}")
            if items:
                records.append(self._make_record(section, url, "\n".join(items)))
        return records

    def _parse_tables(self, soup, section, url):
        records = []
        for table in soup.select("table"):
            lines = []
            for row in table.select("tr"):
                cells = [self._clean(c.get_text()) for c in row.select("td,th") if self._clean(c.get_text())]
                if not cells:
                    continue
                row_text = " | ".join(cells)
                if self.is_armenian_text(row_text) or (lines and cells):
                    lines.append(row_text)
            if lines:
                records.append(self._make_record(section, url, "\n".join(lines)))
        return records