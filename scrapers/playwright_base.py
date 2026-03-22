"""
PlaywrightBankScraper — base class for JS-rendered bank sites.

Key methods:
    _get_rendered(url)
    _get_rendered_with_api_intercept(url, patterns, wait_ms)
    _scrape_urls_rendered(url_list, section)
    _scrape_branches_rendered(url)
"""
import logging
import time
from typing import Optional
from bs4 import BeautifulSoup
from .base import BaseBankScraper, BankRecord, is_armenian_text

logger = logging.getLogger(__name__)


class PlaywrightBankScraper(BaseBankScraper):

    ARMENIAN_WAIT_TIMEOUT = 15_000

    def _get_rendered(self, url: str) -> Optional[BeautifulSoup]:
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return None

        time.sleep(self.delay)
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(
                    locale="hy-AM", accept_downloads=False, java_script_enabled=True,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                )
                page = context.new_page()
                page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
                try:
                    page.goto(url, wait_until="networkidle", timeout=self.ARMENIAN_WAIT_TIMEOUT)
                except PWTimeout:
                    logger.warning("[%s] goto timeout for %s — continuing", self.BANK_NAME, url)
                try:
                    page.wait_for_selector("body", timeout=5000)
                except PWTimeout:
                    pass
                try:
                    page.wait_for_function(
                        "() => { const m=(document.body.innerText||'').match(/[\u0530-\u058F]/g); return m&&m.length>=10; }",
                        timeout=self.ARMENIAN_WAIT_TIMEOUT,
                    )
                except PWTimeout:
                    logger.warning("[%s] Armenian text timeout at %s — parsing anyway", self.BANK_NAME, url)
                try:
                    page.wait_for_load_state("networkidle", timeout=3000)
                except PWTimeout:
                    pass
                html = page.content()
                browser.close()
            return BeautifulSoup(html, "lxml")
        except Exception as exc:
            logger.warning("[%s] Playwright failed for %s: %s", self.BANK_NAME, url, exc)
            return None

    def _get_rendered_with_api_intercept(
        self,
        url: str,
        api_url_patterns: list[str],
        wait_ms: int = 6000,
    ) -> tuple[Optional[BeautifulSoup], list[dict]]:
        """
        Navigate to *url* and intercept all non-HTML responses.
        If *api_url_patterns* is [""] (empty string), intercepts everything.
        Otherwise intercepts only responses whose URL contains a pattern.

        Returns (soup, intercepted_list).
        Each intercepted item: {"url": str, "data": parsed_json_or_None}
        """
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            return None, []

        time.sleep(self.delay)
        intercepted: list[dict] = []
        match_all = api_url_patterns == [""]

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(
                    locale="hy-AM", java_script_enabled=True,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                )
                page = context.new_page()
                page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())

                def handle_response(response):
                    try:
                        resp_url = response.url
                        ct = response.headers.get("content-type", "")
                        if "html" in ct:
                            return
                        if not match_all and not any(pat in resp_url for pat in api_url_patterns):
                            return
                        try:
                            body = response.json()
                            intercepted.append({"url": resp_url, "data": body})
                            logger.info("[%s] Intercepted: %s", self.BANK_NAME, resp_url)
                        except Exception:
                            pass
                    except Exception:
                        pass

                page.on("response", handle_response)

                try:
                    page.goto(url, wait_until="networkidle", timeout=self.ARMENIAN_WAIT_TIMEOUT)
                except PWTimeout:
                    logger.warning("[%s] goto timeout for %s", self.BANK_NAME, url)

                try:
                    page.wait_for_timeout(wait_ms)
                except Exception:
                    pass
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except PWTimeout:
                    pass

                html = page.content()
                browser.close()

            return BeautifulSoup(html, "lxml"), intercepted

        except Exception as exc:
            logger.warning("[%s] API intercept failed for %s: %s", self.BANK_NAME, url, exc)
            return None, []

    def _scrape_urls_rendered(self, url_list, section):
        records = []
        for path, label in url_list:
            url = self.BASE_URL + path
            soup = self._get_rendered(url)
            if soup is None:
                continue
            text = self._extract_armenian_text(soup, url)
            if text:
                records.append(self._make_record(section, url, text))
            else:
                logger.info("[%s] No Armenian content at %s — skipped.", self.BANK_NAME, url)
        return records

    def _scrape_branches_rendered(self, url):
        soup = self._get_rendered(url)
        if soup is None:
            return []
        records = []
        blocks = soup.select(
            ".branch-item,.branch-card,.location-item,address,"
            "[class*='branch'],[class*='location'],[class*='network'],[class*='office']"
        )
        for block in blocks:
            raw = self._clean(block.get_text(separator=" "))
            if is_armenian_text(raw):
                records.append(self._make_record("branches", url, raw))
        if not records:
            text = self._extract_armenian_text(soup, url)
            if text:
                records.append(self._make_record("branches", url, text))
        return records