"""
Unit tests — run with: python -m unittest tests -v

Requirement 9 (Language):
  Every test that exercises content extraction uses Armenian (Հայերեն) HTML
  fixtures.  Tests explicitly verify that:
    - Armenian content IS extracted and preserved verbatim.
    - English-only pages return [] (skipped, not translated).
    - Mixed-language pages yield only the Armenian portions.
    - UTF-8 encoding is never mangled.
"""

import json
import unittest
from unittest.mock import MagicMock
from scrapers.base import BankRecord, BaseBankScraper, is_armenian_text
from scrapers import SCRAPER_REGISTRY, AmeriabankScraper, ArdshinbankScraper, InecobankScraper
from pipeline import deduplicate, filter_short, apply_section_filter

# ---------------------------------------------------------------------------
# Shared HTML builders
# ---------------------------------------------------------------------------

def _make_html(body: str) -> str:
    return f'<html><head><meta charset="utf-8"></head><body>{body}</body></html>'

def _mock_response(text: str, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.content = text.encode("utf-8")
    resp.status_code = status
    resp.raise_for_status = MagicMock()
    resp.encoding = "utf-8"
    resp.headers = {"Content-Type": "text/html; charset=utf-8"}
    return resp

# ---------------------------------------------------------------------------
# Armenian sample strings
# ---------------------------------------------------------------------------

ARM_LOAN = (
    "Սպառողական վարկ՝ մինչև 5,000,000 ՀՀ դրամ: "
    "Տոկոսադրույք՝ տարեկան 14%: Ժամկետ՝ 12-60 ամիս:"
)
ARM_DEPOSIT = (
    "Ժամկետային ավանդ ՀՀ դրամով: "
    "Տոկոսադրույք՝ տարեկան 9.5%: Նվազագույն գումար՝ 100,000 ՀՀ դրամ:"
)
ARM_BRANCH = (
    "Երևան, Վազգեն Սարգսյան 2: "
    "Հեռ.՝ +374 10 56-11-11: Աշխ. ժամ.՝ Երկ-Ուրբ 09:00-18:00:"
)
ENGLISH_ONLY = (
    "Consumer loan up to 5,000,000 AMD at 14% annual interest. "
    "Term: 12 to 60 months. No collateral required."
)


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

class TestIsArmenianText(unittest.TestCase):
    def test_armenian_returns_true(self):
        self.assertTrue(is_armenian_text(ARM_LOAN))

    def test_english_returns_false(self):
        self.assertFalse(is_armenian_text(ENGLISH_ONLY))

    def test_empty_returns_false(self):
        self.assertFalse(is_armenian_text(""))

    def test_numbers_only_returns_false(self):
        self.assertFalse(is_armenian_text("12345 / 67.8%"))

    def test_mixed_majority_armenian(self):
        self.assertTrue(is_armenian_text(ARM_LOAN + " (14% APR)"))

    def test_below_threshold_returns_false(self):
        self.assertFalse(is_armenian_text("Վ abc def ghi jklmno"))

    def test_threshold_exactly_met(self):
        self.assertTrue(is_armenian_text("Վարկ վարկ վ"))



# ---------------------------------------------------------------------------
# BankRecord
# ---------------------------------------------------------------------------

class TestBankRecord(unittest.TestCase):
    def test_to_dict_keys(self):
        r = BankRecord(bank="X", section="credits", url="http://x.am", text=ARM_LOAN)
        self.assertEqual(set(r.to_dict().keys()), {"bank", "section", "url", "text"})

    def test_armenian_text_preserved_verbatim(self):
        r = BankRecord(bank="X", section="deposits", url="http://x.am/d", text=ARM_DEPOSIT)
        self.assertEqual(r.to_dict()["text"], ARM_DEPOSIT)


# ---------------------------------------------------------------------------
# Base helpers
# ---------------------------------------------------------------------------

class DummyScraper(BaseBankScraper):
    BANK_NAME = "Dummy"
    def scrape_credits(self):  return []
    def scrape_deposits(self): return []
    def scrape_branches(self): return []

class TestBaseHelpers(unittest.TestCase):
    def setUp(self):
        self.s = DummyScraper()

    def test_clean_collapses_whitespace(self):
        self.assertEqual(self.s._clean("  foo   bar\n\tbaz  "), "foo bar baz")

    def test_clean_preserves_armenian(self):
        raw = "  Վարկ abc բնակարան def  "
        result = self.s._clean(raw)
        self.assertIn("Վարկ", result)
        self.assertIn("բնակարան", result)

    def test_clean_empty(self):
        self.assertEqual(self.s._clean(""), "")

    def test_make_record_type(self):
        r = self.s._make_record("credits", "http://x", ARM_LOAN)
        self.assertIsInstance(r, BankRecord)
        self.assertEqual(r.bank, "Dummy")

    def test_get_returns_none_on_error(self):
        import requests
        self.s.delay = 0
        self.s.session.get = MagicMock(side_effect=requests.RequestException("fail"))
        self.assertIsNone(self.s._get("http://fail.am"))

    def test_get_sets_utf8_encoding(self):
        self.s.delay = 0
        mock_resp = _mock_response(ARM_LOAN)
        mock_resp.encoding = "iso-8859-1"
        # _get now checks resp.content (bytes) before setting encoding
        mock_resp.content = ARM_LOAN.encode("utf-8")
        mock_resp.headers = {"Content-Type": "text/html; charset=iso-8859-1"}
        self.s.session.get = MagicMock(return_value=mock_resp)
        resp = self.s._get("http://x.am")
        self.assertIsNotNone(resp)
        self.assertEqual(resp.encoding, "utf-8")

    def test_assert_armenian_true(self):
        self.assertTrue(self.s._assert_armenian(ARM_LOAN, "http://x"))

    def test_assert_armenian_false_english(self):
        self.assertFalse(self.s._assert_armenian(ENGLISH_ONLY, "http://x"))

    def test_extract_armenian_returns_none_for_english_page(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(_make_html(f"<p>{ENGLISH_ONLY}</p>"), "lxml")
        self.assertIsNone(self.s._extract_armenian_text(soup, "http://x"))

    def test_extract_armenian_returns_armenian_content(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(_make_html(f"<p>{ARM_LOAN}</p>"), "lxml")
        result = self.s._extract_armenian_text(soup, "http://x")
        self.assertIsNotNone(result)
        self.assertTrue(is_armenian_text(result))

    def test_extract_armenian_strips_english_nav(self):
        from bs4 import BeautifulSoup
        html = _make_html(
            f"<nav><p>{ENGLISH_ONLY}</p></nav>"
            f"<main><p>{ARM_LOAN}</p></main>"
        )
        soup = BeautifulSoup(html, "lxml")
        result = self.s._extract_armenian_text(soup, "http://x")
        self.assertIsNotNone(result)
        self.assertNotIn("Consumer loan", result)

    def test_scrape_all_aggregates(self):
        self.s.scrape_credits  = MagicMock(return_value=[BankRecord("D","credits","u",ARM_LOAN)])
        self.s.scrape_deposits = MagicMock(return_value=[BankRecord("D","deposits","u",ARM_DEPOSIT)])
        self.s.scrape_branches = MagicMock(return_value=[BankRecord("D","branches","u",ARM_BRANCH)])
        self.assertEqual(len(self.s.scrape_all()), 3)

    def test_scrape_all_tolerates_exception(self):
        self.s.scrape_credits  = MagicMock(side_effect=RuntimeError("boom"))
        self.s.scrape_deposits = MagicMock(return_value=[])
        self.s.scrape_branches = MagicMock(return_value=[])
        self.assertEqual(self.s.scrape_all(), [])


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry(unittest.TestCase):
    def test_all_banks_registered(self):
        for slug in ("ameriabank", "ardshinbank", "inecobank"):
            self.assertIn(slug, SCRAPER_REGISTRY)

    def test_scrapers_are_subclasses(self):
        for cls in SCRAPER_REGISTRY.values():
            self.assertTrue(issubclass(cls, BaseBankScraper))

    def test_bank_names_set(self):
        for cls in SCRAPER_REGISTRY.values():
            self.assertNotEqual(cls.BANK_NAME, "Unknown Bank")

    def test_all_urls_use_armenian_locale(self):
        """
        Every scraper must target Armenian-language pages.
        - Ameriabank: Armenian is the DEFAULT language — NO prefix (just /personal/...)
        - Inecobank: uses /hy/ prefix
        - Ardshinbank: uses /for-you/ and /content/ Armenian-slug paths
        """
        # Inecobank uses /hy/ prefix
        for attr in ("CREDIT_URLS", "DEPOSIT_URLS"):
            for path, *_ in getattr(InecobankScraper, attr, []):
                self.assertTrue(
                    path.startswith("/hy/"),
                    f"InecobankScraper.{attr} has non-/hy/ path: {path}"
                )
        self.assertTrue(
            InecobankScraper.BRANCH_URL.startswith("/hy/"),
            f"InecobankScraper.BRANCH_URL is not /hy/: {InecobankScraper.BRANCH_URL}"
        )

        # Ameriabank: Armenian is default — paths start with /personal/ or /service-network
        for attr in ("CREDIT_URLS", "DEPOSIT_URLS"):
            for path, *_ in getattr(AmeriabankScraper, attr, []):
                self.assertFalse(
                    path.startswith("/en/") or path.startswith("/ru/"),
                    f"AmeriabankScraper.{attr} uses non-Armenian locale: {path}"
                )
                self.assertTrue(
                    path.startswith("/personal/") or path.startswith("/saving/"),
                    f"AmeriabankScraper.{attr} has unexpected path: {path}"
                )

        # Ardshinbank: /for-you/ or /content/ Armenian-slug paths
        for attr in ("CREDIT_URLS", "DEPOSIT_URLS"):
            for path, *_ in getattr(ArdshinbankScraper, attr, []):
                self.assertTrue(
                    path.startswith("/for-you/") or path.startswith("/content/"),
                    f"ArdshinbankScraper.{attr} has unexpected path: {path}"
                )


# ---------------------------------------------------------------------------
# Ameriabank
# ---------------------------------------------------------------------------

class TestAmeriabankScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = AmeriabankScraper()
        self.scraper.delay = 0

    def test_credits_armenian_content_extracted(self):
        html = _make_html(f'<div class="product-card"><p>{ARM_LOAN}</p></div>')
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_credits()
        self.assertGreater(len(records), 0)
        for r in records:
            self.assertEqual(r.section, "credits")
            self.assertEqual(r.bank, "Ameriabank")
            self.assertTrue(is_armenian_text(r.text))

    def test_credits_english_only_page_skipped(self):
        html = _make_html(f"<main><p>{ENGLISH_ONLY}</p></main>")
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        self.assertEqual(self.scraper.scrape_credits(), [])

    def test_deposits_fallback_armenian(self):
        html = _make_html(f"<main><p>{ARM_DEPOSIT}</p></main>")
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_deposits()
        self.assertGreater(len(records), 0)
        self.assertTrue(all(is_armenian_text(r.text) for r in records))

    def test_branches_armenian_table(self):
        html = _make_html(f"<table><tr><td>{ARM_BRANCH}</td></tr></table>")
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_branches()
        self.assertGreater(len(records), 0)
        self.assertTrue(all(r.section == "branches" for r in records))
        self.assertTrue(all(is_armenian_text(r.text) for r in records))

    def test_mixed_page_english_nav_stripped(self):
        html = _make_html(
            f"<nav><p>{ENGLISH_ONLY}</p></nav>"
            f"<main><p>{ARM_LOAN}</p></main>"
        )
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_credits()
        self.assertGreater(len(records), 0)
        for r in records:
            self.assertNotIn("Consumer loan", r.text)

    def test_failed_get_returns_empty(self):
        self.scraper._get = MagicMock(return_value=None)
        self.assertEqual(self.scraper.scrape_credits(), [])
        self.assertEqual(self.scraper.scrape_deposits(), [])
        self.assertEqual(self.scraper.scrape_branches(), [])


# ---------------------------------------------------------------------------
# Ardshinbank
# ---------------------------------------------------------------------------

class TestArdshinbankScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = ArdshinbankScraper()
        self.scraper.delay = 0

    def test_table_extraction_armenian_only(self):
        html = _make_html(
            "<table>"
            "<tr><th>Ժամկետ</th><th>Տոկոսադրույք</th></tr>"
            "<tr><td>6 ամիս</td><td>7.5%</td></tr>"
            "<tr><td>12 ամիս</td><td>8.5%</td></tr>"
            "</table>"
        )
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_deposits()
        self.assertGreater(len(records), 0)
        combined = "\n".join(r.text for r in records)
        self.assertIn("8.5%", combined)
        self.assertTrue(is_armenian_text(combined))

    def test_english_table_skipped(self):
        html = _make_html(
            "<table><tr><th>Term</th><th>Rate</th></tr>"
            "<tr><td>6 months</td><td>7.5%</td></tr></table>"
        )
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        self.assertEqual(self.scraper.scrape_deposits(), [])

    def test_branch_armenian_li(self):
        html = _make_html(f"<ul><li>{ARM_BRANCH}</li></ul>")
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_branches()
        self.assertGreater(len(records), 0)
        self.assertTrue(all(is_armenian_text(r.text) for r in records))

    def test_english_branch_page_skipped(self):
        html = _make_html("<ul><li>Yerevan, Tigranyan 4, Tel: +374 10 56-11-11</li></ul>")
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        self.assertEqual(self.scraper.scrape_branches(), [])


# ---------------------------------------------------------------------------
# Inecobank
# ---------------------------------------------------------------------------

class TestInecobankScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = InecobankScraper()
        self.scraper.delay = 0

    def test_dl_armenian_keys_extracted(self):
        html = _make_html(
            "<dl>"
            "<dt>Նվազագույն գումար</dt><dd>100,000 ՀՀ դրամ</dd>"
            "<dt>Առավելագույն ժամկետ</dt><dd>60 ամիս</dd>"
            "</dl>"
        )
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_credits()
        self.assertGreater(len(records), 0)
        combined = "\n".join(r.text for r in records)
        self.assertIn("100,000 ՀՀ դրամ", combined)
        self.assertTrue(is_armenian_text(combined))

    def test_dl_english_keys_skipped(self):
        html = _make_html("<dl><dt>Minimum amount</dt><dd>100,000 AMD</dd></dl>")
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        self.assertEqual(self.scraper.scrape_credits(), [])

    def test_contact_block_armenian(self):
        html = _make_html(f'<div class="contact-block"><p>{ARM_BRANCH}</p></div>')
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        records = self.scraper.scrape_branches()
        self.assertGreater(len(records), 0)
        self.assertTrue(all(is_armenian_text(r.text) for r in records))

    def test_english_contact_block_skipped(self):
        html = _make_html(
            '<div class="contact-block">'
            'Head Office: Yerevan, Baghramyan 23, Mon-Fri 09:00-18:00'
            '</div>'
        )
        self.scraper._get = MagicMock(return_value=_mock_response(html))
        self.assertEqual(self.scraper.scrape_branches(), [])


# ---------------------------------------------------------------------------
# Pipeline post-processing
# ---------------------------------------------------------------------------

class TestPipelineHelpers(unittest.TestCase):
    def _r(self, bank="A", section="credits", url="u", text=None):
        return BankRecord(bank=bank, section=section, url=url, text=text or ARM_LOAN)

    def test_deduplicate_removes_exact(self):
        records = [self._r(), self._r(), self._r(bank="B")]
        self.assertEqual(len(deduplicate(records)), 2)

    def test_deduplicate_keeps_different_sections(self):
        records = [self._r(section="credits"), self._r(section="deposits")]
        self.assertEqual(len(deduplicate(records)), 2)

    def test_filter_short(self):
        records = [self._r(text="կ"), self._r(text=ARM_LOAN)]
        self.assertEqual(len(filter_short(records, min_chars=10)), 1)

    def test_apply_section_filter_empty_keeps_all(self):
        records = [self._r(section="credits"), self._r(section="deposits")]
        self.assertEqual(len(apply_section_filter(records, [])), 2)

    def test_apply_section_filter_filters(self):
        records = [self._r(section="credits"), self._r(section="deposits"), self._r(section="branches")]
        result = apply_section_filter(records, ["credits", "branches"])
        self.assertNotIn("deposits", {r.section for r in result})
        self.assertEqual(len(result), 2)

    def test_pipeline_invalid_bank_raises(self):
        from pipeline import run_pipeline
        with self.assertRaises(ValueError):
            run_pipeline(bank_slugs=["nonexistent_bank_xyz"])


# ---------------------------------------------------------------------------
# UTF-8 / serialisation
# ---------------------------------------------------------------------------

class TestUTF8AndSerialisation(unittest.TestCase):
    def test_armenian_unicode_survives_json_roundtrip(self):
        r = BankRecord(bank="X", section="credits", url="u", text=ARM_LOAN)
        loaded = json.loads(json.dumps(r.to_dict(), ensure_ascii=False))
        self.assertEqual(loaded["text"], ARM_LOAN)
        self.assertTrue(is_armenian_text(loaded["text"]))

    def test_ensure_ascii_false_keeps_script(self):
        r = BankRecord(bank="X", section="deposits", url="u", text=ARM_DEPOSIT)
        dumped = json.dumps(r.to_dict(), ensure_ascii=False)
        self.assertIn("Ժամկետ", dumped)

    def test_to_dict_has_required_keys(self):
        r = BankRecord(bank="X", section="branches", url="u", text=ARM_BRANCH)
        self.assertEqual(set(r.to_dict().keys()), {"bank", "section", "url", "text"})


if __name__ == "__main__":
    unittest.main(verbosity=2)