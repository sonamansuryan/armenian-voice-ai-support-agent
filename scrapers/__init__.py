"""
Scraper registry — maps bank slug → scraper class.
Add new banks here; the pipeline picks them up automatically.
"""
from .base import BaseBankScraper, BankRecord
from .ameriabank import AmeriabankScraper
from .ardshinbank import ArdshinbankScraper
from .inecobank import InecobankScraper

SCRAPER_REGISTRY: dict[str, type[BaseBankScraper]] = {
    "ameriabank": AmeriabankScraper,
    "ardshinbank": ArdshinbankScraper,
    "inecobank": InecobankScraper,
    # "acba": ACBAScraper,        ← add future banks here
    # "evocabank": EvocabankScraper,
}

__all__ = [
    "BaseBankScraper",
    "BankRecord",
    "SCRAPER_REGISTRY",
    "AmeriabankScraper",
    "ArdshinbankScraper",
    "InecobankScraper",
]