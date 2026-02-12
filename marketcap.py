"""
marketcap.py — Провайдер маркеткапов через CoinGecko
Async, кэш, полная загрузка
"""
import asyncio
import time
import logging
from typing import Dict, Optional, Set

import aiohttp

import config

logger = logging.getLogger("oi_scanner")


class MarketCapProvider:
    """
    Кэширующий провайдер маркеткапов через CoinGecko.
    Загружает ВСЕ доступные монеты (до 2500).
    """

    COINGECKO_BASE = "https://api.coingecko.com/api/v3"
    COINGECKO_PRO_BASE = "https://pro-api.coingecko.com/api/v3"

    def __init__(self, api_key: str = "", cache_ttl: int = 300):
        self.api_key = api_key
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, float] = {}  # SYMBOL → mcap_usd
        self._cache_time: float = 0
        self._lock = asyncio.Lock()

    @property
    def _base_url(self) -> str:
        return self.COINGECKO_PRO_BASE if self.api_key else self.COINGECKO_BASE

    def _headers(self) -> Dict:
        h = {"Accept": "application/json"}
        if self.api_key:
            h["x-cg-pro-api-key"] = self.api_key
        return h

    @property
    def is_stale(self) -> bool:
        return not self._cache or (time.time() - self._cache_time) >= self.cache_ttl

    async def refresh_cache(self):
        """Загрузить маркеткапы всех монет"""
        async with self._lock:
            if not self.is_stale:
                return

            logger.info("🔄 Загрузка маркеткапов с CoinGecko...")
            all_coins: Dict[str, float] = {}
            start = time.time()

            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers=self._headers(),
                ) as session:
                    for page in range(1, 11):  # До 2500 монет
                        try:
                            data = await self._fetch_page(session, page)
                            if not data:
                                break

                            for coin in data:
                                sym = coin.get("symbol", "").upper()
                                mcap = coin.get("market_cap")
                                if sym and mcap and mcap > 0:
                                    existing = all_coins.get(sym, 0)
                                    if mcap > existing:
                                        all_coins[sym] = mcap

                            if not self.api_key:
                                await asyncio.sleep(1.5)
                            else:
                                await asyncio.sleep(0.3)

                        except Exception as e:
                            if "429" in str(e):
                                logger.warning(f"⚠️  CoinGecko rate-limit на стр. {page}, стоп")
                                break
                            logger.warning(f"CoinGecko ошибка стр. {page}: {e}")
                            break

            except Exception as e:
                logger.error(f"CoinGecko фатал: {e}")

            if all_coins:
                self._cache = all_coins
                self._cache_time = time.time()
                elapsed = time.time() - start

                # Диагностика
                min_cap = config.MIN_MARKET_CAP
                max_cap = config.MAX_MARKET_CAP
                eligible = sum(
                    1 for v in all_coins.values()
                    if v >= min_cap and (max_cap <= 0 or v <= max_cap)
                )
                logger.info(
                    f"📊 Маркеткапы: {len(all_coins)} монет за {elapsed:.1f}с | "
                    f"{eligible} подходят (≥${min_cap/1e6:.0f}M)"
                )
            else:
                logger.warning("⚠️  Маркеткапы не загружены!")

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int) -> list:
        url = f"{self._base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "250",
            "page": str(page),
            "sparkline": "false",
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    def get_market_cap(self, symbol: str) -> Optional[float]:
        return self._cache.get(symbol.upper())

    def get_eligible_symbols(self) -> Set[str]:
        """Символы которые проходят фильтр по MCap"""
        min_cap = config.MIN_MARKET_CAP
        max_cap = config.MAX_MARKET_CAP
        result = set()
        for sym, cap in self._cache.items():
            if cap >= min_cap:
                if max_cap <= 0 or cap <= max_cap:
                    result.add(sym)
        return result

    def format_mcap(self, mcap: Optional[float]) -> str:
        if mcap is None:
            return "N/A"
        if mcap >= 1e9:
            return f"${mcap / 1e9:.1f}B"
        if mcap >= 1e6:
            return f"${mcap / 1e6:.1f}M"
        if mcap >= 1e3:
            return f"${mcap / 1e3:.0f}K"
        return f"${mcap:.0f}"

    def get_stats(self) -> Dict:
        eligible = len(self.get_eligible_symbols())
        return {
            "cached_coins": len(self._cache),
            "eligible": eligible,
            "cache_age_sec": int(time.time() - self._cache_time) if self._cache_time else -1,
        }
