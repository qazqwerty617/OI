"""
marketcap.py — Провайдер маркеткапов через CoinGecko
Retry с backoff, User-Agent, кэш
"""
import asyncio
import time
import logging
from typing import Dict, Optional, Set

import aiohttp

import config

logger = logging.getLogger("oi_scanner")


class MarketCapProvider:
    COINGECKO_BASE = "https://api.coingecko.com/api/v3"
    COINGECKO_PRO_BASE = "https://pro-api.coingecko.com/api/v3"

    MAX_RETRIES = 3
    RETRY_DELAY = 15  # секунд между retry при 429

    def __init__(self, api_key: str = "", cache_ttl: int = 300):
        self.api_key = api_key
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, float] = {}
        self._cache_time: float = 0
        self._lock = asyncio.Lock()

    @property
    def _base_url(self) -> str:
        return self.COINGECKO_PRO_BASE if self.api_key else self.COINGECKO_BASE

    def _headers(self) -> Dict:
        h = {
            "Accept": "application/json",
            "User-Agent": "OI-Scanner-Bot/1.0",
        }
        if self.api_key:
            h["x-cg-pro-api-key"] = self.api_key
        return h

    @property
    def is_stale(self) -> bool:
        return not self._cache or (time.time() - self._cache_time) >= self.cache_ttl

    async def refresh_cache(self):
        async with self._lock:
            if not self.is_stale:
                return

            logger.info("🔄 Загрузка маркеткапов с CoinGecko...")
            all_coins: Dict[str, float] = {}
            start = time.time()

            try:
                timeout = aiohttp.ClientTimeout(total=60)
                async with aiohttp.ClientSession(
                    timeout=timeout,
                    headers=self._headers(),
                ) as session:
                    for page in range(1, 11):
                        data = await self._fetch_page_with_retry(session, page)
                        if data is None:
                            break

                        if not data:
                            break

                        for coin in data:
                            sym = coin.get("symbol", "").upper()
                            mcap = coin.get("market_cap")
                            if sym and mcap and mcap > 0:
                                existing = all_coins.get(sym, 0)
                                if mcap > existing:
                                    all_coins[sym] = mcap

                        # Пауза между страницами
                        if not self.api_key:
                            await asyncio.sleep(2.0)
                        else:
                            await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"CoinGecko фатал: {e}")

            if all_coins:
                self._cache = all_coins
                self._cache_time = time.time()
                elapsed = time.time() - start

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

    async def _fetch_page_with_retry(self, session: aiohttp.ClientSession,
                                      page: int) -> Optional[list]:
        """Загрузить страницу с retry при 429"""
        url = f"{self._base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "250",
            "page": str(page),
            "sparkline": "false",
        }

        for attempt in range(self.MAX_RETRIES):
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        # Rate limit — ждём и пробуем снова
                        wait = self.RETRY_DELAY * (attempt + 1)
                        logger.warning(
                            f"⚠️  CoinGecko 429 (стр. {page}), "
                            f"retry {attempt + 1}/{self.MAX_RETRIES} через {wait}с..."
                        )
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()
                    return await resp.json()

            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    wait = self.RETRY_DELAY * (attempt + 1)
                    logger.warning(
                        f"⚠️  CoinGecko 429 (стр. {page}), "
                        f"retry {attempt + 1}/{self.MAX_RETRIES} через {wait}с..."
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.warning(f"CoinGecko HTTP {e.status} стр. {page}")
                return None
            except Exception as e:
                logger.warning(f"CoinGecko ошибка стр. {page}: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(5)
                    continue
                return None

        logger.warning(f"⚠️  CoinGecko стр. {page}: все retry исчерпаны")
        return None

    def get_market_cap(self, symbol: str) -> Optional[float]:
        return self._cache.get(symbol.upper())

    def get_eligible_symbols(self) -> Set[str]:
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
