"""
marketcap.py — Высокопроизводительный провайдер маркеткапов
Async загрузка, умный кэш, маппинг символов
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
    
    Оптимизации:
    - Async HTTP через aiohttp (не блокирует event loop)
    - Батч-загрузка всех монет за 4-5 запросов
    - TTL-кэш с фоновым обновлением
    - Быстрый lookup через dict
    """

    COINGECKO_BASE = "https://api.coingecko.com/api/v3"
    COINGECKO_PRO_BASE = "https://pro-api.coingecko.com/api/v3"

    def __init__(self, api_key: str = "", cache_ttl: int = 300):
        self.api_key = api_key
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, float] = {}          # SYMBOL → mcap_usd
        self._cache_time: float = 0
        self._loading = False
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
        """
        Загрузить / обновить маркеткапы.
        Безопасно вызывать многократно — повторные вызовы ждут завершения первого.
        """
        async with self._lock:
            if not self.is_stale:
                return  # Другой корутин уже обновил

            self._loading = True
            logger.info("🔄 Загрузка маркеткапов с CoinGecko...")

            all_coins: Dict[str, float] = {}
            start = time.time()

            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers=self._headers(),
                ) as session:
                    # Грузим страницы параллельно (но с задержкой чтобы не rate-limit)
                    for page in range(1, 8):  # До ~1750 монет
                        try:
                            data = await self._fetch_page(session, page)
                            if not data:
                                break

                            for coin in data:
                                sym = coin.get("symbol", "").upper()
                                mcap = coin.get("market_cap")
                                if sym and mcap and mcap > 0:
                                    # При дублях берём БОЛЬШУЮ капу (основной токен)
                                    existing = all_coins.get(sym, 0)
                                    if mcap > existing:
                                        all_coins[sym] = mcap

                            # Задержка между страницами
                            if not self.api_key:
                                await asyncio.sleep(1.2)
                            else:
                                await asyncio.sleep(0.3)

                        except aiohttp.ClientResponseError as e:
                            if e.status == 429:
                                logger.warning("⚠️  CoinGecko rate-limit, остановка загрузки")
                                break
                            logger.warning(f"CoinGecko HTTP {e.status} стр. {page}")
                            break
                        except Exception as e:
                            logger.warning(f"CoinGecko ошибка стр. {page}: {e}")
                            break

            except Exception as e:
                logger.error(f"CoinGecko критическая ошибка: {e}")

            if all_coins:
                self._cache = all_coins
                self._cache_time = time.time()
                elapsed = time.time() - start
                low_cap = sum(1 for v in all_coins.values() if v <= config.MAX_MARKET_CAP)
                logger.info(
                    f"📊 Маркеткапы: {len(all_coins)} монет за {elapsed:.1f}с | "
                    f"{low_cap} лоукапов (≤${config.MAX_MARKET_CAP/1e6:.1f}M)"
                )
            else:
                logger.warning("⚠️  Не удалось загрузить маркеткапы")

            self._loading = False

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int) -> list:
        """Загрузить одну страницу CoinGecko markets"""
        url = f"{self._base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "250",
            "page": str(page),
            "sparkline": "false",
            "locale": "en",
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    def get_market_cap(self, symbol: str) -> Optional[float]:
        """Получить маркеткап монеты (из кэша)"""
        return self._cache.get(symbol.upper())

    def get_low_cap_symbols(self, max_cap: float = None) -> Set[str]:
        """Быстро получить SET всех лоукап-символов"""
        if max_cap is None:
            max_cap = config.MAX_MARKET_CAP
        return {sym for sym, cap in self._cache.items() if cap <= max_cap}

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
        return {
            "cached_coins": len(self._cache),
            "cache_age_sec": int(time.time() - self._cache_time) if self._cache_time else -1,
            "cache_ttl": self.cache_ttl,
            "low_caps": sum(1 for v in self._cache.values() if v <= config.MAX_MARKET_CAP),
        }
