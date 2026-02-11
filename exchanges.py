"""
exchanges.py — Высокопроизводительный модуль работы с биржами через CCXT
Batch-загрузка OI, funding, тикеров за ОДИН запрос на биржу
"""
import asyncio
import ccxt.async_support as ccxt
import logging
import time
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger("oi_scanner")


class ExchangeManager:
    """
    Управление подключениями к биржам и **batch**-сбор данных.

    Оптимизации:
    - fetch_tickers() → ВСЕ тикеры одним запросом
    - fetch_funding_rates() → ВСЕ фандинги одним запросом
    - fetch_open_interest() → батч где биржа поддерживает
    - Семафоры для rate-limit контроля
    - Кэш рынков, пересканирование раз в 10 мин
    """

    EXCHANGE_NAMES = {
        "binance": "Binance",
        "bybit": "Bybit",
        "okx": "OKX",
        "gateio": "Gate.io",
        "mexc": "MEXC",
        "kucoin": "KuCoin",
        "bingx": "BingX",
        "bitget": "Bitget",
    }

    # Максимум параллельных OI-запросов на биржу
    OI_CONCURRENCY = 5

    def __init__(self, exchange_ids: List[str]):
        self.exchange_ids = exchange_ids
        self.exchanges: Dict[str, Any] = {}
        self._init_errors: Dict[str, str] = {}
        # Кэшированные данные
        self._futures_symbols_cache: Dict[str, List[Dict]] = {}
        self._ticker_cache: Dict[str, Dict[str, Dict]] = {}   # eid → {symbol: ticker}
        self._funding_cache: Dict[str, Dict[str, float]] = {} # eid → {symbol: rate%}
        self._oi_cache: Dict[str, Dict[str, float]] = {}      # eid → {symbol: oi_usd}
        self._spot_ticker_cache: Dict[str, Dict[str, float]] = {}  # eid → {BASE: price}
        # Семафоры
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    async def initialize(self):
        """Инициализация подключений ко всем биржам параллельно"""
        tasks = [self._init_exchange(eid) for eid in self.exchange_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _init_exchange(self, eid: str):
        """Подключиться к одной бирже"""
        try:
            exchange_class = getattr(ccxt, eid, None)
            if not exchange_class:
                self._init_errors[eid] = "not in ccxt"
                logger.warning(f"⚠️  Биржа {eid} не найдена в CCXT")
                return

            exchange = exchange_class({
                "enableRateLimit": True,
                "timeout": 15000,
                "options": {
                    "defaultType": "swap",
                    "adjustForTimeDifference": True,
                },
            })

            await exchange.load_markets()
            self.exchanges[eid] = exchange
            self._semaphores[eid] = asyncio.Semaphore(self.OI_CONCURRENCY)

            # Кэшируем фьючерсные символы
            self._cache_futures_symbols(eid)

            futures_count = len(self._futures_symbols_cache.get(eid, []))
            logger.info(f"✅ {self.EXCHANGE_NAMES.get(eid, eid)}: {futures_count} USDT-перпов")

        except Exception as e:
            self._init_errors[eid] = str(e)[:80]
            logger.error(f"❌ {self.EXCHANGE_NAMES.get(eid, eid)}: {e}")

    def _cache_futures_symbols(self, eid: str):
        """Кэшировать список USDT-perp символов для биржи"""
        exchange = self.exchanges.get(eid)
        if not exchange:
            return

        pairs = []
        for symbol, market in exchange.markets.items():
            is_swap = market.get("swap", False)
            is_linear = market.get("linear", False) or market.get("settle") == "USDT"
            is_usdt = "USDT" in symbol
            is_active = market.get("active", True)

            if is_swap and is_usdt and is_active and (is_linear or market.get("quote") == "USDT"):
                pairs.append({
                    "symbol": symbol,
                    "base": market.get("base", symbol.split("/")[0] if "/" in symbol else symbol),
                    "exchange": eid,
                })

        self._futures_symbols_cache[eid] = pairs

    async def close(self):
        """Закрыть все сессии параллельно"""
        tasks = []
        for exchange in self.exchanges.values():
            tasks.append(self._safe_close(exchange))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def _safe_close(exchange):
        try:
            await exchange.close()
        except Exception:
            pass

    # ═══════════════════════════════════════════
    # BATCH-загрузка данных (весь ключ к скорости)
    # ═══════════════════════════════════════════

    async def fetch_all_data(self, eid: str, target_bases: set = None) -> Dict[str, Dict]:
        """
        Загрузить ВСЕ данные по бирже batch-запросами.
        
        Делает всего 3-4 HTTP-запроса вместо N*4 на каждый символ:
        1. fetch_tickers()      → все цены разом
        2. fetch_funding_rates() → все фандинги разом
        3. fetch_open_interest() → батч или по одному с семафором
        4. fetch_tickers(spot)   → спот-цены (опционально)
        
        Args:
            eid: ID биржи
            target_bases: если задано, загружаем OI только для этих монет (оптимизация)
            
        Returns:
            {symbol: {oi_usd, funding_rate, futures_price, spot_price, base, exchange, ...}}
        """
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}

        name = self.EXCHANGE_NAMES.get(eid, eid)
        start = time.time()

        # 1. Batch: все фьючерсные тикеры
        tickers = await self._fetch_all_tickers(eid)
        
        # 2. Batch: все funding rates
        funding_rates = await self._fetch_all_funding_rates(eid)

        # 3. Определяем для каких символов нужен OI
        futures_pairs = self._futures_symbols_cache.get(eid, [])
        if target_bases:
            target_pairs = [p for p in futures_pairs if p["base"] in target_bases]
        else:
            target_pairs = futures_pairs

        # 4. OI — с семафором (может быть по-одному)
        oi_data = await self._fetch_oi_batch(eid, target_pairs)

        # 5. Спотовые цены — пытаемся batch
        spot_prices = await self._fetch_spot_prices(eid, target_bases or set())

        # 6. Собираем результат
        result = {}
        for pair in target_pairs:
            symbol = pair["symbol"]
            base = pair["base"]

            futures_price = tickers.get(symbol)
            funding_rate = funding_rates.get(symbol)
            oi_usd = oi_data.get(symbol)

            # Пропускаем если нет ключевых данных
            if futures_price is None or funding_rate is None or oi_usd is None:
                continue
            if futures_price <= 0 or oi_usd <= 0:
                continue

            result[symbol] = {
                "exchange": eid,
                "exchange_name": name,
                "symbol": symbol,
                "base": base,
                "oi_usd": oi_usd,
                "funding_rate": funding_rate,
                "futures_price": futures_price,
                "spot_price": spot_prices.get(base),
            }

        elapsed = time.time() - start
        logger.info(f"   📡 {name}: {len(result)} монет с данными за {elapsed:.1f}с")

        return result

    async def _fetch_all_tickers(self, eid: str) -> Dict[str, float]:
        """Batch: все фьючерсные тикеры → {symbol: last_price}"""
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}

        try:
            raw = await exchange.fetch_tickers()
            result = {}
            for symbol, ticker in raw.items():
                last = ticker.get("last")
                if last is not None and float(last) > 0:
                    result[symbol] = float(last)
            return result
        except Exception as e:
            logger.warning(f"fetch_tickers {eid}: {e}")
            return {}

    async def _fetch_all_funding_rates(self, eid: str) -> Dict[str, float]:
        """Batch: все funding rates → {symbol: rate%}"""
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}

        try:
            if hasattr(exchange, "fetch_funding_rates"):
                raw = await exchange.fetch_funding_rates()
                result = {}
                for symbol, fr in raw.items():
                    rate = fr.get("fundingRate")
                    if rate is not None:
                        result[symbol] = float(rate) * 100  # → проценты
                return result

            # Фоллбэк: одиночные запросы с семафором
            return await self._fetch_funding_rates_individually(eid)

        except Exception as e:
            logger.warning(f"fetch_funding_rates {eid}: {e}")
            return await self._fetch_funding_rates_individually(eid)

    async def _fetch_funding_rates_individually(self, eid: str) -> Dict[str, float]:
        """Фоллбэк: funding rates по одному (с семафором)"""
        exchange = self.exchanges.get(eid)
        if not exchange or not hasattr(exchange, "fetch_funding_rate"):
            return {}

        pairs = self._futures_symbols_cache.get(eid, [])
        sem = self._semaphores.get(eid, asyncio.Semaphore(3))

        async def fetch_one(symbol: str) -> Tuple[str, Optional[float]]:
            async with sem:
                try:
                    fr = await exchange.fetch_funding_rate(symbol)
                    rate = fr.get("fundingRate")
                    if rate is not None:
                        return (symbol, float(rate) * 100)
                except Exception:
                    pass
                return (symbol, None)

        tasks = [fetch_one(p["symbol"]) for p in pairs[:100]]  # Лимит
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out = {}
        for r in results:
            if isinstance(r, tuple) and r[1] is not None:
                out[r[0]] = r[1]
        return out

    async def _fetch_oi_batch(self, eid: str, pairs: List[Dict]) -> Dict[str, float]:
        """
        OI: batch или параллельные одиночные запросы с семафором.
        Returns: {symbol: oi_in_usd}
        """
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}

        # Пробуем batch fetch_open_interest для всех пар
        # Большинство бирж поддерживают только по одному
        sem = self._semaphores.get(eid, asyncio.Semaphore(self.OI_CONCURRENCY))
        tickers = await self._fetch_all_tickers(eid)  # Уже кэшировано в вызывающем коде

        async def fetch_one(pair: Dict) -> Tuple[str, Optional[float]]:
            symbol = pair["symbol"]
            async with sem:
                try:
                    if hasattr(exchange, "fetch_open_interest"):
                        oi_data = await exchange.fetch_open_interest(symbol)
                        if oi_data:
                            # Предпочитаем openInterestValue (USD)
                            oi_val = oi_data.get("openInterestValue")
                            if oi_val and float(oi_val) > 0:
                                return (symbol, float(oi_val))

                            # Фоллбэк: amount * price
                            oi_amount = oi_data.get("openInterestAmount")
                            if oi_amount:
                                price = tickers.get(symbol, 0)
                                if price > 0:
                                    return (symbol, float(oi_amount) * price)

                except ccxt.NotSupported:
                    pass
                except ccxt.RateLimitExceeded:
                    await asyncio.sleep(2)
                except Exception:
                    pass
                return (symbol, None)

        tasks = [fetch_one(p) for p in pairs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out = {}
        for r in results:
            if isinstance(r, tuple) and r[1] is not None:
                out[r[0]] = r[1]
        return out

    async def _fetch_spot_prices(self, eid: str, target_bases: set) -> Dict[str, float]:
        """
        Получить спотовые цены для целевых монет.
        Пробуем переключиться на спот и сделать batch.
        Returns: {BASE: price}
        """
        exchange = self.exchanges.get(eid)
        if not exchange or not target_bases:
            return {}

        try:
            # Попробуем создать спотовый инстанс
            eid_class = getattr(ccxt, eid, None)
            if not eid_class:
                return {}

            spot_exchange = eid_class({
                "enableRateLimit": True,
                "timeout": 10000,
                "options": {"defaultType": "spot"},
            })

            try:
                raw = await spot_exchange.fetch_tickers()
                result = {}
                for symbol, ticker in raw.items():
                    # Ищем BASE/USDT
                    parts = symbol.split("/")
                    if len(parts) >= 2 and parts[1] == "USDT" and ":" not in symbol:
                        base = parts[0]
                        if base in target_bases:
                            last = ticker.get("last")
                            if last and float(last) > 0:
                                result[base] = float(last)
                return result
            finally:
                await spot_exchange.close()

        except Exception as e:
            logger.debug(f"Спот-цены {eid}: {e}")
            return {}

    # ═══════════════════════════════════════════
    # Вспомогательные
    # ═══════════════════════════════════════════

    def get_futures_symbols(self, eid: str) -> List[Dict]:
        return self._futures_symbols_cache.get(eid, [])

    def get_connected_exchanges(self) -> List[str]:
        return list(self.exchanges.keys())

    def get_status(self) -> Dict:
        return {
            "connected": list(self.exchanges.keys()),
            "failed": self._init_errors,
            "total_connected": len(self.exchanges),
            "total_failed": len(self._init_errors),
        }
