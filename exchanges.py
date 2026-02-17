"""
exchanges.py — Работа с биржами через CCXT
Robust: fallbacks для каждой биржи, optional funding, verbose logging
"""
import asyncio
import ccxt.async_support as ccxt
import logging
import time
from typing import Dict, List, Optional, Set, Any, Tuple

logger = logging.getLogger("oi_scanner")


class ExchangeManager:
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

    OI_CONCURRENCY = 8

    def __init__(self, exchange_ids: List[str]):
        self.exchange_ids = exchange_ids
        self.exchanges: Dict[str, Any] = {}
        self._init_errors: Dict[str, str] = {}
        self._futures_cache: Dict[str, List[Dict]] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    async def initialize(self):
        tasks = [self._init_exchange(eid) for eid in self.exchange_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _init_exchange(self, eid: str):
        try:
            exchange_class = getattr(ccxt, eid, None)
            if not exchange_class:
                self._init_errors[eid] = "not in ccxt"
                return

            exchange = exchange_class({
                "enableRateLimit": True,
                "timeout": 20000,
                "options": {"defaultType": "swap"},
            })

            await exchange.load_markets()
            self.exchanges[eid] = exchange
            self._semaphores[eid] = asyncio.Semaphore(self.OI_CONCURRENCY)
            self._cache_futures(eid)

            count = len(self._futures_cache.get(eid, []))
            logger.info(f"✅ {self.EXCHANGE_NAMES.get(eid, eid)}: {count} USDT-перпов")

        except Exception as e:
            self._init_errors[eid] = str(e)[:80]
            logger.error(f"❌ {self.EXCHANGE_NAMES.get(eid, eid)}: {e}")

    def _cache_futures(self, eid: str):
        exchange = self.exchanges.get(eid)
        if not exchange:
            return

        pairs = []
        for symbol, market in exchange.markets.items():
            try:
                is_swap = market.get("swap", False) or market.get("future", False)
                is_active = market.get("active", True)
                
                # Проверяем что USDT-маржинальный
                settle = str(market.get("settle", "")).upper()
                quote = str(market.get("quote", "")).upper()
                has_usdt = settle == "USDT" or quote == "USDT" or ":USDT" in symbol

                if is_swap and is_active and has_usdt:
                    base = market.get("base", "")
                    if not base and "/" in symbol:
                        base = symbol.split("/")[0]
                    if base:
                        pairs.append({
                            "symbol": symbol,
                            "base": base,
                            "exchange": eid,
                        })
            except Exception:
                continue

        self._futures_cache[eid] = pairs

    async def close(self):
        for exchange in self.exchanges.values():
            try:
                await exchange.close()
            except Exception:
                pass

    async def fetch_all_data(self, eid: str, target_bases: Set[str] = None) -> Dict[str, Dict]:
        """
        Собрать данные по бирже. Funding — ОПЦИОНАЛЬНЫЙ.
        Если нет funding — монета всё равно включается с funding_rate=None.
        """
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}

        name = self.EXCHANGE_NAMES.get(eid, eid)
        start = time.time()

        # 1. Тикеры + объёмы (batch)
        tickers, volumes = await self._fetch_tickers_safe(eid)

        # 2. Funding rates (batch/individual, с fallback)
        funding = await self._fetch_funding_safe(eid)

        # 3. Целевые пары
        all_pairs = self._futures_cache.get(eid, [])
        if target_bases:
            pairs = [p for p in all_pairs if p["base"].upper() in target_bases]
        else:
            pairs = all_pairs

        if not pairs:
            logger.debug(f"   {name}: 0 целевых пар")
            return {}

        # 4. OI (параллельно с семафором)
        oi_data = await self._fetch_oi_parallel(eid, pairs, tickers)

        # 5. Спотовые цены
        spot_bases = {p["base"] for p in pairs}
        spot_prices = await self._fetch_spot_safe(eid, spot_bases)

        # 6. Собираем результат
        result = {}
        matched = 0
        no_price = 0
        no_oi = 0

        for pair in pairs:
            symbol = pair["symbol"]
            base = pair["base"]

            # Цена: из тикеров
            price = tickers.get(symbol)
            if not price or price <= 0:
                no_price += 1
                continue

            # OI: обязателен
            oi = oi_data.get(symbol)
            if not oi or oi <= 0:
                no_oi += 1
                continue

            # Funding: ОПЦИОНАЛЬНЫЙ!
            fr = funding.get(symbol)  # может быть None

            # Volume 24h
            vol = volumes.get(symbol, 0)

            matched += 1
            result[symbol] = {
                "exchange": eid,
                "exchange_name": name,
                "symbol": symbol,
                "base": base,
                "oi_usd": oi,
                "funding_rate": fr,
                "futures_price": price,
                "spot_price": spot_prices.get(base),
                "volume_24h": vol,
            }

        elapsed = time.time() - start
        logger.info(
            f"   📡 {name}: {matched} монет за {elapsed:.1f}с "
            f"(из {len(pairs)} пар | тикеры:{len(tickers)} фанд:{len(funding)} oi:{len(oi_data)} "
            f"нет_цены:{no_price} нет_oi:{no_oi})"
        )

        return result

    # ═══════════════════════════════════════════
    # ТИКЕРЫ
    # ═══════════════════════════════════════════

    async def _fetch_tickers_safe(self, eid: str) -> tuple:
        """Batch тикеры: ({symbol: last_price}, {symbol: volume_24h_usd})"""
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}, {}
        try:
            raw = await exchange.fetch_tickers()
            prices = {}
            volumes = {}
            for sym, t in raw.items():
                last = t.get("last")
                if last is not None:
                    try:
                        val = float(last)
                        if val > 0:
                            prices[sym] = val
                    except (ValueError, TypeError):
                        pass
                # 24h volume в USDT (quoteVolume)
                qv = t.get("quoteVolume")
                if qv is not None:
                    try:
                        volumes[sym] = float(qv)
                    except (ValueError, TypeError):
                        pass
            return prices, volumes
        except Exception as e:
            logger.warning(f"fetch_tickers {eid}: {e}")
            return {}, {}

    # ═══════════════════════════════════════════
    # FUNDING RATES (с fallback)
    # ═══════════════════════════════════════════

    async def _fetch_funding_safe(self, eid: str) -> Dict[str, float]:
        """
        Funding rates с fallback: batch → individual.
        Returns: {symbol: rate_in_percent}
        """
        exchange = self.exchanges.get(eid)
        if not exchange:
            return {}

        # Попытка 1: batch
        result = await self._try_batch_funding(eid)
        if result:
            return result

        # Попытка 2: individual (с ограничением)
        return await self._try_individual_funding(eid)

    async def _try_batch_funding(self, eid: str) -> Dict[str, float]:
        exchange = self.exchanges.get(eid)
        try:
            if not hasattr(exchange, "fetch_funding_rates"):
                return {}
            raw = await exchange.fetch_funding_rates()
            out = {}
            for sym, fr in raw.items():
                rate = fr.get("fundingRate")
                if rate is not None:
                    try:
                        out[sym] = float(rate) * 100
                    except (ValueError, TypeError):
                        pass
            if out:
                logger.debug(f"   {eid}: batch funding OK ({len(out)} пар)")
            return out
        except Exception as e:
            logger.debug(f"   {eid}: batch funding failed: {e}")
            return {}

    async def _try_individual_funding(self, eid: str) -> Dict[str, float]:
        """Индивидуальные запросы funding rate"""
        exchange = self.exchanges.get(eid)
        if not exchange or not hasattr(exchange, "fetch_funding_rate"):
            logger.debug(f"   {eid}: нет fetch_funding_rate, funding пропущен")
            return {}

        pairs = self._futures_cache.get(eid, [])
        if not pairs:
            return {}

        sem = self._semaphores.get(eid, asyncio.Semaphore(5))
        out = {}
        errors = 0

        # Ограничиваем до 200 символов
        symbols = [p["symbol"] for p in pairs[:200]]

        async def fetch_one(symbol: str):
            nonlocal errors
            async with sem:
                try:
                    fr = await exchange.fetch_funding_rate(symbol)
                    rate = fr.get("fundingRate") if fr else None
                    if rate is not None:
                        out[symbol] = float(rate) * 100
                except Exception:
                    errors += 1

        # Батчаме по 20 штук с паузой
        for i in range(0, len(symbols), 20):
            batch = symbols[i:i + 20]
            await asyncio.gather(*[fetch_one(s) for s in batch], return_exceptions=True)
            if i + 20 < len(symbols):
                await asyncio.sleep(0.5)

        logger.info(f"   {eid}: individual funding: {len(out)} OK, {errors} ошибок")
        return out

    # ═══════════════════════════════════════════
    # OPEN INTEREST
    # ═══════════════════════════════════════════

    async def _fetch_oi_parallel(self, eid: str, pairs: List[Dict],
                                  tickers: Dict[str, float]) -> Dict[str, float]:
        """OI параллельно с семафором"""
        exchange = self.exchanges.get(eid)
        if not exchange or not hasattr(exchange, "fetch_open_interest"):
            return {}

        sem = self._semaphores.get(eid, asyncio.Semaphore(self.OI_CONCURRENCY))
        out = {}
        errors = 0

        async def fetch_one(pair: Dict):
            nonlocal errors
            symbol = pair["symbol"]
            async with sem:
                try:
                    oi = await exchange.fetch_open_interest(symbol)
                    if not oi:
                        return

                    # USD value
                    val = oi.get("openInterestValue")
                    if val:
                        v = float(val)
                        if v > 0:
                            out[symbol] = v
                            return

                    # Fallback: amount * price
                    amt = oi.get("openInterestAmount")
                    if amt:
                        price = tickers.get(symbol, 0)
                        if price > 0:
                            out[symbol] = float(amt) * price

                except ccxt.NotSupported:
                    pass
                except ccxt.RateLimitExceeded:
                    await asyncio.sleep(2)
                except Exception:
                    errors += 1

        # Батчами по 20
        for i in range(0, len(pairs), 20):
            batch = pairs[i:i + 20]
            await asyncio.gather(*[fetch_one(p) for p in batch], return_exceptions=True)
            if i + 20 < len(pairs):
                await asyncio.sleep(0.3)

        if errors > 10:
            logger.debug(f"   {eid}: OI {errors} ошибок")

        return out

    # ═══════════════════════════════════════════
    # СПОТ ЦЕНЫ
    # ═══════════════════════════════════════════

    async def _fetch_spot_safe(self, eid: str, bases: Set[str]) -> Dict[str, float]:
        """Спотовые цены batch"""
        if not bases:
            return {}
        try:
            exchange_class = getattr(ccxt, eid, None)
            if not exchange_class:
                return {}

            spot = exchange_class({
                "enableRateLimit": True,
                "timeout": 15000,
                "options": {"defaultType": "spot"},
            })

            try:
                raw = await spot.fetch_tickers()
                out = {}
                for sym, t in raw.items():
                    if ":" in sym:  # Пропускаем деривативы
                        continue
                    parts = sym.split("/")
                    if len(parts) >= 2 and parts[1] == "USDT":
                        base = parts[0]
                        if base in bases:
                            last = t.get("last")
                            if last and float(last) > 0:
                                out[base] = float(last)
                return out
            finally:
                await spot.close()

        except Exception as e:
            logger.debug(f"Спот {eid}: {e}")
            return {}

    # ═══════════════════════════════════════════
    # Utils
    # ═══════════════════════════════════════════

    def get_futures_symbols(self, eid: str) -> List[Dict]:
        return self._futures_cache.get(eid, [])

    def get_connected_exchanges(self) -> List[str]:
        return list(self.exchanges.keys())

    def get_status(self) -> Dict:
        return {
            "connected": list(self.exchanges.keys()),
            "failed": self._init_errors,
            "total_connected": len(self.exchanges),
            "total_failed": len(self._init_errors),
        }
