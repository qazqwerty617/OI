"""
OI Scanner Bot — Таблетка от бедности 💊
Параллельное сканирование + мини-дашборд с TP/SL

Запуск: python main.py
Дашборд: http://your-server:PORT
"""
import asyncio
import logging
import sys
import time

import config
from exchanges import ExchangeManager
from marketcap import MarketCapProvider
from scanner import StrategyScanner
from telegram_bot import TelegramNotifier
from tracker import SignalTracker
from dashboard import Dashboard

# ═══════════════ Logging ═══════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("oi_scanner")
for noisy in ("ccxt", "httpx", "httpcore", "telegram", "aiohttp", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


class OIScannerBot:
    def __init__(self):
        exchange_ids = []
        for eid in config.EXCHANGES:
            exchange_ids.append("gateio" if eid == "gate" else eid)

        self.exchange_mgr = ExchangeManager(exchange_ids)
        self.mcap_provider = MarketCapProvider(
            api_key=config.COINGECKO_API_KEY,
            cache_ttl=config.MCAP_CACHE_TTL,
        )
        self.scanner = StrategyScanner()
        self.telegram = TelegramNotifier(
            bot_token=config.TELEGRAM_BOT_TOKEN,
            chat_id=config.TELEGRAM_CHAT_ID,
            topic_id=config.TELEGRAM_TOPIC_ID,
        )
        self.tracker = SignalTracker()
        self.dashboard = Dashboard(self.tracker, port=config.DASHBOARD_PORT)
        self._running = False
        self._cycle = 0
        self._total_signals = 0
        self._price_cache: dict = {}

    async def start(self):
        logger.info("═" * 52)
        logger.info("  💊 OI Scanner Bot — Таблетка от бедности")
        logger.info("═" * 52)

        if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
            logger.error("❌ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не задан!")
            return

        # 1. Биржи
        logger.info("📡 Подключение к биржам...")
        await self.exchange_mgr.initialize()

        connected = self.exchange_mgr.get_connected_exchanges()
        if not connected:
            logger.error("❌ Ни одна биржа не подключена!")
            return

        total_pairs = sum(len(self.exchange_mgr.get_futures_symbols(e)) for e in connected)

        # 2. Маркеткапы
        logger.info("💎 Загрузка маркеткапов...")
        await self.mcap_provider.refresh_cache()

        eligible = self.mcap_provider.get_eligible_symbols()
        logger.info(f"🎯 Монет с MCap ≥ ${config.MIN_MARKET_CAP/1e6:.0f}M: {len(eligible)}")

        # 3. Telegram
        await self.telegram.initialize()
        self.telegram.set_refs(self.scanner, self.exchange_mgr, self.mcap_provider)
        await self.telegram.send_startup_message(len(connected), total_pairs)

        # 4. Dashboard
        await self.dashboard.start()

        # 5. Config summary
        logger.info("")
        cap_str = f"${config.MIN_MARKET_CAP/1e6:.0f}M+"
        if config.MAX_MARKET_CAP > 0:
            cap_str += f" (макс ${config.MAX_MARKET_CAP/1e6:.0f}M)"
        logger.info(f"⚙️  OI/MCap ≥ {config.OI_MCAP_RATIO}% | OI ≥ ${config.MIN_OI_USD/1e3:.0f}K | OIРост ≥ {config.MIN_OI_GROWTH_PCT}% за {config.OI_GROWTH_WINDOW//60}мин")
        logger.info(f"⚙️  Funding ≤ {config.MAX_FUNDING_RATE}% | Спред ≤ ±{config.MAX_PRICE_SPREAD}% | MCap: {cap_str}")
        logger.info(f"⚙️  Vol ≥ ${config.MIN_VOLUME_24H/1e3:.0f}K | Score ≥ {config.MIN_SIGNAL_SCORE} | MaxPump ≤ {config.MAX_PRICE_PUMP_PCT}% | CD: {config.SIGNAL_COOLDOWN//60}мин")
        logger.info("")
        logger.info("🔍 Начинаю сканирование...\n")

        # 6. Запуск: scan loop + price update loop
        self._running = True
        await asyncio.gather(
            self._scan_loop(),
            self._price_update_loop(),
        )

    async def _scan_loop(self):
        """Основной цикл сканирования"""
        while self._running:
            try:
                await self._scan_cycle()
            except Exception as e:
                logger.error(f"❌ Ошибка цикла: {e}")
                import traceback
                traceback.print_exc()

            for _ in range(config.SCAN_INTERVAL):
                if not self._running:
                    break
                await asyncio.sleep(1)

    async def _price_update_loop(self):
        """Быстрое обновление цен для трекера (каждые 10с)"""
        while self._running:
            await asyncio.sleep(10)
            try:
                tracked = self.tracker.get_symbols_to_track()
                if not tracked:
                    continue

                # Берём цены с первой доступной биржи (binance если есть)
                for eid in self.exchange_mgr.get_connected_exchanges():
                    exchange = self.exchange_mgr.exchanges.get(eid)
                    if not exchange:
                        continue
                    try:
                        raw = await exchange.fetch_tickers()
                        prices = {}
                        for sym, t in raw.items():
                            last = t.get("last")
                            if last:
                                try:
                                    v = float(last)
                                    if v > 0:
                                        prices[sym] = v
                                        # Также по base
                                        if "/" in sym:
                                            base = sym.split("/")[0]
                                            prices[base] = v
                                except (ValueError, TypeError):
                                    pass
                        if prices:
                            self._price_cache.update(prices)
                        break  # одной биржи достаточно
                    except Exception:
                        continue

                if self._price_cache:
                    closed = self.tracker.update_prices(self._price_cache)
                    # Отправляем уведомления о закрытых сделках в Telegram
                    for c in closed:
                        await self._send_close_notification(c)

            except Exception as e:
                logger.debug(f"Price update err: {e}")

    async def _send_close_notification(self, closed):
        """Уведомление о закрытии сделки"""
        if closed.result == "WIN":
            emoji = "🟢✅"
            text = (
                f"{emoji} *ДЕМО ЛОНГ ЗАКРЫТ — WIN*\n\n"
                f"*{closed.base}* — {closed.exchange_name}\n"
                f"Вход: ${closed.entry_price:.6g}\n"
                f"Закрытие: ${closed.close_price:.6g}\n"
                f"P&L: *+{closed.pnl_pct:.2f}%* ✅\n"
                f"Время: {closed.hold_time_min:.0f} мин\n"
                f"Score: {closed.score}"
            )
        else:
            emoji = "🔴❌"
            text = (
                f"{emoji} *ДЕМО ЛОНГ ЗАКРЫТ — LOSS*\n\n"
                f"*{closed.base}* — {closed.exchange_name}\n"
                f"Вход: ${closed.entry_price:.6g}\n"
                f"Закрытие: ${closed.close_price:.6g}\n"
                f"P&L: *{closed.pnl_pct:.2f}%* ❌\n"
                f"Время: {closed.hold_time_min:.0f} мин\n"
                f"Score: {closed.score}"
            )

        # Добавляем текущий winrate
        sm = self.tracker.get_summary()
        text += f"\n\n📊 Winrate: {sm['winrate']}% ({sm['wins']}W/{sm['losses']}L)"

        await self.telegram._send_with_retry(text)

    async def _scan_cycle(self):
        self._cycle += 1
        t0 = time.time()

        logger.info(f"━━━ Цикл #{self._cycle} ━━━━━━━━━━━━━━━━━━━")

        # Refresh маркеткапов
        if self.mcap_provider.is_stale:
            await self.mcap_provider.refresh_cache()

        eligible_symbols = self.mcap_provider.get_eligible_symbols()
        if not eligible_symbols:
            logger.warning("⚠️  Нет подходящих монет в кэше маркеткапов")
            return

        self.scanner.reset_diagnostics()
        exchanges = self.exchange_mgr.get_connected_exchanges()

        async def scan_one(eid: str):
            try:
                all_data = await self.exchange_mgr.fetch_all_data(eid, target_bases=eligible_symbols)
                if not all_data:
                    return []

                # Запоминаем цены для трекера
                for sym, d in all_data.items():
                    price = d.get("futures_price")
                    if price and price > 0:
                        self._price_cache[sym] = price
                        self._price_cache[d["base"]] = price

                mcap_lookup = dict(self.mcap_provider._cache)
                return self.scanner.evaluate_batch(all_data, mcap_lookup)
            except Exception as e:
                logger.warning(f"⚠️  {eid}: {e}")
                return []

        results = await asyncio.gather(
            *[scan_one(eid) for eid in exchanges],
            return_exceptions=True,
        )

        # Собираем сигналы
        all_signals = []
        for r in results:
            if isinstance(r, list):
                all_signals.extend(r)
        all_signals.sort(key=lambda s: s.score, reverse=True)

        # ТОП-1 лучший за цикл
        top_signals = all_signals[:1]

        # Отправляем + трекинг
        for signal in top_signals:
            await self.telegram.send_signal(signal)
            self.tracker.add_signal(signal)
            self._total_signals += 1
            await asyncio.sleep(1)

        # Обновляем цены трекера
        if self._price_cache:
            closed = self.tracker.update_prices(self._price_cache)
            for c in closed:
                await self._send_close_notification(c)

        # Cleanup
        if self._cycle % 10 == 0:
            self.scanner.cleanup_cooldowns()
            self.scanner.oi_history.cleanup()

        elapsed = time.time() - t0
        diag = self.scanner.get_diagnostics()
        logger.info(f"   📋 Фильтры: {diag}")

        # Dashboard summary
        sm = self.tracker.get_summary()
        if sm["active_count"] > 0:
            logger.info(
                f"   📊 Трекер: {sm['active_count']} актив | "
                f"Avg: {sm['avg_pnl']:+.2f}% | "
                f"WR: {sm['winrate']}% ({sm['wins']}W/{sm['losses']}L)"
            )

        logger.info(
            f"   ✅ Цикл #{self._cycle} за {elapsed:.1f}с | "
            f"Сигналов: {len(top_signals)} (всего: {self._total_signals})"
        )

    async def stop(self):
        self._running = False
        await self.dashboard.stop()
        await self.telegram.shutdown()
        await self.exchange_mgr.close()
        logger.info("👋 Бот остановлен")


async def main():
    bot = OIScannerBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Выход")
