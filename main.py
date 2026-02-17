"""
OI Scanner Bot — Таблетка от бедности 💊
Параллельное сканирование с мини-дашбордом

Запуск: python main.py
Дашборд: http://your-server:8080
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
        self.tracker = SignalTracker(max_signals=10)
        self.dashboard = Dashboard(self.tracker)
        self._running = False
        self._cycle = 0
        self._total_signals = 0

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
        logger.info(f"⚙️  OI/MCap ≥ {config.OI_MCAP_RATIO}% | OI ≥ ${config.MIN_OI_USD/1e3:.0f}K | Funding ≤ {config.MAX_FUNDING_RATE}%")
        logger.info(f"⚙️  Спред ≤ ±{config.MAX_PRICE_SPREAD}% | MCap: {cap_str} | Vol ≥ ${config.MIN_VOLUME_24H/1e3:.0f}K")
        logger.info(f"⚙️  Score ≥ {config.MIN_SIGNAL_SCORE} | Интервал: {config.SCAN_INTERVAL}с")
        logger.info("")
        logger.info("🔍 Начинаю сканирование...\n")

        # 6. Main loop
        self._running = True
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

        # Собираем ВСЕ текущие цены в единый словарь для трекера
        all_prices = {}

        async def scan_one(eid: str):
            try:
                all_data = await self.exchange_mgr.fetch_all_data(eid, target_bases=eligible_symbols)
                if not all_data:
                    return []

                # Запоминаем цены для трекера
                for sym, d in all_data.items():
                    price = d.get("futures_price")
                    if price and price > 0:
                        all_prices[sym] = price
                        all_prices[d["base"]] = price

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

        # Отправляем + трекинг
        for signal in all_signals:
            await self.telegram.send_signal(signal)
            self.tracker.add_signal(signal)
            self._total_signals += 1

        # Обновляем текущие цены для всех отслеживаемых сигналов
        if all_prices:
            self.tracker.update_prices(all_prices)

        # Cleanup
        if self._cycle % 10 == 0:
            self.scanner.cleanup_cooldowns()

        elapsed = time.time() - t0

        diag = self.scanner.get_diagnostics()
        logger.info(f"   📋 Фильтры: {diag}")

        # P&L сводка трекера
        summary = self.tracker.get_summary()
        if summary["active_count"] > 0:
            logger.info(
                f"   📊 Дашборд: {summary['active_count']} актив | "
                f"Avg: {summary['avg_pnl']:+.3f}% | "
                f"Best: {summary['best_pnl']:+.3f}% | "
                f"Win: {summary['profitable']} Loss: {summary['losing']}"
            )

        logger.info(
            f"   ✅ Цикл #{self._cycle} за {elapsed:.1f}с | "
            f"Сигналов: {len(all_signals)} (всего: {self._total_signals})"
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
