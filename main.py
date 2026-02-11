"""
OI Scanner Bot — Таблетка от бедности 💊
Главный модуль: параллельное сканирование, умная фильтрация

Стратегия на ЛОНГ:
  Перегретый OI + Отрицательный фандинг + Справедливая цена + Лоукап

Запуск: python main.py
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

# ═══════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("oi_scanner")

# Suppress noise
for noisy in ("ccxt", "httpx", "httpcore", "telegram", "aiohttp", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


class OIScannerBot:
    """
    Главный класс — оркестрирует сканирование.
    
    Оптимизации:
    - Pre-filter: сначала находим лоукапы, потом запрашиваем OI только для них
    - Параллельное сканирование бирж через asyncio.gather
    - Batch evaluate по результатам
    - Фоновое обновление маркеткапов
    """

    def __init__(self):
        # gateio — правильный ID в ccxt (не "gate")
        exchange_ids = []
        for eid in config.EXCHANGES:
            if eid == "gate":
                exchange_ids.append("gateio")
            else:
                exchange_ids.append(eid)

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
        self._running = False
        self._cycle = 0
        self._total_signals = 0

    async def start(self):
        logger.info("═" * 52)
        logger.info("  💊 OI Scanner Bot — Таблетка от бедности")
        logger.info("═" * 52)

        # Validate config
        if not config.TELEGRAM_BOT_TOKEN:
            logger.error("❌ TELEGRAM_BOT_TOKEN не задан в .env!")
            return
        if not config.TELEGRAM_CHAT_ID:
            logger.error("❌ TELEGRAM_CHAT_ID не задан в .env!")
            return

        # 1. Биржи (параллельно)
        logger.info("📡 Подключение к биржам...")
        await self.exchange_mgr.initialize()

        connected = self.exchange_mgr.get_connected_exchanges()
        if not connected:
            logger.error("❌ Ни одна биржа не подключена!")
            return

        total_pairs = sum(len(self.exchange_mgr.get_futures_symbols(e)) for e in connected)
        logger.info(f"📊 Всего фьючерсных пар: {total_pairs}")

        # 2. Маркеткапы (async)
        logger.info("💎 Загрузка маркеткапов...")
        await self.mcap_provider.refresh_cache()

        # 3. Telegram
        logger.info("📱 Запуск Telegram...")
        await self.telegram.initialize()
        self.telegram.set_refs(self.scanner, self.exchange_mgr, self.mcap_provider)
        await self.telegram.send_startup_message(len(connected), total_pairs)

        # 4. Config summary
        logger.info("")
        logger.info(f"⚙️  OI/MCap ≥ {config.OI_MCAP_RATIO}% | Funding ≤ {config.MAX_FUNDING_RATE}%")
        logger.info(f"⚙️  Спред ≤ ±{config.MAX_PRICE_SPREAD}% | MCap ≤ ${config.MAX_MARKET_CAP/1e6:.0f}M")
        logger.info(f"⚙️  Интервал: {config.SCAN_INTERVAL}с | Cooldown: {config.SIGNAL_COOLDOWN}с")
        logger.info("")
        logger.info("🔍 Начинаю сканирование...\n")

        # 5. Main loop
        self._running = True
        while self._running:
            try:
                await self._scan_cycle()
            except Exception as e:
                logger.error(f"❌ Ошибка цикла: {e}")
                import traceback
                traceback.print_exc()

            # Interruptible sleep
            for _ in range(config.SCAN_INTERVAL):
                if not self._running:
                    break
                await asyncio.sleep(1)

    async def _scan_cycle(self):
        """
        Один цикл сканирования.
        
        Порядок (оптимизирован):
        1. Обновить маркеткапы если устарели
        2. Получить множество лоукап-символов (O(1) lookup)
        3. Для каждой биржи ПАРАЛЛЕЛЬНО:
           a. Batch-загрузить тикеры + фандинги + OI
           b. Batch-оценить все монеты
        4. Отправить сигналы
        """
        self._cycle += 1
        t0 = time.time()
        cycle_signals = 0

        logger.info(f"━━━ Цикл #{self._cycle} ━━━━━━━━━━━━━━━━━━━")

        # 1. Refresh маркеткапов (если кэш протух)
        if self.mcap_provider.is_stale:
            await self.mcap_provider.refresh_cache()

        # 2. Множество лоукапов для pre-filter
        low_cap_set = self.mcap_provider.get_low_cap_symbols()
        if not low_cap_set:
            logger.warning("⚠️  Нет лоукапов в кэше, пропускаю цикл")
            return

        # 3. Параллельное сканирование ВСЕХ бирж
        exchanges = self.exchange_mgr.get_connected_exchanges()

        async def scan_one(eid: str):
            """Сканировать одну биржу"""
            try:
                # Batch-загрузка (3-4 HTTP запроса на всю биржу)
                all_data = await self.exchange_mgr.fetch_all_data(eid, target_bases=low_cap_set)

                if not all_data:
                    return []

                # Batch-evaluate
                mcap_lookup = {sym: cap for sym, cap in self.mcap_provider._cache.items()}
                signals = self.scanner.evaluate_batch(all_data, mcap_lookup)
                return signals

            except Exception as e:
                logger.warning(f"⚠️  Ошибка {eid}: {e}")
                return []

        # Запускаем все биржи параллельно
        results = await asyncio.gather(
            *[scan_one(eid) for eid in exchanges],
            return_exceptions=True,
        )

        # 4. Обработка результатов и отправка сигналов
        all_signals = []
        for r in results:
            if isinstance(r, list):
                all_signals.extend(r)

        # Сортируем все сигналы по score
        all_signals.sort(key=lambda s: s.score, reverse=True)

        for signal in all_signals:
            await self.telegram.send_signal(signal)
            cycle_signals += 1
            self._total_signals += 1

        # Cleanup cooldowns
        if self._cycle % 10 == 0:
            self.scanner.cleanup_cooldowns()

        elapsed = time.time() - t0
        stats = self.scanner.get_stats()
        logger.info(
            f"   ✅ Цикл #{self._cycle} за {elapsed:.1f}с | "
            f"Проверено: {stats['coins_scanned']} | "
            f"Сигналов: {cycle_signals} (всего: {self._total_signals})"
        )

    async def stop(self):
        logger.info("🛑 Останавливаю...")
        self._running = False
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
