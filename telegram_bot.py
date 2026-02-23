"""
telegram_bot.py — Telegram-бот для отправки сигналов
Красивое форматирование, deep links, команды
"""
import asyncio
import logging
from typing import Optional, TYPE_CHECKING

from telegram import Update, Bot
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError, RetryAfter

import config

if TYPE_CHECKING:
    from scanner import Signal

logger = logging.getLogger("oi_scanner")


# Deep-links на биржи (фьючерсы)
EXCHANGE_LINKS = {
    "binance": "https://www.binance.com/en/futures/{base}USDT",
    "bybit": "https://www.bybit.com/trade/usdt/{base}USDT",
    "okx": "https://www.okx.com/trade-swap/{base}-usdt-swap",
    "gateio": "https://www.gate.io/futures_trade/USDT/{base}_USDT",
    "mexc": "https://futures.mexc.com/exchange/{base}_USDT",
    "kucoin": "https://www.kucoin.com/futures/trade/{base}USDTM",
    "bingx": "https://bingx.com/en/perpetual/{base}-USDT/",
    "bitget": "https://www.bitget.com/futures/usdt/{base}USDT",
}

# Эмодзи для factor scores
FACTOR_EMOJI = {
    "oi": "📊",
    "funding": "📉",
    "spread": "⚖️",
    "mcap": "💎",
}


class TelegramNotifier:
    """Telegram-бот с retry-логикой и rich-форматированием"""

    MAX_RETRIES = 3

    def __init__(self, bot_token: str, chat_id: str, topic_id: int = 0):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.topic_id = topic_id
        self.bot: Optional[Bot] = None
        self.app: Optional[Application] = None
        self._scanner_ref = None
        self._exchange_ref = None
        self._mcap_ref = None
        self._messages_sent = 0

    def set_refs(self, scanner, exchange_mgr, mcap_provider):
        self._scanner_ref = scanner
        self._exchange_ref = exchange_mgr
        self._mcap_ref = mcap_provider

    async def initialize(self):
        """Инициализация бота с polling"""
        self.app = Application.builder().token(self.bot_token).build()
        self.bot = self.app.bot

        self.app.add_handler(CommandHandler("status", self._cmd_status))
        self.app.add_handler(CommandHandler("stats", self._cmd_stats))
        self.app.add_handler(CommandHandler("help", self._cmd_help))

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)

        logger.info("📱 Telegram-бот запущен")

    async def shutdown(self):
        try:
            if self.app:
                await self.app.updater.stop()
                await self.app.stop()
                await self.app.shutdown()
        except Exception:
            pass

    async def _send_with_retry(self, text: str, parse_mode=ParseMode.MARKDOWN):
        """Отправка с retry и rate-limit обработкой"""
        for attempt in range(self.MAX_RETRIES):
            try:
                kwargs = {
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                }
                if self.topic_id:
                    kwargs["message_thread_id"] = self.topic_id

                await self.bot.send_message(**kwargs)
                self._messages_sent += 1
                return True

            except RetryAfter as e:
                wait = e.retry_after + 1
                logger.warning(f"Telegram rate-limit, жду {wait}с...")
                await asyncio.sleep(wait)
            except TelegramError as e:
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"❌ Telegram ошибка после {self.MAX_RETRIES} попыток: {e}")
                    return False

        return False

    async def send_signal(self, signal: "Signal"):
        """Отправить сигнал с rich-форматированием"""
        if not self.bot:
            return

        # Интенсивность
        if signal.rocket_score >= 80:
            header = "🚀🚀🚀 РАКЕТА НА ЛОНГ"
        elif signal.rocket_score >= 60:
            header = "🚀🚀 СИЛЬНЫЙ СИГНАЛ"
        elif signal.rocket_score >= 40:
            header = "🚀 СИГНАЛ НА ЛОНГ"
        else:
            header = "💊 СИГНАЛ"

        # Бэквордация?
        backwardation = ""
        if signal.price_spread is not None and signal.price_spread < -0.1:
            backwardation = " 🔻BACKWARDATION"

        # Deep link
        link_tpl = EXCHANGE_LINKS.get(signal.exchange, "")
        trade_url = link_tpl.format(base=signal.base) if link_tpl else ""

        # Factor bars
        factor_bars = ""
        for key, label in [("oi", "OI/MCap"), ("funding", "Фандинг"), ("spread", "Спред"), ("mcap", "MCap")]:
            val = signal.factor_scores.get(key, 0)
            filled = int(val / 25 * 5)
            bar = "█" * filled + "░" * (5 - filled)
            emoji = FACTOR_EMOJI.get(key, "•")
            factor_bars += f"{emoji} {bar} {val}/25\n"

        # Цена
        price = signal.futures_price
        price_str = f"${price:,.4f}" if price >= 1 else f"${price:.6g}"

        lines = [
            f"*{header}*{backwardation}",
            f"*{signal.base}/USDT* — {signal.exchange_name}",
            "",
            f"🎯 *Score: {signal.score}/100* | 🚀 *Rocket: {signal.rocket_score}/100*",
            "",
            f"📊 OI/MCap: *{signal.oi_mcap_str}*",
            f"📈 OI: *{signal.oi_str}*",
            f"🔥 OI рост (10мин): *{signal.oi_growth_str}*",
            f"📉 Funding: *{signal.funding_str}*",
            f"⚖️ Спред: *{signal.spread_str}*",
            f"💎 MCap: *{signal.mcap_str}*",
            f"📦 Volume 24h: *{signal.volume_str}*",
        ]

        # Показать рост цены (если есть)
        if signal.price_growth_pct is not None:
            lines.append(f"💹 Цена за 10мин: *{signal.price_growth_str}*")

        # 🎯 Целевые цены
        targets = signal.get_targets()
        t_con = targets["conservative"]
        t_mod = targets["moderate"]
        t_agg = targets["aggressive"]

        # Формат цены
        def fmt_price(p):
            return f"${p:,.4f}" if p >= 1 else f"${p:.6g}"

        lines.extend([
            "",
            f"💰 Цена входа: {price_str}",
            "",
            "🎯 *Целевые цены:*",
            f"🟢 Осторожный: {fmt_price(t_con['price'])} (+{t_con['pct']}%)",
            f"🟡 Умеренный: {fmt_price(t_mod['price'])} (+{t_mod['pct']}%)",
            f"🔴 Агрессивный: {fmt_price(t_agg['price'])} (+{t_agg['pct']}%)",
            "",
            "```",
            factor_bars.rstrip(),
            "```",
        ])

        if trade_url:
            lines.append(f"\n[📈 Открыть {signal.exchange_name}]({trade_url})")

        text = "\n".join(lines)
        ok = await self._send_with_retry(text)

        if ok:
            logger.info(f"📤 Сигнал: {signal.base} ({signal.exchange_name}) Score={signal.score}")

    async def send_startup_message(self, exchanges: int, pairs: int):
        if not self.bot:
            return

        cap_str = f"≥ ${config.MIN_MARKET_CAP/1e6:.0f}M"
        if config.MAX_MARKET_CAP > 0:
            cap_str += f" (макс ${config.MAX_MARKET_CAP/1e6:.0f}M)"

        text = (
            "🚀 *OI Scanner Bot запущен*\n\n"
            f"📡 Бирж: {exchanges}\n"
            f"🔍 Фьючерсных пар: {pairs}\n"
            f"⏱ Интервал: {config.SCAN_INTERVAL}с\n\n"
            f"*Фильтры:*\n"
            f"• OI/MCap ≥ {config.OI_MCAP_RATIO}%\n"
            f"• OI ≥ ${config.MIN_OI_USD/1e3:.0f}K\n"
            f"• 🔥 OI рост ≥ {config.MIN_OI_GROWTH_PCT}% за {config.OI_GROWTH_WINDOW//60}мин\n"
            f"• 🚫 Цена не выросла > {config.MAX_PRICE_PUMP_PCT}%\n"
            f"• Funding ≤ {config.MAX_FUNDING_RATE}%\n"
            f"• Спред ≤ ±{config.MAX_PRICE_SPREAD}%\n"
            f"• MCap {cap_str}\n"
            f"• Volume ≥ ${config.MIN_VOLUME_24H/1e3:.0f}K\n"
            f"• Score ≥ {config.MIN_SIGNAL_SCORE}\n\n"
            "⚠️ Первые 10 мин сигналов не будет (накопление OI истории)\n"
            "💊 Сканирую..."
        )
        await self._send_with_retry(text)

    # ═══════════════ КОМАНДЫ ═══════════════

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lines = ["📊 *Статус OI Scanner*\n"]

        if self._exchange_ref:
            s = self._exchange_ref.get_status()
            names = [self._exchange_ref.EXCHANGE_NAMES.get(e, e) for e in s["connected"]]
            lines.append(f"📡 Бирж: {s['total_connected']} — {', '.join(names)}")
            if s["failed"]:
                lines.append(f"❌ Ошибки: {', '.join(s['failed'].keys())}")

        if self._mcap_ref:
            ms = self._mcap_ref.get_stats()
            lines.append(f"💎 MCap кэш: {ms['cached_coins']} монет | {ms['eligible']} подходящих")

        if self._scanner_ref:
            ss = self._scanner_ref.get_stats()
            lines.append(f"🔍 Сканировано: {ss['coins_scanned']}")
            lines.append(f"✅ Прошли фильтр: {ss['coins_passed_filter']}")
            lines.append(f"💊 Сигналов: {ss['signals_generated']}")

        lines.append(f"\n📨 Сообщений: {self._messages_sent}")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    async def _cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lines = ["📈 *Статистика*\n"]

        if self._exchange_ref:
            for eid in self._exchange_ref.get_connected_exchanges():
                n = len(self._exchange_ref.get_futures_symbols(eid))
                name = self._exchange_ref.EXCHANGE_NAMES.get(eid, eid)
                lines.append(f"  📡 {name}: {n} пар")

        lines.append(f"\n⚙️ OI≥{config.OI_MCAP_RATIO}% | F≤{config.MAX_FUNDING_RATE}% | MCap≥${config.MIN_MARKET_CAP/1e6:.0f}M | Score≥{config.MIN_SIGNAL_SCORE}")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "💊 *OI Scanner — Таблетка от бедности*\n\n"
            "/status — статус\n"
            "/stats — статистика бирж\n"
            "/help — справка\n\n"
            "_Перегретый OI + Отриц. фандинг + Справедливая + Лоукап_"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
