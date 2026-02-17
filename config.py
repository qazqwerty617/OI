"""
OI Scanner Bot — Конфигурация
Таблетка от бедности 💊

Топ-фильтры для максимально качественных сигналов на лонг.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════
# Telegram
# ═══════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_TOPIC_ID = int(os.getenv("TELEGRAM_TOPIC_ID", "0"))

# ═══════════════════════════════════════════
# ФИЛЬТРЫ СТРАТЕГИИ
# ═══════════════════════════════════════════

# --- OI (Open Interest) ---
# OI / MCap >= 12% → реальный перегрев, не шум
OI_MCAP_RATIO = float(os.getenv("OI_MCAP_RATIO", "12.0"))

# Минимальный OI в долларах → отсекает пыль
MIN_OI_USD = float(os.getenv("MIN_OI_USD", "500000"))

# --- Funding Rate ---
# Funding <= -0.01% → шорты реально платят
MAX_FUNDING_RATE = float(os.getenv("MAX_FUNDING_RATE", "-0.01"))

# --- Спред (Futures vs Spot) ---
# |Spread| <= 2% → справедливая цена
MAX_PRICE_SPREAD = float(os.getenv("MAX_PRICE_SPREAD", "2.0"))

# Бонус за бэквордацию (фьючерс < спот) → x1.5 к score спреда
BACKWARDATION_BONUS = float(os.getenv("BACKWARDATION_BONUS", "1.5"))

# --- Market Cap ---
# MCap >= $2M → не скам/мёртвый проект
MIN_MARKET_CAP = float(os.getenv("MIN_MARKET_CAP", "2000000"))

# MCap верхний лимит (0 = без лимита)
MAX_MARKET_CAP = float(os.getenv("MAX_MARKET_CAP", "0"))

# --- 24h Volume ---
# Объём торгов >= $100K → монета ликвидна
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", "100000"))

# --- Score ---
# Минимальный score для отправки сигнала (0-100)
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "50"))

# ═══════════════════════════════════════════
# Биржи
# ═══════════════════════════════════════════
EXCHANGES = [
    "binance",
    "bybit",
    "okx",
    "gate",
    "mexc",
    "kucoin",
    "bingx",
    "bitget",
]

# ═══════════════════════════════════════════
# Тайминги
# ═══════════════════════════════════════════
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))
MCAP_CACHE_TTL = int(os.getenv("MCAP_CACHE_TTL", "300"))
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", "1200"))

# CoinGecko
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
