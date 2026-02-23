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
# OI / MCap >= 15% → реальный перегрев
OI_MCAP_RATIO = float(os.getenv("OI_MCAP_RATIO", "15.0"))

# Минимальный OI в долларах → только крупные монеты
MIN_OI_USD = float(os.getenv("MIN_OI_USD", "1000000"))

# --- OI Динамика (перегрев) ---
# OI должен вырасти на ≥25% за 10 минут → реальный разгон
MIN_OI_GROWTH_PCT = float(os.getenv("MIN_OI_GROWTH_PCT", "25.0"))
OI_GROWTH_WINDOW = int(os.getenv("OI_GROWTH_WINDOW", "600"))  # 10 мин

# --- Проверка «не опоздали» ---
# Если цена уже выросла на ≥3% за 10 мин → поздно входить
MAX_PRICE_PUMP_PCT = float(os.getenv("MAX_PRICE_PUMP_PCT", "3.0"))

# --- Funding Rate ---
# Funding <= -0.01% → шорты реально платят
MAX_FUNDING_RATE = float(os.getenv("MAX_FUNDING_RATE", "-0.01"))

# --- Спред (Futures vs Spot) ---
# |Spread| <= 2% → справедливая цена
MAX_PRICE_SPREAD = float(os.getenv("MAX_PRICE_SPREAD", "2.0"))

# Бонус за бэквордацию (фьючерс < спот) → x1.5 к score спреда
BACKWARDATION_BONUS = float(os.getenv("BACKWARDATION_BONUS", "1.5"))

# --- Market Cap ---
# MCap >= $5M → только серьёзные проекты
MIN_MARKET_CAP = float(os.getenv("MIN_MARKET_CAP", "5000000"))

# MCap верхний лимит (0 = без лимита)
MAX_MARKET_CAP = float(os.getenv("MAX_MARKET_CAP", "0"))

# --- 24h Volume ---
# Объём торгов >= $500K → реальная ликвидность
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", "500000"))

# --- Score ---
# Минимальный score для отправки сигнала (0-100) — только ТОП
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "70"))

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
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", "7200"))  # 2 часа

# CoinGecko
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")

# Dashboard
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8085"))
