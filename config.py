"""
OI Scanner Bot — Конфигурация
Таблетка от бедности 💊
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
# Пороги стратегии
# ═══════════════════════════════════════════
# OI / Market Cap >= 25% → перегретый OI
OI_MCAP_RATIO = float(os.getenv("OI_MCAP_RATIO", "25.0"))

# Funding rate <= -0.01% → отрицательный фандинг (шорты платят)
MAX_FUNDING_RATE = float(os.getenv("MAX_FUNDING_RATE", "-0.01"))

# |Futures - Spot| / Spot <= 2% → справедливая цена
MAX_PRICE_SPREAD = float(os.getenv("MAX_PRICE_SPREAD", "2.0"))

# Market Cap <= $5M → лоукап
MAX_MARKET_CAP = float(os.getenv("MAX_MARKET_CAP", "5000000"))

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
# Сканирование
# ═══════════════════════════════════════════
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))  # секунды между циклами
MCAP_CACHE_TTL = int(os.getenv("MCAP_CACHE_TTL", "300"))  # кэш маркеткапов, сек
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", "1800"))  # не алертить одну монету чаще 30 мин

# CoinGecko
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")  # опционально, для Pro
