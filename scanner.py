"""
scanner.py — Ядро стратегии «Таблетка от бедности»

ТОП фильтры:
  1. OI/MCap >= 12% — реальный перегрев
  2. OI >= $500K — объём значимый
  3. OI РОСТ >= 15% за 10мин — динамический перегрев
  4. Цена НЕ выросла на 5%+ — не опоздали
  5. Funding <= -0.01% — шорты платят
  6. Спред <= +/-2% — справедливая цена
  7. Бэквордация -> бонус x1.5
  8. MCap >= $2M — не скам
  9. Volume >= $100K — ликвидность
  10. Score >= 60 — только сильные сигналы

Непрерывный скоринг: логарифмический + линейный, 0-100 баллов.
"""
import math
import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import config
from oi_history import OIHistory

logger = logging.getLogger("oi_scanner")


@dataclass(slots=True)
class Signal:
    """Сигнал на лонг"""
    exchange: str
    exchange_name: str
    symbol: str
    base: str
    futures_price: float
    spot_price: Optional[float]
    oi_usd: float
    mcap: float
    oi_mcap_ratio: float
    funding_rate: Optional[float]
    price_spread: Optional[float]
    volume_24h: float
    oi_growth_pct: Optional[float]
    price_growth_pct: Optional[float]
    rocket_score: int
    score: int
    factor_scores: Dict[str, float] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def oi_mcap_str(self) -> str:
        return f"{self.oi_mcap_ratio:.1f}%"

    @property
    def funding_str(self) -> str:
        if self.funding_rate is not None:
            return f"{self.funding_rate:.4f}%"
        return "N/A"

    @property
    def spread_str(self) -> str:
        if self.price_spread is not None:
            return f"{self.price_spread:+.2f}%"
        return "N/A"

    @property
    def mcap_str(self) -> str:
        if self.mcap >= 1e9:
            return f"${self.mcap / 1e9:.1f}B"
        if self.mcap >= 1e6:
            return f"${self.mcap / 1e6:.1f}M"
        return f"${self.mcap / 1e3:.0f}K"

    @property
    def volume_str(self) -> str:
        if self.volume_24h >= 1e6:
            return f"${self.volume_24h / 1e6:.1f}M"
        if self.volume_24h >= 1e3:
            return f"${self.volume_24h / 1e3:.0f}K"
        return f"${self.volume_24h:.0f}"

    @property
    def oi_str(self) -> str:
        if self.oi_usd >= 1e6:
            return f"${self.oi_usd / 1e6:.1f}M"
        return f"${self.oi_usd / 1e3:.0f}K"

    @property
    def oi_growth_str(self) -> str:
        if self.oi_growth_pct is not None:
            return f"+{self.oi_growth_pct:.1f}%"
        return "N/A"

    @property
    def price_growth_str(self) -> str:
        if self.price_growth_pct is not None:
            return f"{self.price_growth_pct:+.1f}%"
        return "N/A"

    def get_targets(self) -> Dict[str, Dict[str, float]]:
        """
        Умное вычисление целевых цен на основе:
        - OI рост (больше рост → сильнее сквиз)
        - Funding (более отрицательный → больше шортов к ликвидации)
        - MCap (меньше → волатильнее, больше потенциал)
        - Volume/MCap (выше → активнее торгуется, резче движения)

        Возвращает 3 таргета: conservative, moderate, aggressive
        """
        base_pct = 3.0  # Базовый ожидаемый рост %

        # 1. OI Growth бонус: чем быстрее растёт OI, тем сильнее сквиз
        if self.oi_growth_pct and self.oi_growth_pct > 0:
            if self.oi_growth_pct >= 80:
                base_pct += 12.0
            elif self.oi_growth_pct >= 50:
                base_pct += 8.0
            elif self.oi_growth_pct >= 30:
                base_pct += 5.0
            else:
                base_pct += 2.0

        # 2. Funding бонус: сильно отрицательный = много шортов
        if self.funding_rate is not None:
            if self.funding_rate <= -0.5:
                base_pct += 10.0  # Экстримальный шорт-сквиз
            elif self.funding_rate <= -0.2:
                base_pct += 7.0
            elif self.funding_rate <= -0.1:
                base_pct += 5.0
            elif self.funding_rate <= -0.05:
                base_pct += 3.0
            elif self.funding_rate <= -0.01:
                base_pct += 1.5

        # 3. MCap множитель: лоукапы движутся сильнее
        if self.mcap <= 10e6:
            base_pct *= 2.0   # <$10M — может x2 от базы
        elif self.mcap <= 30e6:
            base_pct *= 1.6
        elif self.mcap <= 100e6:
            base_pct *= 1.3
        elif self.mcap <= 300e6:
            base_pct *= 1.1
        # >$300M — без множителя

        # 4. Volume/MCap волатильность
        if self.mcap > 0:
            vol_ratio = self.volume_24h / self.mcap
            if vol_ratio >= 1.0:  # 100%+ оборот
                base_pct *= 1.5
            elif vol_ratio >= 0.5:
                base_pct *= 1.3
            elif vol_ratio >= 0.2:
                base_pct *= 1.1

        # 5. Rocket score бонус
        if self.rocket_score >= 80:
            base_pct *= 1.3
        elif self.rocket_score >= 60:
            base_pct *= 1.15

        # Ограничиваем разумными пределами
        base_pct = min(base_pct, 60.0)  # Макс 60%

        price = self.futures_price
        conservative = base_pct * 0.4
        moderate = base_pct * 0.7
        aggressive = base_pct

        return {
            "conservative": {
                "pct": round(conservative, 1),
                "price": round(price * (1 + conservative / 100), 6),
            },
            "moderate": {
                "pct": round(moderate, 1),
                "price": round(price * (1 + moderate / 100), 6),
            },
            "aggressive": {
                "pct": round(aggressive, 1),
                "price": round(price * (1 + aggressive / 100), 6),
            },
        }


class StrategyScanner:
    """Сканер с топ-фильтрами и диагностикой"""

    FILTER_NAMES = [
        "no_mcap", "mcap_low", "mcap_high",
        "oi_ratio_low", "oi_usd_low", "oi_growth_low", "oi_no_data",
        "price_pumped", "volume_low", "vol_ratio_low",
        "funding_high", "spread_high",
        "rocket_low", "score_low", "cooldown", "passed"
    ]

    def __init__(self):
        self._cooldowns: Dict[str, float] = {}
        self.signals_generated = 0
        self.coins_scanned = 0
        self.coins_passed_filter = 0
        self._diag = {k: 0 for k in self.FILTER_NAMES}
        self.oi_history = OIHistory()

    def evaluate_batch(self, all_data: Dict[str, Dict],
                       mcap_lookup: Dict[str, float]) -> List[Signal]:
        """Оценить пачку монет, вернуть топ-сигналы"""
        signals = []
        for symbol, coin_data in all_data.items():
            base = coin_data["base"]
            mcap = mcap_lookup.get(base.upper())
            signal = self._evaluate_one(coin_data, mcap)
            if signal:
                signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        return signals

    def _evaluate_one(self, d: Dict, mcap: Optional[float]) -> Optional[Signal]:
        self.coins_scanned += 1

        base = d["base"]
        symbol = d["symbol"]
        oi_usd = d["oi_usd"]
        funding_rate = d.get("funding_rate")  # может быть None
        futures_price = d["futures_price"]
        spot_price = d.get("spot_price")
        volume_24h = d.get("volume_24h", 0) or 0

        # Записываем OI, цену и объём в историю (каждый цикл!)
        self.oi_history.record(symbol, oi_usd, futures_price, volume_24h)

        # ═══════════════ ФИЛЬТРЫ (жёсткие) ═══════════════

        # 1. MCap
        if (mcap is None or mcap <= 0) and config.MIN_MARKET_CAP > 0:
            self._diag["no_mcap"] += 1
            return None

        if mcap and mcap > 0:
            if mcap < config.MIN_MARKET_CAP:
                self._diag["mcap_low"] += 1
                return None

            if config.MAX_MARKET_CAP > 0 and mcap > config.MAX_MARKET_CAP:
                self._diag["mcap_high"] += 1
                return None

        # 2. OI/MCap ratio
        oi_mcap_ratio = (oi_usd / mcap * 100) if (mcap and mcap > 0) else 0
        if config.OI_MCAP_RATIO > 0 and oi_mcap_ratio < config.OI_MCAP_RATIO:
            self._diag["oi_ratio_low"] += 1
            return None

        # 3. OI в долларах (минимум $1M)
        if oi_usd < config.MIN_OI_USD:
            self._diag["oi_usd_low"] += 1
            return None

        # 4. 🔥 OI РОСТ — динамический перегрев
        oi_growth = self.oi_history.get_growth_pct(symbol, config.OI_GROWTH_WINDOW)
        if oi_growth is None:
            # Нет достаточной истории — ждём накопления данных
            self._diag["oi_no_data"] += 1
            return None
        if oi_growth < config.MIN_OI_GROWTH_PCT:
            self._diag["oi_growth_low"] += 1
            return None

        # 5. 🚫 Проверка «не опоздали» — цена уже выросла?
        price_growth = self.oi_history.get_price_growth_pct(symbol, config.OI_GROWTH_WINDOW)
        if price_growth is not None and price_growth > config.MAX_PRICE_PUMP_PCT:
            self._diag["price_pumped"] += 1
            return None

        # 6. 24h Volume
        if volume_24h < config.MIN_VOLUME_24H:
            self._diag["volume_low"] += 1
            return None

        # 7. 🌋 Волатильность (Volume/MCap) — только активные монеты
        vol_mcap_ratio = (volume_24h / mcap * 100) if (mcap and mcap > 0) else 0
        if config.MIN_VOL_MCAP_RATIO > 0 and vol_mcap_ratio < config.MIN_VOL_MCAP_RATIO:
            self._diag["vol_ratio_low"] += 1
            return None

        # 8. Funding rate — ОБЯЗАТЕЛЬНО отрицательный (шорты платят = сквиз)
        if funding_rate is None or funding_rate > config.MAX_FUNDING_RATE:
            self._diag["funding_high"] += 1
            return None

        # 8. Spread
        price_spread = None
        if spot_price and spot_price > 0:
            price_spread = ((futures_price - spot_price) / spot_price) * 100
            if abs(price_spread) > config.MAX_PRICE_SPREAD:
                self._diag["spread_high"] += 1
                return None

        # ═══════════════ СКОРИНГ ═══════════════
        score, factor_scores = self._calculate_score(
            oi_mcap_ratio, funding_rate, price_spread, mcap, volume_24h, oi_usd,
            oi_growth
        )

        # 9. 🚀 Rocket Score — комбо-детектор шорт-сквиза
        rocket = self.oi_history.get_rocket_score(
            symbol, oi_growth, price_growth, funding_rate
        )
        if rocket < config.MIN_ROCKET_SCORE:
            self._diag["rocket_low"] += 1
            return None

        # 10. Score порог
        if score < config.MIN_SIGNAL_SCORE:
            self._diag["score_low"] += 1
            return None

        # 11. Cooldown (по монете, не по бирже — одна монета = один сигнал)
        cooldown_key = base
        now = time.time()
        if (now - self._cooldowns.get(cooldown_key, 0)) < config.SIGNAL_COOLDOWN:
            self._diag["cooldown"] += 1
            return None

        # ═══════════════ СИГНАЛ 💊 ═══════════════
        self._diag["passed"] += 1
        self.coins_passed_filter += 1
        self._cooldowns[cooldown_key] = now
        self.signals_generated += 1

        return Signal(
            exchange=d["exchange"],
            exchange_name=d["exchange_name"],
            symbol=d["symbol"],
            base=base,
            futures_price=futures_price,
            spot_price=spot_price,
            oi_usd=oi_usd,
            mcap=mcap,
            oi_mcap_ratio=oi_mcap_ratio,
            funding_rate=funding_rate,
            price_spread=price_spread,
            volume_24h=volume_24h,
            oi_growth_pct=oi_growth,
            price_growth_pct=price_growth,
            rocket_score=rocket,
            score=score,
            factor_scores=factor_scores,
        )

    def _calculate_score(self, oi_mcap_ratio: float, funding_rate: Optional[float],
                         price_spread: Optional[float], mcap: float,
                         volume_24h: float, oi_usd: float,
                         oi_growth: float = 0.0) -> tuple:
        """
        Непрерывный скоринг 0-100.
        4 фактора по 25 баллов макс.
        Бэквордация (spread < 0) получает бонус.
        OI рост даёт бонус к OI фактору.
        """
        factor_scores = {}

        # ──── 1. OI/MCap + OI Growth (0-25) ────
        threshold = config.OI_MCAP_RATIO
        ratio_norm = oi_mcap_ratio / threshold
        oi_score = min(20.0, 10.0 * math.log2(1 + ratio_norm))

        # Бонус за абсолютный OI
        if oi_usd >= 2_000_000:
            oi_score = min(22.0, oi_score + 2.0)
        elif oi_usd >= 1_000_000:
            oi_score = min(22.0, oi_score + 1.0)

        # 🔥 Бонус за рост OI (до +5 баллов)
        if oi_growth >= 50:
            oi_score = min(25.0, oi_score + 5.0)
        elif oi_growth >= 30:
            oi_score = min(25.0, oi_score + 3.0)
        elif oi_growth >= 15:
            oi_score = min(25.0, oi_score + 1.5)

        factor_scores["oi"] = round(oi_score, 1)

        # ──── 2. Funding (0-25) ────
        if funding_rate is not None:
            abs_fund = abs(funding_rate)
            if abs_fund >= 0.5:
                fund_score = 25.0
            elif abs_fund >= 0.1:
                fund_score = 22.0 + (abs_fund - 0.1) / 0.4 * 3.0
            elif abs_fund >= 0.05:
                fund_score = 18.0 + (abs_fund - 0.05) / 0.05 * 4.0
            elif abs_fund >= 0.01:
                fund_score = 10.0 + (abs_fund - 0.01) / 0.04 * 8.0
            else:
                fund_score = abs_fund / 0.01 * 10.0
        else:
            fund_score = 5.0  # Нет данных — минимальный скор

        factor_scores["funding"] = round(min(25.0, fund_score), 1)

        # ──── 3. Spread (0-25) + бэквордация ────
        if price_spread is not None:
            abs_spread = abs(price_spread)
            max_spread = config.MAX_PRICE_SPREAD

            # Базовый score: чем меньше спред — тем лучше
            spread_score = max(0.0, 25.0 * (1.0 - (abs_spread / max_spread) ** 0.7))

            # 🔥 БЭКВОРДАЦИЯ: фьючерс < спот → бонус ×1.5
            # Это сильнейший сигнал на лонг!
            if price_spread < -0.1:  # Значимая бэквордация (>0.1%)
                spread_score = min(25.0, spread_score * config.BACKWARDATION_BONUS)
        else:
            spread_score = 8.0  # Нет данных

        factor_scores["spread"] = round(spread_score, 1)

        # ──── 4. MCap (0-25) ────
        # Меньше = лучше: $2M → 25, $10M → 20, $50M → 15, $500M → 7
        if mcap is None or mcap <= 0:
            mcap_score = 15.0  # Средний скор если капа неизвестна
        elif config.MIN_MARKET_CAP > 0:
            if mcap <= config.MIN_MARKET_CAP:
                mcap_score = 25.0
            elif mcap >= 1e9:
                mcap_score = 2.0
            else:
                mcap_score = max(0.0, 25.0 - 4.5 * math.log10(mcap / config.MIN_MARKET_CAP))
        else:
            # Капа есть, но фильтр отключен — даем скор за сам факт наличия данных
            if mcap >= 1e9:
                mcap_score = 5.0
            elif mcap >= 100e6:
                mcap_score = 15.0
            else:
                mcap_score = 20.0

        # Бонус за volume/mcap (высокий оборот = интерес к монете)
        if mcap and mcap > 0 and volume_24h > 0:
            vol_mcap = volume_24h / mcap
            if vol_mcap >= 0.5:  # 50%+ от капы = отлично
                mcap_score = min(25.0, mcap_score + 3.0)
            elif vol_mcap >= 0.2:
                mcap_score = min(25.0, mcap_score + 1.5)

        factor_scores["mcap"] = round(min(25.0, mcap_score), 1)

        total = oi_score + fund_score + spread_score + mcap_score
        return (max(0, min(100, int(total))), factor_scores)

    def get_diagnostics(self) -> str:
        d = self._diag
        total = self.coins_scanned
        if total == 0:
            return "Нет данных"

        parts = []
        for key, label in [
            ("no_mcap", "Нет MCap"),
            ("mcap_low", "MCap↓"),
            ("oi_ratio_low", "OI/MCap↓"),
            ("oi_usd_low", "OI$↓"),
            ("oi_growth_low", "OI↑↓"),
            ("oi_no_data", "OI?"),
            ("price_pumped", "Pump!"),
            ("volume_low", "Vol↓"),
            ("vol_ratio_low", "V/M↓"),
            ("funding_high", "Fund↑"),
            ("spread_high", "Спред↑"),
            ("rocket_low", "🚀↓"),
            ("score_low", "Score↓"),
            ("cooldown", "CD"),
            ("passed", "💊"),
        ]:
            val = d[key]
            if val > 0:
                parts.append(f"{label}:{val}")

        # OI history stats
        oi_stats = self.oi_history.get_stats()
        parts.append(f"OI📊:{oi_stats['tracked_symbols']}")

        return f"Всего:{total} | " + " | ".join(parts)

    def reset_diagnostics(self):
        for k in self._diag:
            self._diag[k] = 0

    def cleanup_cooldowns(self):
        now = time.time()
        expired = [k for k, t in self._cooldowns.items()
                   if (now - t) > config.SIGNAL_COOLDOWN * 2]
        for k in expired:
            del self._cooldowns[k]

    def get_stats(self) -> Dict:
        return {
            "coins_scanned": self.coins_scanned,
            "coins_passed_filter": self.coins_passed_filter,
            "signals_generated": self.signals_generated,
            "active_cooldowns": len(self._cooldowns),
        }

    def reset_cooldowns(self):
        self._cooldowns.clear()
