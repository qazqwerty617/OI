"""
scanner.py — Ядро стратегии «Таблетка от бедности»

ТОП фильтры:
  1. OI/MCap ≥ 12% — реальный перегрев
  2. OI ≥ $500K — объём значимый
  3. Funding ≤ -0.01% — шорты платят
  4. Спред ≤ ±2% — справедливая цена
  5. Бэквордация → бонус ×1.5
  6. MCap ≥ $2M — не скам
  7. Volume ≥ $100K — ликвидность
  8. Score ≥ 50 — только сильные сигналы

Непрерывный скоринг: логарифмический + линейный, 0-100 баллов.
"""
import math
import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import config

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


class StrategyScanner:
    """Сканер с топ-фильтрами и диагностикой"""

    FILTER_NAMES = [
        "no_mcap", "mcap_low", "mcap_high",
        "oi_ratio_low", "oi_usd_low", "volume_low",
        "funding_high", "spread_high",
        "score_low", "cooldown", "passed"
    ]

    def __init__(self):
        self._cooldowns: Dict[str, float] = {}
        self.signals_generated = 0
        self.coins_scanned = 0
        self.coins_passed_filter = 0
        self._diag = {k: 0 for k in self.FILTER_NAMES}

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
        oi_usd = d["oi_usd"]
        funding_rate = d.get("funding_rate")  # может быть None
        futures_price = d["futures_price"]
        spot_price = d.get("spot_price")
        volume_24h = d.get("volume_24h", 0) or 0

        # ═══════════════ ФИЛЬТРЫ (жёсткие) ═══════════════

        # 1. MCap
        if mcap is None or mcap <= 0:
            self._diag["no_mcap"] += 1
            return None

        if mcap < config.MIN_MARKET_CAP:
            self._diag["mcap_low"] += 1
            return None

        if config.MAX_MARKET_CAP > 0 and mcap > config.MAX_MARKET_CAP:
            self._diag["mcap_high"] += 1
            return None

        # 2. OI/MCap ratio
        oi_mcap_ratio = (oi_usd / mcap) * 100
        if oi_mcap_ratio < config.OI_MCAP_RATIO:
            self._diag["oi_ratio_low"] += 1
            return None

        # 3. OI в долларах (минимум $500K)
        if oi_usd < config.MIN_OI_USD:
            self._diag["oi_usd_low"] += 1
            return None

        # 4. 24h Volume
        if volume_24h < config.MIN_VOLUME_24H:
            self._diag["volume_low"] += 1
            return None

        # 5. Funding rate (опциональный, но если есть — фильтруем)
        if funding_rate is not None:
            if funding_rate > config.MAX_FUNDING_RATE:
                self._diag["funding_high"] += 1
                return None

        # 6. Spread
        price_spread = None
        if spot_price and spot_price > 0:
            price_spread = ((futures_price - spot_price) / spot_price) * 100
            if abs(price_spread) > config.MAX_PRICE_SPREAD:
                self._diag["spread_high"] += 1
                return None

        # ═══════════════ СКОРИНГ ═══════════════
        score, factor_scores = self._calculate_score(
            oi_mcap_ratio, funding_rate, price_spread, mcap, volume_24h, oi_usd
        )

        # 7. Score порог
        if score < config.MIN_SIGNAL_SCORE:
            self._diag["score_low"] += 1
            return None

        # 8. Cooldown
        cooldown_key = f"{base}_{d['exchange']}"
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
            score=score,
            factor_scores=factor_scores,
        )

    def _calculate_score(self, oi_mcap_ratio: float, funding_rate: Optional[float],
                         price_spread: Optional[float], mcap: float,
                         volume_24h: float, oi_usd: float) -> tuple:
        """
        Непрерывный скоринг 0-100.
        4 фактора по 25 баллов макс.
        Бэквордация (spread < 0) получает бонус.
        """
        factor_scores = {}

        # ──── 1. OI/MCap (0-25) ────
        # Лог-рост: 12% → ~12, 25% → ~17, 50% → ~21, 100% → ~24
        threshold = config.OI_MCAP_RATIO
        ratio_norm = oi_mcap_ratio / threshold
        oi_score = min(25.0, 12.0 * math.log2(1 + ratio_norm))

        # Бонус за абсолютный OI (если OI > $2M — дополнительно)
        if oi_usd >= 2_000_000:
            oi_score = min(25.0, oi_score + 2.0)
        elif oi_usd >= 1_000_000:
            oi_score = min(25.0, oi_score + 1.0)

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
        if mcap <= config.MIN_MARKET_CAP:
            mcap_score = 25.0
        elif mcap >= 1e9:
            mcap_score = 2.0
        else:
            mcap_score = max(0.0, 25.0 - 4.5 * math.log10(mcap / config.MIN_MARKET_CAP))

        # Бонус за volume/mcap (высокий оборот = интерес к монете)
        if mcap > 0 and volume_24h > 0:
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
            ("volume_low", "Vol↓"),
            ("funding_high", "Fund↑"),
            ("spread_high", "Спред↑"),
            ("score_low", "Score↓"),
            ("cooldown", "CD"),
            ("passed", "💊"),
        ]:
            val = d[key]
            if val > 0:
                parts.append(f"{label}:{val}")

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
