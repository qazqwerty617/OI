"""
scanner.py — Ядро стратегии «Таблетка от бедности»
Точный скоринг и фильтрация по 4 факторам
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
    """Сигнал на лонг — все 4 фактора совпали"""
    exchange: str
    exchange_name: str
    symbol: str
    base: str
    futures_price: float
    spot_price: Optional[float]

    # Факторы
    oi_usd: float
    mcap: float
    oi_mcap_ratio: float  # %
    funding_rate: float   # %
    price_spread: Optional[float]  # %

    # Score
    score: int  # 0-100
    factor_scores: Dict[str, float] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def oi_mcap_str(self) -> str:
        return f"{self.oi_mcap_ratio:.1f}%"

    @property
    def funding_str(self) -> str:
        return f"{self.funding_rate:.4f}%"

    @property
    def spread_str(self) -> str:
        if self.price_spread is not None:
            return f"{self.price_spread:+.2f}%"
        return "N/A"

    @property
    def mcap_str(self) -> str:
        if self.mcap >= 1e6:
            return f"${self.mcap / 1e6:.2f}M"
        if self.mcap >= 1e3:
            return f"${self.mcap / 1e3:.0f}K"
        return f"${self.mcap:.0f}"


class StrategyScanner:
    """
    Сканер стратегии с **непрерывным** скорингом.
    
    В отличие от дискретных шкал, используем:
    - Логарифмические кривые для OI/MCap и MCap (более точное отражение)
    - Линейную интерполяцию для funding и spread
    - Бонус за экстремальные значения
    """

    def __init__(self):
        self._cooldowns: Dict[str, float] = {}  # key → timestamp
        self.signals_generated = 0
        self.coins_scanned = 0
        self.coins_passed_filter = 0

    def evaluate_batch(self, all_data: Dict[str, Dict], mcap_lookup: Dict[str, float]) -> List[Signal]:
        """
        Оценить сразу пачку монет (результат ExchangeManager.fetch_all_data).
        Значительно быстрее чем по-одному.
        
        Returns:
            Список сигналов, отсортированный по score DESC
        """
        signals = []
        for symbol, coin_data in all_data.items():
            base = coin_data["base"]
            mcap = mcap_lookup.get(base.upper())
            
            signal = self._evaluate_one(coin_data, mcap)
            if signal:
                signals.append(signal)

        # Сортируем по score (лучшие первые)
        signals.sort(key=lambda s: s.score, reverse=True)
        return signals

    def _evaluate_one(self, coin_data: Dict, mcap: Optional[float]) -> Optional[Signal]:
        """Оценить одну монету по 4 факторам"""
        self.coins_scanned += 1

        base = coin_data["base"]
        oi_usd = coin_data["oi_usd"]
        funding_rate = coin_data["funding_rate"]
        futures_price = coin_data["futures_price"]
        spot_price = coin_data.get("spot_price")

        # ──── Фактор 4: ЛОУКАП (быстрый фильтр) ────
        if mcap is None or mcap <= 0:
            return None
        if mcap > config.MAX_MARKET_CAP:
            return None

        # ──── Фактор 1: ПЕРЕГРЕТЫЙ OI ────
        oi_mcap_ratio = (oi_usd / mcap) * 100
        if oi_mcap_ratio < config.OI_MCAP_RATIO:
            return None

        # ──── Фактор 2: ОТРИЦАТЕЛЬНЫЙ ФАНДИНГ ────
        if funding_rate > config.MAX_FUNDING_RATE:
            return None

        # ──── Фактор 3: СПРАВЕДЛИВАЯ ЦЕНА ────
        price_spread = None
        if spot_price and spot_price > 0:
            price_spread = ((futures_price - spot_price) / spot_price) * 100
            if abs(price_spread) > config.MAX_PRICE_SPREAD:
                return None

        # ──── ВСЕ 4 ФАКТОРА СОВПАЛИ 💊 ────
        self.coins_passed_filter += 1

        # Cooldown check
        cooldown_key = f"{base}_{coin_data['exchange']}"
        now = time.time()
        if (now - self._cooldowns.get(cooldown_key, 0)) < config.SIGNAL_COOLDOWN:
            return None

        # Score
        score, factor_scores = self._calculate_score(oi_mcap_ratio, funding_rate, price_spread, mcap)

        self._cooldowns[cooldown_key] = now
        self.signals_generated += 1

        return Signal(
            exchange=coin_data["exchange"],
            exchange_name=coin_data["exchange_name"],
            symbol=coin_data["symbol"],
            base=base,
            futures_price=futures_price,
            spot_price=spot_price,
            oi_usd=oi_usd,
            mcap=mcap,
            oi_mcap_ratio=oi_mcap_ratio,
            funding_rate=funding_rate,
            price_spread=price_spread,
            score=score,
            factor_scores=factor_scores,
        )

    def _calculate_score(
        self,
        oi_mcap_ratio: float,
        funding_rate: float,
        price_spread: Optional[float],
        mcap: float,
    ) -> tuple:
        """
        Непрерывный скоринг 0-100.
        
        Каждый фактор: 0-25 баллов.
        Используем log-кривые для более точной оценки:
        - OI/MCap: log-рост, насыщение при ~100%
        - Funding: линейный, бонус при extreme
        - Spread: чем ближе к 0 — тем лучше
        - MCap: log-убывание (меньше = лучше)
        """
        factor_scores = {}

        # 1. OI/MCap (0-25) — логарифмический рост
        # 25% (порог) → 12, 50% → 18, 100%+ → 24
        threshold = config.OI_MCAP_RATIO
        ratio_normalized = oi_mcap_ratio / threshold  # 1.0 = порог
        oi_score = min(25.0, 12.0 * math.log2(1 + ratio_normalized))
        factor_scores["oi"] = round(oi_score, 1)

        # 2. Funding (0-25) — линейный + бонус за extreme
        # -0.01% → 10, -0.05% → 18, -0.1% → 22, -0.5%+ → 25
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
        factor_scores["funding"] = round(min(25.0, fund_score), 1)

        # 3. Spread (0-25) — чем ближе к 0, тем лучше
        if price_spread is not None:
            abs_spread = abs(price_spread)
            max_spread = config.MAX_PRICE_SPREAD
            # 0% → 25, MAX/2 → 15, MAX → 5
            spread_score = max(0.0, 25.0 * (1.0 - (abs_spread / max_spread) ** 0.7))
            factor_scores["spread"] = round(spread_score, 1)
        else:
            spread_score = 10.0  # Нет споте — нейтрально
            factor_scores["spread"] = 10.0

        # 4. MCap (0-25) — логарифмическое убывание (меньше = лучше)
        # $100K → 25, $500K → 22, $1M → 18, $5M → 10
        max_cap = config.MAX_MARKET_CAP
        if mcap <= 0:
            mcap_score = 25.0
        else:
            # log-шкала: чем меньше mcap, тем выше score
            ratio = mcap / max_cap  # 0..1
            mcap_score = max(0.0, 25.0 * (1.0 - math.log10(1 + ratio * 9) / math.log10(10)))
        factor_scores["mcap"] = round(min(25.0, mcap_score), 1)

        total = oi_score + fund_score + spread_score + mcap_score
        return (max(0, min(100, int(total))), factor_scores)

    def cleanup_cooldowns(self):
        """Убрать устаревшие cooldown записи"""
        now = time.time()
        expired = [k for k, t in self._cooldowns.items() if (now - t) > config.SIGNAL_COOLDOWN * 2]
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
