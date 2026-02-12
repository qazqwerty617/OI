"""
scanner.py — Ядро стратегии «Таблетка от бедности»
Точный скоринг с диагностикой на каждом этапе фильтрации
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
    funding_rate: float
    price_spread: Optional[float]
    score: int
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
        return f"{self.price_spread:+.2f}%" if self.price_spread is not None else "N/A"

    @property
    def mcap_str(self) -> str:
        if self.mcap >= 1e9:
            return f"${self.mcap / 1e9:.1f}B"
        if self.mcap >= 1e6:
            return f"${self.mcap / 1e6:.1f}M"
        return f"${self.mcap / 1e3:.0f}K"


class StrategyScanner:
    """Сканер с диагностикой"""

    def __init__(self):
        self._cooldowns: Dict[str, float] = {}
        self.signals_generated = 0
        self.coins_scanned = 0
        self.coins_passed_filter = 0
        # Диагностика: на каком этапе отсеиваются
        self._diag = {"no_mcap": 0, "mcap_low": 0, "mcap_high": 0,
                       "oi_low": 0, "funding_high": 0, "spread_high": 0,
                       "cooldown": 0, "passed": 0}

    def evaluate_batch(self, all_data: Dict[str, Dict], mcap_lookup: Dict[str, float]) -> List[Signal]:
        """Оценить пачку монет, вернуть сигналы отсортированные по score"""
        signals = []
        for symbol, coin_data in all_data.items():
            base = coin_data["base"]
            mcap = mcap_lookup.get(base.upper())
            signal = self._evaluate_one(coin_data, mcap)
            if signal:
                signals.append(signal)

        signals.sort(key=lambda s: s.score, reverse=True)
        return signals

    def _evaluate_one(self, coin_data: Dict, mcap: Optional[float]) -> Optional[Signal]:
        self.coins_scanned += 1

        base = coin_data["base"]
        oi_usd = coin_data["oi_usd"]
        funding_rate = coin_data["funding_rate"]
        futures_price = coin_data["futures_price"]
        spot_price = coin_data.get("spot_price")

        # ──── MCap фильтр ────
        if mcap is None or mcap <= 0:
            self._diag["no_mcap"] += 1
            return None

        if mcap < config.MIN_MARKET_CAP:
            self._diag["mcap_low"] += 1
            return None

        if config.MAX_MARKET_CAP > 0 and mcap > config.MAX_MARKET_CAP:
            self._diag["mcap_high"] += 1
            return None

        # ──── OI/MCap ────
        oi_mcap_ratio = (oi_usd / mcap) * 100
        if oi_mcap_ratio < config.OI_MCAP_RATIO:
            self._diag["oi_low"] += 1
            return None

        # ──── Funding ────
        if funding_rate > config.MAX_FUNDING_RATE:
            self._diag["funding_high"] += 1
            return None

        # ──── Spread ────
        price_spread = None
        if spot_price and spot_price > 0:
            price_spread = ((futures_price - spot_price) / spot_price) * 100
            if abs(price_spread) > config.MAX_PRICE_SPREAD:
                self._diag["spread_high"] += 1
                return None

        # ──── ВСЕ ФИЛЬТРЫ ПРОЙДЕНЫ 💊 ────
        self._diag["passed"] += 1
        self.coins_passed_filter += 1

        # Cooldown
        cooldown_key = f"{base}_{coin_data['exchange']}"
        now = time.time()
        if (now - self._cooldowns.get(cooldown_key, 0)) < config.SIGNAL_COOLDOWN:
            self._diag["cooldown"] += 1
            return None

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

    def _calculate_score(self, oi_mcap_ratio: float, funding_rate: float,
                         price_spread: Optional[float], mcap: float) -> tuple:
        """Непрерывный скоринг 0-100"""
        factor_scores = {}

        # 1. OI/MCap (0-25) — лог-рост
        threshold = config.OI_MCAP_RATIO
        ratio_norm = oi_mcap_ratio / threshold
        oi_score = min(25.0, 12.0 * math.log2(1 + ratio_norm))
        factor_scores["oi"] = round(oi_score, 1)

        # 2. Funding (0-25)
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

        # 3. Spread (0-25)
        if price_spread is not None:
            abs_spread = abs(price_spread)
            max_spread = config.MAX_PRICE_SPREAD
            spread_score = max(0.0, 25.0 * (1.0 - (abs_spread / max_spread) ** 0.7))
        else:
            spread_score = 10.0
        factor_scores["spread"] = round(spread_score, 1)

        # 4. MCap (0-25) — меньше = лучше
        # $3M = 25, $20M = 18, $100M = 12, $1B = 5
        if mcap <= config.MIN_MARKET_CAP:
            mcap_score = 25.0
        else:
            mcap_score = max(0.0, 25.0 - 4.0 * math.log10(mcap / config.MIN_MARKET_CAP))
        factor_scores["mcap"] = round(min(25.0, mcap_score), 1)

        total = oi_score + fund_score + spread_score + mcap_score
        return (max(0, min(100, int(total))), factor_scores)

    def get_diagnostics(self) -> str:
        """Строковая диагностика фильтров"""
        d = self._diag
        total = self.coins_scanned
        if total == 0:
            return "Нет данных"
        return (
            f"Всего: {total} | "
            f"Нет MCap: {d['no_mcap']} | "
            f"MCap<min: {d['mcap_low']} | "
            f"MCap>max: {d['mcap_high']} | "
            f"OI<порог: {d['oi_low']} | "
            f"Fund>порог: {d['funding_high']} | "
            f"Спред>порог: {d['spread_high']} | "
            f"Cooldown: {d['cooldown']} | "
            f"💊 Прошли: {d['passed']}"
        )

    def reset_diagnostics(self):
        for k in self._diag:
            self._diag[k] = 0

    def cleanup_cooldowns(self):
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
