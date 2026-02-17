"""
tracker.py — Трекер сигналов (демо-лонг)
Хранит последние N сигналов, обновляет текущую цену, считает P&L
"""
import time
import logging
from dataclasses import dataclass, field, asdict
from collections import deque
from typing import Dict, List, Optional

logger = logging.getLogger("oi_scanner")


@dataclass
class TrackedSignal:
    """Один отслеживаемый сигнал"""
    id: int
    base: str
    exchange: str
    exchange_name: str
    symbol: str
    entry_price: float
    current_price: float
    score: int
    oi_mcap_ratio: float
    funding_rate: Optional[float]
    mcap: float
    entry_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)

    @property
    def pnl_pct(self) -> float:
        """P&L в % (лонг)"""
        if self.entry_price <= 0:
            return 0.0
        return ((self.current_price - self.entry_price) / self.entry_price) * 100

    @property
    def hold_time_min(self) -> float:
        """Время удержания в минутах"""
        return (time.time() - self.entry_time) / 60

    def to_dict(self) -> Dict:
        d = {
            "id": self.id,
            "base": self.base,
            "exchange": self.exchange,
            "exchange_name": self.exchange_name,
            "symbol": self.symbol,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "pnl_pct": round(self.pnl_pct, 3),
            "score": self.score,
            "oi_mcap_ratio": round(self.oi_mcap_ratio, 1),
            "funding_rate": round(self.funding_rate, 4) if self.funding_rate else None,
            "mcap": self.mcap,
            "entry_time": self.entry_time,
            "hold_time_min": round(self.hold_time_min, 1),
            "last_update": self.last_update,
        }
        return d


class SignalTracker:
    """Трекер последних N сигналов с обновлением цен"""

    MAX_SIGNALS = 10

    def __init__(self, max_signals: int = 10):
        self.MAX_SIGNALS = max_signals
        self._signals: deque = deque(maxlen=max_signals)
        self._counter = 0
        self._total_signals = 0
        self._closed_pnl: List[float] = []  # P&L закрытых (вытесненных)

    def add_signal(self, signal) -> TrackedSignal:
        """Добавить новый сигнал. Если очередь полная — старый вытесняется."""
        self._counter += 1
        self._total_signals += 1

        # Если вытесняем старый — запоминаем его P&L
        if len(self._signals) >= self.MAX_SIGNALS:
            old = self._signals[0]
            self._closed_pnl.append(old.pnl_pct)

        ts = TrackedSignal(
            id=self._counter,
            base=signal.base,
            exchange=signal.exchange,
            exchange_name=signal.exchange_name,
            symbol=signal.symbol,
            entry_price=signal.futures_price,
            current_price=signal.futures_price,
            score=signal.score,
            oi_mcap_ratio=signal.oi_mcap_ratio,
            funding_rate=signal.funding_rate,
            mcap=signal.mcap,
        )

        self._signals.append(ts)
        logger.info(
            f"📌 Трекинг #{ts.id}: {ts.base} @ ${ts.entry_price:.6g} "
            f"(Score: {ts.score})"
        )

        return ts

    def update_prices(self, price_map: Dict[str, float]):
        """Обновить текущие цены для всех отслеживаемых сигналов"""
        now = time.time()
        for ts in self._signals:
            # Пробуем найти цену: по symbol, или по BASE/USDT
            price = price_map.get(ts.symbol)
            if not price:
                price = price_map.get(ts.base)
            if price and price > 0:
                ts.current_price = price
                ts.last_update = now

    def get_all(self) -> List[Dict]:
        """Все текущие сигналы как список dict (от новых к старым)"""
        return [ts.to_dict() for ts in reversed(self._signals)]

    def get_summary(self) -> Dict:
        """Сводка по трекеру"""
        active = list(self._signals)
        if not active:
            return {
                "active_count": 0,
                "total_signals": self._total_signals,
                "avg_pnl": 0,
                "best_pnl": 0,
                "worst_pnl": 0,
                "profitable": 0,
                "losing": 0,
                "closed_total": len(self._closed_pnl),
                "closed_avg_pnl": 0,
            }

        pnls = [s.pnl_pct for s in active]
        profitable = sum(1 for p in pnls if p > 0)
        losing = sum(1 for p in pnls if p < 0)

        closed_avg = (sum(self._closed_pnl) / len(self._closed_pnl)) if self._closed_pnl else 0

        return {
            "active_count": len(active),
            "total_signals": self._total_signals,
            "avg_pnl": round(sum(pnls) / len(pnls), 3),
            "best_pnl": round(max(pnls), 3),
            "worst_pnl": round(min(pnls), 3),
            "profitable": profitable,
            "losing": losing,
            "closed_total": len(self._closed_pnl),
            "closed_avg_pnl": round(closed_avg, 3),
        }
