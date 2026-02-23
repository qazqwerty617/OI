"""
tracker.py — Трекер сигналов с авто-закрытием

Сигналы живут пока не достигнут:
  +10% → WIN (закрывается в историю)
  -10% → LOSS (закрывается в историю)

История ведёт винрейт.
"""
import time
import json
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("oi_scanner")

# Файл для сохранения истории между перезапусками
HISTORY_FILE = "trade_history.json"


@dataclass
class TrackedSignal:
    """Один активный сигнал с умным управлением риском"""
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
    # Умные поля
    targets: Dict[str, Dict] = field(default_factory=dict)
    sl_price: float = 0.0          # Динамический стоп
    realized_pnl: float = 0.0      # Зафиксированная прибыль в %
    position_size: float = 1.0     # Остаток позиции (1.0 = 100%)
    stage: int = 0                 # 0:Entry, 1:TP1 Hit, 2:TP2 Hit
    entry_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    peak_pnl: float = 0.0
    trough_pnl: float = 0.0

    @property
    def unrealized_pnl_pct(self) -> float:
        """P&L текущего остатка позиции"""
        if self.entry_price <= 0:
            return 0.0
        return ((self.current_price - self.entry_price) / self.entry_price) * 100

    @property
    def total_pnl_pct(self) -> float:
        """Общий P&L (реализованный + текущий)"""
        return self.realized_pnl + (self.unrealized_pnl_pct * self.position_size)

    @property
    def hold_time_min(self) -> float:
        return (time.time() - self.entry_time) / 60

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "base": self.base,
            "exchange": self.exchange,
            "exchange_name": self.exchange_name,
            "symbol": self.symbol,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "pnl_pct": round(self.total_pnl_pct, 3),
            "unrealized_pnl": round(self.unrealized_pnl_pct, 3),
            "realized_pnl": round(self.realized_pnl, 3),
            "pos_size": self.position_size,
            "sl_price": self.sl_price,
            "targets": self.targets,
            "stage": self.stage,
            "score": self.score,
            "oi_mcap_ratio": round(self.oi_mcap_ratio, 1),
            "funding_rate": round(self.funding_rate, 4) if self.funding_rate else None,
            "mcap": self.mcap,
            "entry_time": self.entry_time,
            "hold_time_min": round(self.hold_time_min, 1),
            "last_update": self.last_update,
            "peak_pnl": round(self.peak_pnl, 3),
            "trough_pnl": round(self.trough_pnl, 3),
        }


@dataclass
class ClosedTrade:
    """Закрытая сделка в истории"""
    id: int
    base: str
    exchange_name: str
    entry_price: float
    close_price: float
    pnl_pct: float
    result: str  # "WIN" или "LOSS"
    score: int
    entry_time: float
    close_time: float
    hold_time_min: float

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "base": self.base,
            "exchange_name": self.exchange_name,
            "entry_price": self.entry_price,
            "close_price": self.close_price,
            "pnl_pct": round(self.pnl_pct, 3),
            "result": self.result,
            "score": self.score,
            "entry_time": self.entry_time,
            "close_time": self.close_time,
            "hold_time_min": round(self.hold_time_min, 1),
        }


class SignalTracker:
    """
    Трекер с авто-закрытием на ±10%.
    Сигналы живут пока P&L не достигнет порога.
    """

    TP_PCT = 10.0   # Take Profit %
    SL_PCT = -10.0  # Stop Loss %

    def __init__(self):
        self._active: List[TrackedSignal] = []
        self._history: List[ClosedTrade] = []
        self._counter = 0
        self._total_signals = 0
        self._load_history()

    def add_signal(self, signal) -> Optional[TrackedSignal]:
        # Дедупликация: если эта монета уже в активных — игнорируем
        if any(ts.base == signal.base for ts in self._active):
            logger.debug(f"⚠️ {signal.base} уже отслеживается, пропускаю дубликат.")
            return None

        self._counter += 1
        self._total_signals += 1

        # Цели из сканера
        targets = signal.get_targets()
        
        # Начальный стоп: -4%
        sl_price = signal.futures_price * 0.96

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
            targets=targets,
            sl_price=sl_price
        )

        self._active.append(ts)
        logger.info(
            f"📌 Трекинг #{ts.id}: {ts.base} @ ${ts.entry_price:.6g} | "
            f"SL: ${ts.sl_price:.6g} | TP1: +{targets['conservative']['pct']}%"
        )
        return ts

    def clear_all(self):
        """Полная очистка: активные, история, файлы"""
        logger.warning("🧹 Полная очистка дашборда...")
        self._active.clear()
        self._history.clear()
        self._counter = 0
        self._total_signals = 0
        
        if os.path.exists(HISTORY_FILE):
            try:
                os.remove(HISTORY_FILE)
                logger.info("🗑️ Файл истории удален.")
            except Exception as e:
                logger.error(f"❌ Ошибка удаления истории: {e}")
        
        self._save_history()

    def update_prices(self, price_map: Dict[str, float]) -> List[ClosedTrade]:
        """
        Обновить цены и проверить TP/SL (Умный риск-менеджмент)
        """
        now = time.time()
        newly_closed = []

        for ts in self._active:
            price = price_map.get(ts.symbol) or price_map.get(ts.base)
            if price and price > 0:
                ts.current_price = price
                ts.last_update = now

                pnl = ts.unrealized_pnl_pct
                ts.peak_pnl = max(ts.peak_pnl, pnl)
                ts.trough_pnl = min(ts.trough_pnl, pnl)

                # --- ЛОГИКА ВЫХОДА ---
                
                # 1. TP1 (Conservative): Фикс 50% и Стоп в БУ (+0.5%)
                if ts.stage == 0 and price >= ts.targets["conservative"]["price"]:
                    profit = pnl * 0.5
                    ts.realized_pnl += profit
                    ts.position_size = 0.5
                    ts.sl_price = ts.entry_price * 1.005  # BE
                    ts.stage = 1
                    logger.info(f"🚀 #{ts.id} {ts.base}: TP1 Hit (+{pnl:.1f}%). Фикс 50%, SL в БУ.")

                # 2. TP2 (Moderate): Фикс еще 25% (итого 75%) и Стоп в TP1
                elif ts.stage == 1 and price >= ts.targets["moderate"]["price"]:
                    profit = pnl * 0.25
                    ts.realized_pnl += profit
                    ts.position_size = 0.25
                    ts.sl_price = ts.targets["conservative"]["price"]
                    ts.stage = 2
                    logger.info(f"🔥 #{ts.id} {ts.base}: TP2 Hit (+{pnl:.1f}%). Фикс 25%, SL подтянут.")

                # 3. TP3 (Aggressive): Полный фикс
                elif ts.stage == 2 and price >= ts.targets["aggressive"]["price"]:
                    ts.realized_pnl += pnl * 0.25
                    ts.position_size = 0
                    closed = self._close_trade(ts, "WIN (TP3)")
                    newly_closed.append(closed)

                # 4. Stop Loss (Стоп может быть динамическим)
                elif price <= ts.sl_price:
                    res = "WIN (SL-Trailing)" if ts.total_pnl_pct > 0 else "LOSS"
                    closed = self._close_trade(ts, res)
                    newly_closed.append(closed)

        # Удаляем закрытые
        if newly_closed:
            closed_ids = {c.id for c in newly_closed}
            self._active = [s for s in self._active if s.id not in closed_ids]
            self._save_history()

        return newly_closed

    def _close_trade(self, ts: TrackedSignal, result: str) -> ClosedTrade:
        now = time.time()
        total_pnl = ts.total_pnl_pct
        
        closed = ClosedTrade(
            id=ts.id,
            base=ts.base,
            exchange_name=ts.exchange_name,
            entry_price=ts.entry_price,
            close_price=ts.current_price,
            pnl_pct=total_pnl,
            result=result,
            score=ts.score,
            entry_time=ts.entry_time,
            close_time=now,
            hold_time_min=ts.hold_time_min,
        )
        self._history.append(closed)

        # Эмодзи в логах
        emoji = "🟢" if total_pnl > 0 else "🔴"
        logger.info(
            f"{emoji} Закрыт #{ts.id} {ts.base}: {result} | "
            f"Итого P&L: {total_pnl:+.2f}% | Мин: {ts.trough_pnl:.1f}%"
        )
        return closed

    def get_active(self) -> List[Dict]:
        return [ts.to_dict() for ts in sorted(self._active, key=lambda s: s.entry_time, reverse=True)]

    def get_history(self, limit: int = 50) -> List[Dict]:
        return [c.to_dict() for c in reversed(self._history[-limit:])]

    def get_summary(self) -> Dict:
        active_pnls = [s.total_pnl_pct for s in self._active]
        wins = sum(1 for c in self._history if c.pnl_pct > 0)
        losses = sum(1 for c in self._history if c.pnl_pct <= 0)
        total_closed = wins + losses
        winrate = (wins / total_closed * 100) if total_closed > 0 else 0

        avg_active = (sum(active_pnls) / len(active_pnls)) if active_pnls else 0
        best_active = max(active_pnls) if active_pnls else 0
        worst_active = min(active_pnls) if active_pnls else 0

        # Средний P&L закрытых
        closed_pnls = [c.pnl_pct for c in self._history]
        avg_closed = (sum(closed_pnls) / len(closed_pnls)) if closed_pnls else 0

        return {
            "active_count": len(self._active),
            "total_signals": self._total_signals,
            "avg_pnl": round(avg_active, 3),
            "best_pnl": round(best_active, 3),
            "worst_pnl": round(worst_active, 3),
            "wins": wins,
            "losses": losses,
            "winrate": round(winrate, 1),
            "total_closed": total_closed,
            "avg_closed_pnl": round(avg_closed, 3),
        }

    def get_symbols_to_track(self) -> List[str]:
        """Символы для обновления цен"""
        return [ts.symbol for ts in self._active]

    def _save_history(self):
        try:
            data = [c.to_dict() for c in self._history[-200:]]
            with open(HISTORY_FILE, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def _load_history(self):
        try:
            if os.path.exists(HISTORY_FILE):
                with open(HISTORY_FILE) as f:
                    data = json.load(f)
                for d in data:
                    self._history.append(ClosedTrade(**d))
                if self._history:
                    logger.info(f"📜 Загружено {len(self._history)} сделок из истории")
        except Exception:
            pass
