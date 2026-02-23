"""
oi_history.py — Трекер динамики OI и цен

Хранит снэпшоты OI и цен каждой монеты за последние 15 минут.
Позволяет определить:
  1. Рост OI за N минут (перегрев)
  2. Рост цены за N минут (уже поздно?)
"""
import time
import logging
from collections import defaultdict, deque
from typing import Dict, Optional, Tuple

logger = logging.getLogger("oi_scanner")

# Максимальное время хранения снэпшотов (15 мин)
MAX_HISTORY_SEC = 900


class OIHistory:
    """
    Хранит историю OI и цен для каждого символа.
    Снэпшоты: deque[(timestamp, oi_usd, price)]
    """

    def __init__(self):
        # symbol -> deque[(ts, oi_usd, price)]
        self._data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    def record(self, symbol: str, oi_usd: float, price: float = 0.0):
        """Записать текущий снэпшот OI и цены"""
        now = time.time()
        self._data[symbol].append((now, oi_usd, price))

    def record_batch(self, all_data: Dict[str, Dict]):
        """Записать OI и цены для пачки монет из fetch_all_data"""
        for symbol, d in all_data.items():
            oi = d.get("oi_usd", 0)
            price = d.get("futures_price", 0)
            if oi > 0:
                self.record(symbol, oi, price)

    def get_growth_pct(self, symbol: str, window_sec: int = 600) -> Optional[float]:
        """
        Вернуть % роста OI за последние window_sec секунд.
        None если недостаточно данных (нет старых записей).
        """
        snapshots = self._data.get(symbol)
        if not snapshots or len(snapshots) < 2:
            return None

        now = time.time()
        current_oi = snapshots[-1][1]  # последний OI

        # Ищем самый старый снэпшот в пределах окна
        oldest_in_window = None
        for ts, oi, price in snapshots:
            age = now - ts
            if age <= window_sec:
                oldest_in_window = (ts, oi, price)
                break  # первый в пределах окна = самый старый

        if oldest_in_window is None:
            return None

        old_oi = oldest_in_window[1]
        if old_oi <= 0:
            return None

        # Минимальная "возрастность" данных — хотя бы 3 минуты истории
        age = now - oldest_in_window[0]
        if age < 180:  # 3 мин
            return None

        growth = ((current_oi - old_oi) / old_oi) * 100
        return growth

    def get_price_growth_pct(self, symbol: str, window_sec: int = 600) -> Optional[float]:
        """
        Вернуть % роста ЦЕНЫ за последние window_sec секунд.
        Используется для проверки "не опоздали ли мы" — если цена
        уже выросла на 5%+, то входить поздно.
        None если недостаточно данных.
        """
        snapshots = self._data.get(symbol)
        if not snapshots or len(snapshots) < 2:
            return None

        now = time.time()
        current_price = snapshots[-1][2]  # последняя цена
        if current_price <= 0:
            return None

        # Ищем самый старый снэпшот в пределах окна
        oldest_in_window = None
        for ts, oi, price in snapshots:
            age = now - ts
            if age <= window_sec:
                oldest_in_window = (ts, oi, price)
                break

        if oldest_in_window is None:
            return None

        old_price = oldest_in_window[2]
        if old_price <= 0:
            return None

        age = now - oldest_in_window[0]
        if age < 180:  # 3 мин
            return None

        growth = ((current_price - old_price) / old_price) * 100
        return growth

    def cleanup(self):
        """Удалить записи старше MAX_HISTORY_SEC"""
        now = time.time()
        cutoff = now - MAX_HISTORY_SEC
        empty_keys = []

        for symbol, snapshots in self._data.items():
            while snapshots and snapshots[0][0] < cutoff:
                snapshots.popleft()
            if not snapshots:
                empty_keys.append(symbol)

        for k in empty_keys:
            del self._data[k]

    def get_stats(self) -> Dict:
        """Статистика для диагностики"""
        total_symbols = len(self._data)
        total_snapshots = sum(len(d) for d in self._data.values())
        return {
            "tracked_symbols": total_symbols,
            "total_snapshots": total_snapshots,
        }
