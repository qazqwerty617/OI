"""
oi_history.py — Трекер динамики OI, цен и объёмов

Хранит снэпшоты каждой монеты за последние 15 минут.
Определяет:
  1. Рост OI за N минут (перегрев)
  2. Рост цены за N минут (уже поздно?)
  3. Рост volume за N минут (подтверждение ракеты)
  4. Rocket Score — комбинация всех факторов
"""
import time
import logging
from collections import defaultdict, deque
from typing import Dict, Optional

logger = logging.getLogger("oi_scanner")

# Максимальное время хранения снэпшотов (15 мин)
MAX_HISTORY_SEC = 900


class OIHistory:
    """
    Хранит историю OI, цен и объёмов для каждого символа.
    Снэпшоты: deque[(timestamp, oi_usd, price, volume)]
    """

    def __init__(self):
        # symbol -> deque[(ts, oi_usd, price, volume)]
        self._data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    def record(self, symbol: str, oi_usd: float, price: float = 0.0, volume: float = 0.0):
        """Записать текущий снэпшот"""
        now = time.time()
        self._data[symbol].append((now, oi_usd, price, volume))

    def record_batch(self, all_data: Dict[str, Dict]):
        """Записать данные для пачки монет из fetch_all_data"""
        for symbol, d in all_data.items():
            oi = d.get("oi_usd", 0)
            price = d.get("futures_price", 0)
            volume = d.get("volume_24h", 0)
            if oi > 0:
                self.record(symbol, oi, price, volume)

    def _get_oldest_in_window(self, symbol: str, window_sec: int):
        """Найти самый старый снэпшот в пределах окна с минимум 3 мин истории"""
        snapshots = self._data.get(symbol)
        if not snapshots or len(snapshots) < 2:
            return None, None

        now = time.time()
        current = snapshots[-1]

        # Ищем самый старый снэпшот в пределах окна
        oldest = None
        for snap in snapshots:
            age = now - snap[0]
            if age <= window_sec:
                oldest = snap
                break

        if oldest is None:
            return None, None

        # Минимум 3 минуты истории
        age = now - oldest[0]
        if age < 180:
            return None, None

        return oldest, current

    def get_growth_pct(self, symbol: str, window_sec: int = 600) -> Optional[float]:
        """% роста OI за window_sec секунд"""
        oldest, current = self._get_oldest_in_window(symbol, window_sec)
        if oldest is None or current is None:
            return None

        old_oi = oldest[1]
        if old_oi <= 0:
            return None

        return ((current[1] - old_oi) / old_oi) * 100

    def get_price_growth_pct(self, symbol: str, window_sec: int = 600) -> Optional[float]:
        """% роста ЦЕНЫ за window_sec секунд"""
        oldest, current = self._get_oldest_in_window(symbol, window_sec)
        if oldest is None or current is None:
            return None

        old_price = oldest[2]
        if old_price <= 0 or current[2] <= 0:
            return None

        return ((current[2] - old_price) / old_price) * 100

    def get_volume_growth_pct(self, symbol: str, window_sec: int = 600) -> Optional[float]:
        """% роста VOLUME за window_sec секунд"""
        oldest, current = self._get_oldest_in_window(symbol, window_sec)
        if oldest is None or current is None:
            return None

        old_vol = oldest[3]
        if old_vol <= 0 or current[3] <= 0:
            return None

        return ((current[3] - old_vol) / old_vol) * 100

    def get_rocket_score(self, symbol: str, oi_growth: Optional[float],
                          price_growth: Optional[float],
                          funding_rate: Optional[float]) -> int:
        """
        Rocket Score 0-100: комбинация факторов шорт-сквиза.

        Механика ракеты:
          - OI растёт быстро (шорты заходят)
          - Фандинг сильно отрицательный (шорты платят)
          - Цена ещё на месте или чуть вниз (пружина сжата)
          - Объём прёт (интерес рынка)

        Чем больше факторов совпадает, тем сильнее ракета.
        """
        score = 0

        # 1. OI рост (0-35 баллов)
        if oi_growth is not None:
            if oi_growth >= 80:
                score += 35
            elif oi_growth >= 50:
                score += 30
            elif oi_growth >= 35:
                score += 25
            elif oi_growth >= 25:
                score += 20
            elif oi_growth >= 15:
                score += 10

        # 2. Негативный фандинг = шорты (0-30 баллов)
        if funding_rate is not None:
            if funding_rate <= -0.5:
                score += 30  # Экстрим — шорты ДИКО платят
            elif funding_rate <= -0.2:
                score += 25
            elif funding_rate <= -0.1:
                score += 20
            elif funding_rate <= -0.05:
                score += 15
            elif funding_rate <= -0.01:
                score += 10

        # 3. Цена на месте/чуть вниз = пружина (0-20 баллов)
        if price_growth is not None:
            if -2.0 <= price_growth <= 0.5:
                score += 20  # Идеал: цена стоит или чуть просела
            elif -3.0 <= price_growth <= 1.0:
                score += 15
            elif -5.0 <= price_growth <= 2.0:
                score += 10
            elif price_growth <= -5.0:
                score += 5  # Слишком сильно упала — опасно

        # 4. Volume рост (0-15 баллов)
        vol_growth = self.get_volume_growth_pct(symbol)
        if vol_growth is not None:
            if vol_growth >= 30:
                score += 15
            elif vol_growth >= 15:
                score += 10
            elif vol_growth >= 5:
                score += 5

        return min(100, score)

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
