"""
dashboard.py — Мини веб-дашборд с историей и винрейтом
aiohttp сервер, авто-обновление каждые 3с
"""
import logging
from typing import Optional

from aiohttp import web

logger = logging.getLogger("oi_scanner")


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OI Scanner Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'JetBrains Mono', monospace;
    background: #0a0a0f;
    color: #e0e0e0;
    min-height: 100vh;
    padding: 16px;
  }
  .header {
    text-align: center; padding: 16px 0;
    border-bottom: 1px solid #1a1a2e; margin-bottom: 16px;
  }
  .header h1 {
    font-size: 1.4em;
    background: linear-gradient(135deg, #667eea, #764ba2);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .subtitle { color: #555; font-size: 0.7em; margin-top: 4px; }
  .pulse {
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    background: #00e676; margin-right: 5px;
    animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  /* Summary */
  .summary {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
    gap: 8px; margin-bottom: 16px;
  }
  .card {
    background: #12121e; border: 1px solid #1a1a2e;
    border-radius: 8px; padding: 12px; text-align: center;
  }
  .card .label { font-size: 0.6em; color: #555; text-transform: uppercase; letter-spacing: 1px; }
  .card .value { font-size: 1.3em; font-weight: 700; margin-top: 4px; }
  .green { color: #00e676; }
  .red { color: #ff5252; }
  .purple { color: #b388ff; }
  .yellow { color: #ffd740; }
  .white { color: #fff; }

  /* Section headers */
  .section-title {
    font-size: 0.85em; font-weight: 700; color: #888;
    margin: 16px 0 8px; padding-bottom: 6px;
    border-bottom: 1px solid #1a1a2e;
    text-transform: uppercase; letter-spacing: 2px;
  }

  /* Tables */
  .tbl-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.75em; }
  thead th {
    background: #12121e; color: #666; font-weight: 600;
    text-transform: uppercase; letter-spacing: 1px; font-size: 0.7em;
    padding: 10px 8px; text-align: left;
    border-bottom: 2px solid #1a1a2e; position: sticky; top: 0;
  }
  tbody tr { border-bottom: 1px solid #111; transition: background 0.2s; }
  tbody tr:hover { background: #151525; }
  td { padding: 10px 8px; white-space: nowrap; }
  .coin { font-weight: 700; color: #fff; }
  .exch { background: #1a1a2e; border-radius: 4px; padding: 2px 6px; font-size: 0.75em; color: #888; }
  .pnl { font-weight: 700; font-size: 1.05em; }
  .badge {
    display: inline-block; padding: 2px 7px; border-radius: 5px;
    font-weight: 700; font-size: 0.8em;
  }
  .badge-win { background: #1b3a1b; color: #00e676; }
  .badge-loss { background: #3a1b1b; color: #ff5252; }
  .badge-score-h { background: #1b3a1b; color: #00e676; }
  .badge-score-m { background: #3a3a1b; color: #ffd740; }
  .badge-score-l { background: #3a1b1b; color: #ff5252; }
  .time { color: #555; font-size: 0.85em; }

  /* Progress bar for P&L */
  .pnl-bar {
    width: 80px; height: 6px; background: #1a1a2e;
    border-radius: 3px; overflow: hidden; display: inline-block;
    vertical-align: middle; margin-left: 6px;
  }
  .pnl-fill {
    height: 100%; border-radius: 3px; transition: width 0.3s;
  }

  .no-data { text-align: center; padding: 40px 20px; color: #333; font-size: 1em; }

  .footer {
    text-align: center; color: #222; font-size: 0.6em;
    margin-top: 16px; padding-top: 12px; border-top: 1px solid #111;
  }

  @media (max-width: 600px) {
    body { padding: 8px; }
    .card .value { font-size: 1em; }
    table { font-size: 0.65em; }
    td, th { padding: 6px 4px; }
  }
</style>
</head>
<body>

<div class="header">
  <h1>💊 OI Scanner Dashboard</h1>
  <div class="subtitle"><span class="pulse"></span>Live Demo Trading • TP: +10% / SL: -10%</div>
</div>

<div class="summary" id="summary">
  <div class="card"><div class="label">Активных</div><div class="value purple" id="s-active">-</div></div>
  <div class="card"><div class="label">Средний P&L</div><div class="value" id="s-avg">-</div></div>
  <div class="card"><div class="label">Лучший</div><div class="value green" id="s-best">-</div></div>
  <div class="card"><div class="label">Худший</div><div class="value red" id="s-worst">-</div></div>
  <div class="card"><div class="label">Winrate</div><div class="value yellow" id="s-wr">-</div></div>
  <div class="card"><div class="label">W / L</div><div class="value white" id="s-wl">-</div></div>
</div>

<div class="section-title">📊 Активные позиции</div>
<div class="tbl-wrap">
<table>
  <thead><tr>
    <th>#</th><th>Монета</th><th>Биржа</th><th>Вход</th><th>Текущая</th>
    <th>P&L</th><th>Прогресс</th><th>Score</th><th>Время</th>
  </tr></thead>
  <tbody id="active-body">
    <tr><td colspan="9" class="no-data">Ожидание сигналов...</td></tr>
  </tbody>
</table>
</div>

<div class="section-title">📜 История (закрытые ±10%)</div>
<div class="tbl-wrap">
<table>
  <thead><tr>
    <th>#</th><th>Монета</th><th>Биржа</th><th>Вход</th><th>Закрытие</th>
    <th>P&L</th><th>Результат</th><th>Score</th><th>Длительность</th>
  </tr></thead>
  <tbody id="history-body">
    <tr><td colspan="9" class="no-data">Пока нет закрытых сделок</td></tr>
  </tbody>
</table>
</div>

<div class="footer">OI Scanner Bot • Auto TP +10% / SL -10% • Обновление каждые 3с</div>

<script>
const FP = p => !p ? '-' : p >= 1 ? '$'+p.toFixed(4) : '$'+p.toPrecision(4);
const FPNL = p => (p>=0?'+':'')+p.toFixed(2)+'%';
const FT = m => m<60 ? Math.floor(m)+'м' : Math.floor(m/60)+'ч '+Math.floor(m%60)+'м';
const SC = s => s>=70?'badge-score-h':s>=50?'badge-score-m':'badge-score-l';
const PC = p => p>=0?'green':'red';

function pnlBar(pnl) {
  // -10% to +10% → 0-100%
  const pct = Math.min(100, Math.max(0, (pnl + 10) / 20 * 100));
  const color = pnl >= 0 ? '#00e676' : '#ff5252';
  return `<div class="pnl-bar"><div class="pnl-fill" style="width:${pct}%;background:${color}"></div></div>`;
}

async function fetchData() {
  try {
    const r = await fetch('/api/signals');
    const d = await r.json();
    const sm = d.summary;

    document.getElementById('s-active').textContent = sm.active_count;
    const avgEl = document.getElementById('s-avg');
    avgEl.textContent = FPNL(sm.avg_pnl); avgEl.className = 'value ' + PC(sm.avg_pnl);
    document.getElementById('s-best').textContent = FPNL(sm.best_pnl);
    document.getElementById('s-worst').textContent = FPNL(sm.worst_pnl);
    document.getElementById('s-wr').textContent = sm.winrate + '%';
    document.getElementById('s-wl').textContent = sm.wins + ' / ' + sm.losses;

    // Active
    const ab = document.getElementById('active-body');
    if (!d.active || !d.active.length) {
      ab.innerHTML = '<tr><td colspan="9" class="no-data">Ожидание сигналов...</td></tr>';
    } else {
      ab.innerHTML = d.active.map(s =>
        `<tr>
          <td>${s.id}</td>
          <td><span class="coin">${s.base}</span></td>
          <td><span class="exch">${s.exchange_name}</span></td>
          <td>${FP(s.entry_price)}</td>
          <td>${FP(s.current_price)}</td>
          <td class="pnl ${PC(s.pnl_pct)}">${FPNL(s.pnl_pct)}</td>
          <td>${pnlBar(s.pnl_pct)}</td>
          <td><span class="badge ${SC(s.score)}">${s.score}</span></td>
          <td class="time">${FT(s.hold_time_min)}</td>
        </tr>`
      ).join('');
    }

    // History
    const hb = document.getElementById('history-body');
    if (!d.history || !d.history.length) {
      hb.innerHTML = '<tr><td colspan="9" class="no-data">Пока нет закрытых сделок</td></tr>';
    } else {
      hb.innerHTML = d.history.map(s =>
        `<tr>
          <td>${s.id}</td>
          <td><span class="coin">${s.base}</span></td>
          <td><span class="exch">${s.exchange_name}</span></td>
          <td>${FP(s.entry_price)}</td>
          <td>${FP(s.close_price)}</td>
          <td class="pnl ${PC(s.pnl_pct)}">${FPNL(s.pnl_pct)}</td>
          <td><span class="badge ${s.result==='WIN'?'badge-win':'badge-loss'}">${s.result}</span></td>
          <td><span class="badge ${SC(s.score)}">${s.score}</span></td>
          <td class="time">${FT(s.hold_time_min)}</td>
        </tr>`
      ).join('');
    }
  } catch(e) { console.error(e); }
}

fetchData();
setInterval(fetchData, 3000);
</script>
</body>
</html>"""


class Dashboard:
    def __init__(self, tracker, host="0.0.0.0", port=8085):
        self.tracker = tracker
        self.host = host
        self.port = port
        self.app = web.Application()
        self._runner = None
        self.app.router.add_get("/", self._page)
        self.app.router.add_get("/api/signals", self._api)

    async def start(self):
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info(f"📊 Dashboard: http://{self.host}:{self.port}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _page(self, req):
        return web.Response(text=HTML_PAGE, content_type="text/html")

    async def _api(self, req):
        return web.json_response({
            "active": self.tracker.get_active(),
            "history": self.tracker.get_history(),
            "summary": self.tracker.get_summary(),
        })
