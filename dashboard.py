"""
dashboard.py — Мини веб-дашборд
aiohttp сервер на порту 8080
"""
import json
import logging
from typing import Optional

from aiohttp import web

logger = logging.getLogger("oi_scanner")


HTML_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>💊 OI Scanner — Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: 'JetBrains Mono', monospace;
    background: #0a0a0f;
    color: #e0e0e0;
    min-height: 100vh;
    padding: 20px;
  }

  .header {
    text-align: center;
    padding: 20px 0;
    border-bottom: 1px solid #1a1a2e;
    margin-bottom: 20px;
  }

  .header h1 {
    font-size: 1.5em;
    background: linear-gradient(135deg, #667eea, #764ba2);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }

  .header .subtitle {
    color: #666;
    font-size: 0.75em;
    margin-top: 5px;
  }

  /* Summary cards */
  .summary {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 10px;
    margin-bottom: 20px;
  }

  .card {
    background: #12121e;
    border: 1px solid #1a1a2e;
    border-radius: 10px;
    padding: 15px;
    text-align: center;
  }

  .card .label {
    font-size: 0.65em;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 1px;
  }

  .card .value {
    font-size: 1.4em;
    font-weight: 700;
    margin-top: 5px;
  }

  .green { color: #00e676; }
  .red { color: #ff5252; }
  .purple { color: #b388ff; }
  .yellow { color: #ffd740; }

  /* Signals table */
  .signals-container {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.8em;
  }

  thead th {
    background: #12121e;
    color: #888;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-size: 0.7em;
    padding: 12px 10px;
    text-align: left;
    border-bottom: 2px solid #1a1a2e;
    position: sticky;
    top: 0;
  }

  tbody tr {
    border-bottom: 1px solid #111;
    transition: background 0.2s;
  }

  tbody tr:hover {
    background: #151525;
  }

  td {
    padding: 12px 10px;
    white-space: nowrap;
  }

  .coin-name {
    font-weight: 700;
    color: #fff;
    font-size: 1em;
  }

  .exchange-tag {
    background: #1a1a2e;
    border-radius: 4px;
    padding: 2px 6px;
    font-size: 0.75em;
    color: #888;
  }

  .pnl-cell {
    font-weight: 700;
    font-size: 1.1em;
  }

  .score-badge {
    display: inline-block;
    padding: 3px 8px;
    border-radius: 6px;
    font-weight: 700;
    font-size: 0.85em;
  }

  .score-high { background: #1b3a1b; color: #00e676; }
  .score-mid { background: #3a3a1b; color: #ffd740; }
  .score-low { background: #3a1b1b; color: #ff5252; }

  .hold-time {
    color: #666;
    font-size: 0.85em;
  }

  .pulse {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #00e676;
    margin-right: 5px;
    animation: pulse 2s infinite;
  }

  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
  }

  .footer {
    text-align: center;
    color: #333;
    font-size: 0.65em;
    margin-top: 20px;
    padding-top: 15px;
    border-top: 1px solid #111;
  }

  .no-signals {
    text-align: center;
    padding: 60px 20px;
    color: #444;
    font-size: 1.1em;
  }

  @media (max-width: 600px) {
    body { padding: 10px; }
    .card .value { font-size: 1.1em; }
    table { font-size: 0.7em; }
    td, th { padding: 8px 5px; }
  }
</style>
</head>
<body>

<div class="header">
  <h1>💊 OI Scanner Dashboard</h1>
  <div class="subtitle"><span class="pulse"></span> Live Demo Trading</div>
</div>

<div class="summary" id="summary">
  <div class="card">
    <div class="label">Активных</div>
    <div class="value purple" id="s-active">-</div>
  </div>
  <div class="card">
    <div class="label">Средний P&L</div>
    <div class="value" id="s-avg">-</div>
  </div>
  <div class="card">
    <div class="label">Лучший</div>
    <div class="value green" id="s-best">-</div>
  </div>
  <div class="card">
    <div class="label">Худший</div>
    <div class="value red" id="s-worst">-</div>
  </div>
  <div class="card">
    <div class="label">В плюсе</div>
    <div class="value green" id="s-profit">-</div>
  </div>
  <div class="card">
    <div class="label">В минусе</div>
    <div class="value red" id="s-loss">-</div>
  </div>
</div>

<div class="signals-container">
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>Монета</th>
        <th>Биржа</th>
        <th>Вход</th>
        <th>Текущая</th>
        <th>P&L</th>
        <th>Score</th>
        <th>OI/MCap</th>
        <th>Время</th>
      </tr>
    </thead>
    <tbody id="signals-body">
      <tr><td colspan="9" class="no-signals">Ожидание сигналов...</td></tr>
    </tbody>
  </table>
</div>

<div class="footer">
  OI Scanner Bot • Demo Trading • Обновление каждые 5с
</div>

<script>
function formatPrice(p) {
  if (!p) return '-';
  return p >= 1 ? '$' + p.toFixed(4) : '$' + p.toPrecision(4);
}

function formatPnl(pnl) {
  const sign = pnl >= 0 ? '+' : '';
  return sign + pnl.toFixed(3) + '%';
}

function formatTime(min) {
  if (min < 60) return Math.floor(min) + 'м';
  const h = Math.floor(min / 60);
  const m = Math.floor(min % 60);
  return h + 'ч ' + m + 'м';
}

function scoreClass(s) {
  if (s >= 70) return 'score-high';
  if (s >= 50) return 'score-mid';
  return 'score-low';
}

function pnlClass(p) {
  return p >= 0 ? 'green' : 'red';
}

async function fetchData() {
  try {
    const resp = await fetch('/api/signals');
    const data = await resp.json();

    // Summary
    const sm = data.summary;
    document.getElementById('s-active').textContent = sm.active_count;
    document.getElementById('s-avg').textContent = formatPnl(sm.avg_pnl);
    document.getElementById('s-avg').className = 'value ' + pnlClass(sm.avg_pnl);
    document.getElementById('s-best').textContent = formatPnl(sm.best_pnl);
    document.getElementById('s-worst').textContent = formatPnl(sm.worst_pnl);
    document.getElementById('s-profit').textContent = sm.profitable;
    document.getElementById('s-loss').textContent = sm.losing;

    // Signals table
    const tbody = document.getElementById('signals-body');
    const signals = data.signals;

    if (!signals || signals.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" class="no-signals">Ожидание сигналов...</td></tr>';
      return;
    }

    let html = '';
    for (const s of signals) {
      const pnlCls = pnlClass(s.pnl_pct);
      html += '<tr>' +
        '<td>' + s.id + '</td>' +
        '<td><span class="coin-name">' + s.base + '</span></td>' +
        '<td><span class="exchange-tag">' + s.exchange_name + '</span></td>' +
        '<td>' + formatPrice(s.entry_price) + '</td>' +
        '<td>' + formatPrice(s.current_price) + '</td>' +
        '<td class="pnl-cell ' + pnlCls + '">' + formatPnl(s.pnl_pct) + '</td>' +
        '<td><span class="score-badge ' + scoreClass(s.score) + '">' + s.score + '</span></td>' +
        '<td>' + s.oi_mcap_ratio + '%</td>' +
        '<td class="hold-time">' + formatTime(s.hold_time_min) + '</td>' +
        '</tr>';
    }
    tbody.innerHTML = html;

  } catch (err) {
    console.error('Fetch error:', err);
  }
}

// Initial + auto-refresh every 5 seconds
fetchData();
setInterval(fetchData, 5000);
</script>
</body>
</html>"""


class Dashboard:
    """Мини веб-дашборд на aiohttp"""

    def __init__(self, tracker, host: str = "0.0.0.0", port: int = 8080):
        self.tracker = tracker
        self.host = host
        self.port = port
        self.app = web.Application()
        self._runner: Optional[web.AppRunner] = None

        self.app.router.add_get("/", self._handle_page)
        self.app.router.add_get("/api/signals", self._handle_api)

    async def start(self):
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info(f"📊 Dashboard: http://{self.host}:{self.port}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _handle_page(self, request: web.Request) -> web.Response:
        return web.Response(text=HTML_PAGE, content_type="text/html")

    async def _handle_api(self, request: web.Request) -> web.Response:
        data = {
            "signals": self.tracker.get_all(),
            "summary": self.tracker.get_summary(),
        }
        return web.json_response(data)
