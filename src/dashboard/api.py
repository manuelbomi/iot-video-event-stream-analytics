"""Live dashboard: FastAPI + WebSocket front end for the stream processor.

This service does two things:

1. Subscribes to the same MQTT occupancy topics as the stream processor
   and maintains its own rolling per-zone stats (reusing `StreamProcessor`
   for that -- same Welford/anomaly/heatmap logic, just for display here).
2. Exposes a `/alerts` webhook receiver endpoint, so the *separate*
   stream-processor service can push anomaly alerts here instead of the
   dashboard having to poll it. This mirrors how you'd wire a real
   alerting pipeline: the detector pushes, consumers of the alert don't
   have to ask "anything new?" on a timer.

The browser side connects over WebSocket (`/ws`) and receives a fresh
JSON snapshot roughly once a second: current occupancy per zone, rolling
mean/std, a small time-bucketed heatmap, and the most recent alerts.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from src.processor.stream_processor import StreamProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [dashboard] %(message)s")
logger = logging.getLogger("dashboard")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
BROADCAST_INTERVAL_SECONDS = float(os.environ.get("DASHBOARD_BROADCAST_INTERVAL", "1.0"))

app = FastAPI(title="IoT Video Event Stream Analytics Dashboard")

# The dashboard keeps its own StreamProcessor instance purely to compute
# display statistics from the raw occupancy stream. It intentionally has
# no webhook_url configured -- alert *delivery* is the standalone
# processor service's job; this instance just mirrors the numbers.
processor = StreamProcessor()

# Alerts received via the /alerts webhook from the standalone processor
# service. Kept separate from `processor.alerts` (which would only ever
# fire if this dashboard's own rolling stats happened to cross the
# threshold) so the dashboard always reflects what the real processor saw.
received_alerts: list[dict] = []
MAX_RECEIVED_ALERTS = 200

_connections: set[WebSocket] = set()
_broadcast_task: Optional[asyncio.Task] = None


class AlertPayload(BaseModel):
    zone: str
    timestamp: str
    occupancy: float
    z_score: float
    mean: float
    std: float
    reason: str


def get_combined_snapshot() -> dict:
    snapshot = processor.get_snapshot()
    snapshot["alerts"] = list(reversed(received_alerts[-50:]))
    return snapshot


@app.on_event("startup")
async def on_startup() -> None:
    global _broadcast_task
    try:
        processor.start(host=MQTT_HOST, port=MQTT_PORT, client_id="dashboard", blocking=False)
        logger.info("Dashboard connected to MQTT broker at %s:%s", MQTT_HOST, MQTT_PORT)
    except Exception as exc:  # noqa: BLE001 - dashboard should still serve even if MQTT is down
        logger.error("Could not connect to MQTT broker at startup: %s", exc)
    _broadcast_task = asyncio.create_task(_broadcast_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if _broadcast_task is not None:
        _broadcast_task.cancel()
    processor.stop()


async def _broadcast_loop() -> None:
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL_SECONDS)
        if not _connections:
            continue
        snapshot = get_combined_snapshot()
        stale = set()
        for ws in list(_connections):
            try:
                await ws.send_json(snapshot)
            except Exception:  # noqa: BLE001
                stale.add(ws)
        _connections.difference_update(stale)


@app.post("/alerts")
async def receive_alert(alert: AlertPayload) -> dict:
    """Webhook endpoint the stream processor posts anomaly alerts to."""
    received_alerts.append(alert.model_dump())
    del received_alerts[:-MAX_RECEIVED_ALERTS]
    logger.warning("Received alert: %s", alert.model_dump())
    return {"status": "ok"}


@app.get("/api/snapshot")
async def api_snapshot() -> dict:
    return get_combined_snapshot()


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    _connections.add(websocket)
    try:
        await websocket.send_json(get_combined_snapshot())
        while True:
            # We don't expect the client to send anything, but we need to
            # await something so we notice disconnects promptly.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _connections.discard(websocket)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return _DASHBOARD_HTML


_DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Zone Occupancy Dashboard</title>
<style>
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; background: #0f1117; color: #e6e6e6; }
  h1 { font-size: 1.4rem; margin-bottom: 0.25rem; }
  .sub { color: #9aa0aa; margin-bottom: 1.5rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
  .card { background: #1a1d27; border-radius: 10px; padding: 1rem 1.25rem; border: 1px solid #2a2e3a; }
  .zone-name { font-weight: 600; font-size: 1.1rem; margin-bottom: 0.5rem; }
  .metric { display: flex; justify-content: space-between; font-size: 0.9rem; color: #c7cbd4; padding: 2px 0; }
  .metric b { color: #fff; }
  .anomaly { border-color: #a83244; box-shadow: 0 0 0 1px #a83244 inset; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #2a2e3a; }
  th { color: #9aa0aa; }
  .status { color: #9aa0aa; font-size: 0.8rem; }
</style>
</head>
<body>
<h1>Zone Occupancy Dashboard</h1>
<div class="sub">Live rolling stats and anomaly alerts, updated over WebSocket.</div>
<div id="status" class="status">connecting...</div>
<div id="zones" class="grid"></div>
<h2>Recent alerts</h2>
<table>
  <thead><tr><th>Time</th><th>Zone</th><th>Occupancy</th><th>Z-score</th><th>Reason</th></tr></thead>
  <tbody id="alerts"></tbody>
</table>
<script>
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(proto + "://" + location.host + "/ws");
  const status = document.getElementById("status");
  ws.onopen = () => status.textContent = "live";
  ws.onclose = () => { status.textContent = "disconnected, retrying..."; setTimeout(connect, 2000); };
  ws.onmessage = (event) => render(JSON.parse(event.data));
}

function render(snapshot) {
  const zonesEl = document.getElementById("zones");
  zonesEl.innerHTML = "";
  const zoneNames = Object.keys(snapshot.zones || {}).sort();
  for (const zone of zoneNames) {
    const z = snapshot.zones[zone];
    const latest = z.latest || {};
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <div class="zone-name">${zone}</div>
      <div class="metric"><span>Current occupancy</span><b>${latest.occupancy ?? "-"}</b></div>
      <div class="metric"><span>Rolling mean</span><b>${z.stats.mean}</b></div>
      <div class="metric"><span>Rolling std</span><b>${z.stats.std}</b></div>
      <div class="metric"><span>Samples seen</span><b>${z.stats.count}</b></div>
    `;
    zonesEl.appendChild(card);
  }
  const alertsBody = document.getElementById("alerts");
  alertsBody.innerHTML = "";
  for (const alert of (snapshot.alerts || [])) {
    const row = document.createElement("tr");
    row.innerHTML = `<td>${alert.timestamp}</td><td>${alert.zone}</td><td>${alert.occupancy}</td><td>${alert.z_score}</td><td>${alert.reason}</td>`;
    alertsBody.appendChild(row);
  }
}

connect();
</script>
</body>
</html>
"""
