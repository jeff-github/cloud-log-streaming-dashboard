#!/usr/bin/env python3
"""
Portal Server Log Monitor (Cloud Run)

- Polls Cloud Run service logs via gcloud
- Parses JSON output
- Serves a tiny web UI with:
  - Live stream (SSE)
  - Counts for Errors/Warnings/Info
  - Filters to show only Error/Warning
  - Click-to-expand details

Requirements:
- Python 3.9+
- gcloud CLI installed + authenticated
    gcloud auth login
    gcloud config set project example-project   (optional; script also passes --project)
    gcloud run services list --region europe-west9

Run:
  python monitor_portal_server.py --service portal-server --project example-project --region europe-west9

Then open:
  http://127.0.0.1:8787
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import shutil
import platform
from pathlib import Path
import threading
import time
from collections import deque
from dataclasses import dataclass
from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Any, Deque, Dict, List, Optional, Tuple


DEFAULT_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Portal Server Monitor</title>
  <style>
    :root { color-scheme: dark; }
    body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background:#0b0f14; color:#e6edf3; }
    header { padding: 14px 16px; border-bottom: 1px solid #1f2a37; display:flex; gap:14px; align-items:center; flex-wrap:wrap; }
    .pill { border:1px solid #2a3a4d; border-radius:999px; padding:6px 10px; font-size:13px; background:#0f1620; }
    .pill strong { font-weight:700; }
    .controls { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-left:auto; }
    label { display:flex; gap:8px; align-items:center; font-size:13px; color:#cbd5e1; }
    input[type="checkbox"] { transform: scale(1.05); }
    main { display:grid; grid-template-columns: 1.2fr 0.8fr; gap: 12px; padding: 12px; }
    @media (max-width: 1100px) { main { grid-template-columns: 1fr; } }
    .card { border:1px solid #1f2a37; border-radius: 14px; background:#0f1620; overflow:hidden; }
    .card h2 { margin:0; padding:10px 12px; font-size:14px; border-bottom:1px solid #1f2a37; color:#cbd5e1; letter-spacing:0.2px; }
    table { width:100%; border-collapse: collapse; }
    thead th { position:sticky; top:0; background:#0f1620; z-index:1; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #17212f; font-size:12px; vertical-align:top; }
    tr:hover { background:#0c2238; cursor:pointer; }
    .sev-ERROR { color:#ff6b6b; font-weight:700; }
    .sev-WARNING { color:#ffd166; font-weight:700; }
    .sev-INFO { color:#8ecae6; }
    .sev-DEFAULT { color:#94a3b8; }
    .muted { color:#94a3b8; }
    .detail { padding: 10px 12px; white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size:12px; }
    .detail .k { color:#8ecae6; }
    .detail .v { color:#e6edf3; }
    .statusbar { padding: 8px 12px; border-top:1px solid #1f2a37; display:flex; gap:10px; align-items:center; justify-content:space-between; font-size:12px; color:#94a3b8; }
    .dot { width:8px; height:8px; border-radius:50%; display:inline-block; background:#94a3b8; margin-right:6px; }
    .dot.live { background:#22c55e; box-shadow: 0 0 10px rgba(34,197,94,0.25); }
    .nowrap { white-space:nowrap; }
    .btn { border:1px solid #2a3a4d; background:#0b1220; color:#e6edf3; padding:6px 10px; border-radius:10px; font-size:12px; }
    .btn:hover { background:#0c2238; cursor:pointer; }
    .search { border:1px solid #2a3a4d; background:#0b1220; color:#e6edf3; padding:6px 10px; border-radius:10px; font-size:12px; min-width: 260px; }
  </style>
</head>
<body>
  <header>
    <div class="pill">Service: <strong id="svc">portal-server</strong></div>
    <div class="pill">Region: <strong id="region">europe-west9</strong></div>
    <div class="pill">Project: <strong id="project">example-project</strong></div>

    <div class="pill">Errors: <strong id="cntErr">0</strong></div>
    <div class="pill">Warnings: <strong id="cntWarn">0</strong></div>
    <div class="pill">Info: <strong id="cntInfo">0</strong></div>

    <div class="controls">
      <label><input id="fErr" type="checkbox" checked /> Errors</label>
      <label><input id="fWarn" type="checkbox" checked /> Warnings</label>
      <label><input id="fInfo" type="checkbox" /> Info</label>
      <input id="search" class="search" placeholder="filter text (message, route, userId, etc.)" />
      <button id="clear" class="btn" title="Clear current table (does not affect counting)">Clear</button>
    </div>
  </header>

  <main>
    <section class="card">
      <h2>Live View (click a row to inspect)</h2>
      <div style="max-height: 72vh; overflow:auto;">
        <table>
          <thead>
            <tr>
              <th class="nowrap">Time (UTC)</th>
              <th class="nowrap">Severity</th>
              <th>Message</th>
              <th class="nowrap">Logger</th>
            </tr>
          </thead>
          <tbody id="rows"></tbody>
        </table>
      </div>
      <div class="statusbar">
        <div><span id="dot" class="dot"></span><span id="status">connecting…</span></div>
        <div class="nowrap"><span class="muted">Last event:</span> <span id="last">—</span></div>
      </div>
    </section>

    <aside class="card">
      <h2>Selected Event Details</h2>
      <div id="detail" class="detail muted">Click a log row to inspect details.</div>
    </aside>
  </main>

<script>
(() => {
  const els = {
    svc: document.getElementById('svc'),
    region: document.getElementById('region'),
    project: document.getElementById('project'),
    cntErr: document.getElementById('cntErr'),
    cntWarn: document.getElementById('cntWarn'),
    cntInfo: document.getElementById('cntInfo'),
    fErr: document.getElementById('fErr'),
    fWarn: document.getElementById('fWarn'),
    fInfo: document.getElementById('fInfo'),
    rows: document.getElementById('rows'),
    detail: document.getElementById('detail'),
    status: document.getElementById('status'),
    dot: document.getElementById('dot'),
    last: document.getElementById('last'),
    search: document.getElementById('search'),
    clear: document.getElementById('clear')
  };

  const state = {
    buffer: [],
    maxRows: 400,
    connected: false,
    search: ''
  };

  function sevClass(sev) {
    if (sev === 'ERROR' || sev === 'CRITICAL' || sev === 'ALERT' || sev === 'EMERGENCY') return 'sev-ERROR';
    if (sev === 'WARNING') return 'sev-WARNING';
    if (sev === 'INFO' || sev === 'NOTICE') return 'sev-INFO';
    return 'sev-DEFAULT';
  }

  function allowed(sev) {
    if ((sev === 'ERROR' || sev === 'CRITICAL' || sev === 'ALERT' || sev === 'EMERGENCY') && els.fErr.checked) return true;
    if (sev === 'WARNING' && els.fWarn.checked) return true;
    if ((sev === 'INFO' || sev === 'NOTICE' || sev === 'DEFAULT' || sev === 'DEBUG') && els.fInfo.checked) return true;
    return false;
  }

  function matchesSearch(ev) {
    const q = (state.search || '').trim().toLowerCase();
    if (!q) return true;
    const hay = [
      ev.message || '',
      ev.logger || '',
      ev.timestamp || '',
      JSON.stringify(ev.fields || {})
    ].join(' ').toLowerCase();
    return hay.includes(q);
  }

  function renderRows() {
    els.rows.innerHTML = '';
    const slice = state.buffer.slice(-state.maxRows);
    for (const ev of slice) {
      if (!allowed(ev.severity)) continue;
      if (!matchesSearch(ev)) continue;
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="nowrap muted">${ev.timestamp || ''}</td>
        <td class="nowrap ${sevClass(ev.severity)}">${ev.severity || ''}</td>
        <td>${escapeHtml(ev.message || '')}</td>
        <td class="nowrap muted">${escapeHtml(ev.logger || '')}</td>
      `;
      tr.addEventListener('click', () => showDetail(ev));
      els.rows.appendChild(tr);
    }
  }

  function showDetail(ev) {
    const fields = ev.fields || {};
    const pretty = JSON.stringify(fields, null, 2);
    els.detail.classList.remove('muted');
    els.detail.innerHTML =
      `<div><span class="k">timestamp</span>: <span class="v">${escapeHtml(ev.timestamp || '')}</span></div>` +
      `<div><span class="k">severity</span>: <span class="v">${escapeHtml(ev.severity || '')}</span></div>` +
      `<div><span class="k">logger</span>: <span class="v">${escapeHtml(ev.logger || '')}</span></div>` +
      `<div><span class="k">message</span>: <span class="v">${escapeHtml(ev.message || '')}</span></div>` +
      `<hr style="border:0;border-top:1px solid #1f2a37;margin:10px 0;">` +
      `<div class="k">raw_fields</div>` +
      `<pre style="margin:8px 0 0; white-space:pre-wrap;">${escapeHtml(pretty)}</pre>`;
  }

  function escapeHtml(s) {
    return (s ?? '').toString()
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function setConnected(on) {
    state.connected = on;
    els.dot.classList.toggle('live', on);
    els.status.textContent = on ? 'live' : 'disconnected (retrying…)';
  }

  function updateCounts(c) {
    els.cntErr.textContent = c.errors ?? 0;
    els.cntWarn.textContent = c.warnings ?? 0;
    els.cntInfo.textContent = c.info ?? 0;
  }

  els.fErr.addEventListener('change', renderRows);
  els.fWarn.addEventListener('change', renderRows);
  els.fInfo.addEventListener('change', renderRows);
  els.search.addEventListener('input', () => { state.search = els.search.value; renderRows(); });
  els.clear.addEventListener('click', () => { state.buffer = []; renderRows(); });

  const evts = new EventSource('/events');
  evts.onopen = () => setConnected(true);
  evts.onerror = () => setConnected(false);

  evts.addEventListener('init', (e) => {
    try {
      const data = JSON.parse(e.data);
      els.svc.textContent = data.meta?.service ?? els.svc.textContent;
      els.region.textContent = data.meta?.region ?? els.region.textContent;
      els.project.textContent = data.meta?.project ?? els.project.textContent;
      updateCounts(data.counts || {});
      state.buffer = data.events || [];
      renderRows();
    } catch {}
  });

  evts.addEventListener('counts', (e) => {
    try { updateCounts(JSON.parse(e.data)); } catch {}
  });

  evts.addEventListener('log', (e) => {
    try {
      const ev = JSON.parse(e.data);
      state.buffer.push(ev);
      if (state.buffer.length > 2000) state.buffer.splice(0, state.buffer.length - 2000);
      els.last.textContent = ev.timestamp || '—';
      // incremental append is possible, but keep it simple + reliable:
      renderRows();
    } catch {}
  });
})();
</script>
</body>
</html>
"""


@dataclass
class LogEvent:
    timestamp: str
    severity: str
    logger: str
    message: str
    fields: Dict[str, Any]
    dedupe_key: str  # insertId or fallback key

    def to_jsonable(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "severity": self.severity,
            "logger": self.logger,
            "message": self.message,
            "fields": self.fields,
        }


class MonitorState:
    def __init__(self, max_events: int = 2000) -> None:
        self.lock = threading.Lock()
        self.events: Deque[LogEvent] = deque(maxlen=max_events)
        self.seen: set[str] = set()
        self.counts = {"errors": 0, "warnings": 0, "info": 0}
        self.meta: Dict[str, str] = {}

    def add_events(self, new_events: List[LogEvent]) -> List[LogEvent]:
        added: List[LogEvent] = []
        with self.lock:
            for ev in new_events:
                if ev.dedupe_key in self.seen:
                    continue
                self.seen.add(ev.dedupe_key)
                self.events.append(ev)
                added.append(ev)
                sev = (ev.severity or "").upper()
                if sev in ("ERROR", "CRITICAL", "ALERT", "EMERGENCY"):
                    self.counts["errors"] += 1
                elif sev == "WARNING":
                    self.counts["warnings"] += 1
                elif sev in ("INFO", "NOTICE", "DEFAULT", "DEBUG"):
                    self.counts["info"] += 1
        return added

    def snapshot(self) -> Tuple[List[Dict[str, Any]], Dict[str, int], Dict[str, str]]:
        with self.lock:
            return (
                [e.to_jsonable() for e in list(self.events)],
                dict(self.counts),
                dict(self.meta),
            )


class SSEBroker:
    """Fan-out for server-sent events."""
    def __init__(self) -> None:
        self.clients_lock = threading.Lock()
        self.clients: List["queue.Queue[str]"] = []

    def add_client(self) -> "queue.Queue[str]":
        q: "queue.Queue[str]" = queue.Queue(maxsize=2000)
        with self.clients_lock:
            self.clients.append(q)
        return q

    def remove_client(self, q: "queue.Queue[str]") -> None:
        with self.clients_lock:
            if q in self.clients:
                self.clients.remove(q)

    def broadcast(self, event: str, data: Any) -> None:
        payload = f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
        dead: List["queue.Queue[str]"] = []
        with self.clients_lock:
            for q in self.clients:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                self.clients.remove(q)


def _safe_get(d: Dict[str, Any], path: List[str]) -> Optional[Any]:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def parse_log_entries(entries: List[Dict[str, Any]]) -> List[LogEvent]:
    out: List[LogEvent] = []
    for e in entries:
        ts = (e.get("timestamp") or e.get("receiveTimestamp") or "").strip()
        sev = (e.get("severity") or "DEFAULT").strip().upper()
        logger = (e.get("logName") or e.get("labels", {}).get("container_name") or "").split("/")[-1]
        insert_id = (e.get("insertId") or "").strip()

        # Prefer textPayload, then jsonPayload.message, then protoPayload.status.message, then fallback.
        msg = e.get("textPayload")
        if not msg:
            jp = e.get("jsonPayload") or {}
            if isinstance(jp, dict):
                msg = jp.get("message") or jp.get("msg") or jp.get("error") or None
        if not msg:
            msg = _safe_get(e, ["protoPayload", "status", "message"])
        if not msg:
            msg = json.dumps(e.get("jsonPayload") or e, ensure_ascii=False)[:500]

        fields = {
            "resource": e.get("resource"),
            "labels": e.get("labels"),
            "httpRequest": e.get("httpRequest"),
            "trace": e.get("trace"),
            "spanId": e.get("spanId"),
            "jsonPayload": e.get("jsonPayload"),
            "textPayload": e.get("textPayload"),
            "protoPayload": e.get("protoPayload"),
            "logName": e.get("logName"),
        }
        # Compact None values
        fields = {k: v for k, v in fields.items() if v is not None}

        dedupe_key = insert_id or f"{ts}|{sev}|{logger}|{str(msg)[:120]}"
        out.append(LogEvent(timestamp=ts, severity=sev, logger=logger, message=str(msg), fields=fields, dedupe_key=dedupe_key))
    # Return sorted by timestamp for stable UI
    out.sort(key=lambda x: x.timestamp)
    return out


def resolve_gcloud_executable(explicit: Optional[str] = None) -> str:
    """
    Resolve gcloud executable on Windows/macOS/Linux.
    - If explicit is provided, use it.
    - Else try PATH (gcloud, gcloud.cmd).
    - On Windows, also try common install locations.
    """
    if explicit:
        return explicit

    found = shutil.which("gcloud") or shutil.which("gcloud.cmd") or shutil.which("gcloud.exe")
    if found:
        return found

    if platform.system().lower().startswith("win"):
        candidates = [
            r"C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
            r"C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        ]
        # Also try user-local install
        user = os.environ.get("USERPROFILE") or ""
        if user:
            candidates.append(str(Path(user) / "AppData" / "Local" / "Google" / "Cloud SDK" / "google-cloud-sdk" / "bin" / "gcloud.cmd"))

        for c in candidates:
            if c and os.path.exists(c):
                return c

    # Last resort: just return "gcloud" so the error message mentions it
    return "gcloud"


def run_gcloud_logs_read(service: str, project: str, region: str, limit: int, freshness: str, gcloud_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Uses Cloud Run logs (a wrapper around Cloud Logging) and forces JSON output for reliable parsing.
    """
    gcloud_exe = resolve_gcloud_executable(gcloud_path)
    # On Windows, gcloud is commonly a .cmd file. CreateProcess cannot execute .cmd directly;
    # invoke it via cmd.exe /c.
    if platform.system().lower().startswith('win') and str(gcloud_exe).lower().endswith(('.cmd', '.bat')):
        cmd = [
            'cmd.exe', '/c', gcloud_exe, 'run', 'services', 'logs', 'read', service,
            '--project', project,
            '--region', region,
            '--limit', str(limit),
            '--freshness', freshness,
            '--format', 'json',
        ]
    else:
        cmd = [
            gcloud_exe, 'run', 'services', 'logs', 'read', service,
        "--project", project,
        "--region", region,
        "--limit", str(limit),
        "--freshness", freshness,
        "--format", "json",
    ]
    # Some environments print warnings to stderr; keep them for debugging.
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Could not run gcloud. Windows can't find the executable.\n\n"
            f"Tried: {cmd[0]}\n\n"
            f"Fix:\n"
            f"  - Ensure Google Cloud SDK is installed and 'gcloud' works in this same PowerShell\n"
            f"  - Run: where.exe gcloud\n"
            f"  - Or pass --gcloud \"C:\\...\\gcloud.cmd\" to this script\n"
        ) from e
    if proc.returncode != 0:
        raise RuntimeError(
            "gcloud command failed.\n\n"
            f"Command: {' '.join(cmd)}\n\n"
            f"STDOUT:\n{proc.stdout}\n\n"
            f"STDERR:\n{proc.stderr}\n"
        )

    # gcloud returns JSON array
    try:
        data = json.loads(proc.stdout or "[]")
        if isinstance(data, list):
            return data
        return []
    except json.JSONDecodeError:
        # If auth prompts or other text leaked into stdout, surface a friendly error
        raise RuntimeError(
            "Could not parse gcloud JSON output.\n\n"
            "Tip: make sure gcloud is authenticated:\n"
            "  gcloud auth login\n"
            "  gcloud auth application-default login\n\n"
            f"Raw output:\n{proc.stdout[:2000]}\n"
        )


def poller_thread(state: MonitorState, broker: SSEBroker, args: argparse.Namespace) -> None:
    poll = max(2.0, float(args.poll))
    last_missing_gcloud_ts = 0.0
    while True:
        try:
            entries = run_gcloud_logs_read(
                service=args.service,
                project=args.project,
                region=args.region,
                limit=args.limit,
                freshness=args.freshness,
                gcloud_path=getattr(args, 'gcloud', None),
            )
            events = parse_log_entries(entries)
            added = state.add_events(events)
            if added:
                # Broadcast counts first (so header updates quickly)
                _, counts, _ = state.snapshot()
                broker.broadcast("counts", counts)
                # Then stream each new log
                for ev in added[-500:]:
                    broker.broadcast("log", ev.to_jsonable())
        except Exception as exc:
            # If gcloud isn't found, throttle the noise (common on Windows PATH issues)
            if isinstance(exc, FileNotFoundError) or "WinError 2" in str(exc):
                now = time.time()
                if now - last_missing_gcloud_ts < 60:
                    time.sleep(poll)
                    continue
                last_missing_gcloud_ts = now

            # Broadcast an "ERROR" event into the stream so you see failures in the UI
            err_ev = LogEvent(
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                severity="ERROR",
                logger="portal_server_monitor",
                message=str(exc),
                fields={"exception": repr(exc)},
                dedupe_key=f"monitor_err|{time.time()}",
            )
            state.add_events([err_ev])
            _, counts, _ = state.snapshot()
            broker.broadcast("counts", counts)
            broker.broadcast("log", err_ev.to_jsonable())

        time.sleep(poll)


class Handler(BaseHTTPRequestHandler):
    server_version = "PortalServerMonitor/1.3"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/" or self.path.startswith("/?"):
            self._send_html(DEFAULT_HTML)
            return
        if self.path.startswith("/health"):
            self._send_text("ok\n")
            return
        if self.path.startswith("/events"):
            self._handle_sse()
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, fmt: str, *args: Any) -> None:
        # Quiet server logs; uncomment for debugging:
        # super().log_message(fmt, *args)
        pass

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str) -> None:
        body = text.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_sse(self) -> None:
        broker: SSEBroker = self.server.broker  # type: ignore[attr-defined]
        state: MonitorState = self.server.state  # type: ignore[attr-defined]

        q = broker.add_client()
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            events, counts, meta = state.snapshot()
            init_payload = {"meta": meta, "counts": counts, "events": events[-400:]}
            self.wfile.write(f"event: init\ndata: {json.dumps(init_payload, ensure_ascii=False)}\n\n".encode("utf-8"))
            self.wfile.flush()

            while True:
                try:
                    msg = q.get(timeout=15.0)
                except queue.Empty:
                    # keep-alive comment
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                try:
                    self.wfile.write(msg.encode("utf-8"))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    break
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # Browser tab closed / host network stack aborts SSE frequently on Windows
            pass
        finally:
            broker.remove_client(q)


def main() -> int:
    ap = argparse.ArgumentParser(description="Monitor a Cloud Run service via gcloud logs and display in a web UI.")
    ap.add_argument("--service", default="portal-server", help="Cloud Run service name (default: portal-server)")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--region", default="europe-west9", help="Cloud Run region")
    ap.add_argument("--gcloud", default=None, help="Path to gcloud executable (optional). If not set, uses PATH / auto-detect on Windows.")
    ap.add_argument("--port", type=int, default=8787, help="Local web server port (default: 8787)")
    ap.add_argument("--poll", type=float, default=10.0, help="Polling interval seconds (default: 10)")
    ap.add_argument("--limit", type=int, default=200, help="How many logs to fetch each poll (default: 200)")
    ap.add_argument("--freshness", default="30m", help="Freshness window for gcloud logs read (default: 30m)")
    args = ap.parse_args()

    state = MonitorState(max_events=2000)
    state.meta = {"service": args.service, "project": args.project, "region": args.region}
    broker = SSEBroker()

    t = threading.Thread(target=poller_thread, args=(state, broker, args), daemon=True)
    t.start()

    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    httpd.state = state  # type: ignore[attr-defined]
    httpd.broker = broker  # type: ignore[attr-defined]

    print(f"[portal-monitor] Serving UI: http://127.0.0.1:{args.port}")
    try:
        from shutil import which
        resolved = resolve_gcloud_executable(getattr(args, "gcloud", None))
        print(f"[portal-monitor] Resolved gcloud: {resolved}")
    except Exception:
        pass
    print(f"[portal-monitor] Polling: gcloud run services logs read {args.service} --project {args.project} --region {args.region} --limit {args.limit} --freshness {args.freshness} --format=json")
    print("[portal-monitor] Ctrl+C to stop.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[portal-monitor] Stopping...")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
