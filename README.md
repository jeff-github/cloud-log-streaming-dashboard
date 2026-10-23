# Cloud Log Streaming Dashboard

Python dashboard that polls Cloud Run logs and streams updates to local browsers
with Server-Sent Events (SSE). Includes severity filters/counters and expandable details.

## Run (Python 3.10+)
Requires authenticated `gcloud` with permission to read logs. No Python packages needed.
```powershell
python monitor_portal_server.py --project YOUR_PROJECT --service YOUR_SERVICE --region YOUR_REGION
```
Open http://127.0.0.1:8787. Configure `--poll`, `--freshness`, `--limit`, and optionally
`--gcloud` for a Windows executable path.

## Design and limits
Background polling, a threaded HTTP server, locks and bounded per-client queues
support concurrent viewers. Slow clients are removed from the broadcast list.
Cloud logs are polled; browser transport is SSE, not WebSocket. This local tool
has no multi-user authentication or alert delivery. Do not expose it publicly.
Logs are not automatically redacted. The event deque is bounded but the deduplication
set can grow during long sessions. CI compiles code and tests SSE fan-out offline.

## Status
Private portfolio preparation copy of existing project code. Original credentials,
run data and environment defaults are excluded. No open-source license is granted;
confirm ownership and sharing rights before making this repository public.
