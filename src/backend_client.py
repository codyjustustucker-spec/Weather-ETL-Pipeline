# Backend exclusive only for integration (No ETL logic here)

# src/backend_client.py
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Iterable

import requests


def record_event(
    *,
    system_id: int,
    stage: str,
    event: str,
    store_path: str = "data/telemetry/events.jsonl",
    run_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    level: str = "info",
) -> str:

    os.makedirs(os.path.dirname(store_path), exist_ok=True)

    rid = run_id or str(uuid.uuid4())

    status = "error" if event == "fail" else "ok"

    doc = {
        "system_id": int(system_id),
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": rid,
        "stage": stage,         # e.g. "extract" | "transform" | "load"
        "event_type": event,
        # e.g. "start" | "success" | "fail" | "counts"
        "status": status,
        "latency_ms": 0,
        "level": level,         # "info" | "warn" | "error"
        "payload": payload or {},
    }

    with open(store_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    return rid


def send_events_to_backend(
    *,
    backend_url: str,
    store_path: str = "data/telemetry/events.jsonl",
    timeout_s: int = 5,
    api_key: Optional[str] = None,
    max_events: Optional[int] = None,
    retries: int = 2,
    backoff_s: float = 0.75,
) -> Dict[str, Any]:
    """
    Read locally stored events and POST them to the backend as JSON.
    On success, clears the local store.
    """
    if not os.path.exists(store_path):
        return {"sent": 0, "cleared": False, "detail": "no store file"}

    events: list[dict[str, Any]] = []
    with open(store_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                # skip corrupt lines rather than failing everything
                continue
            if max_events is not None and len(events) >= max_events:
                break

    if not events:
        return {"sent": 0, "cleared": False, "detail": "no valid events"}

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    body = {"events": events}

    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(backend_url, json=body,
                              headers=headers, timeout=timeout_s)
            if 200 <= r.status_code < 300:
                # clear store only if we sent everything we read
                # (simple version: clear entire file)
                open(store_path, "w", encoding="utf-8").close()
                return {"sent": len(events), "cleared": True, "status_code": r.status_code}
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            last_err = str(e)

        if attempt < retries:
            time.sleep(backoff_s * (2**attempt))

    return {"sent": 0, "cleared": False, "error": last_err}
