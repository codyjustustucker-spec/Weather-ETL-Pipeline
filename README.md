# Weather ETL Pipeline with Telemetry

## Overview

This project is a production‑style **Python ETL pipeline** that ingests weather data from the Open‑Meteo API, processes it through extract / transform / load stages, and **emits structured telemetry events** to an external observability backend.

The system demonstrates not just data ingestion, but **operational visibility**: every ETL run reports stage‑level events, status, and health metrics to a backend service, enabling monitoring, debugging, and system health evaluation.

This repository focuses on the **ETL side** of the architecture.

---

## Architecture

**High‑level flow:**

1. Load configuration from YAML
2. Extract weather data from Open‑Meteo
3. Transform raw API responses into structured records
4. Load results into local storage
5. Record telemetry events for each stage
6. Send telemetry to backend observer
7. Backend computes health over a rolling window

```
Open‑Meteo API
      ↓
[ Extract ]
      ↓
[ Transform ]
      ↓
[ Load ]
      ↓
Local Telemetry Buffer (JSONL)
      ↓
Observer Backend  →  /systems/{id}/health
```

---

## Key Features

* Modular ETL design (extract / transform / load)
* YAML‑based configuration
* Structured logging and telemetry
* Buffered telemetry (store‑and‑forward)
* Backend health integration
* System‑scoped observability (`system_id`)
* Correlated runs via `run_id`

---

## Project Structure

```
weather_etl_pipeline/
├── src/
│   ├── main.py              # ETL entrypoint
│   ├── extract.py           # Extract stage
│   ├── transform.py         # Transform stage
│   ├── load.py              # Load stage
│   ├── backend_client.py    # Telemetry recording & sending
│   └── config.py            # Config loading
├── config/
│   └── settings.yaml        # Runtime configuration
├── data/
│   └── telemetry/
│       └── events.jsonl     # Local telemetry buffer
├── tests/
├── README.md
└── pyproject.toml / requirements.txt
```

---

## Configuration

All runtime settings are defined in `config/settings.yaml`.

Example:

```yaml
api_base_url: https://api.open-meteo.com/v1/forecast
api_timeout: 10
latitude: 0
longitude: 0

LSO_URL: http://127.0.0.1:8000
LSO_SYSTEM_ID: 3
```

Configuration is loaded once at startup and passed explicitly through the ETL.

---

## Telemetry Model

Each ETL run emits structured telemetry events with the following fields:

| Field        | Purpose                            |
| ------------ | ---------------------------------- |
| `system_id`  | Identifies the ETL system instance |
| `run_id`     | Correlates all events from one run |
| `stage`      | `extract`, `transform`, or `load`  |
| `event_type` | `start`, `success`, `fail`, etc.   |
| `status`     | `ok` or `error`                    |
| `latency_ms` | Stage execution time               |
| `payload`    | Counts, metadata, error details    |
| `ts`         | UTC timestamp                      |

Events are first written locally to `events.jsonl`, then batch‑sent to the backend.

---

## Backend Integration

Telemetry is sent to a system‑scoped backend endpoint:

```
POST /systems/{system_id}/events
GET  /systems/{system_id}/health
```

The backend aggregates recent events and exposes a health summary, including:

* Event counts
* Error rate
* Requests per second
* Latency percentiles
* Overall system health status

Example health response:

```json
{
  "system_id": 3,
  "health": "OK",
  "total": 8,
  "errors": 0,
  "error_rate": 0.0
}
```

---

## Running the Pipeline

### 1. Start the backend

```bash
uvicorn app.main:app --reload
```

### 2. Run the ETL

From the project root:

```bash
python -m src.main
```

### 3. Check system health

```bash
curl http://127.0.0.1:8000/systems/3/health
```

---

## Design Decisions

* **Buffered telemetry** prevents data loss if the backend is unavailable
* **Explicit config passing** avoids global state and hidden dependencies
* **Schema‑validated events** ensure backend compatibility
* **System‑scoped endpoints** allow multiple ETLs to share one observer backend

---

## What This Project Demonstrates

* Real‑world ETL structure (not a toy script)
* Backend integration and API contracts
* Observability and health reporting
* Debugging through HTTP status codes (404 vs 422)
* Schema alignment between producer and consumer

---

## Future Improvements

* Measure real stage latency instead of placeholder values
* Persist transformed data to a database
* Add retries / circuit breakers per stage
* Add CI tests for telemetry schema compliance
* Support multiple data sources

