# Backend (Flask + DuckDB)

## Setup

- Create and activate a virtual environment
  - Windows (PowerShell): `python -m venv .venv; .\\.venv\\Scripts\\Activate.ps1`
  - macOS/Linux: `python -m venv .venv && source .venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`

## Run

- Start the server: `python app.py`
- Health endpoints:
  - Liveness: `GET http://localhost:5000/healthz`
  - Readiness: `GET http://localhost:5000/readyz`
  - Metrics: `GET http://localhost:5000/metricsz` (text)
  - Debug (redacted env/config): `GET http://localhost:5000/debugz`

## Logging

- Structured JSON logs to STDOUT, with optional pretty mode via env.
- File logs in `./logs/backend.log.jsonl` with rotation.

Env vars:

- `LOG_PRETTY=true|false` pretty JSON to console (default false)
- `LOG_LEVEL=TRACE|DEBUG|INFO|WARN|ERROR` (default auto from `APP_ENV`)
- `APP_ENV=development|local|production` to infer default level
- `LOG_FILE_ENABLED=true|false` enable file logging (default true)
- `LOG_DIR=./logs` directory for log files
- `LOG_BACKUPS=5` number of rotated files to keep
- `LOG_MAX_BYTES=5242880` max file size before rotation
- `NATIVE_BINARY=true` enable time+size rotation (auto when `sys.frozen`)

Correlation IDs:

- Provide `X-Trace-Id` or `X-Correlation-Id` in requests; backend echoes `X-Trace-Id` in responses and includes it in every log line.
