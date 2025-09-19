# Prompt-Visualizer — Docker Setup

This repo runs a Flask backend, a Next.js frontend, and an Ollama service for local LLM fallback. The backend reads a CSV, produces a normalized CSV, converts it to Parquet, and exposes a `/ask` endpoint that can generate SQL via LLMs (OpenAI, Anthropic) or fallback to a local `duckdb-nsql` model via Ollama.

**What You Get**

- Backend (Flask) on `http://localhost:5000`
- Frontend (Next.js) on `http://localhost:3000`
- Ollama (LLM runtime) on `http://localhost:11434`

## Prerequisites

- Docker and Docker Compose
- A CSV in `backend/uploads/` (example: `top_100_saas_companies_2025.csv`)

## Quick Start

1. Normalize the CSV into `backend/data/` if there you wish to provide a different csv. The current folder has normalized data of top_100_saas_companies_2025

- Local (Python on host):
  - `python backend/normalize_csv.py -i backend/uploads/<your.csv>`
- Or using Docker (no host Python required):
  - `docker compose run --rm backend python normalize_csv.py -u /app/uploads -d /app/data -i /app/uploads/<your.csv>`

2. (Optional) Add API keys for better answers

- Create `backend/.env` (already git-ignored) and add:
  - `OPENAI_API_KEY=...`
  - `ANTHROPIC_API_KEY=...`
- Alternatively, export them in your shell so Compose passes them through:
  - Linux/macOS: `export OPENAI_API_KEY=...` `export ANTHROPIC_API_KEY=...`
  - PowerShell: `$env:OPENAI_API_KEY='...'` `$env:ANTHROPIC_API_KEY='...'`

3. Bring up the stack

- `docker compose up --build`
- On first run, Compose starts an `ollama` service and a one-shot `ollama-init` helper that pulls the `duckdb-nsql` model. If the model pull times out, rerun: `docker compose run --rm ollama-init` or `docker compose exec ollama ollama pull duckdb-nsql`.

4. Open the app

- Frontend: `http://localhost:3000`
- Backend health: `http://localhost:5000/healthz`

## How It Works

- On backend start:
  - Expects a normalized CSV in `backend/data/` (created in step 1).
  - Converts it to Parquet and creates a DuckDB view `data`.
  - Refuses to start if no normalized CSV/Parquet is available.
- The `/ask` endpoint accepts a natural-language question and tries providers in order:
  1. OpenAI (if `OPENAI_API_KEY` set)
  2. Anthropic (if `ANTHROPIC_API_KEY` set)
  3. Local `duckdb-nsql` via Ollama (fallback)
- Special behaviors:
  - “pie chart” → the LLM returns `label`/`pct` columns.
  - “scatter plot” → the LLM returns `x`/`y` columns.
  - “histogram” or “distribution(s)” → the LLM returns `bin`/`n` columns.
  - If the question is out of scope, the backend answers with `"insufficient data"` and the frontend displays that notice.

## Example Requests

- Frontend: type questions in the input and click “Ask”.
- Direct API:
  - `POST http://localhost:5000/ask`
  - Body: `{ "question": "Create a pie chart representing industry breakdown" }`

## Notes On Accuracy

- Adding your API keys to `backend/.env` lets the backend use OpenAI or Anthropic to generate SQL. This is generally more accurate.
- Without keys, the system falls back to the local `duckdb-nsql` model via Ollama. It works offline but may be less accurate than external APIs.

## Troubleshooting

- Backend exits with “Parquet not ready”:
  - Make sure you normalized your CSV into `backend/data/` (step 1), then `docker compose up` again.
- Ollama model missing:
  - `docker compose run --rm ollama-init` to pull `duckdb-nsql` again.
- CORS errors from the frontend:
  - Backend is already configured to allow `http://localhost:3000`.

## Useful Commands

- Start services: `docker compose up --build`
- Stop services: `docker compose down`
- Re-pull model: `docker compose run --rm ollama-init`
- Normalize (in container): `docker compose run --rm backend python normalize_csv.py -u /app/uploads -d /app/data -i /app/uploads/<your.csv>`
- Tail logs: `docker compose logs -f --tail=200`
