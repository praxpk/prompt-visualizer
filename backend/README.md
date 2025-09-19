# Backend (Flask + DuckDB)

## Setup

- Create and activate a virtual environment
  - Windows (PowerShell): `python -m venv .venv; .\\.venv\\Scripts\\Activate.ps1`
  - macOS/Linux: `python -m venv .venv && source .venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`

## Run

- Start the server: `python app.py`
- Health check: `GET http://localhost:5000/health`

