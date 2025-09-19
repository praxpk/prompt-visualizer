from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import requests

try:
    import duckdb
except Exception:  # duckdb may not be installed in some environments
    duckdb = None  # type: ignore
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except Exception:
    # dotenv is optional; ignore if unavailable
    pass

try:
    from .logging_setup import configure_logging, ensure_trace_id_from_headers
except Exception:
    from logging_setup import configure_logging, ensure_trace_id_from_headers  # type: ignore


# Simple in-process counters for metrics
START_TIME = time.time()
REQUESTS_TOTAL = 0
ERRORS_TOTAL = 0

# Parquet/DB globals
PARQUET_PATH: Optional[str] = None
DUCK_CONN: Optional["duckdb.DuckDBPyConnection"] = None  # type: ignore[name-defined]


def _uploads_dir() -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.getenv("UPLOADS_DIR", os.path.join(base_dir, "uploads"))


def _data_dir() -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.getenv("DATA_DIR", os.path.join(base_dir, "data"))


def _find_latest_csv(upload_dir: str) -> Optional[str]:
    try:
        files = [
            os.path.join(upload_dir, f)
            for f in os.listdir(upload_dir)
            if f.lower().endswith(".csv") and os.path.isfile(os.path.join(upload_dir, f))
        ]
    except FileNotFoundError:
        return None
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def _csv_to_parquet(csv_path: str, parquet_path: str) -> None:
    if duckdb is None:
        raise RuntimeError("duckdb is not installed; cannot convert CSV to Parquet")
    # Normalize path for DuckDB on Windows
    csv_sql = csv_path.replace("\\", "/").replace("'", "''")
    parquet_sql = parquet_path.replace("\\", "/").replace("'", "''")
    # Clean and cast key columns during conversion
    transform_sql = f'''
WITH src AS (
  SELECT * FROM read_csv_auto('{csv_sql}', header=True)
), norm AS (
  SELECT
    *,
    lower(replace(replace(replace(trim(CAST("ARR" AS VARCHAR)), '$', ''), ',', ''), ' ', '')) AS arr_norm,
    lower(replace(replace(replace(trim(CAST("Valuation" AS VARCHAR)), '$', ''), ',', ''), ' ', '')) AS valuation_norm,
    lower(replace(replace(replace(trim(CAST("Total Funding" AS VARCHAR)), '$', ''), ',', ''), ' ', '')) AS total_funding_norm,
    trim(CAST("Employees" AS VARCHAR)) AS employees_txt
  FROM src
)
SELECT
  -- Keep all original columns
  * EXCLUDE (arr_norm, valuation_norm, total_funding_norm, employees_txt),
  -- Add normalized numeric columns as integers
  TRY_CAST(REPLACE(employees_txt, ',', '') AS BIGINT) AS employees_num,
  TRY_CAST(
    TRY_CAST(regexp_extract(arr_norm, '([0-9]*\\.?[0-9]+)', 1) AS DOUBLE) *
    CASE lower(nullif(regexp_extract(arr_norm, '([0-9]*\\.?[0-9]+)\\s*([btm])', 2), ''))
      WHEN 't' THEN 1e12
      WHEN 'b' THEN 1e9
      WHEN 'm' THEN 1e6
      ELSE 1
    END
  AS BIGINT) AS arr_num,
  TRY_CAST(
    TRY_CAST(regexp_extract(valuation_norm, '([0-9]*\\.?[0-9]+)', 1) AS DOUBLE) *
    CASE lower(nullif(regexp_extract(valuation_norm, '([0-9]*\\.?[0-9]+)\\s*([btm])', 2), ''))
      WHEN 't' THEN 1e12
      WHEN 'b' THEN 1e9
      WHEN 'm' THEN 1e6
      ELSE 1
    END
  AS BIGINT) AS valuation_num,
  TRY_CAST(
    TRY_CAST(regexp_extract(total_funding_norm, '([0-9]*\\.?[0-9]+)', 1) AS DOUBLE) *
    CASE lower(nullif(regexp_extract(total_funding_norm, '([0-9]*\\.?[0-9]+)\\s*([btm])', 2), ''))
      WHEN 't' THEN 1e12
      WHEN 'b' THEN 1e9
      WHEN 'm' THEN 1e6
      ELSE 1
    END
  AS BIGINT) AS total_funding_num
FROM norm
'''
    duckdb.execute(f"COPY ({transform_sql}) TO '{parquet_sql}' (FORMAT PARQUET);")


def _ensure_parquet(logger) -> Optional[str]:
    """Ensure a Parquet exists from the latest normalized CSV in data/. Return path or None."""
    data_dir = _data_dir()
    csv_path = _find_latest_csv(data_dir)
    if not csv_path:
        logger.info("No normalized CSV found in data directory", extra={"data": data_dir})
        return None
    parquet_path = os.path.splitext(csv_path)[0] + ".parquet"
    try:
        need_build = True
        if os.path.exists(parquet_path):
            need_build = os.path.getmtime(csv_path) > os.path.getmtime(parquet_path)
        if need_build:
            logger.info(
                "Converting CSV to Parquet",
                extra={"csv": csv_path, "parquet": parquet_path},
            )
            _csv_to_parquet(csv_path, parquet_path)
        else:
            logger.info("Parquet up-to-date", extra={"parquet": parquet_path})
        return parquet_path
    except Exception as e:
        logger.exception("Failed to prepare Parquet: %s", e)
        return None


def query_parquet(sql: str, params: Optional[Tuple[Any, ...]] = None, max_rows: int = 1000) -> Dict[str, Any]:
    """Run a read-only SQL query against the Parquet view `data`.

    Returns a dict with keys: columns, rows, rowcount.
    """
    if duckdb is None or DUCK_CONN is None:
        raise RuntimeError("duckdb is not initialized")
    # Crude read-only guard
    if not sql.strip().lower().startswith("select"):
        raise ValueError("Only SELECT statements are allowed")
    cur = DUCK_CONN.execute(sql, params or ())
    # Apply a safeguard limit if the query doesn't specify one
    # We cannot easily inject a LIMIT safely; encourage clients to pass LIMIT when needed.
    rows = cur.fetchmany(max_rows)
    cols = [d[0] for d in cur.description] if cur.description else []
    data = [dict(zip(cols, r)) for r in rows]
    return {"columns": cols, "rows": data, "rowcount": len(data)}


def _get_data_columns(logger=None) -> List[str]:
    if DUCK_CONN is None:
        return [
            "Company Name",
            "Founded Year",
            "HQ",
            "Industry",
            "Total Funding",
            "ARR",
            "Valuation",
            "Employees",
            "Top Investors",
            "Product",
            "G2 Rating",
        ]
    try:
        cur = DUCK_CONN.execute("SELECT * FROM data LIMIT 0")
        cols = [d[0] for d in (cur.description or [])]
        return cols
    except Exception as e:
        if logger:
            logger.warning("Failed to fetch columns from view: %s", e)
        return []


def _sql_prompt_context(
    columns: List[str],
    pie_chart: bool = False,
    scatter_plot: bool = False,
    histogram: bool = False,
) -> str:
    col_list = ", ".join([f'"{c}"' if re.search(r"[^A-Za-z0-9_]", c) else c for c in columns])
    ctx = (
        "You are an assistant that writes DuckDB SQL for a single table.\n"
        "- Table name: data\n"
        f"- Columns: {col_list}\n"
        "- If the user question cannot be answered from only this table, reply exactly: insufficient data\n"
        "- Output only a single SQL SELECT statement referencing table data.\n"
        "- Quote identifiers with spaces using double quotes.\n"
        "- Prefer including a LIMIT when appropriate.\n"
    )
    if pie_chart:
        ctx += (
            "- The user asked for a pie chart. Write SQL that returns percentages per category.\n"
            "- The result MUST include at least columns: label, pct (percentage 0-100). Optionally include value (count).\n"
            "- Choose a categorical column for label (e.g., Industry, HQ) based on the question.\n"
            "- Compute pct as 100.0 * value / SUM(value) OVER () and round appropriately.\n"
            "- Alias the columns exactly as label and pct (and value if included).\n"
        )
    if scatter_plot:
        ctx += (
            "- The user asked for a scatter plot. Write SQL that returns two numeric columns.\n"
            "- The result MUST include columns: x and y. Optionally include label (e.g., Company Name).\n"
            "- Choose appropriate numeric fields for x and y from the available columns (e.g., founded_year, employees_num, arr_num, valuation_num, total_funding_num, g2_rating).\n"
            "- Ensure the query filters out NULLs in x and y.\n"
            "- Alias the columns exactly as x and y (and label if included).\n"
            "- Prefer including a LIMIT to keep result sets reasonable.\n"
        )
    if histogram:
        ctx += (
            "- The user asked for a histogram/distribution. Write SQL that groups numeric values into bins and counts rows per bin.\n"
            "- Choose an appropriate numeric column based on the question (e.g., valuation_num, total_funding_num, arr_num, employees_num, founded_year, g2_rating).\n"
            "- The result MUST include columns: bin and n (count).\n"
            "- Use FLOOR(value/<bin_width>) to create integer bin indices where suitable, or a reasonable binning method; filter out NULLs.\n"
            "- Order results by bin ascending.\n"
            "- Alias columns exactly as bin and n.\n"
        )
    return ctx


def _extract_sql(text: str) -> Optional[str]:
    if not text:
        return None
    t = text.strip()
    if "insufficient data" in t.lower():
        return "INSUFFICIENT_DATA"
    # Remove code fences
    t = re.sub(r"^```(sql)?", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"```$", "", t).strip()
    # Heuristic: find first SELECT ...; if no semicolon, return from SELECT onward
    m = re.search(r"select\b", t, flags=re.IGNORECASE)
    if not m:
        return None
    sql = t[m.start():].strip()
    # Trim trailing commentary
    parts = sql.split("\n")
    # If multiple lines and fences, we already removed; still keep full
    return sql


def _openai_generate_sql(question: str, context: str, timeout: float = 15.0) -> Optional[str]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            "messages": [
                {"role": "system", "content": context},
                {"role": "user", "content": question},
            ],
            "temperature": 0.1,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            return "FALLBACK"
        data = resp.json()
        txt = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return _extract_sql(txt)
    except Exception:
        return "FALLBACK"


def _anthropic_generate_sql(question: str, context: str, timeout: float = 15.0) -> Optional[str]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307"),
            "max_tokens": 500,
            "temperature": 0.1,
            "system": context,
            "messages": [{"role": "user", "content": question}],
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            return "FALLBACK"
        data = resp.json()
        contents = data.get("content", [])
        txt = "".join([c.get("text", "") for c in contents if c.get("type") == "text"]) or ""
        return _extract_sql(txt)
    except Exception:
        return "FALLBACK"


def _ollama_duckdb_nsql_generate_sql(question: str, context: str, timeout: float = 20.0) -> Optional[str]:
    # Requires `ollama run duckdb-nsql` model available locally
    try:
        url = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
        prompt = (
            f"{context}\n\nQuestion: {question}\n\n"
            "Return only a single DuckDB-compatible SQL SELECT statement over table data, or the exact words: insufficient data."
        )
        payload = {"model": os.getenv("OLLAMA_MODEL", "duckdb-nsql"), "prompt": prompt, "stream": False}
        resp = requests.post(url, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            return None
        data = resp.json()
        txt = data.get("response", "")
        return _extract_sql(txt)
    except Exception:
        return None


def generate_sql_from_question(
    question: str,
    logger=None,
    pie_chart: bool = False,
    scatter_plot: bool = False,
    histogram: bool = False,
) -> Tuple[str, Optional[str]]:
    """Try OpenAI, then Anthropic, then local Ollama duckdb-nsql. Returns (source, sql_or_insufficient)."""
    cols = _get_data_columns(logger)
    context = _sql_prompt_context(cols, pie_chart=pie_chart, scatter_plot=scatter_plot, histogram=histogram)

    res = _openai_generate_sql(question, context)
    if res == "INSUFFICIENT_DATA":
        return ("openai", res)
    if res == "FALLBACK":
        res = None
    if isinstance(res, str) and res:
        return ("openai", res)

    res = _anthropic_generate_sql(question, context)
    if res == "INSUFFICIENT_DATA":
        return ("anthropic", res)
    if res == "FALLBACK":
        res = None
    if isinstance(res, str) and res:
        return ("anthropic", res)

    res = _ollama_duckdb_nsql_generate_sql(question, context)
    if isinstance(res, str) and res:
        return ("ollama-duckdb-nsql", res)

    # Final fallback: insufficient
    return ("none", "INSUFFICIENT_DATA")


def _redact_env(env: Dict[str, str]) -> Dict[str, str]:
    redacted: Dict[str, str] = {}
    for k, v in env.items():
        key = k.upper()
        if any(s in key for s in ["SECRET", "KEY", "TOKEN", "PASS", "PWD"]):
            redacted[k] = "***REDACTED***"
        else:
            redacted[k] = v
    return redacted


def create_app() -> Flask:
    app = Flask(__name__)
    # Enable CORS for local frontend (localhost:3000)
    CORS(
        app,
        resources={r"/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]}},
        supports_credentials=True,
        expose_headers=["X-Trace-Id"],
    )

    # Configure logging (console JSON with pretty toggle + file output)
    logger = configure_logging(app_name="backend")

    # Prepare Parquet and DuckDB view
    global PARQUET_PATH, DUCK_CONN
    PARQUET_PATH = _ensure_parquet(logger)
    if not PARQUET_PATH:
        logger.error("Parquet not available; refusing to start. Place a CSV in uploads/ or set UPLOADS_DIR.")
        raise RuntimeError("Parquet not ready")
    if duckdb is not None:
        try:
            DUCK_CONN = duckdb.connect()
            parquet_sql = PARQUET_PATH.replace("\\", "/").replace("'", "''")
            DUCK_CONN.execute(
                f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('{parquet_sql}');"
            )
            logger.info("DuckDB view initialized", extra={"view": "data", "parquet": PARQUET_PATH})
        except Exception as e:
            logger.exception("Failed to initialize DuckDB view: %s", e)
            raise

    @app.before_request
    def _before_request() -> None:
        global REQUESTS_TOTAL
        REQUESTS_TOTAL += 1
        trace_id = ensure_trace_id_from_headers()
        # Echo back correlation id for frontend propagation
        request.trace_id = trace_id  # type: ignore[attr-defined]
        request._start_ts = time.time()  # type: ignore[attr-defined]

    @app.after_request
    def _after_request(resp: Response) -> Response:
        # Attach trace id header
        tid = getattr(request, "trace_id", None)
        if tid:
            resp.headers["X-Trace-Id"] = tid
        # Structured access log
        try:
            duration_ms = int((time.time() - getattr(request, "_start_ts", time.time())) * 1000)
        except Exception:
            duration_ms = -1
        logger.info(
            "http_request",
            extra={
                "http": {
                    "method": request.method,
                    "path": request.path,
                    "status": resp.status_code,
                    "duration_ms": duration_ms,
                }
            },
        )
        return resp

    @app.errorhandler(Exception)
    def _handle_error(e: Exception):  # type: ignore[no-redef]
        global ERRORS_TOTAL
        ERRORS_TOTAL += 1
        logger.exception("Unhandled exception: %s", e)
        return jsonify({"error": "internal_server_error"}), 500

    # Health endpoints
    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok"})

    # Back-compat for existing /health route
    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.get("/readyz")
    def readyz():
        if duckdb is None:
            return jsonify({"status": "error", "db": False, "reason": "duckdb_not_installed"}), 503
        try:
            res = duckdb.execute("SELECT 1").fetchone()
            db_ok = bool(res and res[0] == 1)
        except Exception as e:
            logger.warning("Readiness check failed: %s", e)
            return jsonify({"status": "error", "db": False}), 500
        return jsonify({"status": "ok", "db": db_ok}), 200

    @app.post("/ask")
    def ask():
        if PARQUET_PATH is None or DUCK_CONN is None:
            return (
                jsonify({"error": "parquet_not_ready", "message": "No Parquet prepared in uploads"}),
                503,
            )
        try:
            body = request.get_json(silent=True) or {}
            question = body.get("question") or body.get("q") or body.get("prompt")
            if not isinstance(question, str) or not question.strip():
                return jsonify({"error": "invalid_request", "message": "Provide natural language question in 'question'"}), 400

            pie_chart = bool(re.search(r"\bpie\s*-?\s*chart\b", question, flags=re.IGNORECASE))
            scatter_plot = bool(re.search(r"\bscatter\s*-?\s*plot\b", question, flags=re.IGNORECASE))
            histogram = bool(re.search(r"\b(histogram|distribution|distributions)\b", question, flags=re.IGNORECASE))
            source, sql = generate_sql_from_question(
                question,
                logger=logger,
                pie_chart=pie_chart,
                scatter_plot=scatter_plot,
                histogram=histogram,
            )
            if sql == "INSUFFICIENT_DATA":
                return jsonify({
                    "status": "ok",
                    "answer": "insufficient data",
                    "source": source,
                    "parquet": PARQUET_PATH,
                    "view": "data",
                })
            if not isinstance(sql, str) or not sql.strip():
                return jsonify({"error": "llm_failed", "message": "Could not generate SQL"}), 502

            # Guard: only SELECT
            if not sql.strip().lower().startswith("select"):
                return jsonify({"error": "bad_query", "message": "Generated SQL must be SELECT"}), 400

            result = query_parquet(sql)
            resp_payload = {
                "status": "ok",
                "source": source,
                "question": question,
                "sql": sql,
                "result": result,
            }
            if pie_chart:
                resp_payload["pie_chart"] = True
                resp_payload["expected_columns"] = ["label", "pct"]
            if scatter_plot:
                resp_payload["scatter_plot"] = True
                resp_payload["expected_columns"] = ["x", "y"]
            if histogram and not pie_chart and not scatter_plot:
                resp_payload["histogram"] = True
                resp_payload["expected_columns"] = ["bin", "n"]
            return jsonify(resp_payload)
        except ValueError as ve:
            return jsonify({"error": "bad_query", "message": str(ve)}), 400
        except Exception as e:
            logger.exception("ask endpoint failed: %s", e)
            return jsonify({"error": "internal_error"}), 500

    @app.get("/metricsz")
    def metricsz():
        uptime = int(time.time() - START_TIME)
        lines = [
            f"requests_total {REQUESTS_TOTAL}",
            f"errors_total {ERRORS_TOTAL}",
            f"uptime_seconds {uptime}",
        ]
        body = "\n".join(lines) + "\n"
        return Response(body, mimetype="text/plain")

    @app.get("/debugz")
    def debugz():
        # Provide a redacted snapshot of env + relevant config
        env = _redact_env(dict(os.environ))
        cfg = {
            "env": env,
            "config": {
                "LOG_LEVEL": os.getenv("LOG_LEVEL"),
                "LOG_PRETTY": os.getenv("LOG_PRETTY"),
                "LOG_FILE_ENABLED": os.getenv("LOG_FILE_ENABLED"),
                "LOG_DIR": os.getenv("LOG_DIR"),
                "LOG_BACKUPS": os.getenv("LOG_BACKUPS"),
                "LOG_MAX_BYTES": os.getenv("LOG_MAX_BYTES"),
                "APP_ENV": os.getenv("APP_ENV") or os.getenv("ENV"),
            },
        }
        return jsonify(cfg)

    # Example root to verify server is alive
    @app.get("/")
    def root():
        meta = {"parquet_ready": PARQUET_PATH is not None}
        return jsonify({"status": "ok", "message": "Prompt-Visualizer backend", **meta})

    logger.info("Backend initialized", extra={"component": "startup"})
    return app


if __name__ == "__main__":
    app = create_app()
    # Bind to all interfaces for local testing / docker use
    app.run(host="0.0.0.0", port=5000, debug=os.getenv("FLASK_DEBUG", "1") == "1")
