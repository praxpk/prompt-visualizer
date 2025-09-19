import json
import logging
import logging.handlers
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

try:
    # Only available when running inside Flask request context
    from flask import g, has_request_context, request
except Exception:
    # Fallbacks for non-Flask contexts
    def has_request_context() -> bool:  # type: ignore
        return False

    class g:  # type: ignore
        trace_id = None

    request = None  # type: ignore


# ----- Custom TRACE level -----
TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def trace(self: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self._log(TRACE_LEVEL_NUM, message, args, **kwargs)

    logging.Logger.trace = trace  # type: ignore[attr-defined]


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _current_trace_id() -> Optional[str]:
    if has_request_context():
        tid = getattr(g, "trace_id", None)
        if tid:
            return tid
    return None


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Add contextual request info if available
        record.trace_id = _current_trace_id() or "-"
        record.pid = os.getpid()
        record.process_name = getattr(record, "processName", "")
        record.thread_name = getattr(record, "threadName", "")
        if has_request_context():
            try:
                record.path = request.path
                record.method = request.method
                record.remote_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
            except Exception:
                record.path = record.method = record.remote_addr = "-"
        else:
            record.path = record.method = record.remote_addr = "-"
        return True


class JSONFormatter(logging.Formatter):
    def __init__(self, *, pretty: bool = False) -> None:
        super().__init__()
        self.pretty = pretty

    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": getattr(record, "trace_id", "-"),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "process": getattr(record, "pid", os.getpid()),
            "thread": getattr(record, "thread_name", ""),
            "path": getattr(record, "path", "-"),
            "method": getattr(record, "method", "-"),
            "remote": getattr(record, "remote_addr", "-"),
        }
        # Attach user-provided extras into an "extra" object to keep schema stable
        standard_keys = set(base.keys()) | {
            "name",
            "msg",
            "args",
            "levelno",
            "pathname",
            "filename",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
        }
        extras: Dict[str, Any] = {}
        for k, v in record.__dict__.items():
            if k not in standard_keys and not k.startswith("_"):
                extras[k] = v
        if extras:
            base["extra"] = extras
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        if self.pretty:
            return json.dumps(base, indent=2, ensure_ascii=False)
        return json.dumps(base, ensure_ascii=False)


class SizeAndTimeRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    Rotates on time AND size. Triggers rollover when either condition is met.
    """

    def __init__(
        self,
        filename: str,
        when: str = "midnight",
        interval: int = 1,
        backupCount: int = 7,
        encoding: Optional[str] = None,
        delay: bool = False,
        utc: bool = False,
        atTime: Optional[datetime] = None,
        maxBytes: int = 0,
    ) -> None:
        super().__init__(
            filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            delay=delay,
            utc=utc,
            atTime=atTime,
        )
        self.maxBytes = maxBytes

    def shouldRollover(self, record: logging.LogRecord) -> int:  # type: ignore[override]
        # Time-based check first
        t = int(time.time())
        if t >= self.rolloverAt:
            return 1
        # Size-based check
        if self.maxBytes > 0:
            if self.stream is None:  # pragma: no cover
                self.stream = self._open()
            self.stream.seek(0, os.SEEK_END)
            if self.stream.tell() >= self.maxBytes:
                return 1
        return 0


def _detect_level() -> int:
    # Explicit level via env takes precedence
    explicit = os.getenv("LOG_LEVEL", "").strip().upper()
    if explicit:
        mapping = {
            "TRACE": TRACE_LEVEL_NUM,
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARN": logging.WARN,
            "WARNING": logging.WARN,
            "ERROR": logging.ERROR,
        }
        return mapping.get(explicit, logging.INFO)

    # Implicit based on APP_ENV
    app_env = os.getenv("APP_ENV", os.getenv("ENV", "")).strip().lower()
    if app_env in {"development", "dev"}:
        return TRACE_LEVEL_NUM
    if app_env in {"local"}:
        return logging.DEBUG
    return logging.INFO


def configure_logging(app_name: str = "app") -> logging.Logger:
    # Ensure trace id exists on non-request logs if user wants to set one
    # No-op here; request filter will insert '-' when absent

    root = logging.getLogger()
    root.setLevel(_detect_level())

    # Clear existing handlers to avoid duplication on reloads
    for h in list(root.handlers):
        root.removeHandler(h)

    pretty = _env_bool("LOG_PRETTY", False)
    json_console = JSONFormatter(pretty=pretty)
    json_file = JSONFormatter(pretty=False)
    ctx_filter = RequestContextFilter()

    # Console handler (always enabled)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(root.level)
    ch.setFormatter(json_console)
    ch.addFilter(ctx_filter)
    root.addHandler(ch)

    # File handler (enabled by default; can be disabled)
    file_enabled = _env_bool("LOG_FILE_ENABLED", True)
    log_dir = os.getenv("LOG_DIR", os.path.join(os.getcwd(), "logs"))
    os.makedirs(log_dir, exist_ok=True)
    logfile = os.path.join(log_dir, f"{app_name}.log.jsonl")

    keep = _env_int("LOG_BACKUPS", 5)
    max_bytes = _env_int("LOG_MAX_BYTES", 5 * 1024 * 1024)  # 5MB
    utc = _env_bool("LOG_UTC", True)

    # Only enable rotation combo when running as a native/frozen binary
    native = bool(getattr(sys, "frozen", False) or _env_bool("NATIVE_BINARY", False))

    if file_enabled:
        if native:
            fh: logging.Handler = SizeAndTimeRotatingFileHandler(
                logfile,
                when="midnight",
                interval=1,
                backupCount=keep,
                utc=utc,
                maxBytes=max_bytes,
                encoding="utf-8",
            )
        else:
            # In dev/non-native, keep it simple but still rotate by size
            fh = logging.handlers.RotatingFileHandler(
                logfile, maxBytes=max_bytes, backupCount=keep, encoding="utf-8"
            )
        fh.setLevel(root.level)
        fh.setFormatter(json_file)
        fh.addFilter(ctx_filter)
        root.addHandler(fh)

    # Add convenience child logger
    logger = logging.getLogger(app_name)
    logger.setLevel(root.level)
    return logger


def ensure_trace_id_from_headers() -> str:
    """Fetch or create a trace_id for the current request and attach to g."""
    tid = None
    if has_request_context():
        try:
            tid = request.headers.get("X-Trace-Id") or request.headers.get("X-Correlation-Id")
        except Exception:
            tid = None
    if not tid:
        tid = uuid.uuid4().hex
    if has_request_context():
        g.trace_id = tid
    return tid


__all__ = [
    "configure_logging",
    "ensure_trace_id_from_headers",
    "TRACE_LEVEL_NUM",
]
