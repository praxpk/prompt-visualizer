FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ .

EXPOSE 5000

ENV FLASK_DEBUG=1 \
    OLLAMA_URL=http://ollama:11434/api/generate \
    UPLOADS_DIR=/app/uploads

CMD ["python", "app.py"]

