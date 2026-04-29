# Python reporting API — pymssql / FreeTDS (no ODBC)

FROM python:3.11-slim AS base

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build deps for pymssql wheels / source builds + runtime FreeTDS + fonts for PDFs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        freetds-bin \
        freetds-common \
        pkg-config \
        build-essential \
        freetds-dev \
        fonts-dejavu-core \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Dependencies first (better layer cache)
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Application code last
COPY . .

# Drop root (UID/GID 1000 — adjust if your host bind-mounts need another uid)
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Respect PORT (e.g. Cloud Run / platforms that set PORT)
ENV PORT=8000
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT}"]

# Optional: basic health signal for orchestrators (adjust path if needed)
# HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
#   CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${PORT}/health', timeout=3)" || exit 1
