# Python reporting API – DB via pymssql (NTLM/FreeTDS, no ODBC driver required)
# Set DB_BACKEND=pymssql and DB_DOMAIN, DB_USERNAME, DB_PASSWORD for NTLM in Docker

FROM python:3.11-slim

WORKDIR /app

# pymssql uses FreeTDS; install for SQL Server connectivity (no Microsoft ODBC repo)
# Install fonts so PDFs render correctly (avoid hollow squares for text on live)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        freetds-dev pkg-config \
        fonts-dejavu-core \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Python env
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Run as an unprivileged user; it owns /app because exports are written under it
# (reports_export/, transaction/, exports/).
RUN useradd --system --uid 10001 --home /app app && chown -R app:app /app
USER app

# App listens on 8000; override with PORT env
EXPOSE 8000

# Same DB env as Node: DB_HOST, DB_PORT, DB_NAME, DB_USE_WINDOWS_AUTH=0, DB_DOMAIN, DB_USERNAME, DB_PASSWORD
# --no-server-header: don't advertise "server: uvicorn" (WAPT11 server fingerprinting)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header"]
