# Python reporting API – DB via pymssql (NTLM/FreeTDS, no ODBC driver required)
# Set DB_BACKEND=pymssql and DB_DOMAIN, DB_USERNAME, DB_PASSWORD for NTLM in Docker

FROM python:3.11-slim

WORKDIR /app

# pymssql uses FreeTDS; install for SQL Server connectivity (no Microsoft ODBC repo)
RUN apt-get update \
    && apt-get install -y --no-install-recommends freetds-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Python env
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# App listens on 8000; override with PORT env
EXPOSE 8000

# Same DB env as Node: DB_HOST, DB_PORT, DB_NAME, DB_USE_WINDOWS_AUTH=0, DB_DOMAIN, DB_USERNAME, DB_PASSWORD
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
