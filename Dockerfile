# Python reporting API – same DB connection as Node (NTLM / Windows-style auth via env)
# In Docker we use NTLM: DB_USE_WINDOWS_AUTH=0, DB_DOMAIN, DB_USERNAME, DB_PASSWORD (Trusted_Connection not available on Linux)

FROM python:3.11-slim

WORKDIR /app

# Install Microsoft ODBC Driver 18 for SQL Server (required for pyodbc)
# Microsoft repo expects key at /usr/share/keyrings/microsoft-prod.gpg (signed-by in prod.list)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 apt-transport-https ca-certificates \
    && mkdir -p /usr/share/keyrings \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev \
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
