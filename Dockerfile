FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install FreeTDS (NTLM-capable), ODBC and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-bin \
    freetds-dev \
    unixodbc \
    unixodbc-dev \
    build-essential \
    gcc \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy FreeTDS configuration
COPY freetds/odbcinst.ini /etc/odbcinst.ini
COPY freetds/odbc.ini /etc/odbc.ini
COPY freetds/freetds.conf /etc/freetds/freetds.conf

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
