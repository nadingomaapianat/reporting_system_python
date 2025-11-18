FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies + Microsoft ODBC Driver 18
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    build-essential \
    gcc \
    libpq-dev \
    ca-certificates \
    fontconfig \
    \
    # Microsoft GPG key & repo for Debian 12 (Bookworm)
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    \
    # Cleanup
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure directories exist
RUN mkdir -p Disclosures template reports_export exports utils/fonts

# Update font cache
RUN fc-cache -fv

# (Optional) Demo files — remove in production
RUN echo "This is a demo file." > Disclosures/Sample_Disclosure_Report_2025.pdf && \
    echo "This is a demo file." > template/Monthly_Report_Template.docx && \
    echo "This is a demo export file." > reports_export/sample_export.xlsx

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
