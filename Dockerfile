FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    unixodbc-dev \
    curl \
    gnupg2 \
    ca-certificates \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure all necessary directories exist (matching code expectations)
RUN mkdir -p files/disclosures files/template files/reports reports_export exports

# Create demo files for testing
RUN echo "This is a demo file for testing purposes." > files/disclosures/Sample_Disclosure_Report_2025.pdf && \
    echo "This is a demo file for testing purposes." > files/disclosures/Financial_Disclosure_Demo.pdf && \
    echo "This is a demo file for testing purposes." > files/disclosures/Risk_Assessment_Sample.pdf && \
    echo "This is a demo file for testing purposes." > files/disclosures/Compliance_Report_Demo.pdf && \
    echo "This is a demo template file for testing purposes." > files/template/Monthly_Report_Template.docx && \
    echo "This is a demo template file for testing purposes." > files/template/Risk_Assessment_Template.docx && \
    echo "This is a demo template file for testing purposes." > files/template/Financial_Analysis_Template.docx && \
    echo "This is a demo template file for testing purposes." > files/template/Compliance_Checklist_Template.docx && \
    echo "This is a demo template file for testing purposes." > files/template/Dashboard_Report_Template.docx && \
    mkdir -p reports_export/$(date +%Y-%m-%d) && \
    echo "This is a demo export file for testing purposes." > reports_export/sample_export.xlsx && \
    echo "This is a demo export file for testing purposes." > reports_export/demo_report.docx && \
    echo "This is a demo export file for testing purposes." > reports_export/$(date +%Y-%m-%d)/dynamic_report_$(date +%Y%m%d_%H%M%S).xlsx && \
    echo "This is a demo export file for testing purposes." > reports_export/$(date +%Y-%m-%d)/monthly_report_$(date +%Y%m%d_%H%M%S).docx


EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
