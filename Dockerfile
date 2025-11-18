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
    fonts-dejavu \
    fonts-dejavu-core \
    fonts-dejavu-extra \
    fonts-noto \
    fonts-noto-core \
    fonts-noto-extra \
    fonts-noto-ui-core \
    fonts-liberation \
    fontconfig \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/* \
    && fc-cache -fv

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure all necessary directories exist
RUN mkdir -p Disclosures template reports_export exports utils/fonts

# Note: Custom fonts in utils/fonts/ are already copied by "COPY . ." above
# If the directory is empty, that's fine - system fonts will be used

# Ensure font cache is updated after font installation
RUN fc-cache -fv

# Create demo files for testing (optional - can be removed if not needed)
RUN echo "This is a demo file for testing purposes." > Disclosures/Sample_Disclosure_Report_2025.pdf && \
    echo "This is a demo file for testing purposes." > Disclosures/Financial_Disclosure_Demo.pdf && \
    echo "This is a demo file for testing purposes." > Disclosures/Risk_Assessment_Sample.pdf && \
    echo "This is a demo file for testing purposes." > Disclosures/Compliance_Report_Demo.pdf && \
    echo "This is a demo template file for testing purposes." > template/Monthly_Report_Template.docx && \
    echo "This is a demo template file for testing purposes." > template/Risk_Assessment_Template.docx && \
    echo "This is a demo template file for testing purposes." > template/Financial_Analysis_Template.docx && \
    echo "This is a demo template file for testing purposes." > template/Compliance_Checklist_Template.docx && \
    echo "This is a demo template file for testing purposes." > template/Dashboard_Report_Template.docx && \
    mkdir -p reports_export && \
    echo "This is a demo export file for testing purposes." > reports_export/sample_export.xlsx && \
    echo "This is a demo export file for testing purposes." > reports_export/demo_report.docx


EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
