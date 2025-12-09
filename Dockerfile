FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install FreeTDS, ODBC, fonts and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    unixodbc \
    unixodbc-dev \
    freetds-bin \
    freetds-dev \
    tdsodbc \
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
    && rm -rf /var/lib/apt/lists/*

# Configure FreeTDS ODBC driver and DSN
RUN printf "[FreeTDS]\nDescription=FreeTDS Driver for SQL Server\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nUsageCount=1\n" \
    > /etc/odbcinst.ini

RUN printf "[SQLServerNTLM]\nDriver=FreeTDS\nServer=10.240.10.202\nPort=5555\nDatabase=NEWDCC-V4-UAT\nTDS_Version=7.3\nUseNTLMv2=Yes\n" \
    > /etc/odbc.ini

# Copy Python dependencies list and install
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Ensure all necessary directories exist
RUN mkdir -p Disclosures template reports_export exports utils/fonts

# Refresh font cache
RUN fc-cache -fv

# Create demo files for testing (optional)
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
