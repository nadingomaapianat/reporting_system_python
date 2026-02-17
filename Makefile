# Reporting Python – common commands
# On Windows without make: python database_service.py
.PHONY: test-db run install

# Test database connection (uses environment.env; NTLM when DB_BACKEND=pymssql)
test-db:
	python database_service.py

# Run API (development)
run:
	python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Install dependencies
install:
	pip install -r requirements.txt
