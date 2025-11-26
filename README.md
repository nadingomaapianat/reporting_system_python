# Reporting System - Python Backend

A professional, modular reporting system for GRC dashboards with PDF and Excel export capabilities.

## 🏗️ Architecture

The system is organized into a clean, modular structure:

```
reporting_system_python/
├── config/                 # Configuration management
│   ├── __init__.py
│   └── settings.py        # Database, API, and export settings
├── models/                # Data models
│   ├── __init__.py
│   └── data_models.py     # Pydantic models for data structures
├── services/              # Business logic services
│   ├── __init__.py
│   ├── database_service.py    # Database operations
│   ├── api_service.py         # Node.js API communication
│   ├── pdf_service.py         # PDF generation
│   └── excel_service.py       # Excel generation
├── utils/                 # Utilities and API routes
│   ├── __init__.py
│   └── api_routes.py      # FastAPI route definitions
├── main_new.py           # New modular main application
├── main.py               # Legacy main application (for reference)
├── shared_pdf_utils.py   # Shared PDF utilities
├── export_utils.py       # Export utilities
└── README.md             # This file
```

## 🚀 Features

### Core Functionality
- **Modular Architecture**: Clean separation of concerns
- **Professional Code Structure**: Easy to maintain and extend
- **Type Safety**: Full type hints and data models
- **Error Handling**: Comprehensive error handling throughout
- **Configuration Management**: Centralized configuration
- **Service Layer**: Reusable business logic

### Export Capabilities
- **PDF Reports**: Professional PDF generation with charts and tables
- **Excel Reports**: Excel files with formatting and charts
- **Chart Generation**: Matplotlib-based chart creation
- **Custom Headers**: Configurable logos, company info, watermarks
- **Arabic Support**: Full Arabic text support
- **Multi-line Text**: Proper text wrapping in reports

### Dashboard Support
- **Risks Dashboard**: Complete risk management reporting
- **Controls Dashboard**: Control management reporting
- **Card-specific Exports**: Export individual dashboard cards
- **Full Reports**: Export complete dashboard data

## 🛠️ Installation

1. **Install Dependencies**:
```bash
pip install fastapi uvicorn pyodbc openpyxl reportlab matplotlib aiohttp redis
```

2. **Database Setup**:
   - Configure SQL Server connection in `config/settings.py`
   - Ensure database is accessible

3. **Run the Application**:
```bash
# New modular version
python main_new.py

# Legacy version (for reference)
python main.py
```

## 📚 Usage

### API Endpoints

#### Risks Dashboard
- `GET /api/grc/risks/export-pdf` - Export risks as PDF
- `GET /api/grc/risks/export-excel` - Export risks as Excel

#### Controls Dashboard
- `GET /api/grc/controls/export-pdf` - Export controls as PDF
- `GET /api/grc/controls/export-excel` - Export controls as Excel

#### Health Check
- `GET /health` - System health status

### Query Parameters

All export endpoints support:
- `startDate` - Start date filter (optional)
- `endDate` - End date filter (optional)
- `headerConfig` - JSON string with header configuration
- `cardType` - Specific card to export (optional)
- `onlyCard` - Export only specific card (boolean)
- `onlyChart` - Export only chart (boolean)
- `chartType` - Type of chart to export (optional)
- `onlyOverallTable` - Export only overall table (boolean)

### Example Usage

```bash
# Export all risks as PDF
curl "https://reporting-system-python.pianat.ai/api/grc/risks/export-pdf"

# Export specific risk card as Excel
curl "https://reporting-system-python.pianat.ai/api/grc/risks/export-excel?cardType=totalRisks&onlyCard=true"

# Export with date range
curl "https://reporting-system-python.pianat.ai/api/grc/controls/export-pdf?startDate=2024-01-01&endDate=2024-12-31"
```

## 🔧 Configuration

### Database Configuration
Edit `config/settings.py`:
```python
DATABASE_CONFIG = {
    'server': 'your-server',
    'port': '1433',
    'database': 'your-database',
    'username': 'your-username',
    'password': 'your-password',
    # ... other settings
}
```

### API Configuration
```python
API_CONFIG = {
    'node_api_url': 'http://localhost:3002',
    'python_api_url': 'https://reporting-system-python.pianat.ai',
    'timeout': 30
}
```

### Export Configuration
```python
EXPORT_CONFIG = {
    'max_rows_per_sheet': 10000,
    'chart_dpi': 150,
    'chart_figsize': (8, 4),
    'default_font_size': 10,
    # ... other settings
}
```

## 🏛️ Architecture Benefits

### 1. **Separation of Concerns**
- **Config**: Centralized configuration management
- **Models**: Data structure definitions
- **Services**: Business logic and data operations
- **Utils**: API routes and utilities

### 2. **Reusability**
- Services can be reused across different endpoints
- Common functionality is centralized
- Easy to add new dashboard types

### 3. **Maintainability**
- Clear code organization
- Easy to locate and fix issues
- Simple to add new features

### 4. **Testability**
- Each service can be tested independently
- Mock dependencies easily
- Clear interfaces between components

### 5. **Scalability**
- Easy to add new services
- Simple to extend functionality
- Clear patterns for new features

## 🔄 Migration from Legacy Code

The new modular structure replaces the monolithic `main.py` file:

1. **Configuration**: Moved to `config/settings.py`
2. **Data Models**: Defined in `models/data_models.py`
3. **Database Operations**: Moved to `services/database_service.py`
4. **API Communication**: Moved to `services/api_service.py`
5. **PDF Generation**: Moved to `services/pdf_service.py`
6. **Excel Generation**: Moved to `services/excel_service.py`
7. **API Routes**: Moved to `utils/api_routes.py`

## 🚀 Future Enhancements

- **Authentication**: Add JWT-based authentication
- **Caching**: Implement Redis caching for better performance
- **Logging**: Add structured logging
- **Monitoring**: Add health checks and metrics
- **Testing**: Add comprehensive test suite
- **Documentation**: Add OpenAPI/Swagger documentation

## 📝 Development Guidelines

1. **Code Style**: Follow PEP 8 guidelines
2. **Type Hints**: Always use type hints
3. **Error Handling**: Implement comprehensive error handling
4. **Documentation**: Add docstrings to all functions
5. **Testing**: Write tests for new functionality
6. **Modularity**: Keep components loosely coupled

## 🤝 Contributing

1. Follow the established architecture patterns
2. Add type hints to all new code
3. Include error handling
4. Update documentation
5. Test thoroughly before submitting

---

**Note**: The legacy `main.py` file is kept for reference but should not be used for new development. Use `main_new.py` for the modular architecture.
