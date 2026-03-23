# Higher Ed Data Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A production-grade ETL (Extract, Transform, Load) pipeline designed for higher education data processing. Built with clean architecture principles, this modular framework supports scalable data workflows from multiple sources to various destinations.

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Usage](#-usage)
- [Configuration](#-configuration)
- [Project Structure](#-project-structure)
- [Development](#-development)
- [Testing](#-testing)
- [Deployment](#-deployment)
- [Contributing](#-contributing)
- [License](#-license)

## ✨ Features

### Core Capabilities
- **Multi-source extraction**: Files (CSV, JSON, Parquet, Excel), databases (PostgreSQL, MySQL), APIs, cloud storage (S3, GCS)
- **Flexible transformations**: Data cleaning, type conversion, validation, aggregation, and custom transformations
- **Multi-destination loading**: Local files, databases, cloud storage with automatic format handling
- **Pipeline orchestration**: Fluent API for building complex ETL workflows

### Engineering Excellence
- **Modular architecture**: Separation of concerns with pluggable components
- **Type safety**: Full type hints with Pydantic validation
- **Comprehensive logging**: Structured logging with loguru, file rotation, and monitoring
- **Error handling**: Retry logic, graceful degradation, and detailed error reporting
- **Cloud-ready**: Native support for AWS, GCP, and Azure cloud services
- **Scalable design**: Batch processing, chunked reads, and parallel execution support

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      ETL Pipeline Orchestrator                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌───────────┐    ┌───────────────┐    ┌───────────┐            │
│  │  Extract  │───▶│   Transform   │───▶│    Load   │            │
│  └───────────┘    └───────────────┘    └───────────┘            │
│       │                   │                   │                  │
│       ▼                   ▼                   ▼                  │
│  ┌─────────┐        ┌─────────┐        ┌─────────┐              │
│  │  Files  │        │  Clean  │        │  Files  │              │
│  │   DBs   │        │Validate │        │   DBs   │              │
│  │  APIs   │        │  Enrich │        │  Cloud  │              │
│  │  Cloud  │        │Aggregate│        │         │              │
│  └─────────┘        └─────────┘        └─────────┘              │
│                                                                   │
├─────────────────────────────────────────────────────────────────┤
│  Config Management  │  Logging & Monitoring  │  Utilities        │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/your-org/higher_ed_data_pipeline.git
cd higher_ed_data_pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize directories
python main.py init

# Run a simple pipeline
python main.py run \
    --source data/raw/students.csv \
    --dest data/processed/students.parquet
```

## 📦 Installation

### Prerequisites
- Python 3.10 or higher
- pip or pipenv

### Standard Installation

```bash
pip install -r requirements.txt
```

### Development Installation

```bash
pip install -e ".[dev]"
```

### With Cloud Support

```bash
# AWS support
pip install -e ".[aws]"

# GCP support
pip install -e ".[gcp]"

# All cloud providers
pip install -e ".[all-cloud]"
```

## 📖 Usage

### Command Line Interface

```bash
# Show help
python main.py --help

# Run a simple ETL pipeline
python main.py run -s input.csv -d output.parquet

# With options
python main.py run \
    --source data/raw/enrollments.csv \
    --dest data/processed/enrollments.parquet \
    --source-format csv \
    --dest-format parquet \
    --clean \
    --standardize

# Validate configuration
python main.py validate

# Show pipeline info
python main.py info
```

### Python API

```python
from higher_ed_data_pipeline import ETLPipeline, Settings
from higher_ed_data_pipeline.etl import Extractor, Transformer, Loader

# Initialize components
settings = Settings()
extractor = Extractor(settings)
transformer = Transformer(settings)
loader = Loader(settings)

# Simple pipeline
pipeline = ETLPipeline(settings, name="Student Data Pipeline")
result = pipeline.run_simple(
    source="data/raw/students.csv",
    destination="data/processed/students.parquet",
    clean=True,
    standardize_columns=True,
)

print(f"Status: {result.status}")
print(f"Rows processed: {result.rows_processed}")
```

### Advanced Pipeline

```python
from higher_ed_data_pipeline import ETLPipeline

# Build complex pipeline with fluent API
pipeline = (ETLPipeline(name="Enrollment Analytics")
    .extract(extractor.from_csv, "data/raw/enrollments.csv")
    .transform(transformer.standardize_columns)
    .transform(transformer.clean, handle_missing="fill")
    .transform(transformer.convert_types, {
        "enrollment_date": "datetime",
        "credits": "int",
        "gpa": "float"
    })
    .transform(transformer.filter_rows, "credits > 0")
    .transform(transformer.add_derived_columns, {
        "academic_year": lambda r: r["enrollment_date"].year,
        "is_full_time": lambda r: r["credits"] >= 12
    })
    .load(loader.to_parquet, "data/processed/enrollments.parquet"))

result = pipeline.run()

if result.status.value == "success":
    print(f"Pipeline completed in {result.duration_seconds:.2f}s")
else:
    print(f"Pipeline failed: {result.error}")
```

### Working with Databases

```python
# Extract from database
df = extractor.from_database("""
    SELECT s.student_id, s.name, e.course_id, e.grade
    FROM students s
    JOIN enrollments e ON s.student_id = e.student_id
    WHERE e.semester = '2024-Spring'
""")

# Load to database
loader.to_database(df, "student_enrollments", if_exists="append")

# Upsert with conflict handling
loader.to_database_upsert(
    df,
    "student_enrollments",
    primary_key=["student_id", "course_id"]
)
```

### Cloud Storage Integration

```python
# Extract from S3
df = extractor.from_s3(
    bucket="my-data-bucket",
    key="raw/students.parquet",
    file_format="parquet"
)

# Load to S3
loader.to_s3(
    df,
    bucket="my-data-bucket",
    key="processed/students.parquet",
    file_format="parquet"
)

# Load to GCS
loader.to_gcs(
    df,
    bucket="my-gcs-bucket",
    blob_name="processed/students.parquet"
)
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Application
ENVIRONMENT=development
DEBUG=false
LOG_LEVEL=INFO

# Database
DATABASE_URL=postgresql://user:password@localhost:5432/higher_ed

# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket

# GCP
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=your-bucket

# Processing
BATCH_SIZE=10000
MAX_WORKERS=4
RETRY_ATTEMPTS=3
```

### Settings Class

```python
from higher_ed_data_pipeline.config.settings import Settings, get_settings

# Use global settings
settings = get_settings()

# Or create custom settings
settings = Settings(
    environment="production",
    log_level="INFO",
    batch_size=50000,
    max_workers=8,
)
```

## 📁 Project Structure

```
higher_ed_data_pipeline/
├── data/
│   ├── raw/              # Source data files
│   ├── staging/          # Intermediate processing
│   └── processed/        # Final output files
├── higher_ed_data_pipeline/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py   # Configuration management
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py    # Data extraction
│   │   ├── transform.py  # Data transformation
│   │   ├── load.py       # Data loading
│   │   └── pipeline.py   # Pipeline orchestration
│   └── utils/
│       ├── __init__.py
│       ├── logging.py    # Logging utilities
│       └── helpers.py    # Helper functions
├── logs/                 # Application logs
├── scripts/              # Utility scripts
├── sql/                  # SQL queries and schemas
├── tests/
│   ├── __init__.py
│   └── test_settings.py
├── .env.example
├── .gitignore
├── main.py               # CLI entry point
├── pyproject.toml        # Project configuration
├── README.md
└── requirements.txt
```

## 🛠 Development

### Setup Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Code Quality Tools

```bash
# Format code
black higher_ed_data_pipeline tests

# Sort imports
isort higher_ed_data_pipeline tests

# Lint
flake8 higher_ed_data_pipeline tests

# Type check
mypy higher_ed_data_pipeline
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=higher_ed_data_pipeline --cov-report=html

# Run specific test file
pytest tests/test_settings.py

# Run with verbose output
pytest -v --tb=long
```

## 🚢 Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py", "run", "--config", "/config/pipeline.yaml"]
```

### Docker Compose

```yaml
version: '3.8'
services:
  etl-pipeline:
    build: .
    environment:
      - ENVIRONMENT=production
      - DATABASE_URL=postgresql://user:pass@db:5432/higher_ed
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    depends_on:
      - db
  
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: higher_ed
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
```

### Cloud Deployment

The pipeline is designed to run on:
- **AWS**: Lambda, ECS, Batch, Step Functions
- **GCP**: Cloud Functions, Cloud Run, Dataflow
- **Azure**: Functions, Container Apps, Data Factory

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Coding Standards
- Follow PEP 8 style guide
- Use type hints for all functions
- Write docstrings for public APIs
- Add tests for new features
- Update documentation as needed

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [pandas](https://pandas.pydata.org/) - Data manipulation
- [SQLAlchemy](https://www.sqlalchemy.org/) - Database connectivity
- [Pydantic](https://pydantic-docs.helpmanual.io/) - Data validation
- [loguru](https://github.com/Delgan/loguru) - Logging
- [Click](https://click.palletsprojects.com/) - CLI framework

---

**Built with ❤️ by the Data Engineering Team**
