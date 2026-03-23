# Higher Ed Data Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A **fully automated**, production-grade ETL pipeline for higher education data. All data is fetched programmatically from live sources - **no manual file downloads required**.

## Key Principle

> **Live Data Only**: This pipeline fetches data directly from authoritative sources via APIs. The raw data lake stores timestamped snapshots, enabling reproducible analysis and complete audit trails.

## 📋 Table of Contents

- [Features](#-features)
- [Data Sources](#-data-sources)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Usage](#-usage)
- [Data Lake](#-data-lake)
- [Configuration](#-configuration)
- [Project Structure](#-project-structure)
- [Development](#-development)
- [License](#-license)

## ✨ Features

### Automated Data Collection
- **College Scorecard API**: Institution details, enrollment, costs, outcomes
- **IPEDS Data System**: Comprehensive postsecondary education statistics
- **No manual downloads**: All data fetched programmatically
- **Timestamped storage**: Every fetch creates a new versioned snapshot

### Data Lake Architecture
- **Raw layer**: Immutable, timestamped source data
- **Staging layer**: Intermediate processing
- **Processed layer**: Clean, transformed data ready for analysis
- **Metadata tracking**: Full lineage for every dataset

### Engineering Excellence
- **Fully reproducible**: Re-run any pipeline at any time
- **Modular architecture**: Separation of concerns with pluggable components
- **Type safety**: Full type hints with Pydantic validation
- **Comprehensive logging**: Structured logging with file rotation

## 📊 Data Sources

### College Scorecard (US Department of Education)
Free API providing data on US colleges and universities:
- Institution information (name, location, type)
- Enrollment statistics and demographics
- Costs, tuition, and financial aid
- Completion and graduation rates
- Post-graduation earnings outcomes

**API Key**: Get free at https://api.data.gov/signup/

### IPEDS (Integrated Postsecondary Education Data System)
Comprehensive data from NCES (no API key required):
- **HD**: Institutional Directory
- **IC**: Institutional Characteristics (tuition, programs)
- **EFFY**: Fall Enrollment
- **SFA**: Student Financial Aid
- **GR**: Graduation Rates
- **C**: Completions/Degrees

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Automated ETL Pipeline                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐  │
│  │  Live Sources   │───▶│   Transform     │───▶│    Data Lake    │  │
│  │  (APIs)         │    │   & Validate    │    │    Storage      │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘  │
│          │                                              │            │
│          ▼                                              ▼            │
│  ┌─────────────────┐                         ┌─────────────────┐    │
│  │ College Score-  │                         │ data/raw/       │    │
│  │ card API        │                         │   └─ {source}/  │    │
│  │                 │                         │      └─ {ts}.pq │    │
│  │ IPEDS Data      │                         │ data/processed/ │    │
│  │ System          │                         │   └─ {output}.pq│    │
│  └─────────────────┘                         └─────────────────┘    │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

```bash
# Clone and setup
git clone https://github.com/tabasumh27/ETL-Data-Pipeline.git
cd "ETL Data Pipeline"

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set your API key (optional but recommended)
export COLLEGE_SCORECARD_API_KEY="your_api_key_here"

# Run all automated pipelines
python -m higher_ed_data_pipeline.runner run-all
```

## 📖 Usage

### Automated Pipeline Runner

```bash
# Run all configured pipelines
python -m higher_ed_data_pipeline.runner run-all

# Run a specific pipeline
python -m higher_ed_data_pipeline.runner run --pipeline ipeds_directory

# List available pipelines
python -m higher_ed_data_pipeline.runner list-pipelines

# List available data sources
python -m higher_ed_data_pipeline.runner list-sources

# Check data lake status
python -m higher_ed_data_pipeline.runner status
```

### Python API

```python
from higher_ed_data_pipeline import Extractor, DataLake, AutomatedRunner, Settings

# Initialize
settings = Settings()
extractor = Extractor(settings)

# Fetch from College Scorecard (auto-stores in raw data lake)
df = extractor.from_college_scorecard(
    field_groups=["basic", "enrollment", "cost"],
    state="CA",  # California institutions
    store_raw=True
)

print(f"Fetched {len(df)} institutions")
print(df[["name", "city", "student_size", "cost_tuition_in_state"]].head())
```

### Fetch IPEDS Data

```python
# Fetch IPEDS institutional directory
df_directory = extractor.from_ipeds(
    survey="HD",  # Directory
    year=2023,
    store_raw=True
)

# Fetch IPEDS enrollment data
df_enrollment = extractor.from_ipeds(
    survey="EFFY",  # Fall Enrollment
    year=2023,
    store_raw=True
)

# Fetch graduation rates
df_graduation = extractor.from_ipeds(
    survey="GR",
    year=2023,
    store_raw=True
)
```

### Using the Data Lake

```python
from higher_ed_data_pipeline import DataLake

lake = DataLake(settings)

# List all datasets
datasets = lake.list_datasets()
for ds in datasets:
    print(f"{ds['source']}/{ds['dataset']}: {ds['versions']} versions")

# Load latest version
df = lake.load_latest(source="college_scorecard", dataset="all_institutions")

# Load specific version by timestamp
df = lake.load_version(
    source="ipeds",
    dataset="hd_2023",
    timestamp="20240315_143022"
)

# Get dataset metadata
metadata = lake.get_metadata(source="ipeds", dataset="hd_2023")
print(f"Rows: {metadata['row_count']}, Columns: {metadata['column_count']}")
```

### Run Complete Pipeline

```python
from higher_ed_data_pipeline import AutomatedRunner

runner = AutomatedRunner(settings)

# Run all pipelines
results = runner.run_all()

# Check results
for result in results:
    print(f"{result.pipeline_id}: {result.status.value}")
    print(f"  Rows: {result.rows_processed:,} → {result.rows_output:,}")
    print(f"  Duration: {result.duration_seconds:.2f}s")

# Run specific pipeline
result = runner.run_by_name("college_scorecard_full")
```

## 🗄 Data Lake

The data lake follows a medallion architecture:

```
data/
├── raw/                          # Bronze layer - immutable source data
│   ├── college_scorecard/
│   │   ├── all_institutions_20240315_143022.parquet
│   │   ├── all_institutions_20240315_143022.parquet.metadata.json
│   │   └── ...
│   └── ipeds/
│       ├── hd_2023_20240315_150000.parquet
│       ├── effy_2023_20240315_151000.parquet
│       └── ...
├── staging/                      # Silver layer - intermediate
└── processed/                    # Gold layer - ready for analysis
    ├── institutions_processed_20240315_160000.parquet
    └── ipeds_enrollment_processed_20240315_161000.parquet
```

### Data Lake Features

- **Immutable raw data**: Source data is never modified
- **Timestamped versions**: Every fetch creates a new snapshot
- **Metadata tracking**: Schema, row counts, source info stored with each file
- **Easy rollback**: Access any historical version
- **Retention policies**: Configurable cleanup for old versions

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Application
ENVIRONMENT=development
DEBUG=false
LOG_LEVEL=INFO

# Data Source API Keys
COLLEGE_SCORECARD_API_KEY=your_api_key_here

# Processing
BATCH_SIZE=10000
MAX_WORKERS=4
```

### Get API Keys

1. **College Scorecard**: Free at https://api.data.gov/signup/
2. **IPEDS**: No API key required (public data)

## 📁 Project Structure

```
higher_ed_data_pipeline/
├── data/
│   ├── raw/                  # Raw data lake (timestamped snapshots)
│   ├── staging/              # Intermediate processing
│   └── processed/            # Final outputs
├── higher_ed_data_pipeline/
│   ├── config/
│   │   └── settings.py       # Configuration management
│   ├── etl/
│   │   ├── extract.py        # Data extraction (including live sources)
│   │   ├── transform.py      # Data transformation
│   │   ├── load.py           # Data loading
│   │   └── pipeline.py       # Pipeline orchestration
│   ├── sources/
│   │   ├── base.py           # Base data source class
│   │   ├── college_scorecard.py  # College Scorecard API
│   │   └── ipeds.py          # IPEDS data source
│   ├── storage/
│   │   └── __init__.py       # Data lake management
│   ├── utils/
│   │   ├── logging.py        # Logging utilities
│   │   └── helpers.py        # Helper functions
│   └── runner.py             # Automated pipeline runner
├── logs/                     # Application logs
├── tests/                    # Test suite
├── main.py                   # CLI entry point
├── pyproject.toml            # Project configuration
├── requirements.txt          # Dependencies
└── README.md
```

## 🛠 Development

### Setup Development Environment

```bash
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

### Run Tests

```bash
pytest tests/ -v
pytest --cov=higher_ed_data_pipeline
```

### Code Quality

```bash
black higher_ed_data_pipeline tests
isort higher_ed_data_pipeline tests
flake8 higher_ed_data_pipeline tests
mypy higher_ed_data_pipeline
```

## 🐳 Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run automated pipelines
CMD ["python", "-m", "higher_ed_data_pipeline.runner", "run-all"]
```

## 🔄 Scheduled Execution

### Cron (Linux/Mac)

```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/project && /path/to/venv/bin/python -m higher_ed_data_pipeline.runner run-all
```

### GitHub Actions

```yaml
name: Automated ETL
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
  workflow_dispatch:  # Manual trigger

jobs:
  etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python -m higher_ed_data_pipeline.runner run-all
        env:
          COLLEGE_SCORECARD_API_KEY: ${{ secrets.SCORECARD_API_KEY }}
```

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

**Built for reproducible, automated higher education data analysis**
