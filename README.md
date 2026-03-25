# College Scorecard ETL Data Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-2019+-CC2927.svg)](https://www.microsoft.com/sql-server)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A **production-grade ETL pipeline** that extracts U.S. college and university data from the Department of Education's College Scorecard API, validates and transforms it, and loads it into SQL Server for analytics.

---

##  Table of Contents

- [Problem Statement](#-problem-statement)
- [Data Source](#-data-source)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Quick Start](#-quick-start)
- [Running the Pipeline](#-running-the-pipeline)
- [Example Output](#-example-output)
- [Error Handling & Validation](#-error-handling--validation)
- [Why ETL? Architecture Decisions](#-why-etl-architecture-decisions)
- [Future Improvements](#-future-improvements)
- [Project Structure](#-project-structure)


---

##  Problem Statement

### Business Challenge

Students, parents, and policymakers need **data-driven insights** to make informed decisions about higher education. With over 6,000 accredited institutions in the United States, comparing schools on metrics like admission rates, graduation outcomes, tuition costs, and post-graduation earnings is overwhelming without proper data infrastructure.

### Solution

This ETL pipeline automates the collection, validation, and storage of comprehensive college data, enabling:

| Stakeholder | Use Case |
|-------------|----------|
| **Students & Parents** | Compare schools by admission rates, completion rates, and ROI |
| **Higher Ed Institutions** | Benchmark performance against peer institutions |
| **Data Analysts** | Build dashboards for enrollment trends and outcome analysis |
| **Policymakers** | Track educational outcomes across states and institution types |

### Key Metrics Enabled

| Metric | Business Value |
|--------|----------------|
| Admission Rate | Understand selectivity and acceptance likelihood |
| Completion Rate | Measure student success and retention |
| Student Size | Assess campus experience and resource availability |
| 10-Year Earnings | Evaluate return on investment |
| Tuition Costs | Compare affordability across institutions |
| Size Category | Segment schools for peer comparisons |

---

##  Data Source

### College Scorecard API (U.S. Department of Education)

The [College Scorecard](https://collegescorecard.ed.gov/) is the federal government's authoritative source for data on U.S. colleges and universities.

| Attribute | Details |
|-----------|---------|
| **Provider** | U.S. Department of Education |
| **Access** | Free API (requires API key from [api.data.gov](https://api.data.gov/signup/)) |
| **Coverage** | 6,197 accredited institutions |
| **Update Frequency** | Annual (academic year data) |
| **Data Points** | 2,000+ fields per institution |

**Data Categories Extracted:**
- **Identity**: Institution name, city, state, ZIP, URL
- **Enrollment**: Student size, graduate student count
- **Selectivity**: Overall admission rate
- **Outcomes**: Completion rate, 4-year completion rate
- **Costs**: In-state tuition, out-of-state tuition
- **Financial**: Median debt, 10-year median earnings

---

##  Architecture

### Pipeline Flow: Extract → Validate → Transform → Load

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ETL PIPELINE ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐    ┌───────────┐    ┌────────────┐    ┌──────────────────┐  │
│   │ EXTRACT  │───▶│ VALIDATE  │───▶│ TRANSFORM  │───▶│      LOAD        │  │
│   │          │    │           │    │            │    │                  │  │
│   │ API Call │    │ Schema    │    │ Clean      │    │ SQL Server       │  │
│   │ Paginated│    │ Nulls     │    │ Derive     │    │ Bulk Insert      │  │
│   │ 6,197    │    │ Ranges    │    │ Standardize│    │ 6,150 records    │  │
│   │ records  │    │ Duplicates│    │ Categorize │    │                  │  │
│   └──────────┘    └───────────┘    └────────────┘    └──────────────────┘  │
│        │                │                │                    │             │
│        ▼                ▼                ▼                    ▼             │
│   ┌──────────┐    ┌───────────┐    ┌────────────┐    ┌──────────────────┐  │
│   │ data/raw │    │ Validation│    │ data/      │    │ dbo.college_     │  │
│   │ .json    │    │ Report    │    │ processed/ │    │ scorecard        │  │
│   └──────────┘    └───────────┘    │ .csv       │    │ (SQL Server)     │  │
│                                    └────────────┘    └──────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Stage Breakdown

| Stage | Description | Input | Output | Duration |
|-------|-------------|-------|--------|----------|
| **Extract** | Fetch data via paginated API calls (100 records/page) | College Scorecard API | 6,197 raw records + JSON backup | ~73s |
| **Validate** | Schema validation, null checks, range validation, duplicate removal | Raw DataFrame | 6,150 cleaned records | <1s |
| **Transform** | Column standardization, derived metrics, size categorization | Validated DataFrame | Analytics-ready DataFrame | <1s |
| **Load** | Bulk insert with batching (500 rows/batch) | Transformed DataFrame | SQL Server table | ~5s |

**Total Pipeline Duration:** ~80 seconds for 6,150 records

---

##  Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Language** | Python 3.10+ | Core pipeline logic |
| **Data Processing** | Pandas 2.0+ | DataFrame operations, transformations |
| **Database ORM** | SQLAlchemy 2.0+ | Connection pooling, transaction management |
| **Database Driver** | pyodbc | SQL Server ODBC connectivity |
| **Database** | SQL Server 2019 (Azure SQL Edge) | Production data warehouse |
| **Containerization** | Docker | Database portability and isolation |
| **Configuration** | python-dotenv | Secure environment variable management |
| **HTTP Client** | requests | API communication with retry logic |
| **Data Formats** | JSON, CSV, Parquet | Multi-format storage for flexibility |

---

##  Quick Start

### Prerequisites

- Python 3.10+
- Docker Desktop
- College Scorecard API key ([Get free key](https://api.data.gov/signup/))

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/ETL-Data-Pipeline.git
cd "ETL Data Pipeline"
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
# or: venv\Scripts\activate  # Windows
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start SQL Server (Docker)

```bash
docker run -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=YourStrong@Pass123" \
  -p 1433:1433 \
  --name sql_server_etl \
  -d mcr.microsoft.com/azure-sql-edge
```

### Step 5: Configure Environment

Create `.env` file in project root:

```env
# API Configuration
COLLEGE_SCORECARD_API_KEY=your_api_key_here

# Database Configuration
DATABASE_URL=mssql+pyodbc://sa:YourStrong%40Pass123@localhost:1433/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

---

##  Running the Pipeline

### Execute Full Pipeline

```bash
python main.py
```

### Expected Output

```
======================================================================
STAGE 1: EXTRACT - Starting
======================================================================
Target: Fetch at least 1000 records from College Scorecard API
Fetching page 1... Page 1: 100 records fetched (1.89s)
...
Total records fetched: 6197

======================================================================
STAGE 2: VALIDATE - Starting
======================================================================
✓ All 5 required columns present
✓ No critical null values
⚠ Found 47 duplicate records - removing

======================================================================
STAGE 3: TRANSFORM - Starting
======================================================================
✓ Created 'admission_rate_percentage'
✓ Created 'completion_rate_percentage'
✓ Created 'size_category'

======================================================================
STAGE 4: LOAD - Starting
======================================================================
✓ Successfully loaded 6150 rows (4.56 seconds, 1348 rows/second)

######################################################################
PIPELINE EXECUTION SUMMARY
######################################################################
Overall Status: ✓ SUCCESS
Duration: 78.24 seconds

Stage           Status     Records In   Records Out  Time (s)
---------------------------------------------------------------
EXTRACT         ✓ Pass     0            6197         73.34
VALIDATE        ✓ Pass     6197         6150         0.01
TRANSFORM       ✓ Pass     6150         6150         0.09
LOAD            ✓ Pass     6150         6150         4.74
```

---

##  Example Output

### Sample Data (First 5 Records)

| school_name | school_state | student_size | admission_rate | completion_rate | size_category |
|-------------|--------------|--------------|----------------|-----------------|---------------|
| Alabama A & M University | AL | 6,207 | 0.8209 | 0.2912 | medium |
| University of Alabama at Birmingham | AL | 13,856 | 0.8073 | 0.5885 | medium |
| University of Alabama in Huntsville | AL | 7,249 | 0.7828 | 0.4891 | medium |
| Alabama State University | AL | 4,006 | 0.9787 | 0.2748 | small |
| The University of Alabama | AL | 32,580 | 0.8025 | 0.7206 | large |

### Data Statistics

| Metric | Value |
|--------|-------|
| Total Institutions | 6,150 |
| States Covered | 59 (including territories) |
| Admission Rate Range | 0% - 100% |
| Average Completion Rate | 56.36% |
| Small Schools (<5,000) | 4,706 (76.5%) |
| Medium Schools (5,000-15,000) | 540 (8.8%) |
| Large Schools (>15,000) | 211 (3.4%) |

---

##  Error Handling & Validation

### Validation Rules Applied

| Check | Description | Action |
|-------|-------------|--------|
| **Schema Validation** | Verify required columns exist | Fail pipeline if missing |
| **Null Checks** | Critical columns (name, state) must be non-null | Flag records with nulls |
| **Data Type Validation** | Ensure numeric columns are numeric | Convert or flag |
| **Range Validation** | Rates must be 0.0-1.0, sizes must be ≥0 | Flag out-of-range values |
| **Duplicate Detection** | Check for duplicate (name, state) pairs | Remove duplicates, keep first |

### Error Handling Strategy

- **Try/Except Wrapping**: Each stage wrapped in error handling
- **Immediate Halt**: Pipeline stops on first failure (fail-fast)
- **Detailed Logging**: Full traceback captured for debugging
- **Status Reporting**: Clear success/failure indication per stage

---

##  Why ETL? Architecture Decisions

### Why ETL Instead of ELT?

| Factor | ETL (Our Choice) | ELT Alternative |
|--------|------------------|-----------------|
| **Data Quality** | Validate BEFORE loading ensures clean warehouse | Raw data in warehouse requires post-load cleaning |
| **Storage Costs** | Only store clean, transformed data | Store raw + transformed (higher costs) |
| **Processing** | Python/Pandas for complex transformations | SQL-based transformations (limited flexibility) |
| **Schema Control** | Define schema before load | Schema-on-read complexity |
| **Use Case Fit** | Structured API data with known schema | Better for unstructured/semi-structured data lakes |

**Decision**: ETL is optimal for this use case because:
1. College Scorecard API returns structured JSON with known schema
2. Data quality issues (duplicates, nulls) should be caught before loading
3. Business logic (size categorization) is easier to implement in Python
4. Final dataset is relatively small (6,150 records) - no need for distributed processing

### Why Validation is Critical

1. **Data Integrity**: Catch malformed records before they corrupt analytics
2. **Business Rules**: Enforce domain constraints (rates between 0-1)
3. **Deduplication**: Prevent double-counting in aggregations
4. **Audit Trail**: Log all validation decisions for compliance

### Why Raw → Processed Data Layers?

| Layer | Purpose | Retention |
|-------|---------|-----------|
| **Raw (data/raw/)** | Immutable source snapshot for audit/reprocessing | Permanent |
| **Staging (data/staging/)** | Intermediate processing artifacts | Temporary |
| **Processed (data/processed/)** | Clean, analytics-ready data | Permanent |
| **Database** | Query-optimized storage for dashboards | Permanent |

**Benefits**:
- **Reproducibility**: Re-run transformations from raw data
- **Debugging**: Compare raw vs. processed to identify issues
- **Compliance**: Maintain source records for audits

---

##  Future Improvements

### Cloud Migration (Azure SQL)

- Migrate from local Docker SQL Server to Azure SQL Database
- Benefits: Managed service, auto-scaling, built-in high availability, geo-redundancy

### Data Warehouse Architecture

- Implement star schema with fact/dimension tables
- Add IPEDS and BLS data sources for comprehensive analytics
- Use Azure Synapse for scalable analytics workloads

### Workflow Orchestration (Airflow/Prefect/Dagster)

```python
# Future: Apache Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG('college_scorecard_etl', schedule='@daily') as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_data)
    validate = PythonOperator(task_id='validate', python_callable=validate_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_data)
    
    extract >> validate >> transform >> load
```

**Benefits of Orchestration Tools**:
- Scheduled execution (daily/weekly refreshes)
- Automatic retries with exponential backoff
- Dependency management between tasks
- Web UI for monitoring and alerting
- Historical run tracking

### Additional Improvements

| Improvement | Priority | Effort |
|-------------|----------|--------|
| Add unit tests (pytest) | High | Medium |
| Implement incremental loads | High | High |
| Add data quality alerts (Slack/email) | Medium | Low |
| Integrate CI/CD (GitHub Actions) | Medium | Medium |
| Add more data sources (IPEDS, BLS) | Low | High |
| Build Tableau/Power BI dashboard | Low | Medium |

---

## 📁 Project Structure

```
ETL Data Pipeline/
├── main.py                      # Pipeline orchestrator (entry point)
├── requirements.txt             # Python dependencies
├── pyproject.toml              # Project configuration
├── .env                        # Environment variables (not in git)
├── README.md                   # This file
│
├── scripts/
│   ├── extract_scorecard.py    # Extract module
│   ├── validate.py             # Validation module
│   ├── transform.py            # Transformation module
│   ├── load.py                 # SQL Server loader
│   └── test_db_connection.py   # Connection testing utility
│
├── sql/
│   ├── analytics.sql           # Analytics queries
│   └── views.sql               # Database views
│
├── docs/
│   └── data_dictionary.md      # Column documentation
│
├── data/
│   ├── raw/                    # Immutable source data (JSON)
│   ├── staging/                # Intermediate files (CSV, Parquet)
│   └── processed/              # Final analytics-ready data
│
├── higher_ed_data_pipeline/    # Core library modules
│   ├── config/settings.py      # Configuration management
│   ├── etl/                    # ETL utilities
│   ├── sources/                # Data source connectors
│   └── utils/                  # Helper functions
│
├── tests/                      # Unit tests
└── logs/                       # Application logs
```


---

**Built for reproducible, automated higher education data analysis**
