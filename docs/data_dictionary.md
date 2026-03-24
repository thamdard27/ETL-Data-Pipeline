# Data Dictionary: College Scorecard ETL Pipeline

## Overview

This document provides a comprehensive reference for all columns in the `dbo.college_scorecard` table, which is the final output of the ETL pipeline. The table contains higher education institution data sourced from the U.S. Department of Education's College Scorecard API.

---

## Table Information

| Property | Value |
|----------|-------|
| **Database** | SQL Server (Azure SQL Edge) |
| **Schema** | dbo |
| **Table Name** | college_scorecard |
| **Total Columns** | 26 |
| **Record Count** | ~6,150 institutions |
| **Primary Source** | College Scorecard API (api.data.gov) |
| **Update Frequency** | Annual (reflects latest academic year data) |

---

## Column Reference

### Institution Identification

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `school_id` | INT | No | Unique identifier for the institution (OPEID) | 100654 |
| `school_name` | VARCHAR(255) | No | Official name of the institution | "University of Alabama" |
| `school_city` | VARCHAR(100) | No | City where the institution is located | "Tuscaloosa" |
| `school_state` | VARCHAR(2) | No | Two-letter state/territory code | "AL" |
| `school_zip` | VARCHAR(10) | Yes | 5-digit or 9-digit ZIP code | "35487" |
| `school_url` | VARCHAR(255) | Yes | Institution's official website URL | "www.ua.edu" |

### Geographic Coordinates

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `latitude` | FLOAT | Yes | Geographic latitude coordinate | 33.2142 |
| `longitude` | FLOAT | Yes | Geographic longitude coordinate | -87.5391 |

### Institution Classification

| Column Name | Data Type | Nullable | Description | Values |
|-------------|-----------|----------|-------------|--------|
| `ownership` | INT | Yes | Institution ownership type | 1=Public, 2=Private Non-Profit, 3=Private For-Profit |
| `region_id` | INT | Yes | Geographic region code (Bureau of Economic Analysis) | 1-9 |
| `locale` | INT | Yes | Campus setting classification | 11=City-Large, 12=City-Midsize, etc. |
| `operating` | INT | Yes | Operational status indicator | 1=Operating, 0=Closed |

### Enrollment Metrics

| Column Name | Data Type | Nullable | Description | Range |
|-------------|-----------|----------|-------------|-------|
| `student_size` | FLOAT | Yes | Total undergraduate enrollment | 0 - 200,000+ |
| `grad_students` | FLOAT | Yes | Total graduate student enrollment | 0 - 50,000+ |
| `size_category` | VARCHAR(10) | Yes | Derived size classification | "small", "medium", "large" |

**Size Category Definitions:**
- **small**: < 5,000 undergraduate students
- **medium**: 5,000 - 15,000 undergraduate students  
- **large**: > 15,000 undergraduate students

### Admission Metrics

| Column Name | Data Type | Nullable | Description | Range |
|-------------|-----------|----------|-------------|-------|
| `admission_rate` | FLOAT | Yes | Overall admission rate (decimal) | 0.0 - 1.0 |
| `admission_rate_percentage` | FLOAT | Yes | Admission rate as percentage (derived) | 0.0 - 100.0 |

**Interpretation:**
- Values closer to 0.0 indicate highly selective institutions
- Values closer to 1.0 indicate open admission policies
- NULL indicates admission rate is not reported (often for open admission schools)

### Completion Metrics

| Column Name | Data Type | Nullable | Description | Range |
|-------------|-----------|----------|-------------|-------|
| `completion_rate` | FLOAT | Yes | Overall completion rate within 150% of normal time (decimal) | 0.0 - 1.0 |
| `completion_rate_4yr` | FLOAT | Yes | 4-year completion rate (4-year institutions only) | 0.0 - 1.0 |
| `completion_rate_percentage` | FLOAT | Yes | Completion rate as percentage (derived) | 0.0 - 100.0 |

**Interpretation:**
- Calculated for first-time, full-time students
- "150% of normal time" = 6 years for 4-year programs, 3 years for 2-year programs
- Higher values indicate better student outcomes

### Financial Metrics

| Column Name | Data Type | Nullable | Description | Unit |
|-------------|-----------|----------|-------------|------|
| `tuition_in_state` | FLOAT | Yes | Annual in-state tuition and fees | USD |
| `tuition_out_of_state` | FLOAT | Yes | Annual out-of-state tuition and fees | USD |
| `median_debt` | FLOAT | Yes | Median debt of completers | USD |
| `earnings_10yr_median` | FLOAT | Yes | Median earnings 10 years after entry | USD |

**Notes:**
- Tuition reflects the most recent academic year
- Median debt includes federal loans only
- Earnings data has a 10-year lag from enrollment

### Metadata Columns

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `extracted_at` | VARCHAR(50) | No | Timestamp when data was extracted | "2026-03-24T15:31:20" |
| `source` | VARCHAR(50) | No | Data source identifier | "college_scorecard_api" |

---

## Data Quality Notes

### Null Value Prevalence

| Column | Null Rate | Reason |
|--------|-----------|--------|
| `admission_rate` | ~69% | Many schools have open admission (no rate reported) |
| `grad_students` | ~67% | Not applicable for institutions without graduate programs |
| `completion_rate_4yr` | ~65% | Only applicable for 4-year institutions |
| `tuition_*` | ~41% | Some schools don't report tuition data |
| `earnings_10yr_median` | ~18% | Data not available for all cohorts |
| `student_size` | ~11% | Small number of schools don't report enrollment |

### Data Validation Applied

| Validation | Description |
|------------|-------------|
| **Schema Check** | Verified all required columns exist |
| **Null Check** | `school_name` and `school_state` must not be null |
| **Range Check** | Rates constrained to 0.0-1.0, sizes ≥ 0 |
| **Duplicate Removal** | 47 duplicate (name, state) pairs removed |
| **Type Enforcement** | Numeric columns cast to appropriate types |

---

## Derived Columns

These columns are calculated during the Transform stage:

| Column | Formula | Purpose |
|--------|---------|---------|
| `admission_rate_percentage` | `admission_rate * 100` | Dashboard-ready percentage format |
| `completion_rate_percentage` | `completion_rate * 100` | Dashboard-ready percentage format |
| `size_category` | Categorical based on `student_size` thresholds | Segment institutions for analysis |

---

## Common Query Patterns

### Filter by State
```sql
SELECT * FROM dbo.college_scorecard WHERE school_state = 'CA';
```

### Filter by Size Category
```sql
SELECT * FROM dbo.college_scorecard WHERE size_category = 'large';
```

### Filter by Selectivity
```sql
-- Highly selective schools (admission rate < 25%)
SELECT * FROM dbo.college_scorecard WHERE admission_rate < 0.25;
```

### Top Performers
```sql
-- Schools with completion rate > 80%
SELECT * FROM dbo.college_scorecard 
WHERE completion_rate > 0.80 
ORDER BY completion_rate DESC;
```

---

## Related Documentation

| Document | Location | Description |
|----------|----------|-------------|
| Analytics Queries | [sql/analytics.sql](../sql/analytics.sql) | Pre-built analytics queries |
| Database Views | [sql/views.sql](../sql/views.sql) | Dashboard-ready views |
| ETL Pipeline | [main.py](../main.py) | Pipeline orchestrator |
| API Reference | [College Scorecard API](https://collegescorecard.ed.gov/data/) | Source documentation |

---

## Change Log

| Date | Version | Changes |
|------|---------|---------|
| 2026-03-24 | 1.0 | Initial data dictionary created |

---

## Contact

For questions about this data dictionary or the ETL pipeline, contact the Data Engineering team.
