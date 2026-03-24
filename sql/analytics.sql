-- ============================================================================
-- COLLEGE SCORECARD ETL PIPELINE - ANALYTICS QUERIES
-- ============================================================================
-- Description: Production-ready analytics queries for the college_scorecard table
-- Database: SQL Server (Azure SQL Edge)
-- Table: dbo.college_scorecard
-- Author: Data Engineering Team
-- Last Updated: 2026-03-24
-- ============================================================================

-- ============================================================================
-- SECTION 1: INDEX RECOMMENDATIONS
-- ============================================================================
-- Create indexes to optimize query performance for common analytics patterns

-- Primary index on school identifier
CREATE UNIQUE INDEX IF NOT EXISTS IX_college_scorecard_school_id 
    ON dbo.college_scorecard (school_id);

-- Index for state-based filtering (common in regional analytics)
CREATE INDEX IF NOT EXISTS IX_college_scorecard_state 
    ON dbo.college_scorecard (school_state);

-- Index for size category analysis
CREATE INDEX IF NOT EXISTS IX_college_scorecard_size_category 
    ON dbo.college_scorecard (size_category);

-- Composite index for admission/completion rate analysis
CREATE INDEX IF NOT EXISTS IX_college_scorecard_rates 
    ON dbo.college_scorecard (admission_rate, completion_rate);

-- Index for student size ranking queries
CREATE INDEX IF NOT EXISTS IX_college_scorecard_student_size 
    ON dbo.college_scorecard (student_size DESC);

GO

-- ============================================================================
-- SECTION 2: TOP SCHOOLS BY STUDENT SIZE
-- ============================================================================
-- Business Use Case: Identify the largest institutions for resource planning,
-- peer benchmarking, and understanding market concentration in higher education.

-- Query 2.1: Top 25 Schools by Total Enrollment
SELECT TOP 25
    school_name,
    school_state,
    school_city,
    student_size,
    size_category,
    admission_rate_percentage,
    completion_rate_percentage,
    tuition_in_state,
    tuition_out_of_state,
    RANK() OVER (ORDER BY student_size DESC) AS size_rank
FROM dbo.college_scorecard
WHERE student_size IS NOT NULL
ORDER BY student_size DESC;

-- Query 2.2: Top 10 Schools by State with Student Size
-- Shows largest school in each state
SELECT 
    school_state,
    school_name,
    student_size,
    size_category,
    admission_rate_percentage,
    completion_rate_percentage
FROM (
    SELECT 
        school_state,
        school_name,
        student_size,
        size_category,
        admission_rate_percentage,
        completion_rate_percentage,
        ROW_NUMBER() OVER (PARTITION BY school_state ORDER BY student_size DESC) AS state_rank
    FROM dbo.college_scorecard
    WHERE student_size IS NOT NULL
) ranked
WHERE state_rank = 1
ORDER BY student_size DESC;

-- Query 2.3: Student Size Statistics by Size Category
SELECT 
    size_category,
    COUNT(*) AS institution_count,
    SUM(student_size) AS total_students,
    AVG(student_size) AS avg_students,
    MIN(student_size) AS min_students,
    MAX(student_size) AS max_students,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct_of_institutions
FROM dbo.college_scorecard
WHERE size_category IS NOT NULL
GROUP BY size_category
ORDER BY 
    CASE size_category 
        WHEN 'large' THEN 1 
        WHEN 'medium' THEN 2 
        WHEN 'small' THEN 3 
    END;

GO

-- ============================================================================
-- SECTION 3: AVERAGE ADMISSION RATE BY STATE
-- ============================================================================
-- Business Use Case: Compare selectivity across states to understand regional
-- competitiveness and guide student application strategies.

-- Query 3.1: State-Level Admission Rate Statistics
SELECT 
    school_state,
    COUNT(*) AS total_schools,
    COUNT(admission_rate) AS schools_with_rate,
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    CAST(MIN(admission_rate) * 100 AS DECIMAL(5,2)) AS min_admission_rate_pct,
    CAST(MAX(admission_rate) * 100 AS DECIMAL(5,2)) AS max_admission_rate_pct,
    CAST(STDEV(admission_rate) * 100 AS DECIMAL(5,2)) AS stddev_admission_rate_pct
FROM dbo.college_scorecard
GROUP BY school_state
HAVING COUNT(admission_rate) >= 5  -- Only states with meaningful sample
ORDER BY AVG(admission_rate) ASC;  -- Most selective states first

-- Query 3.2: States Ranked by Selectivity (Most to Least Selective)
SELECT 
    school_state,
    COUNT(*) AS institution_count,
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    RANK() OVER (ORDER BY AVG(admission_rate) ASC) AS selectivity_rank,
    CASE 
        WHEN AVG(admission_rate) < 0.30 THEN 'Highly Selective'
        WHEN AVG(admission_rate) < 0.50 THEN 'Selective'
        WHEN AVG(admission_rate) < 0.70 THEN 'Moderately Selective'
        ELSE 'Open Admission'
    END AS selectivity_tier
FROM dbo.college_scorecard
WHERE admission_rate IS NOT NULL
GROUP BY school_state
HAVING COUNT(*) >= 10
ORDER BY AVG(admission_rate) ASC;

-- Query 3.3: Admission Rate Distribution by State and Size Category
SELECT 
    school_state,
    size_category,
    COUNT(*) AS school_count,
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct
FROM dbo.college_scorecard
WHERE admission_rate IS NOT NULL 
  AND size_category IS NOT NULL
GROUP BY school_state, size_category
HAVING COUNT(*) >= 3
ORDER BY school_state, 
    CASE size_category 
        WHEN 'large' THEN 1 
        WHEN 'medium' THEN 2 
        WHEN 'small' THEN 3 
    END;

GO

-- ============================================================================
-- SECTION 4: SCHOOLS WITH HIGHEST COMPLETION RATES
-- ============================================================================
-- Business Use Case: Identify institutions with best student outcomes for
-- accreditation reviews, student advising, and policy benchmarking.

-- Query 4.1: Top 50 Schools by Completion Rate
SELECT TOP 50
    school_name,
    school_state,
    school_city,
    completion_rate_percentage,
    admission_rate_percentage,
    student_size,
    size_category,
    tuition_in_state,
    earnings_10yr_median,
    RANK() OVER (ORDER BY completion_rate DESC) AS completion_rank
FROM dbo.college_scorecard
WHERE completion_rate IS NOT NULL
ORDER BY completion_rate DESC;

-- Query 4.2: High-Performing Schools (Completion Rate > 80%)
SELECT 
    school_name,
    school_state,
    completion_rate_percentage,
    admission_rate_percentage,
    student_size,
    earnings_10yr_median,
    CASE 
        WHEN completion_rate >= 0.90 THEN 'Elite (90%+)'
        WHEN completion_rate >= 0.80 THEN 'Excellent (80-90%)'
        ELSE 'Other'
    END AS performance_tier
FROM dbo.college_scorecard
WHERE completion_rate >= 0.80
ORDER BY completion_rate DESC;

-- Query 4.3: Completion Rate vs Admission Rate Correlation Analysis
-- Identifies schools that buck the trend (high selectivity != high completion)
SELECT 
    school_name,
    school_state,
    admission_rate_percentage,
    completion_rate_percentage,
    student_size,
    CASE 
        WHEN admission_rate < 0.30 AND completion_rate > 0.80 THEN 'Selective + High Completion'
        WHEN admission_rate >= 0.70 AND completion_rate > 0.70 THEN 'Open + High Completion'
        WHEN admission_rate < 0.30 AND completion_rate < 0.50 THEN 'Selective + Low Completion'
        WHEN admission_rate >= 0.70 AND completion_rate < 0.40 THEN 'Open + Low Completion'
        ELSE 'Average'
    END AS admission_completion_profile
FROM dbo.college_scorecard
WHERE admission_rate IS NOT NULL 
  AND completion_rate IS NOT NULL
ORDER BY 
    CASE 
        WHEN admission_rate < 0.30 AND completion_rate > 0.80 THEN 1
        WHEN admission_rate >= 0.70 AND completion_rate > 0.70 THEN 2
        ELSE 3
    END,
    completion_rate DESC;

GO

-- ============================================================================
-- SECTION 5: DISTRIBUTION OF SCHOOL SIZE CATEGORIES
-- ============================================================================
-- Business Use Case: Understand the composition of U.S. higher education by
-- institution size for market analysis and resource allocation.

-- Query 5.1: Size Category Distribution Overview
SELECT 
    COALESCE(size_category, 'Unknown') AS size_category,
    COUNT(*) AS institution_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct_of_total,
    SUM(COALESCE(student_size, 0)) AS total_enrollment,
    AVG(student_size) AS avg_enrollment,
    AVG(completion_rate_percentage) AS avg_completion_rate_pct,
    AVG(admission_rate_percentage) AS avg_admission_rate_pct
FROM dbo.college_scorecard
GROUP BY size_category
ORDER BY 
    CASE size_category 
        WHEN 'large' THEN 1 
        WHEN 'medium' THEN 2 
        WHEN 'small' THEN 3 
        ELSE 4 
    END;

-- Query 5.2: Size Category by State
SELECT 
    school_state,
    SUM(CASE WHEN size_category = 'small' THEN 1 ELSE 0 END) AS small_schools,
    SUM(CASE WHEN size_category = 'medium' THEN 1 ELSE 0 END) AS medium_schools,
    SUM(CASE WHEN size_category = 'large' THEN 1 ELSE 0 END) AS large_schools,
    COUNT(*) AS total_schools,
    CAST(SUM(CASE WHEN size_category = 'large' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_large
FROM dbo.college_scorecard
GROUP BY school_state
HAVING COUNT(*) >= 20
ORDER BY SUM(CASE WHEN size_category = 'large' THEN 1 ELSE 0 END) DESC;

-- Query 5.3: Size Category Performance Comparison
SELECT 
    size_category,
    COUNT(*) AS school_count,
    
    -- Admission metrics
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    
    -- Completion metrics
    CAST(AVG(completion_rate) * 100 AS DECIMAL(5,2)) AS avg_completion_rate_pct,
    
    -- Financial metrics
    AVG(tuition_in_state) AS avg_tuition_in_state,
    AVG(tuition_out_of_state) AS avg_tuition_out_of_state,
    AVG(median_debt) AS avg_median_debt,
    AVG(earnings_10yr_median) AS avg_10yr_earnings
FROM dbo.college_scorecard
WHERE size_category IS NOT NULL
GROUP BY size_category
ORDER BY 
    CASE size_category 
        WHEN 'large' THEN 1 
        WHEN 'medium' THEN 2 
        WHEN 'small' THEN 3 
    END;

GO

-- ============================================================================
-- SECTION 6: EXECUTIVE SUMMARY QUERIES
-- ============================================================================
-- Quick overview queries for dashboards and reports

-- Query 6.1: Overall Dataset Summary
SELECT 
    COUNT(*) AS total_institutions,
    COUNT(DISTINCT school_state) AS states_covered,
    SUM(student_size) AS total_enrollment,
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    CAST(AVG(completion_rate) * 100 AS DECIMAL(5,2)) AS avg_completion_rate_pct,
    CAST(AVG(tuition_in_state) AS INT) AS avg_in_state_tuition,
    CAST(AVG(earnings_10yr_median) AS INT) AS avg_10yr_earnings
FROM dbo.college_scorecard;

-- Query 6.2: Quick State Ranking (Top 10 by Institution Count)
SELECT TOP 10
    school_state,
    COUNT(*) AS institution_count,
    SUM(student_size) AS total_enrollment,
    CAST(AVG(completion_rate) * 100 AS DECIMAL(5,2)) AS avg_completion_pct
FROM dbo.college_scorecard
GROUP BY school_state
ORDER BY COUNT(*) DESC;

-- Query 6.3: Data Quality Metrics
SELECT 
    'Total Records' AS metric,
    CAST(COUNT(*) AS VARCHAR(20)) AS value
FROM dbo.college_scorecard
UNION ALL
SELECT 
    'Records with Admission Rate',
    CAST(COUNT(admission_rate) AS VARCHAR(20))
FROM dbo.college_scorecard
UNION ALL
SELECT 
    'Records with Completion Rate',
    CAST(COUNT(completion_rate) AS VARCHAR(20))
FROM dbo.college_scorecard
UNION ALL
SELECT 
    'Records with Student Size',
    CAST(COUNT(student_size) AS VARCHAR(20))
FROM dbo.college_scorecard
UNION ALL
SELECT 
    'Records with Size Category',
    CAST(COUNT(size_category) AS VARCHAR(20))
FROM dbo.college_scorecard;

GO

-- ============================================================================
-- END OF ANALYTICS QUERIES
-- ============================================================================
