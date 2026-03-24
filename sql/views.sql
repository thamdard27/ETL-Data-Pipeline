-- ============================================================================
-- COLLEGE SCORECARD ETL PIPELINE - DATABASE VIEWS
-- ============================================================================
-- Description: Enterprise-level database views for BI dashboards (Tableau/Power BI)
-- Database: SQL Server (Azure SQL Edge)
-- Table: dbo.college_scorecard
-- Author: Data Engineering Team
-- Last Updated: 2026-03-24
-- ============================================================================
-- 
-- USAGE NOTES:
-- - These views are optimized for dashboard consumption
-- - All percentage fields are pre-calculated for direct visualization
-- - NULL handling ensures no errors in BI tools
-- - Views follow naming convention: vw_<domain>_<purpose>
--
-- ============================================================================

-- ============================================================================
-- VIEW 1: vw_admissions_summary
-- ============================================================================
-- Purpose: Aggregated admission metrics by state for admission trend analysis
-- 
-- Use Cases:
--   - Tableau: State-level selectivity heatmap
--   - Power BI: Regional admission rate comparison charts
--   - Reporting: Annual admission trends by geography
--
-- Key Metrics:
--   - Total institutions per state
--   - Average, min, max admission rates
--   - Selectivity tier classification
--   - Student enrollment totals
-- ============================================================================

CREATE OR ALTER VIEW dbo.vw_admissions_summary AS
SELECT 
    -- Geographic dimensions
    school_state AS state_code,
    
    -- Institution counts
    COUNT(*) AS total_institutions,
    COUNT(admission_rate) AS institutions_with_admission_data,
    COUNT(CASE WHEN admission_rate IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS data_completeness_pct,
    
    -- Admission rate metrics (as percentages for dashboards)
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    CAST(MIN(admission_rate) * 100 AS DECIMAL(5,2)) AS min_admission_rate_pct,
    CAST(MAX(admission_rate) * 100 AS DECIMAL(5,2)) AS max_admission_rate_pct,
    CAST(STDEV(admission_rate) * 100 AS DECIMAL(5,2)) AS stddev_admission_rate_pct,
    
    -- Selectivity distribution
    SUM(CASE WHEN admission_rate < 0.25 THEN 1 ELSE 0 END) AS highly_selective_count,
    SUM(CASE WHEN admission_rate >= 0.25 AND admission_rate < 0.50 THEN 1 ELSE 0 END) AS selective_count,
    SUM(CASE WHEN admission_rate >= 0.50 AND admission_rate < 0.75 THEN 1 ELSE 0 END) AS moderately_selective_count,
    SUM(CASE WHEN admission_rate >= 0.75 THEN 1 ELSE 0 END) AS open_admission_count,
    
    -- Selectivity tier (for state-level classification)
    CASE 
        WHEN AVG(admission_rate) < 0.40 THEN 'Highly Competitive'
        WHEN AVG(admission_rate) < 0.60 THEN 'Competitive'
        WHEN AVG(admission_rate) < 0.80 THEN 'Moderately Competitive'
        ELSE 'Accessible'
    END AS state_selectivity_tier,
    
    -- Enrollment metrics
    SUM(COALESCE(student_size, 0)) AS total_enrollment,
    AVG(student_size) AS avg_enrollment_per_institution,
    
    -- Size composition
    SUM(CASE WHEN size_category = 'small' THEN 1 ELSE 0 END) AS small_institution_count,
    SUM(CASE WHEN size_category = 'medium' THEN 1 ELSE 0 END) AS medium_institution_count,
    SUM(CASE WHEN size_category = 'large' THEN 1 ELSE 0 END) AS large_institution_count,
    
    -- Financial context
    CAST(AVG(tuition_in_state) AS INT) AS avg_in_state_tuition,
    CAST(AVG(tuition_out_of_state) AS INT) AS avg_out_of_state_tuition,
    
    -- Ranking for dashboards
    RANK() OVER (ORDER BY AVG(admission_rate) ASC) AS selectivity_rank
    
FROM dbo.college_scorecard
GROUP BY school_state;

GO

-- ============================================================================
-- VIEW 2: vw_state_performance
-- ============================================================================
-- Purpose: Comprehensive state-level performance metrics for outcome analysis
-- 
-- Use Cases:
--   - Tableau: State performance scorecards
--   - Power BI: Completion rate vs earnings scatter plots
--   - Reporting: State-by-state educational outcome comparison
--
-- Key Metrics:
--   - Completion rate statistics
--   - Post-graduation earnings
--   - Cost and debt metrics
--   - ROI calculations
-- ============================================================================

CREATE OR ALTER VIEW dbo.vw_state_performance AS
SELECT 
    -- Geographic dimension
    school_state AS state_code,
    
    -- Institution counts
    COUNT(*) AS total_institutions,
    COUNT(completion_rate) AS institutions_with_completion_data,
    
    -- Completion rate metrics (as percentages)
    CAST(AVG(completion_rate) * 100 AS DECIMAL(5,2)) AS avg_completion_rate_pct,
    CAST(MIN(completion_rate) * 100 AS DECIMAL(5,2)) AS min_completion_rate_pct,
    CAST(MAX(completion_rate) * 100 AS DECIMAL(5,2)) AS max_completion_rate_pct,
    CAST(STDEV(completion_rate) * 100 AS DECIMAL(5,2)) AS stddev_completion_rate_pct,
    
    -- Completion tier distribution
    SUM(CASE WHEN completion_rate >= 0.80 THEN 1 ELSE 0 END) AS excellent_completion_count,
    SUM(CASE WHEN completion_rate >= 0.60 AND completion_rate < 0.80 THEN 1 ELSE 0 END) AS good_completion_count,
    SUM(CASE WHEN completion_rate >= 0.40 AND completion_rate < 0.60 THEN 1 ELSE 0 END) AS moderate_completion_count,
    SUM(CASE WHEN completion_rate < 0.40 THEN 1 ELSE 0 END) AS low_completion_count,
    
    -- Performance tier (for state-level classification)
    CASE 
        WHEN AVG(completion_rate) >= 0.65 THEN 'Top Performing'
        WHEN AVG(completion_rate) >= 0.50 THEN 'Above Average'
        WHEN AVG(completion_rate) >= 0.40 THEN 'Average'
        ELSE 'Below Average'
    END AS state_performance_tier,
    
    -- Earnings metrics
    CAST(AVG(earnings_10yr_median) AS INT) AS avg_10yr_earnings,
    CAST(MIN(earnings_10yr_median) AS INT) AS min_10yr_earnings,
    CAST(MAX(earnings_10yr_median) AS INT) AS max_10yr_earnings,
    
    -- Cost metrics
    CAST(AVG(tuition_in_state) AS INT) AS avg_tuition_in_state,
    CAST(AVG(tuition_out_of_state) AS INT) AS avg_tuition_out_of_state,
    CAST(AVG(median_debt) AS INT) AS avg_median_debt,
    
    -- ROI Proxy: Earnings to Debt Ratio (higher is better)
    CASE 
        WHEN AVG(median_debt) > 0 
        THEN CAST(AVG(earnings_10yr_median) / NULLIF(AVG(median_debt), 0) AS DECIMAL(5,2))
        ELSE NULL 
    END AS earnings_to_debt_ratio,
    
    -- ROI Proxy: Earnings to Tuition Ratio (higher is better)
    CASE 
        WHEN AVG(tuition_in_state) > 0 
        THEN CAST(AVG(earnings_10yr_median) / NULLIF(AVG(tuition_in_state), 0) AS DECIMAL(5,2))
        ELSE NULL 
    END AS earnings_to_tuition_ratio,
    
    -- Enrollment
    SUM(COALESCE(student_size, 0)) AS total_enrollment,
    AVG(student_size) AS avg_institution_size,
    
    -- Combined admission + completion (selectivity vs outcomes)
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    
    -- Value score: Completion - (Debt/10000) + (Earnings/50000)
    -- Higher score = better value proposition
    CAST(
        (AVG(completion_rate) * 100) 
        - (COALESCE(AVG(median_debt), 0) / 1000) 
        + (COALESCE(AVG(earnings_10yr_median), 0) / 1000)
    AS DECIMAL(10,2)) AS value_score,
    
    -- Rankings for dashboards
    RANK() OVER (ORDER BY AVG(completion_rate) DESC) AS completion_rank,
    RANK() OVER (ORDER BY AVG(earnings_10yr_median) DESC) AS earnings_rank,
    RANK() OVER (ORDER BY AVG(median_debt) ASC) AS debt_rank_best_first
    
FROM dbo.college_scorecard
GROUP BY school_state;

GO

-- ============================================================================
-- VIEW 3: vw_institution_detail
-- ============================================================================
-- Purpose: Institution-level detail view for drill-down dashboards
-- 
-- Use Cases:
--   - School comparison tools
--   - Detailed institution profiles
--   - Search and filter interfaces
-- ============================================================================

CREATE OR ALTER VIEW dbo.vw_institution_detail AS
SELECT 
    -- Institution identifiers
    school_id,
    school_name,
    school_city,
    school_state,
    school_zip,
    school_url,
    
    -- Location (for map visualizations)
    latitude,
    longitude,
    
    -- Size metrics
    student_size,
    grad_students,
    size_category,
    COALESCE(size_category, 'Unknown') AS size_category_display,
    
    -- Admission metrics (as percentages)
    admission_rate,
    COALESCE(admission_rate_percentage, 0) AS admission_rate_pct,
    CASE 
        WHEN admission_rate < 0.25 THEN 'Highly Selective (<25%)'
        WHEN admission_rate < 0.50 THEN 'Selective (25-50%)'
        WHEN admission_rate < 0.75 THEN 'Moderately Selective (50-75%)'
        WHEN admission_rate IS NOT NULL THEN 'Open Admission (75%+)'
        ELSE 'No Data'
    END AS selectivity_tier,
    
    -- Completion metrics (as percentages)
    completion_rate,
    completion_rate_4yr,
    COALESCE(completion_rate_percentage, 0) AS completion_rate_pct,
    CASE 
        WHEN completion_rate >= 0.80 THEN 'Excellent (80%+)'
        WHEN completion_rate >= 0.60 THEN 'Good (60-80%)'
        WHEN completion_rate >= 0.40 THEN 'Moderate (40-60%)'
        WHEN completion_rate IS NOT NULL THEN 'Low (<40%)'
        ELSE 'No Data'
    END AS completion_tier,
    
    -- Financial metrics
    tuition_in_state,
    tuition_out_of_state,
    median_debt,
    earnings_10yr_median,
    
    -- Calculated ROI metrics
    CASE 
        WHEN median_debt > 0 
        THEN CAST(earnings_10yr_median * 1.0 / median_debt AS DECIMAL(5,2))
        ELSE NULL 
    END AS earnings_to_debt_ratio,
    
    -- Classification
    ownership,
    region_id,
    locale,
    operating,
    
    -- Metadata
    extracted_at,
    source
    
FROM dbo.college_scorecard;

GO

-- ============================================================================
-- VIEW 4: vw_size_category_analysis
-- ============================================================================
-- Purpose: Pre-aggregated size category metrics for quick dashboard loading
-- ============================================================================

CREATE OR ALTER VIEW dbo.vw_size_category_analysis AS
SELECT 
    COALESCE(size_category, 'Unknown') AS size_category,
    
    -- Counts
    COUNT(*) AS institution_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct_of_total,
    
    -- Enrollment
    SUM(COALESCE(student_size, 0)) AS total_enrollment,
    AVG(student_size) AS avg_enrollment,
    
    -- Admission metrics
    CAST(AVG(admission_rate) * 100 AS DECIMAL(5,2)) AS avg_admission_rate_pct,
    
    -- Completion metrics
    CAST(AVG(completion_rate) * 100 AS DECIMAL(5,2)) AS avg_completion_rate_pct,
    
    -- Financial metrics
    CAST(AVG(tuition_in_state) AS INT) AS avg_tuition_in_state,
    CAST(AVG(tuition_out_of_state) AS INT) AS avg_tuition_out_of_state,
    CAST(AVG(median_debt) AS INT) AS avg_median_debt,
    CAST(AVG(earnings_10yr_median) AS INT) AS avg_10yr_earnings,
    
    -- Display order for charts
    CASE size_category 
        WHEN 'large' THEN 1 
        WHEN 'medium' THEN 2 
        WHEN 'small' THEN 3 
        ELSE 4 
    END AS display_order

FROM dbo.college_scorecard
GROUP BY size_category;

GO

-- ============================================================================
-- DOCUMENTATION: VIEW USAGE EXAMPLES
-- ============================================================================
/*

-- Example 1: State Admission Summary for Tableau/Power BI
SELECT * FROM dbo.vw_admissions_summary
ORDER BY selectivity_rank;

-- Example 2: State Performance Dashboard
SELECT 
    state_code,
    avg_completion_rate_pct,
    avg_10yr_earnings,
    value_score,
    state_performance_tier
FROM dbo.vw_state_performance
WHERE total_institutions >= 50
ORDER BY value_score DESC;

-- Example 3: Institution Search with Filters
SELECT 
    school_name,
    school_state,
    selectivity_tier,
    completion_tier,
    tuition_in_state,
    earnings_10yr_median
FROM dbo.vw_institution_detail
WHERE size_category = 'large'
  AND admission_rate_pct BETWEEN 40 AND 70
ORDER BY completion_rate_pct DESC;

-- Example 4: Size Category Comparison Chart Data
SELECT * FROM dbo.vw_size_category_analysis
ORDER BY display_order;

*/

-- ============================================================================
-- END OF VIEWS
-- ============================================================================
