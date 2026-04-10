-- ============================================================
-- STEP 3: Forecast with Snowflake ML
-- PS: "Train the ML Forecasting model per geography. Evaluate
-- MAPE and generate 30-day forward projections."
--
-- Architecture:
--   Reads from:  MARTS.FEATURES_ENGINEERED
--   Writes to:   MARTS.FORECAST_OUTPUT (replaces stub)
--                MARTS.FORECAST_EVAL (MAPE per country)
--
-- DATA QUALITY NOTES (from CSV analysis):
--   DROPPED from ML: RECOVERY_RATE (58% NULL), VACC_IMPACT_PROXY (36% NULL),
--     DOUBLING_TIME_DAYS (69% outliers), WOW_GROWTH_PCT (extreme outliers),
--     WOW_CHANGE_7D_AVG (correlated), CASE_DEATH_RATIO (outliers)
--   KEPT: 8 clean exogenous features, all COALESCEd to 0
--
-- Run in: SQL Worksheet (run section by section)
-- ============================================================

USE DATABASE HACKATHON;
USE SCHEMA MARTS;


-- ═══════════════════════════════════════════════
--  PHASE A: Prepare clean training data
--  Only features with 0% or <3% NULLs
--  All NULLs replaced with 0
-- ═══════════════════════════════════════════════

-- Use CASES_7D_AVG as target (smoother than raw NEW_CASES)
-- Filter to last 12 months only (recent patterns matter most for forecasting)
CREATE OR REPLACE VIEW ML_TRAINING_DATA AS
SELECT
    COUNTRY_REGION,
    DATE,
    CASES_7D_AVG AS TARGET_CASES,

    -- Tier 1: 0% NULLs — safe as-is
    DEATHS_7D_AVG,
    DAILY_VACCINATIONS,
    PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
    VACC_7D_AVG,

    -- Tier 2: <2.5% NULLs — COALESCE to 0
    COALESCE(VACC_RATE_LAG_14D, 0) AS VACC_RATE_LAG_14D,
    COALESCE(VACC_RATE_LAG_21D, 0) AS VACC_RATE_LAG_21D,
    COALESCE(VACC_RATE_LAG_28D, 0) AS VACC_RATE_LAG_28D

    -- DROPPED (data quality issues):
    -- RECOVERY_RATE         → 58% NULLs (JHU stopped tracking mid-2021)
    -- VACC_IMPACT_PROXY     → 36% NULLs (no data pre-vaccination)
    -- DOUBLING_TIME_DAYS    → 69% outliers (values up to 1.6M days)
    -- WOW_GROWTH_PCT        → extreme outliers (up to 117,000%)
    -- WOW_CHANGE_7D_AVG     → 105 NULLs, correlated with CASES_7D_AVG
    -- CASE_DEATH_RATIO      → 698 NULLs, outliers up to 7,350

FROM MARTS.FEATURES_ENGINEERED
WHERE CASES_7D_AVG IS NOT NULL
  AND DATE >= DATEADD('month', -12, (SELECT MAX(DATE) FROM MARTS.FEATURES_ENGINEERED))
ORDER BY COUNTRY_REGION, DATE;

-- Verify: should be 17,145 rows, 10 columns, 0 NULLs
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT COUNTRY_REGION) AS countries,
    SUM(CASE WHEN DEATHS_7D_AVG IS NULL THEN 1 ELSE 0 END) AS null_deaths_7d,
    SUM(CASE WHEN DAILY_VACCINATIONS IS NULL THEN 1 ELSE 0 END) AS null_daily_vacc,
    SUM(CASE WHEN VACC_RATE_LAG_14D IS NULL THEN 1 ELSE 0 END) AS null_lag14,
    SUM(CASE WHEN VACC_RATE_LAG_21D IS NULL THEN 1 ELSE 0 END) AS null_lag21,
    SUM(CASE WHEN VACC_RATE_LAG_28D IS NULL THEN 1 ELSE 0 END) AS null_lag28
FROM ML_TRAINING_DATA;
-- ALL should be 0. If not, STOP and investigate.


-- ═══════════════════════════════════════════════
--  PHASE B: Train-Test Split & Evaluation Model
--  Holdout: last 30 days
--  Purpose: calculate MAPE per country for judges
-- ═══════════════════════════════════════════════

-- Training split: everything except last 30 days
CREATE OR REPLACE VIEW ML_TRAIN_SPLIT AS
SELECT *
FROM ML_TRAINING_DATA
WHERE DATE <= DATEADD('day', -30, (SELECT MAX(DATE) FROM ML_TRAINING_DATA));

-- Test actuals (for MAPE comparison)
CREATE OR REPLACE VIEW ML_TEST_ACTUALS AS
SELECT COUNTRY_REGION, DATE, TARGET_CASES AS ACTUAL_CASES
FROM ML_TRAINING_DATA
WHERE DATE > DATEADD('day', -30, (SELECT MAX(DATE) FROM ML_TRAINING_DATA));

-- Exogenous features for the holdout 30 days (no target column)
CREATE OR REPLACE VIEW ML_TEST_EXOGENOUS AS
SELECT
    COUNTRY_REGION,
    DATE,
    DEATHS_7D_AVG,
    DAILY_VACCINATIONS,
    PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
    VACC_7D_AVG,
    VACC_RATE_LAG_14D,
    VACC_RATE_LAG_21D,
    VACC_RATE_LAG_28D
FROM ML_TRAINING_DATA
WHERE DATE > DATEADD('day', -30, (SELECT MAX(DATE) FROM ML_TRAINING_DATA));

-- Train evaluation model (~2-3 min)
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST eval_forecast_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ML_TRAIN_SPLIT'),
    SERIES_COLNAME => 'COUNTRY_REGION',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'TARGET_CASES'
);

-- Predict holdout period using known exogenous values
CALL eval_forecast_model!FORECAST(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ML_TEST_EXOGENOUS'),
    SERIES_COLNAME => 'COUNTRY_REGION',
    TIMESTAMP_COLNAME => 'DATE',
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

-- Store evaluation predictions
CREATE OR REPLACE TABLE EVAL_PREDICTIONS AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM EVAL_PREDICTIONS LIMIT 20;


-- ═══════════════════════════════════════════════
--  PHASE B2: Calculate MAPE per country
-- ═══════════════════════════════════════════════

CREATE OR REPLACE TABLE FORECAST_EVAL AS
SELECT
    e.SERIES AS COUNTRY_REGION,
    COUNT(*) AS eval_days,

    -- MAE: Mean Absolute Error (robust, always works)
    ROUND(AVG(ABS(a.ACTUAL_CASES - e.FORECAST)), 2) AS MAE,

    -- sMAPE: Symmetric MAPE (handles near-zero values properly)
    -- Formula: 200 * |actual - forecast| / (|actual| + |forecast|)
    -- Range: 0-200%, where 0 = perfect
    ROUND(AVG(
        CASE
            WHEN (ABS(a.ACTUAL_CASES) + ABS(e.FORECAST)) > 0
            THEN 200.0 * ABS(a.ACTUAL_CASES - e.FORECAST)
                 / (ABS(a.ACTUAL_CASES) + ABS(e.FORECAST))
            ELSE 0
        END
    ), 2) AS SMAPE,

    -- MAPE: only on days with meaningful case counts (>100)
    -- avoids division-by-near-zero inflation
    ROUND(AVG(
        CASE
            WHEN a.ACTUAL_CASES > 100
            THEN ABS(a.ACTUAL_CASES - e.FORECAST) / a.ACTUAL_CASES * 100
            ELSE NULL
        END
    ), 2) AS MAPE_FILTERED,

    -- Weighted MAPE: weighted by actual case volume
    -- gives more weight to high-count days (more meaningful)
    ROUND(
        SUM(ABS(a.ACTUAL_CASES - e.FORECAST))
        / NULLIF(SUM(a.ACTUAL_CASES), 0) * 100
    , 2) AS WMAPE,

    -- Context stats
    SUM(a.ACTUAL_CASES) AS total_actual,
    SUM(ROUND(GREATEST(e.FORECAST, 0), 0)) AS total_forecast,
    ROUND(AVG(a.ACTUAL_CASES), 0) AS avg_daily_actual,
    SUM(CASE WHEN a.ACTUAL_CASES <= 100 THEN 1 ELSE 0 END) AS low_count_days

FROM EVAL_PREDICTIONS e
INNER JOIN ML_TEST_ACTUALS a
    ON e.SERIES = a.COUNTRY_REGION
    AND e.TS = a.DATE
GROUP BY e.SERIES
ORDER BY SMAPE;

-- View evaluation results
SELECT
    COUNTRY_REGION,
    eval_days,
    MAE,
    SMAPE,
    MAPE_FILTERED,
    WMAPE,
    avg_daily_actual,
    low_count_days
FROM MARTS.FORECAST_EVAL
ORDER BY SMAPE;

-- Overall averages
SELECT
    ROUND(AVG(MAE), 2) AS overall_mae,
    ROUND(AVG(SMAPE), 2) AS overall_smape,
    ROUND(AVG(WMAPE), 2) AS overall_wmape
FROM MARTS.FORECAST_EVAL;


-- ═══════════════════════════════════════════════
--  PHASE C: Train final model on ALL data
--  Then generate 30-day forward projections
-- ═══════════════════════════════════════════════

-- Train on full dataset (~2-3 min)
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST final_forecast_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ML_TRAINING_DATA'),
    SERIES_COLNAME => 'COUNTRY_REGION',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'TARGET_CASES'
);

-- Build future exogenous data for next 30 days
-- Strategy: carry forward last known values per country
-- (vaccination rates don't change drastically in 30 days)
CREATE OR REPLACE TABLE FUTURE_EXOGENOUS AS
WITH last_known AS (
    SELECT
        COUNTRY_REGION,
        DEATHS_7D_AVG,
        DAILY_VACCINATIONS,
        PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
        VACC_7D_AVG,
        VACC_RATE_LAG_14D,
        VACC_RATE_LAG_21D,
        VACC_RATE_LAG_28D
    FROM ML_TRAINING_DATA
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COUNTRY_REGION ORDER BY DATE DESC) = 1
),
future_dates AS (
    SELECT
        lk.COUNTRY_REGION,
        DATEADD('day', seq.SEQ, (SELECT MAX(DATE) FROM ML_TRAINING_DATA)) AS DATE,
        lk.DEATHS_7D_AVG,
        lk.DAILY_VACCINATIONS,
        lk.PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
        lk.VACC_7D_AVG,
        lk.VACC_RATE_LAG_14D,
        lk.VACC_RATE_LAG_21D,
        lk.VACC_RATE_LAG_28D
    FROM last_known lk
    CROSS JOIN (SELECT SEQ4() + 1 AS SEQ FROM TABLE(GENERATOR(ROWCOUNT => 30))) seq
)
SELECT * FROM future_dates;

-- Verify: 15 countries × 30 days = 450 rows, 0 NULLs
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT COUNTRY_REGION) AS countries,
       SUM(CASE WHEN DEATHS_7D_AVG IS NULL THEN 1 ELSE 0 END) AS any_nulls
FROM FUTURE_EXOGENOUS;

-- Generate 30-day forward forecast
CALL final_forecast_model!FORECAST(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FUTURE_EXOGENOUS'),
    SERIES_COLNAME => 'COUNTRY_REGION',
    TIMESTAMP_COLNAME => 'DATE',
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

-- Store raw forecast
CREATE OR REPLACE TABLE FORECAST_RAW AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM FORECAST_RAW LIMIT 20;


-- ═══════════════════════════════════════════════
--  PHASE D: Build FORECAST_OUTPUT for Cortex pipeline
--  Combines actuals + forward projections
-- ═══════════════════════════════════════════════

CREATE OR REPLACE TABLE FORECAST_OUTPUT AS
WITH recent_actuals AS (
    SELECT
        f.COUNTRY_REGION,
        f.DATE AS DS,
        f.CASES_7D_AVG AS ACTUAL_CASES,
        f.NEW_DEATHS AS ACTUAL_DEATHS,
        f.CASES_7D_AVG AS FORECASTED_CASES,
        NULL AS FORECAST_LOWER,
        NULL AS FORECAST_UPPER,
        0 AS MAPE,
        FALSE AS IS_FORECAST
    FROM MARTS.FEATURES_ENGINEERED f
    WHERE f.DATE >= DATEADD('day', -90, (SELECT MAX(DATE) FROM MARTS.FEATURES_ENGINEERED))
),
forward_forecast AS (
    SELECT
        fr.SERIES AS COUNTRY_REGION,
        fr.TS AS DS,
        NULL AS ACTUAL_CASES,
        NULL AS ACTUAL_DEATHS,
        ROUND(GREATEST(fr.FORECAST, 0), 0) AS FORECASTED_CASES,
        ROUND(GREATEST(fr.LOWER_BOUND, 0), 0) AS FORECAST_LOWER,
        ROUND(GREATEST(fr.UPPER_BOUND, 0), 0) AS FORECAST_UPPER,
        fe.WMAPE AS MAPE,
        TRUE AS IS_FORECAST
    FROM FORECAST_RAW fr
    LEFT JOIN MARTS.FORECAST_EVAL fe ON fr.SERIES = fe.COUNTRY_REGION
)
SELECT * FROM recent_actuals
UNION ALL
SELECT * FROM forward_forecast
ORDER BY COUNTRY_REGION, DS;

-- Final verification
SELECT
    COUNTRY_REGION,
    SUM(CASE WHEN IS_FORECAST = FALSE THEN 1 ELSE 0 END) AS historical_days,
    SUM(CASE WHEN IS_FORECAST = TRUE THEN 1 ELSE 0 END) AS forecast_days,
    MIN(DS) AS start_date,
    MAX(DS) AS end_date
FROM MARTS.FORECAST_OUTPUT
GROUP BY COUNTRY_REGION
ORDER BY COUNTRY_REGION;


-- ═══════════════════════════════════════════════
--  PHASE E: Feature Importance (for explainability)
-- ═══════════════════════════════════════════════

CALL final_forecast_model!EXPLAIN_FEATURE_IMPORTANCE();

CREATE OR REPLACE TABLE FEATURE_IMPORTANCE AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM MARTS.FEATURE_IMPORTANCE
ORDER BY SERIES, RANK;
