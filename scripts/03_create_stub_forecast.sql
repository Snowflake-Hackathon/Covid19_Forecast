-- ============================================================
-- STEP 3 (Stub): Create Stub Forecast Table
-- This mimics the output your ML teammate will produce.
-- Your Cortex pipeline reads from this table.
-- When real forecasts are ready, just replace this table.
--
-- Architecture: Writes to MARTS schema
-- ============================================================

USE DATABASE HACKATHON;
USE SCHEMA MARTS;

-- Agreed-upon schema with ML teammate
CREATE OR REPLACE TABLE FORECAST_OUTPUT AS
WITH top_countries AS (
    SELECT OWID_NAME AS COUNTRY_REGION
    FROM HACKATHON.INT.TARGET_COUNTRIES
),
recent_data AS (
    SELECT
        j.COUNTRY_REGION,
        j.DATE AS DS,
        SUM(CASE WHEN j.CASE_TYPE = 'Confirmed' THEN j.CASES ELSE 0 END) AS ACTUAL_CASES,
        SUM(CASE WHEN j.CASE_TYPE = 'Deaths' THEN j.CASES ELSE 0 END) AS ACTUAL_DEATHS
    FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19 j
    INNER JOIN top_countries tc ON j.COUNTRY_REGION = tc.COUNTRY_REGION
    WHERE j.DATE >= '2023-01-01'
    GROUP BY j.COUNTRY_REGION, j.DATE
)
SELECT
    COUNTRY_REGION,
    DS,
    ACTUAL_CASES,
    ACTUAL_DEATHS,
    -- Stub forecasted values (add noise to actuals)
    ROUND(ACTUAL_CASES * (1 + UNIFORM(-0.10, 0.15, RANDOM())), 0) AS FORECASTED_CASES,
    -- Stub MAPE per row
    ROUND(ABS(UNIFORM(-0.05, 0.12, RANDOM())) * 100, 2) AS MAPE,
    -- Stub 30-day flag
    CASE
        WHEN DS >= DATEADD('day', -30, (SELECT MAX(DATE) FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19))
        THEN TRUE ELSE FALSE
    END AS IS_FORECAST
FROM recent_data;

-- Verify
SELECT COUNTRY_REGION, COUNT(*) AS "ROWS",
       AVG(MAPE) AS avg_mape,
       MIN(DS) AS min_date, MAX(DS) AS max_date
FROM MARTS.FORECAST_OUTPUT
GROUP BY COUNTRY_REGION
ORDER BY COUNTRY_REGION;
