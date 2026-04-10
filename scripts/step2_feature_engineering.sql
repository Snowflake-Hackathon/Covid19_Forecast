-- ============================================================
-- STEP 2: Feature Engineering (SQL fallback version)
-- PS: "Create rolling averages, doubling-time features,
-- and vaccination rate lags in Snowpark."
--
-- Architecture:
--   Reads from:  INT.DAILY_COUNTRY_SERIES
--   Writes to:   INT.BASE_FILLED (gap-filled intermediate)
--                 MARTS.FEATURES_ENGINEERED (ML-ready output)
--
-- Primary: Use Snowpark Python Notebook (step2_notebook.py)
-- This SQL version is the fallback if notebook has issues.
--
-- Run in: Snowflake Worksheet
-- ============================================================

USE DATABASE HACKATHON;


-- ═══════════════════════════════════════════════
--  INT: Date Spine & Gap Fill
-- ═══════════════════════════════════════════════

USE SCHEMA INT;

-- Date spine — ensures no gaps for ML Forecasting
CREATE OR REPLACE VIEW DATE_SPINE AS
WITH bounds AS (
    SELECT MIN(DATE) AS min_dt, MAX(DATE) AS max_dt
    FROM INT.DAILY_COUNTRY_SERIES
),
spine AS (
    SELECT DATEADD('day', SEQ4(), min_dt) AS DATE
    FROM bounds, TABLE(GENERATOR(ROWCOUNT => 1200))
    WHERE DATEADD('day', SEQ4(), min_dt) <= max_dt
)
SELECT tc.OWID_NAME AS COUNTRY_REGION, s.DATE
FROM INT.TARGET_COUNTRIES tc
CROSS JOIN (SELECT DISTINCT DATE FROM spine) s;


-- Gap-filled base with forward-filled vaccinations
CREATE OR REPLACE TABLE BASE_FILLED AS
SELECT
    ds.COUNTRY_REGION,
    ds.DATE,

    -- Daily values: 0 if missing
    COALESCE(d.NEW_CASES, 0) AS NEW_CASES,
    COALESCE(d.NEW_DEATHS, 0) AS NEW_DEATHS,
    COALESCE(d.NEW_RECOVERED, 0) AS NEW_RECOVERED,

    -- Cumulative: forward-fill
    COALESCE(d.CUMULATIVE_CASES,
        LAST_VALUE(d.CUMULATIVE_CASES IGNORE NULLS) OVER (
            PARTITION BY ds.COUNTRY_REGION ORDER BY ds.DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0
    ) AS CUMULATIVE_CASES,

    COALESCE(d.CUMULATIVE_DEATHS,
        LAST_VALUE(d.CUMULATIVE_DEATHS IGNORE NULLS) OVER (
            PARTITION BY ds.COUNTRY_REGION ORDER BY ds.DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0
    ) AS CUMULATIVE_DEATHS,

    COALESCE(d.CUMULATIVE_RECOVERED,
        LAST_VALUE(d.CUMULATIVE_RECOVERED IGNORE NULLS) OVER (
            PARTITION BY ds.COUNTRY_REGION ORDER BY ds.DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0
    ) AS CUMULATIVE_RECOVERED,

    -- Vaccination: forward-fill
    COALESCE(d.DAILY_VACCINATIONS, 0) AS DAILY_VACCINATIONS,

    COALESCE(d.PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
        LAST_VALUE(d.PEOPLE_FULLY_VACCINATED_PER_HUNDRED IGNORE NULLS) OVER (
            PARTITION BY ds.COUNTRY_REGION ORDER BY ds.DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0
    ) AS PEOPLE_FULLY_VACCINATED_PER_HUNDRED,

    COALESCE(d.PEOPLE_VACCINATED_PER_HUNDRED,
        LAST_VALUE(d.PEOPLE_VACCINATED_PER_HUNDRED IGNORE NULLS) OVER (
            PARTITION BY ds.COUNTRY_REGION ORDER BY ds.DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0
    ) AS PEOPLE_VACCINATED_PER_HUNDRED

FROM INT.DATE_SPINE ds
LEFT JOIN INT.DAILY_COUNTRY_SERIES d
    ON ds.COUNTRY_REGION = d.COUNTRY_REGION AND ds.DATE = d.DATE
ORDER BY ds.COUNTRY_REGION, ds.DATE;


-- ═══════════════════════════════════════════════
--  MARTS: Feature Engineering
-- ═══════════════════════════════════════════════

USE SCHEMA MARTS;

CREATE OR REPLACE TABLE FEATURES_ENGINEERED AS
WITH rolling AS (
    SELECT
        COUNTRY_REGION,
        DATE,
        NEW_CASES,
        NEW_DEATHS,
        NEW_RECOVERED,
        CUMULATIVE_CASES,
        CUMULATIVE_DEATHS,
        CUMULATIVE_RECOVERED,
        DAILY_VACCINATIONS,
        PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
        PEOPLE_VACCINATED_PER_HUNDRED,

        -- Rolling Averages
        ROUND(AVG(NEW_CASES) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS CASES_7D_AVG,

        ROUND(AVG(NEW_CASES) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ), 2) AS CASES_14D_AVG,

        ROUND(AVG(NEW_DEATHS) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS DEATHS_7D_AVG,

        ROUND(AVG(DAILY_VACCINATIONS) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0) AS VACC_7D_AVG

    FROM INT.BASE_FILLED
),

growth AS (
    SELECT
        r.*,

        -- Doubling Time
        LAG(CUMULATIVE_CASES, 7) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
        ) AS _cum_cases_7d_ago,

        CASE
            WHEN LAG(CUMULATIVE_CASES, 7) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE) > 0
                 AND CUMULATIVE_CASES > LAG(CUMULATIVE_CASES, 7) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE)
            THEN ROUND(
                7.0 * LN(2) / LN(
                    CUMULATIVE_CASES / LAG(CUMULATIVE_CASES, 7) OVER (
                        PARTITION BY COUNTRY_REGION ORDER BY DATE)
                ), 2)
            ELSE NULL
        END AS DOUBLING_TIME_DAYS,

        -- Week-over-Week Growth
        LAG(CASES_7D_AVG, 7) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
        ) AS _cases_7d_avg_prev,

        CASES_7D_AVG - LAG(CASES_7D_AVG, 7) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
        ) AS WOW_CHANGE_7D_AVG,

        CASE
            WHEN LAG(CASES_7D_AVG, 7) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE) > 0
            THEN ROUND(
                (CASES_7D_AVG - LAG(CASES_7D_AVG, 7) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE))
                / LAG(CASES_7D_AVG, 7) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE) * 100
            , 2)
            ELSE NULL
        END AS WOW_GROWTH_PCT,

        -- Vaccination Rate Lags (PS requirement)
        LAG(PEOPLE_FULLY_VACCINATED_PER_HUNDRED, 14) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
        ) AS VACC_RATE_LAG_14D,

        LAG(PEOPLE_FULLY_VACCINATED_PER_HUNDRED, 21) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
        ) AS VACC_RATE_LAG_21D,

        LAG(PEOPLE_FULLY_VACCINATED_PER_HUNDRED, 28) OVER (
            PARTITION BY COUNTRY_REGION ORDER BY DATE
        ) AS VACC_RATE_LAG_28D

    FROM rolling r
),

explainability AS (
    SELECT
        g.*,

        -- Case-Death Ratio (deaths lag cases by ~14 days)
        CASE
            WHEN LAG(CASES_7D_AVG, 14) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE) > 0
            THEN ROUND(
                DEATHS_7D_AVG / LAG(CASES_7D_AVG, 14) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE) * 100
            , 2)
            ELSE NULL
        END AS CASE_DEATH_RATIO,

        -- Surge Flag: >50% WoW growth
        CASE WHEN WOW_GROWTH_PCT > 50 THEN 1 ELSE 0 END AS SURGE_FLAG,

        -- Vaccination Impact Proxy
        CASE
            WHEN VACC_RATE_LAG_21D > 0 AND _cases_7d_avg_prev > 0
            THEN ROUND(
                (CASES_7D_AVG - _cases_7d_avg_prev) / _cases_7d_avg_prev * 100
                - (VACC_RATE_LAG_21D - COALESCE(
                    LAG(VACC_RATE_LAG_21D, 7) OVER (
                        PARTITION BY COUNTRY_REGION ORDER BY DATE), 0))
            , 2)
            ELSE NULL
        END AS VACC_IMPACT_PROXY,

        -- Recovery Rate
        CASE
            WHEN CASES_7D_AVG > 0 AND NEW_RECOVERED > 0
            THEN ROUND(
                AVG(NEW_RECOVERED) OVER (
                    PARTITION BY COUNTRY_REGION ORDER BY DATE
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                / CASES_7D_AVG * 100
            , 2)
            ELSE NULL
        END AS RECOVERY_RATE

    FROM growth g
)

SELECT
    COUNTRY_REGION,
    DATE,

    -- Core daily
    NEW_CASES,
    NEW_DEATHS,
    NEW_RECOVERED,

    -- Cumulative
    CUMULATIVE_CASES,
    CUMULATIVE_DEATHS,

    -- Rolling averages
    CASES_7D_AVG,
    CASES_14D_AVG,
    DEATHS_7D_AVG,
    VACC_7D_AVG,

    -- Growth
    DOUBLING_TIME_DAYS,
    WOW_CHANGE_7D_AVG,
    WOW_GROWTH_PCT,

    -- Vaccination
    DAILY_VACCINATIONS,
    PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
    VACC_RATE_LAG_14D,
    VACC_RATE_LAG_21D,
    VACC_RATE_LAG_28D,

    -- Explainability
    CASE_DEATH_RATIO,
    SURGE_FLAG,
    VACC_IMPACT_PROXY,
    RECOVERY_RATE

FROM explainability
ORDER BY COUNTRY_REGION, DATE;


-- ─────────────────────────────────────────────
-- Verification
-- ─────────────────────────────────────────────

-- Row counts per country (should be identical — date spine)
SELECT COUNTRY_REGION, COUNT(*) AS rows,
       MIN(DATE) AS start_dt, MAX(DATE) AS end_dt,
       COUNT(DISTINCT DATE) AS unique_dates
FROM MARTS.FEATURES_ENGINEERED
GROUP BY COUNTRY_REGION
ORDER BY COUNTRY_REGION;

-- Feature spot-check
SELECT *
FROM MARTS.FEATURES_ENGINEERED
WHERE COUNTRY_REGION = 'United States'
  AND DATE BETWEEN '2021-06-01' AND '2021-06-15'
ORDER BY DATE;

-- NULL check
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CASES_7D_AVG IS NULL THEN 1 ELSE 0 END) AS null_cases_7d,
    SUM(CASE WHEN DOUBLING_TIME_DAYS IS NULL THEN 1 ELSE 0 END) AS null_doubling,
    SUM(CASE WHEN VACC_RATE_LAG_14D IS NULL THEN 1 ELSE 0 END) AS null_vacc_lag14,
    SUM(CASE WHEN CASE_DEATH_RATIO IS NULL THEN 1 ELSE 0 END) AS null_cdr,
    SUM(CASE WHEN RECOVERY_RATE IS NULL THEN 1 ELSE 0 END) AS null_recovery
FROM MARTS.FEATURES_ENGINEERED;

-- Vaccination lag sanity check
SELECT COUNTRY_REGION, DATE,
       PEOPLE_FULLY_VACCINATED_PER_HUNDRED AS current_vax,
       VACC_RATE_LAG_14D, VACC_RATE_LAG_21D, VACC_RATE_LAG_28D
FROM MARTS.FEATURES_ENGINEERED
WHERE COUNTRY_REGION = 'India'
  AND DATE BETWEEN '2021-08-01' AND '2021-08-10'
ORDER BY DATE;
