-- ============================================================
-- STEP 1: Ingest & Profile
-- PS: "Load COVID-19 Epidemiological data. Profile case, death,
-- and vaccination time-series across 10+ countries."
--
-- Architecture:
--   STG  → Raw Marketplace views (no transforms)
--   INT  → Cleaned, pivoted, joined daily series
--   MARTS → Data profile & vaccination timeline
--
-- VERIFIED FACTS:
--   - CASES is CUMULATIVE → use DIFFERENCE for daily deltas
--   - CASE_TYPE: Confirmed, Deaths, Recovered, Active
--   - Most countries have NO null PROVINCE_STATE rows
--     (except France, Indonesia, South Africa)
--   - Korea not found in JHU → replaced with Turkey
--   - Date range: 2020-01-22 to 2023-03-09
--
-- Run in: Snowflake Worksheet
-- ============================================================

USE DATABASE HACKATHON;


-- ═══════════════════════════════════════════════
--  LAYER 1: STG (Staging)
--  Thin views over raw Marketplace tables.
--  No logic, just aliases for clean references.
-- ═══════════════════════════════════════════════

USE SCHEMA STG;

CREATE OR REPLACE VIEW STG_JHU_COVID_19 AS
SELECT
    COUNTRY_REGION,
    PROVINCE_STATE,
    COUNTY,
    DATE,
    CASE_TYPE,
    CASES          AS CUMULATIVE_VALUE,
    DIFFERENCE     AS DAILY_VALUE,
    ISO3166_1,
    ISO3166_2,
    LAT,
    LONG,
    LAST_UPDATED_DATE,
    LAST_REPORTED_FLAG
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19;


CREATE OR REPLACE VIEW STG_OWID_VACCINATIONS AS
SELECT
    COUNTRY_REGION,
    DATE,
    ISO3166_1,
    TOTAL_VACCINATIONS,
    PEOPLE_VACCINATED,
    PEOPLE_FULLY_VACCINATED,
    DAILY_VACCINATIONS_RAW,
    DAILY_VACCINATIONS,
    TOTAL_VACCINATIONS_PER_HUNDRED,
    PEOPLE_VACCINATED_PER_HUNDRED,
    PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
    DAILY_VACCINATIONS_PER_MILLION,
    VACCINES,
    LAST_OBSERVATION_DATE,
    LAST_UPDATE_DATE,
    LAST_REPORTED_FLAG
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.OWID_VACCINATIONS;


CREATE OR REPLACE VIEW STG_ECDC_GLOBAL AS
SELECT
    COUNTRY_REGION,
    CONTINENTEXP     AS CONTINENT,
    ISO3166_1,
    CASES            AS CUMULATIVE_CASES,
    DEATHS           AS CUMULATIVE_DEATHS,
    CASES_SINCE_PREV_DAY AS DAILY_CASES,
    DEATHS_SINCE_PREV_DAY AS DAILY_DEATHS,
    POPULATION,
    DATE,
    LAST_UPDATE_DATE,
    LAST_REPORTED_FLAG
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL;


-- ═══════════════════════════════════════════════
--  LAYER 2: INT (Intermediate)
--  Cleaned, pivoted, joined. The "kitchen."
-- ═══════════════════════════════════════════════

USE SCHEMA INT;

-- ─────────────────────────────────────────────
-- Target Countries Mapping (15 countries)
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW TARGET_COUNTRIES AS
SELECT JHU_NAME, OWID_NAME FROM (
    VALUES
        ('United States',  'United States'),
        ('India',          'India'),
        ('Brazil',         'Brazil'),
        ('Germany',        'Germany'),
        ('France',         'France'),
        ('United Kingdom', 'United Kingdom'),
        ('Japan',          'Japan'),
        ('Australia',      'Australia'),
        ('Canada',         'Canada'),
        ('Italy',          'Italy'),
        ('Mexico',         'Mexico'),
        ('Spain',          'Spain'),
        ('Indonesia',      'Indonesia'),
        ('South Africa',   'South Africa'),
        ('Turkey',         'Turkey')
) AS t(JHU_NAME, OWID_NAME);

-- Quick check: confirm all 15 JHU names exist in staging
SELECT tc.JHU_NAME,
       COUNT(j.COUNTRY_REGION) AS stg_rows
FROM INT.TARGET_COUNTRIES tc
LEFT JOIN STG.STG_JHU_COVID_19 j
    ON tc.JHU_NAME = j.COUNTRY_REGION
    AND j.DATE = '2021-06-01'
    AND j.CASE_TYPE = 'Confirmed'
GROUP BY tc.JHU_NAME
ORDER BY stg_rows;


-- ─────────────────────────────────────────────
-- Clean Pivoted Daily Country Series
-- Reads from STG, writes to INT
-- CRITICAL: Uses DAILY_VALUE (DIFFERENCE), not CUMULATIVE_VALUE
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE DAILY_COUNTRY_SERIES AS
WITH jhu_daily AS (
    SELECT
        j.COUNTRY_REGION,
        j.DATE,
        -- Daily new values from DIFFERENCE (aliased as DAILY_VALUE in STG)
        SUM(CASE WHEN j.CASE_TYPE = 'Confirmed' THEN j.DAILY_VALUE ELSE 0 END) AS NEW_CASES,
        SUM(CASE WHEN j.CASE_TYPE = 'Deaths'    THEN j.DAILY_VALUE ELSE 0 END) AS NEW_DEATHS,
        SUM(CASE WHEN j.CASE_TYPE = 'Recovered'  THEN j.DAILY_VALUE ELSE 0 END) AS NEW_RECOVERED,
        -- Cumulative totals (sum across provinces)
        SUM(CASE WHEN j.CASE_TYPE = 'Confirmed' THEN j.CUMULATIVE_VALUE ELSE 0 END) AS CUMULATIVE_CASES,
        SUM(CASE WHEN j.CASE_TYPE = 'Deaths'    THEN j.CUMULATIVE_VALUE ELSE 0 END) AS CUMULATIVE_DEATHS,
        SUM(CASE WHEN j.CASE_TYPE = 'Recovered'  THEN j.CUMULATIVE_VALUE ELSE 0 END) AS CUMULATIVE_RECOVERED
    FROM STG.STG_JHU_COVID_19 j
    INNER JOIN INT.TARGET_COUNTRIES tc ON j.COUNTRY_REGION = tc.JHU_NAME
    GROUP BY j.COUNTRY_REGION, j.DATE
),

-- Clip negative daily values (JHU retroactive corrections)
jhu_cleaned AS (
    SELECT
        COUNTRY_REGION,
        DATE,
        GREATEST(NEW_CASES, 0) AS NEW_CASES,
        GREATEST(NEW_DEATHS, 0) AS NEW_DEATHS,
        GREATEST(NEW_RECOVERED, 0) AS NEW_RECOVERED,
        CUMULATIVE_CASES,
        CUMULATIVE_DEATHS,
        CUMULATIVE_RECOVERED
    FROM jhu_daily
),

-- Vaccination data from STG
vacc AS (
    SELECT
        v.COUNTRY_REGION AS OWID_NAME,
        v.DATE,
        v.TOTAL_VACCINATIONS,
        v.PEOPLE_VACCINATED,
        v.PEOPLE_FULLY_VACCINATED,
        v.DAILY_VACCINATIONS,
        v.PEOPLE_VACCINATED_PER_HUNDRED,
        v.PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
        v.DAILY_VACCINATIONS_PER_MILLION
    FROM STG.STG_OWID_VACCINATIONS v
    INNER JOIN INT.TARGET_COUNTRIES tc ON v.COUNTRY_REGION = tc.OWID_NAME
)

SELECT
    tc.OWID_NAME AS COUNTRY_REGION,
    j.DATE,

    -- Cases & Deaths (daily)
    j.NEW_CASES,
    j.NEW_DEATHS,
    j.NEW_RECOVERED,

    -- Cumulative
    j.CUMULATIVE_CASES,
    j.CUMULATIVE_DEATHS,
    j.CUMULATIVE_RECOVERED,

    -- Vaccination
    v.TOTAL_VACCINATIONS,
    v.PEOPLE_VACCINATED,
    v.PEOPLE_FULLY_VACCINATED,
    v.DAILY_VACCINATIONS,
    v.PEOPLE_VACCINATED_PER_HUNDRED,
    v.PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
    v.DAILY_VACCINATIONS_PER_MILLION

FROM INT.TARGET_COUNTRIES tc
INNER JOIN jhu_cleaned j ON tc.JHU_NAME = j.COUNTRY_REGION
LEFT JOIN vacc v ON tc.OWID_NAME = v.OWID_NAME AND j.DATE = v.DATE
ORDER BY COUNTRY_REGION, DATE;

-- Verify row counts
SELECT
    COUNTRY_REGION,
    COUNT(*) AS total_days,
    MIN(DATE) AS first_date,
    MAX(DATE) AS last_date,
    SUM(NEW_CASES) AS total_new_cases,
    SUM(NEW_DEATHS) AS total_new_deaths,
    COUNT(DAILY_VACCINATIONS) AS vacc_data_days
FROM INT.DAILY_COUNTRY_SERIES
GROUP BY COUNTRY_REGION
ORDER BY total_new_cases DESC;


-- ═══════════════════════════════════════════════
--  LAYER 3: MARTS (for profiling deliverables)
--  Consumption-ready tables for judges & dashboard
-- ═══════════════════════════════════════════════

USE SCHEMA MARTS;

-- ─────────────────────────────────────────────
-- Data Profile — comprehensive per-country stats
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE DATA_PROFILE AS
SELECT
    COUNTRY_REGION,
    COUNT(DISTINCT DATE) AS total_days,
    MIN(DATE) AS series_start,
    MAX(DATE) AS series_end,

    -- Cases
    MAX(CUMULATIVE_CASES) AS total_confirmed_cases,
    ROUND(AVG(NEW_CASES), 2) AS avg_daily_cases,
    MAX(NEW_CASES) AS peak_daily_cases,

    -- Deaths
    MAX(CUMULATIVE_DEATHS) AS total_deaths,
    ROUND(AVG(NEW_DEATHS), 2) AS avg_daily_deaths,
    MAX(NEW_DEATHS) AS peak_daily_deaths,
    ROUND(MAX(CUMULATIVE_DEATHS) / NULLIF(MAX(CUMULATIVE_CASES), 0) * 100, 2) AS case_fatality_rate_pct,

    -- Recovered
    MAX(CUMULATIVE_RECOVERED) AS total_recovered,
    ROUND(MAX(CUMULATIVE_RECOVERED) / NULLIF(MAX(CUMULATIVE_CASES), 0) * 100, 2) AS recovery_rate_pct,
    MAX(CASE WHEN NEW_RECOVERED > 0 THEN DATE END) AS last_recovered_date,

    -- Vaccination
    MAX(TOTAL_VACCINATIONS) AS total_vaccinations,
    MAX(PEOPLE_FULLY_VACCINATED_PER_HUNDRED) AS max_fully_vaccinated_pct,
    ROUND(AVG(DAILY_VACCINATIONS), 0) AS avg_daily_vaccinations,
    MAX(DAILY_VACCINATIONS) AS peak_daily_vaccinations,
    MIN(CASE WHEN TOTAL_VACCINATIONS > 0 THEN DATE END) AS vaccination_start_date,
    COUNT(CASE WHEN DAILY_VACCINATIONS IS NOT NULL THEN 1 END) AS vacc_data_days,

    -- Data Quality Flags (for fairness note deliverable)
    SUM(CASE WHEN NEW_CASES = 0 THEN 1 ELSE 0 END) AS zero_case_days,
    ROUND(SUM(CASE WHEN NEW_CASES = 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS zero_case_pct

FROM INT.DAILY_COUNTRY_SERIES
GROUP BY COUNTRY_REGION
ORDER BY total_confirmed_cases DESC;

SELECT * FROM MARTS.DATA_PROFILE;


-- ─────────────────────────────────────────────
-- Vaccination Coverage Timeline (for dashboard)
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW VACCINATION_TIMELINE AS
SELECT
    COUNTRY_REGION,
    DATE,
    PEOPLE_VACCINATED_PER_HUNDRED,
    PEOPLE_FULLY_VACCINATED_PER_HUNDRED,
    DAILY_VACCINATIONS
FROM INT.DAILY_COUNTRY_SERIES
WHERE TOTAL_VACCINATIONS IS NOT NULL
  AND TOTAL_VACCINATIONS > 0
ORDER BY COUNTRY_REGION, DATE;
