-- ============================================================
-- VERIFICATION QUERIES — Run these first, share results back
-- Run in: Snowflake Worksheet
-- ============================================================

USE DATABASE HACKATHON;
USE SCHEMA CORTEX_AI;

-- ─────────────────────────────────────────────
-- V1: CONFIRMED — CASES is cumulative, DIFFERENCE is daily
--     (Already verified, but run for Deaths too)
-- EXPECTED: CASES increases monotonically, DIFFERENCE fluctuates
-- ─────────────────────────────────────────────
SELECT DATE, CASE_TYPE, CASES, DIFFERENCE
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19
WHERE COUNTRY_REGION = 'India'
  AND DATE BETWEEN '2020-03-01' AND '2020-03-15'
  AND PROVINCE_STATE IS NULL
ORDER BY CASE_TYPE, DATE;

-- Yes so for deaths it is as per the expectation.


-- If the above returns 0 rows, PROVINCE_STATE is never NULL for India.
-- Run this fallback:
SELECT DATE, CASE_TYPE, PROVINCE_STATE, CASES, DIFFERENCE
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19
WHERE COUNTRY_REGION = 'India'
  AND DATE = '2020-06-01'
  AND CASE_TYPE = 'Confirmed'
ORDER BY CASES DESC
LIMIT 15;
-- >>> TELL ME: Does PROVINCE_STATE IS NULL return rows, or is it always populated?
-- Here I could just see one single row with PROVINCE_STATE as NULL, and that is for the date 2020-06-01, for the case type 'Confirmed'. 




-- ─────────────────────────────────────────────
-- V2: Sub-national granularity check
--     Need to know if we aggregate provinces or use country-level rows
-- ─────────────────────────────────────────────
SELECT
    COUNTRY_REGION,
    COUNT(DISTINCT PROVINCE_STATE) AS num_provinces,
    SUM(CASE WHEN PROVINCE_STATE IS NULL THEN 1 ELSE 0 END) AS null_province_rows,
    COUNT(*) AS total_rows
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19
WHERE COUNTRY_REGION IN ('United States', 'India', 'Brazil', 'Germany', 'France',
    'United Kingdom', 'Japan', 'Korea, South', 'Australia', 'Canada',
    'Italy', 'Mexico', 'Spain', 'Indonesia', 'South Africa')
  AND DATE = '2021-06-01'
  AND CASE_TYPE = 'Confirmed'
GROUP BY COUNTRY_REGION
ORDER BY COUNTRY_REGION;
-- >>> TELL ME: Which countries have null_province_rows > 0? Which have multiple provinces?

-- So null province is zero for all the countries except France, Indonesia, South Africa.


-- ─────────────────────────────────────────────
-- V3: Country name check — JHU names for our 15 targets
--     Some might be different (Korea, South vs South Korea)
-- ─────────────────────────────────────────────
SELECT DISTINCT COUNTRY_REGION
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19
WHERE COUNTRY_REGION IN ('United States', 'India', 'Brazil', 'Germany', 'France',
    'United Kingdom', 'Japan', 'Korea, South', 'South Korea', 'Australia', 'Canada',
    'Italy', 'Mexico', 'Spain', 'Indonesia', 'South Africa')
ORDER BY COUNTRY_REGION;
-- >>> TELL ME: Which names appear? Is it "Korea, South" or "South Korea"?
-- I can just see 14 rows written and cannot see South Korea, Korea, South.


-- ─────────────────────────────────────────────
-- V4: Same check for OWID_VACCINATIONS
-- ─────────────────────────────────────────────
SELECT DISTINCT COUNTRY_REGION
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.OWID_VACCINATIONS
WHERE COUNTRY_REGION IN ('United States', 'India', 'Brazil', 'Germany', 'France',
    'United Kingdom', 'Japan', 'Korea, South', 'South Korea', 'Australia', 'Canada',
    'Italy', 'Mexico', 'Spain', 'Indonesia', 'South Africa')
ORDER BY COUNTRY_REGION;
-- >>> TELL ME: Which names appear here?
-- South Korea is present here, but Korea, South is not. So we will have to use South Korea for the vaccination data.

-- ─────────────────────────────────────────────
-- V5: OWID vaccination data coverage per country
-- ─────────────────────────────────────────────
SELECT
    COUNTRY_REGION,
    MIN(DATE) AS first_vacc_date,
    MAX(DATE) AS last_vacc_date,
    COUNT(*) AS total_rows,
    COUNT(DAILY_VACCINATIONS) AS has_daily_vacc,
    COUNT(PEOPLE_FULLY_VACCINATED_PER_HUNDRED) AS has_fully_vaxxed_pct,
    MAX(PEOPLE_FULLY_VACCINATED_PER_HUNDRED) AS max_fully_vaxxed_pct
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.OWID_VACCINATIONS
WHERE COUNTRY_REGION IN ('United States', 'India', 'Brazil', 'Germany', 'France',
    'United Kingdom', 'Japan', 'South Korea', 'Korea, South', 'Australia', 'Canada',
    'Italy', 'Mexico', 'Spain', 'Indonesia', 'South Africa')
GROUP BY COUNTRY_REGION
ORDER BY COUNTRY_REGION;
-- >>> TELL ME: All 15 countries present? Any with very few rows or NULL vaccination data?
-- Yep, all 15 countries are present. 


-- ─────────────────────────────────────────────
-- V6: CASE_TYPE values — confirm only 'Confirmed' and 'Deaths'
-- ─────────────────────────────────────────────
SELECT DISTINCT CASE_TYPE
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19;
-- >>> TELL ME: What values appear? (expecting: Confirmed, Deaths — anything else?)
-- These four exists: Deaths
-- Recovered
-- Active
-- Confirmed


-- ─────────────────────────────────────────────
-- V7: Date range of JHU data
-- ─────────────────────────────────────────────
SELECT MIN(DATE) AS earliest, MAX(DATE) AS latest
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19;
-- >>> TELL ME: What's the date range?
-- Data range: 2020-01-22 to	2023-03-09
