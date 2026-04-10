-- ============================================================
-- STEP 2: Explore the Marketplace COVID-19 Dataset
-- Run this AFTER you've clicked "Get" on the Starschema dataset
-- in the Marketplace (Data Products → Marketplace → search
-- "COVID-19 epidemiological" → Get)
-- ============================================================

-- See what databases are available (find the COVID one)
SHOW DATABASES LIKE '%COVID%';

-- Explore the shared database structure
-- NOTE: Replace the database name below with what SHOW DATABASES returned
-- Common names: COVID19_EPIDEMIOLOGICAL_DATA or STARSCHEMA_COVID19
SHOW SCHEMAS IN DATABASE COVID19_EPIDEMIOLOGICAL_DATA;
SHOW TABLES IN DATABASE COVID19_EPIDEMIOLOGICAL_DATA;

-- Preview the main table (adjust table path based on what you find above)
SELECT *
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19
LIMIT 20;

-- Check available columns
DESCRIBE TABLE COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19;

-- Check which countries have data and row counts
SELECT COUNTRY_REGION, COUNT(*) AS row_count
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19
GROUP BY COUNTRY_REGION
ORDER BY row_count DESC
LIMIT 30;

-- Check date range
SELECT MIN(DATE), MAX(DATE)
FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.JHU_COVID_19;
