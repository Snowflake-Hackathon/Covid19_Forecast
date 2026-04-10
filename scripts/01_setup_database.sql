-- ============================================================
-- STEP 0: Database & Schema Setup — 3-Layer Architecture
--
--   HACKATHON.STG    → Staging: thin views over Marketplace raw data
--   HACKATHON.INT    → Intermediate: cleaned, joined, gap-filled
--   HACKATHON.MARTS  → Marts: ML-ready features, forecasts, narratives
--
-- Run in: Snowflake Worksheet (run this FIRST before anything else)
-- ============================================================

-- Create project database
CREATE DATABASE IF NOT EXISTS HACKATHON;

-- Three-layer schema architecture
CREATE SCHEMA IF NOT EXISTS HACKATHON.STG;     -- raw staging views
CREATE SCHEMA IF NOT EXISTS HACKATHON.INT;     -- intermediate transforms
CREATE SCHEMA IF NOT EXISTS HACKATHON.MARTS;   -- consumption-ready tables

-- Check your warehouse
SHOW WAREHOUSES;

-- Set warehouse (adjust name if different)
USE WAREHOUSE COMPUTE_WH;

-- Verify
SELECT SCHEMA_NAME
FROM HACKATHON.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME IN ('STG', 'INT', 'MARTS')
ORDER BY SCHEMA_NAME;
