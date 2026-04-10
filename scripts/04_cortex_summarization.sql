-- ============================================================
-- STEP 4: Cortex AI Summarization Pipeline (Fixed)
-- Uses CORTEX.COMPLETE() to generate per-country outbreak
-- narratives from forecast data.
--
-- Architecture: All objects in MARTS schema
-- Reads from:  MARTS.FORECAST_OUTPUT + MARTS.FEATURES_ENGINEERED
--              + MARTS.FORECAST_EVAL
-- Writes to:   MARTS.COUNTRY_STATS, MARTS.OUTBREAK_NARRATIVES,
--              MARTS.GLOBAL_SUMMARY
--
-- FIXES:
--   - Separates actuals window from forecast window properly
--   - Uses FEATURES_ENGINEERED for reliable actual stats
--   - Uses FORECAST_OUTPUT only for forward projections
--   - Pulls WMAPE from FORECAST_EVAL (not from FORECAST_OUTPUT)
--   - All numbers validated before feeding to Cortex
--
-- Run in: SQL Worksheet
-- ============================================================

USE DATABASE HACKATHON;
USE SCHEMA MARTS;


-- ─────────────────────────────────────────────
-- 4A: Country Stats — computed from clean sources
-- Actuals from FEATURES_ENGINEERED (reliable)
-- Forecasts from FORECAST_OUTPUT (IS_FORECAST=TRUE only)
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW COUNTRY_STATS AS
WITH
-- Last date of ACTUAL data (not forecast)
actual_bounds AS (
    SELECT MAX(DATE) AS max_actual_date
    FROM MARTS.FEATURES_ENGINEERED
),

-- Actual stats: last 30 days and previous 30 days of REAL data
actual_stats AS (
    SELECT
        f.COUNTRY_REGION,

        -- Last 30 days of actual data
        SUM(CASE WHEN f.DATE > DATEADD('day', -30, ab.max_actual_date)
            THEN f.NEW_CASES ELSE 0 END) AS cases_last_30d,

        -- Previous 30 days (days -60 to -31)
        SUM(CASE WHEN f.DATE BETWEEN DATEADD('day', -60, ab.max_actual_date)
                                    AND DATEADD('day', -30, ab.max_actual_date)
            THEN f.NEW_CASES ELSE 0 END) AS cases_prev_30d,

        -- Deaths last 30 days
        SUM(CASE WHEN f.DATE > DATEADD('day', -30, ab.max_actual_date)
            THEN f.NEW_DEATHS ELSE 0 END) AS deaths_last_30d,

        -- Latest 7d averages (for context in prompt)
        ROUND(AVG(CASE WHEN f.DATE > DATEADD('day', -7, ab.max_actual_date)
            THEN f.CASES_7D_AVG END), 0) AS latest_7d_avg_cases,

        -- Vaccination coverage
        MAX(f.PEOPLE_FULLY_VACCINATED_PER_HUNDRED) AS vacc_coverage_pct

    FROM MARTS.FEATURES_ENGINEERED f
    CROSS JOIN actual_bounds ab
    WHERE f.DATE > DATEADD('day', -60, ab.max_actual_date)
    GROUP BY f.COUNTRY_REGION
),

-- Forecast stats: sum of 30-day forward projections
forecast_stats AS (
    SELECT
        COUNTRY_REGION,
        SUM(FORECASTED_CASES) AS forecast_30d_cases,
        ROUND(AVG(FORECASTED_CASES), 0) AS forecast_daily_avg
    FROM MARTS.FORECAST_OUTPUT
    WHERE IS_FORECAST = TRUE
    GROUP BY COUNTRY_REGION
),

-- Model accuracy from evaluation
eval_stats AS (
    SELECT
        COUNTRY_REGION,
        SMAPE,
        WMAPE,
        MAE
    FROM MARTS.FORECAST_EVAL
),

-- Combine all
combined AS (
    SELECT
        a.COUNTRY_REGION,
        a.cases_last_30d,
        a.cases_prev_30d,
        a.deaths_last_30d,
        a.latest_7d_avg_cases,
        a.vacc_coverage_pct,
        COALESCE(f.forecast_30d_cases, 0) AS forecast_30d_cases,
        COALESCE(f.forecast_daily_avg, 0) AS forecast_daily_avg,
        COALESCE(e.SMAPE, 0) AS avg_mape,
        COALESCE(e.WMAPE, 0) AS avg_wmape,

        -- Percent change: actual last 30d vs previous 30d
        CASE
            WHEN a.cases_prev_30d > 0
            THEN ROUND((a.cases_last_30d - a.cases_prev_30d)
                       / a.cases_prev_30d * 100, 2)
            ELSE 0
        END AS pct_change_30d

    FROM actual_stats a
    LEFT JOIN forecast_stats f ON a.COUNTRY_REGION = f.COUNTRY_REGION
    LEFT JOIN eval_stats e ON a.COUNTRY_REGION = e.COUNTRY_REGION
)

SELECT
    *,
    -- Risk tier classification
    CASE
        WHEN pct_change_30d > 20 THEN 'HIGH'
        WHEN pct_change_30d > 5  THEN 'MODERATE'
        ELSE 'LOW'
    END AS RISK_TIER
FROM combined;

-- Preview — verify numbers make sense before feeding to Cortex
SELECT COUNTRY_REGION, cases_last_30d, cases_prev_30d, deaths_last_30d,
       forecast_30d_cases, pct_change_30d, avg_mape, RISK_TIER
FROM MARTS.COUNTRY_STATS
ORDER BY CASE RISK_TIER WHEN 'HIGH' THEN 1 WHEN 'MODERATE' THEN 2 ELSE 3 END;
-- >>> CHECK: Do these numbers look reasonable? If yes, proceed to 4B.


-- ─────────────────────────────────────────────
-- 4B: Generate outbreak narratives using Cortex COMPLETE()
-- One LLM call per country — saves credits, better output
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE OUTBREAK_NARRATIVES AS
SELECT
    COUNTRY_REGION,
    RISK_TIER,
    cases_last_30d,
    forecast_30d_cases,
    pct_change_30d,
    avg_mape,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'You are a public health intelligence analyst. Write a briefing for a government health official who is NOT a data scientist.\n\n',
            'COUNTRY: ', COUNTRY_REGION, '\n',
            'DATA PERIOD: Last 60 days of confirmed COVID-19 data.\n\n',
            'ACTUAL DATA:\n',
            '- Total new cases in last 30 days: ', TO_VARCHAR(ROUND(cases_last_30d)), '\n',
            '- Total new cases in previous 30 days: ', TO_VARCHAR(ROUND(cases_prev_30d)), '\n',
            '- 30-day trend: ', TO_VARCHAR(pct_change_30d), '% change\n',
            '- Deaths in last 30 days: ', TO_VARCHAR(ROUND(deaths_last_30d)), '\n',
            '- Current daily average (7-day): ', TO_VARCHAR(latest_7d_avg_cases), ' cases/day\n',
            '- Vaccination coverage: ', TO_VARCHAR(ROUND(vacc_coverage_pct, 1)), '% fully vaccinated\n\n',
            'ML FORECAST (next 30 days):\n',
            '- Predicted total cases: ', TO_VARCHAR(ROUND(forecast_30d_cases)), '\n',
            '- Predicted daily average: ', TO_VARCHAR(forecast_daily_avg), ' cases/day\n',
            '- Model accuracy (sMAPE): ', TO_VARCHAR(ROUND(avg_mape, 1)), '%\n\n',
            'RISK TIER: ', RISK_TIER, '\n\n',
            'INSTRUCTIONS:\n',
            '- Write exactly 3-4 sentences.\n',
            '- Sentence 1: State whether cases are rising, falling, or stable and by how much.\n',
            '- Sentence 2: What the ML forecast projects for the next 30 days.\n',
            '- Sentence 3: The risk level and one specific recommended action.\n',
            '- Sentence 4 (optional): Note vaccination coverage if relevant.\n',
            '- Use plain English. No percentages larger than the ones provided. No jargon.\n',
            '- Do NOT invent numbers. Only use the data provided above.'
        )
    ) AS narrative,
    CURRENT_TIMESTAMP() AS generated_at
FROM MARTS.COUNTRY_STATS;

-- View the generated narratives
SELECT COUNTRY_REGION, RISK_TIER, narrative
FROM MARTS.OUTBREAK_NARRATIVES
ORDER BY
    CASE RISK_TIER WHEN 'HIGH' THEN 1 WHEN 'MODERATE' THEN 2 ELSE 3 END;


-- ─────────────────────────────────────────────
-- 4C: Global executive summary using COMPLETE()
-- Single summary across all countries for dashboard header
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE GLOBAL_SUMMARY AS
WITH agg AS (
    SELECT
        LISTAGG(
            CONCAT(COUNTRY_REGION, ' (', RISK_TIER, ', ',
                   TO_VARCHAR(pct_change_30d), '% trend, ',
                   TO_VARCHAR(ROUND(cases_last_30d)), ' cases)'),
            '; '
        ) WITHIN GROUP (ORDER BY pct_change_30d DESC) AS all_countries_summary,
        COUNT(CASE WHEN RISK_TIER = 'HIGH' THEN 1 END) AS high_risk_count,
        COUNT(CASE WHEN RISK_TIER = 'MODERATE' THEN 1 END) AS moderate_risk_count,
        COUNT(CASE WHEN RISK_TIER = 'LOW' THEN 1 END) AS low_risk_count,
        SUM(cases_last_30d) AS global_cases_30d,
        SUM(deaths_last_30d) AS global_deaths_30d
    FROM MARTS.COUNTRY_STATS
)
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'You are a WHO-style public health analyst. Write a 4-5 sentence global executive summary.\n\n',
            'DATA (15 countries monitored):\n',
            all_countries_summary, '\n\n',
            'TOTALS:\n',
            '- Global cases (last 30d): ', TO_VARCHAR(ROUND(global_cases_30d)), '\n',
            '- Global deaths (last 30d): ', TO_VARCHAR(ROUND(global_deaths_30d)), '\n',
            '- High risk countries: ', TO_VARCHAR(high_risk_count), '\n',
            '- Moderate risk: ', TO_VARCHAR(moderate_risk_count), '\n',
            '- Low risk: ', TO_VARCHAR(low_risk_count), '\n\n',
            'INSTRUCTIONS:\n',
            '- Write exactly 4-5 sentences for senior health officials.\n',
            '- Mention which regions need attention by name.\n',
            '- State the overall global trajectory (improving/worsening/mixed).\n',
            '- Use plain English. Do NOT invent numbers beyond what is provided.\n',
            '- Be factual and measured in tone.'
        )
    ) AS global_narrative,
    high_risk_count,
    moderate_risk_count,
    low_risk_count,
    CURRENT_TIMESTAMP() AS generated_at
FROM agg;

SELECT * FROM MARTS.GLOBAL_SUMMARY;


-- ─────────────────────────────────────────────
-- 4D: Policy Recommendations via Cortex
-- Per-country actionable policy suggestions
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE POLICY_RECOMMENDATIONS AS
SELECT
    COUNTRY_REGION,
    RISK_TIER,
    cases_last_30d,
    pct_change_30d,
    vacc_coverage_pct,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'You are a public health policy advisor for the WHO.\n\n',
            'COUNTRY: ', COUNTRY_REGION, '\n',
            'RISK TIER: ', RISK_TIER, '\n',
            'Cases (last 30d): ', TO_VARCHAR(ROUND(cases_last_30d)), '\n',
            'Trend: ', TO_VARCHAR(pct_change_30d), '% change over 30 days\n',
            'Vaccination coverage: ', TO_VARCHAR(ROUND(vacc_coverage_pct, 1)), '% fully vaccinated\n',
            'Forecast (next 30d): ', TO_VARCHAR(ROUND(forecast_30d_cases)), ' predicted cases\n\n',
            'Based on this data, provide exactly 3 policy recommendations:\n',
            '1. IMMEDIATE ACTION (next 7 days): One specific action the health ministry should take right now.\n',
            '2. SHORT-TERM STRATEGY (next 30 days): One strategic intervention for the coming month.\n',
            '3. RESOURCE ALLOCATION: One specific recommendation on where to allocate health resources.\n\n',
            'Rules:\n',
            '- Each recommendation must be 1-2 sentences max.\n',
            '- Be specific to the country situation — not generic advice.\n',
            '- If vaccination coverage is low, address it. If cases are rising, address containment.\n',
            '- If the country is LOW risk, focus on surveillance and preparedness, not alarm.\n',
            '- Use plain English. No jargon.'
        )
    ) AS policy_recommendations,
    CURRENT_TIMESTAMP() AS generated_at
FROM MARTS.COUNTRY_STATS;

SELECT COUNTRY_REGION, RISK_TIER, policy_recommendations
FROM MARTS.POLICY_RECOMMENDATIONS
ORDER BY CASE RISK_TIER WHEN 'HIGH' THEN 1 WHEN 'MODERATE' THEN 2 ELSE 3 END;


-- ─────────────────────────────────────────────
-- 4E: Explainability Narratives via Cortex
-- Uses FEATURE_IMPORTANCE to explain WHY forecasts trend
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE EXPLAINABILITY_NARRATIVES AS
WITH top_features AS (
    SELECT
        SERIES AS COUNTRY_REGION,
        LISTAGG(
            CONCAT(RANK, '. ', FEATURE_NAME, ' (importance: ', TO_VARCHAR(ROUND(SCORE, 3)), ')'),
            '\n'
        ) WITHIN GROUP (ORDER BY RANK) AS feature_list
    FROM MARTS.FEATURE_IMPORTANCE
    WHERE RANK <= 5
    GROUP BY SERIES
),
country_context AS (
    SELECT
        cs.COUNTRY_REGION,
        cs.RISK_TIER,
        cs.pct_change_30d,
        cs.latest_7d_avg_cases,
        cs.vacc_coverage_pct,
        tf.feature_list
    FROM MARTS.COUNTRY_STATS cs
    LEFT JOIN top_features tf ON cs.COUNTRY_REGION = tf.COUNTRY_REGION
)
SELECT
    COUNTRY_REGION,
    RISK_TIER,
    feature_list,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'You are an epidemiologist explaining ML model predictions to a health official.\n\n',
            'COUNTRY: ', COUNTRY_REGION, '\n',
            'RISK TIER: ', RISK_TIER, '\n',
            'Current trend: ', TO_VARCHAR(pct_change_30d), '% change in 30 days\n',
            'Daily average: ', TO_VARCHAR(latest_7d_avg_cases), ' cases/day\n',
            'Vaccination: ', TO_VARCHAR(ROUND(vacc_coverage_pct, 1)), '%\n\n',
            'The ML model identified these as the TOP FACTORS driving the forecast for this country:\n',
            COALESCE(feature_list, 'Feature importance data not available'), '\n\n',
            'INSTRUCTIONS:\n',
            '- In 2-3 sentences, explain in plain English WHY the model predicts this trajectory.\n',
            '- Connect the top features to real-world meaning (e.g., "vaccination rates lagged by 21 days had the highest impact, suggesting that recent vaccination efforts are the primary driver of declining cases").\n',
            '- Do NOT list the features. Instead, weave them into a narrative explanation.\n',
            '- Be specific to this country. No generic statements.'
        )
    ) AS explanation,
    CURRENT_TIMESTAMP() AS generated_at
FROM country_context;

SELECT COUNTRY_REGION, RISK_TIER, explanation
FROM MARTS.EXPLAINABILITY_NARRATIVES
ORDER BY CASE RISK_TIER WHEN 'HIGH' THEN 1 WHEN 'MODERATE' THEN 2 ELSE 3 END;
