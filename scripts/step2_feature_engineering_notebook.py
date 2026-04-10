# ============================================================
# STEP 2: Feature Engineering — Snowpark Python Notebook
#
# Architecture:
#   Reads from:  HACKATHON.INT.DAILY_COUNTRY_SERIES
#   Writes to:   HACKATHON.INT.BASE_FILLED (intermediate)
#                HACKATHON.MARTS.FEATURES_ENGINEERED (ML-ready)
#
# HOW TO USE:
#   1. Snowsight → Projects → Notebooks → + Notebook
#   2. Database: HACKATHON, Schema: INT
#   3. Split at "# --- CELL --- " markers into separate cells
#   4. Run cells in order
# ============================================================


# --- CELL 1: Setup ---
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
import streamlit as st

session = get_active_session()

st.title("Step 2: Feature Engineering Pipeline")
st.caption("Snowpark DataFrame API — COVID-19 Public Health Intelligence")
st.markdown("""
**Architecture:** `STG` (raw) → `INT` (cleaned) → `MARTS` (ML-ready)

This notebook reads from `INT.DAILY_COUNTRY_SERIES` and writes to `MARTS.FEATURES_ENGINEERED`.
""")


# --- CELL 2: Create gap-filled base table in INT ---
st.subheader("2A: Date Spine & Gap Fill → INT.BASE_FILLED")
st.write("ML Forecasting requires regular daily intervals with no gaps.")

# Date spine
session.sql("""
    CREATE OR REPLACE TEMPORARY TABLE _DATE_SPINE AS
    WITH bounds AS (
        SELECT MIN(DATE) AS min_dt, MAX(DATE) AS max_dt
        FROM HACKATHON.INT.DAILY_COUNTRY_SERIES
    ),
    spine AS (
        SELECT DATEADD('day', SEQ4(), min_dt) AS DATE
        FROM bounds, TABLE(GENERATOR(ROWCOUNT => 1200))
        WHERE DATEADD('day', SEQ4(), min_dt) <= max_dt
    )
    SELECT tc.OWID_NAME AS COUNTRY_REGION, s.DATE
    FROM HACKATHON.INT.TARGET_COUNTRIES tc
    CROSS JOIN (SELECT DISTINCT DATE FROM spine) s
""").collect()

# Gap-fill and write to INT
session.sql("""
    CREATE OR REPLACE TABLE HACKATHON.INT.BASE_FILLED AS
    SELECT
        ds.COUNTRY_REGION,
        ds.DATE,
        COALESCE(d.NEW_CASES, 0) AS NEW_CASES,
        COALESCE(d.NEW_DEATHS, 0) AS NEW_DEATHS,
        COALESCE(d.NEW_RECOVERED, 0) AS NEW_RECOVERED,
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
    FROM _DATE_SPINE ds
    LEFT JOIN HACKATHON.INT.DAILY_COUNTRY_SERIES d
        ON ds.COUNTRY_REGION = d.COUNTRY_REGION AND ds.DATE = d.DATE
""").collect()

df = session.table("HACKATHON.INT.BASE_FILLED")
st.success(f"INT.BASE_FILLED created: {df.count()} rows, {len(df.columns)} columns")
df.show(5)


# --- CELL 3: Define window specifications ---
st.subheader("2B: Window Specifications")

w = Window.partition_by("COUNTRY_REGION").order_by("DATE")
w_7d = w.rows_between(-6, Window.CURRENT_ROW)
w_14d = w.rows_between(-13, Window.CURRENT_ROW)

st.code("""
w       = partition_by(COUNTRY).order_by(DATE)
w_7d    = w.rows_between(-6, CURRENT)     # 7-day rolling
w_14d   = w.rows_between(-13, CURRENT)    # 14-day rolling
""", language="text")


# --- CELL 4: Rolling Averages ---
st.subheader("2C: Rolling Averages")

df = df.with_column("CASES_7D_AVG",
    F.round(F.avg("NEW_CASES").over(w_7d), 2))

df = df.with_column("CASES_14D_AVG",
    F.round(F.avg("NEW_CASES").over(w_14d), 2))

df = df.with_column("DEATHS_7D_AVG",
    F.round(F.avg("NEW_DEATHS").over(w_7d), 2))

df = df.with_column("VACC_7D_AVG",
    F.round(F.avg("DAILY_VACCINATIONS").over(w_7d), 0))

st.write("**Preview: United States rolling averages**")
df.filter(F.col("COUNTRY_REGION") == "United States").sort("DATE").select(
    "DATE", "NEW_CASES", "CASES_7D_AVG", "CASES_14D_AVG"
).show(10)


# --- CELL 5: Doubling Time ---
st.subheader("2D: Doubling Time")
st.write("Formula: `7 × ln(2) / ln(cumulative_today / cumulative_7d_ago)`")

df = df.with_column("_CUM_7D_AGO",
    F.lag("CUMULATIVE_CASES", 7).over(w))

LN2 = 0.6931471805599453
df = df.with_column("DOUBLING_TIME_DAYS",
    F.when(
        (F.col("_CUM_7D_AGO") > 0) &
        (F.col("CUMULATIVE_CASES") > F.col("_CUM_7D_AGO")),
        F.round(
            F.lit(7.0) * F.lit(LN2) / F.ln(F.col("CUMULATIVE_CASES") / F.col("_CUM_7D_AGO")),
            2
        )
    )
)

st.write("**Preview: India doubling time during Delta wave**")
df.filter(
    (F.col("COUNTRY_REGION") == "India") & (F.col("DATE") >= "2021-04-01")
).sort("DATE").select("DATE", "CUMULATIVE_CASES", "DOUBLING_TIME_DAYS").show(10)


# --- CELL 6: Week-over-Week Growth ---
st.subheader("2E: Week-over-Week Growth")

df = df.with_column("_CASES_7D_AVG_PREV",
    F.lag("CASES_7D_AVG", 7).over(w))

df = df.with_column("WOW_CHANGE_7D_AVG",
    F.col("CASES_7D_AVG") - F.col("_CASES_7D_AVG_PREV"))

df = df.with_column("WOW_GROWTH_PCT",
    F.when(F.col("_CASES_7D_AVG_PREV") > 0,
        F.round(
            (F.col("CASES_7D_AVG") - F.col("_CASES_7D_AVG_PREV")) /
            F.col("_CASES_7D_AVG_PREV") * 100,
            2
        )
    )
)

st.write("WoW growth computed: % change in 7-day average vs previous week")


# --- CELL 7: Vaccination Rate Lags ---
st.subheader("2F: Vaccination Rate Lags")
st.info("""
**Why lags?** Vaccines take 2-4 weeks to produce full immune response.
Today's case count is influenced by vaccination levels from weeks ago, not today's.
- **LAG 14d**: partial immunity effect
- **LAG 21d**: near-full immunity
- **LAG 28d**: full immunity
""")

df = df.with_column("VACC_RATE_LAG_14D",
    F.lag("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 14).over(w))

df = df.with_column("VACC_RATE_LAG_21D",
    F.lag("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 21).over(w))

df = df.with_column("VACC_RATE_LAG_28D",
    F.lag("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 28).over(w))

st.write("**Preview: India vaccination lags**")
df.filter(
    (F.col("COUNTRY_REGION") == "India") & (F.col("DATE") >= "2021-08-01")
).sort("DATE").select(
    "DATE", "PEOPLE_FULLY_VACCINATED_PER_HUNDRED",
    "VACC_RATE_LAG_14D", "VACC_RATE_LAG_21D", "VACC_RATE_LAG_28D"
).show(10)


# --- CELL 8: Explainability Features ---
st.subheader("2G: Explainability Features")
st.markdown("""
| Feature | What it explains |
|---------|-----------------|
| `CASE_DEATH_RATIO` | Are deaths keeping pace with cases? (14d lag) |
| `SURGE_FLAG` | Binary alert when WoW growth > 50% |
| `VACC_IMPACT_PROXY` | Is vaccination reducing case growth? |
| `RECOVERY_RATE` | Are recoveries keeping up with new cases? |
""")

# Case-Death Ratio
df = df.with_column("_CASES_7D_AVG_14D_AGO",
    F.lag("CASES_7D_AVG", 14).over(w))

df = df.with_column("CASE_DEATH_RATIO",
    F.when(F.col("_CASES_7D_AVG_14D_AGO") > 0,
        F.round(F.col("DEATHS_7D_AVG") / F.col("_CASES_7D_AVG_14D_AGO") * 100, 2)
    )
)

# Surge Flag
df = df.with_column("SURGE_FLAG",
    F.when(F.col("WOW_GROWTH_PCT") > 50, F.lit(1)).otherwise(F.lit(0)))

# Vaccination Impact Proxy
df = df.with_column("_VACC_LAG_21D_PREV",
    F.lag("VACC_RATE_LAG_21D", 7).over(w))

df = df.with_column("VACC_IMPACT_PROXY",
    F.when(
        (F.col("VACC_RATE_LAG_21D") > 0) & (F.col("_CASES_7D_AVG_PREV") > 0),
        F.round(
            (F.col("CASES_7D_AVG") - F.col("_CASES_7D_AVG_PREV")) /
            F.col("_CASES_7D_AVG_PREV") * 100
            - (F.col("VACC_RATE_LAG_21D") - F.coalesce(F.col("_VACC_LAG_21D_PREV"), F.lit(0))),
            2
        )
    )
)

# Recovery Rate
df = df.with_column("_RECOVERED_7D_AVG",
    F.round(F.avg("NEW_RECOVERED").over(w_7d), 2))

df = df.with_column("RECOVERY_RATE",
    F.when(
        (F.col("CASES_7D_AVG") > 0) & (F.col("_RECOVERED_7D_AVG") > 0),
        F.round(F.col("_RECOVERED_7D_AVG") / F.col("CASES_7D_AVG") * 100, 2)
    )
)

st.success("All 4 explainability features computed")


# --- CELL 9: Write to MARTS ---
st.subheader("2H: Write → MARTS.FEATURES_ENGINEERED")

features = df.select(
    "COUNTRY_REGION", "DATE",
    # Core daily
    "NEW_CASES", "NEW_DEATHS", "NEW_RECOVERED",
    # Cumulative
    "CUMULATIVE_CASES", "CUMULATIVE_DEATHS",
    # Rolling averages
    "CASES_7D_AVG", "CASES_14D_AVG", "DEATHS_7D_AVG", "VACC_7D_AVG",
    # Growth
    "DOUBLING_TIME_DAYS", "WOW_CHANGE_7D_AVG", "WOW_GROWTH_PCT",
    # Vaccination
    "DAILY_VACCINATIONS", "PEOPLE_FULLY_VACCINATED_PER_HUNDRED",
    "VACC_RATE_LAG_14D", "VACC_RATE_LAG_21D", "VACC_RATE_LAG_28D",
    # Explainability
    "CASE_DEATH_RATIO", "SURGE_FLAG", "VACC_IMPACT_PROXY", "RECOVERY_RATE"
).sort("COUNTRY_REGION", "DATE")

features.write.mode("overwrite").save_as_table("HACKATHON.MARTS.FEATURES_ENGINEERED")

result = session.table("HACKATHON.MARTS.FEATURES_ENGINEERED")
st.success(f"MARTS.FEATURES_ENGINEERED written: {result.count()} rows, {len(result.columns)} columns")


# --- CELL 10: Validation ---
st.subheader("2I: Validation")

st.write("**Row counts per country (should be identical — date spine ensures no gaps):**")
result.group_by("COUNTRY_REGION").agg(
    F.count("*").alias("ROWS"),
    F.min("DATE").alias("START"),
    F.max("DATE").alias("END"),
    F.count_distinct("DATE").alias("UNIQUE_DATES")
).sort("COUNTRY_REGION").show()

st.write("**NULL counts per key feature:**")
null_checks = result.agg(
    F.count("*").alias("TOTAL"),
    F.sum(F.when(F.col("CASES_7D_AVG").is_null(), 1).otherwise(0)).alias("NULL_CASES_7D"),
    F.sum(F.when(F.col("DOUBLING_TIME_DAYS").is_null(), 1).otherwise(0)).alias("NULL_DOUBLING"),
    F.sum(F.when(F.col("VACC_RATE_LAG_14D").is_null(), 1).otherwise(0)).alias("NULL_VACC_LAG14"),
    F.sum(F.when(F.col("CASE_DEATH_RATIO").is_null(), 1).otherwise(0)).alias("NULL_CDR"),
    F.sum(F.when(F.col("RECOVERY_RATE").is_null(), 1).otherwise(0)).alias("NULL_RECOVERY"),
)
null_checks.show()

st.write("**Sample: United States, June 2021:**")
result.filter(
    (F.col("COUNTRY_REGION") == "United States") &
    (F.col("DATE") >= "2021-06-01") &
    (F.col("DATE") <= "2021-06-10")
).sort("DATE").show()

st.markdown("""
---
### Data Flow Summary
```
STG.STG_JHU_COVID_19 ─┐
                       ├→ INT.DAILY_COUNTRY_SERIES → INT.BASE_FILLED → MARTS.FEATURES_ENGINEERED
STG.STG_OWID_VACCINATIONS ┘                                                     ↓
                                                                          ML Forecasting (Step 3)
                                                                                 ↓
                                                                     MARTS.FORECAST_OUTPUT
                                                                                 ↓
                                                              MARTS.OUTBREAK_NARRATIVES (Step 4)
                                                                                 ↓
                                                                    Streamlit Dashboard (Step 5)
```
""")
