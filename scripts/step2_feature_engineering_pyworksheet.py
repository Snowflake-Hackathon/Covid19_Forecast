import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window


def main(session: snowpark.Session) -> str:

    # ── Gap-fill: date spine + forward-fill ──
    session.sql("""
        CREATE OR REPLACE TABLE HACKATHON.INT.BASE_FILLED AS
        WITH bounds AS (
            SELECT MIN(DATE) AS min_dt, MAX(DATE) AS max_dt
            FROM HACKATHON.INT.DAILY_COUNTRY_SERIES
        ),
        spine AS (
            SELECT DATEADD('day', SEQ4(), min_dt) AS DATE
            FROM bounds, TABLE(GENERATOR(ROWCOUNT => 1200))
            WHERE DATEADD('day', SEQ4(), min_dt) <= max_dt
        ),
        date_spine AS (
            SELECT tc.OWID_NAME AS COUNTRY_REGION, s.DATE
            FROM HACKATHON.INT.TARGET_COUNTRIES tc
            CROSS JOIN (SELECT DISTINCT DATE FROM spine) s
        )
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
        FROM date_spine ds
        LEFT JOIN HACKATHON.INT.DAILY_COUNTRY_SERIES d
            ON ds.COUNTRY_REGION = d.COUNTRY_REGION AND ds.DATE = d.DATE
    """).collect()

    # ── Load base filled table ──
    df = session.table("HACKATHON.INT.BASE_FILLED")

    # ── Window specs ──
    w = Window.partition_by("COUNTRY_REGION").order_by("DATE")
    w_7d = w.rows_between(-6, Window.CURRENT_ROW)
    w_14d = w.rows_between(-13, Window.CURRENT_ROW)

    # ── Rolling Averages ──
    df = df.with_column("CASES_7D_AVG",
        F.round(F.avg("NEW_CASES").over(w_7d), 2))
    df = df.with_column("CASES_14D_AVG",
        F.round(F.avg("NEW_CASES").over(w_14d), 2))
    df = df.with_column("DEATHS_7D_AVG",
        F.round(F.avg("NEW_DEATHS").over(w_7d), 2))
    df = df.with_column("VACC_7D_AVG",
        F.round(F.avg("DAILY_VACCINATIONS").over(w_7d), 0))

    # ── Doubling Time ──
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

    # ── Week-over-Week Growth ──
    df = df.with_column("_CASES_7D_AVG_PREV",
        F.lag("CASES_7D_AVG", 7).over(w))
    df = df.with_column("WOW_CHANGE_7D_AVG",
        F.col("CASES_7D_AVG") - F.col("_CASES_7D_AVG_PREV"))
    df = df.with_column("WOW_GROWTH_PCT",
        F.when(F.col("_CASES_7D_AVG_PREV") > 0,
            F.round(
                (F.col("CASES_7D_AVG") - F.col("_CASES_7D_AVG_PREV")) /
                F.col("_CASES_7D_AVG_PREV") * 100, 2
            )
        )
    )

    # ── Vaccination Rate Lags ──
    df = df.with_column("VACC_RATE_LAG_14D",
        F.lag("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 14).over(w))
    df = df.with_column("VACC_RATE_LAG_21D",
        F.lag("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 21).over(w))
    df = df.with_column("VACC_RATE_LAG_28D",
        F.lag("PEOPLE_FULLY_VACCINATED_PER_HUNDRED", 28).over(w))

    # ── Explainability: Case-Death Ratio ──
    df = df.with_column("_CASES_7D_AVG_14D_AGO",
        F.lag("CASES_7D_AVG", 14).over(w))
    df = df.with_column("CASE_DEATH_RATIO",
        F.when(F.col("_CASES_7D_AVG_14D_AGO") > 0,
            F.round(F.col("DEATHS_7D_AVG") / F.col("_CASES_7D_AVG_14D_AGO") * 100, 2)
        )
    )

    # ── Explainability: Surge Flag ──
    df = df.with_column("SURGE_FLAG",
        F.when(F.col("WOW_GROWTH_PCT") > 50, F.lit(1)).otherwise(F.lit(0)))

    # ── Explainability: Vaccination Impact Proxy ──
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

    # ── Explainability: Recovery Rate ──
    df = df.with_column("_RECOVERED_7D_AVG",
        F.round(F.avg("NEW_RECOVERED").over(w_7d), 2))
    df = df.with_column("RECOVERY_RATE",
        F.when(
            (F.col("CASES_7D_AVG") > 0) & (F.col("_RECOVERED_7D_AVG") > 0),
            F.round(F.col("_RECOVERED_7D_AVG") / F.col("CASES_7D_AVG") * 100, 2)
        )
    )

    # ── Select final columns and write to MARTS ──
    features = df.select(
        "COUNTRY_REGION", "DATE",
        "NEW_CASES", "NEW_DEATHS", "NEW_RECOVERED",
        "CUMULATIVE_CASES", "CUMULATIVE_DEATHS",
        "CASES_7D_AVG", "CASES_14D_AVG", "DEATHS_7D_AVG", "VACC_7D_AVG",
        "DOUBLING_TIME_DAYS", "WOW_CHANGE_7D_AVG", "WOW_GROWTH_PCT",
        "DAILY_VACCINATIONS", "PEOPLE_FULLY_VACCINATED_PER_HUNDRED",
        "VACC_RATE_LAG_14D", "VACC_RATE_LAG_21D", "VACC_RATE_LAG_28D",
        "CASE_DEATH_RATIO", "SURGE_FLAG", "VACC_IMPACT_PROXY", "RECOVERY_RATE"
    ).sort("COUNTRY_REGION", "DATE")

    features.write.mode("overwrite").save_as_table("HACKATHON.MARTS.FEATURES_ENGINEERED")

    # ── Return result as DataFrame (Python Worksheet requirement) ──
    return session.table("HACKATHON.MARTS.FEATURES_ENGINEERED")
