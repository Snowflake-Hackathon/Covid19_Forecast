# ============================================================
# STEP 5: Streamlit Dashboard (Enhanced)
#   Projects → Streamlit → + Streamlit App
#   Database: HACKATHON | Schema: MARTS | Warehouse: yours
#
# 9 Tabs:
#   1. Forecast Charts (with confidence bands)
#   2. Risk Tiers
#   3. AI Outbreak Summaries
#   4. Country Comparison & Leaderboards
#   5. Explainability (feature importance + Cortex narratives)
#   6. Policy Recommendations
#   7. Data Profile + Fairness Note
#   8. Model Evaluation
#   9. Architecture
# ============================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

st.set_page_config(page_title="Public Health Intelligence", layout="wide")

st.title("Public Health Trend Intelligence")
st.caption("ML Forecasting + Cortex AI | TAMU CSEGSA x Snowflake Hackathon 2026")


# ══════════════════════════════════════════════
#  DATA LOADING
# ══════════════════════════════════════════════

@st.cache_data
def load_global_summary():
    return session.sql("SELECT * FROM HACKATHON.MARTS.GLOBAL_SUMMARY").to_pandas()

@st.cache_data
def load_narratives():
    return session.sql("""
        SELECT COUNTRY_REGION, RISK_TIER, CASES_LAST_30D, FORECAST_30D_CASES,
               PCT_CHANGE_30D, AVG_MAPE, NARRATIVE
        FROM HACKATHON.MARTS.OUTBREAK_NARRATIVES
        ORDER BY CASE RISK_TIER WHEN 'HIGH' THEN 1 WHEN 'MODERATE' THEN 2 ELSE 3 END
    """).to_pandas()

@st.cache_data
def load_forecast_output():
    return session.sql("""
        SELECT COUNTRY_REGION, DS, ACTUAL_CASES, FORECASTED_CASES,
               FORECAST_LOWER, FORECAST_UPPER, IS_FORECAST, MAPE
        FROM HACKATHON.MARTS.FORECAST_OUTPUT
        ORDER BY COUNTRY_REGION, DS
    """).to_pandas()

@st.cache_data
def load_features():
    return session.sql("""
        SELECT COUNTRY_REGION, DATE, CASES_7D_AVG, CASES_14D_AVG, DEATHS_7D_AVG,
               NEW_CASES, NEW_DEATHS, CUMULATIVE_CASES,
               PEOPLE_FULLY_VACCINATED_PER_HUNDRED, DAILY_VACCINATIONS,
               SURGE_FLAG, WOW_GROWTH_PCT
        FROM HACKATHON.MARTS.FEATURES_ENGINEERED
        ORDER BY COUNTRY_REGION, DATE
    """).to_pandas()

@st.cache_data
def load_country_stats():
    return session.sql("SELECT * FROM HACKATHON.MARTS.COUNTRY_STATS").to_pandas()

@st.cache_data
def load_data_profile():
    return session.sql("SELECT * FROM HACKATHON.MARTS.DATA_PROFILE").to_pandas()

@st.cache_data
def load_forecast_eval():
    return session.sql("SELECT * FROM HACKATHON.MARTS.FORECAST_EVAL").to_pandas()

@st.cache_data
def load_feature_importance():
    try:
        df = session.sql("SELECT REPLACE(SERIES, '\"', '') AS SERIES, RANK, FEATURE_NAME, SCORE FROM HACKATHON.MARTS.FEATURE_IMPORTANCE ORDER BY SERIES, RANK").to_pandas()
        df["SERIES"] = df["SERIES"].str.strip().str.strip('"')
        return df
    except:
        return pd.DataFrame()

@st.cache_data
def load_policy_recommendations():
    try:
        return session.sql("SELECT * FROM HACKATHON.MARTS.POLICY_RECOMMENDATIONS").to_pandas()
    except:
        return pd.DataFrame()

@st.cache_data
def load_explainability():
    try:
        return session.sql("SELECT * FROM HACKATHON.MARTS.EXPLAINABILITY_NARRATIVES").to_pandas()
    except:
        return pd.DataFrame()

# Load all
global_summary = load_global_summary()
narratives = load_narratives()
forecast_df = load_forecast_output()
features_df = load_features()
country_stats = load_country_stats()
data_profile = load_data_profile()
forecast_eval = load_forecast_eval()
feature_imp = load_feature_importance()
policy_recs = load_policy_recommendations()
explain_df = load_explainability()


# ══════════════════════════════════════════════
#  GLOBAL EXECUTIVE SUMMARY (top of page)
# ══════════════════════════════════════════════

st.markdown("---")
st.subheader("Global Situation Overview")
if not global_summary.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Countries Monitored", "15")
    col2.metric("High Risk", int(global_summary["HIGH_RISK_COUNT"].iloc[0]))
    col3.metric("Moderate Risk", int(global_summary["MODERATE_RISK_COUNT"].iloc[0]))
    col4.metric("Low Risk", int(global_summary["LOW_RISK_COUNT"].iloc[0]))
    st.info(global_summary["GLOBAL_NARRATIVE"].iloc[0])

# Surge alerts
if not features_df.empty:
    latest_date = features_df["DATE"].max()
    latest_surges = features_df[
        (features_df["DATE"] == latest_date) & (features_df["SURGE_FLAG"] == 1)
    ]
    if not latest_surges.empty:
        surge_countries = ", ".join(latest_surges["COUNTRY_REGION"].tolist())
        st.error(f"SURGE ALERT: {surge_countries} — WoW growth >50% detected on latest data")


# ══════════════════════════════════════════════
#  TABS
# ══════════════════════════════════════════════

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Forecast",
    "Risk Tiers",
    "AI Summaries",
    "Compare",
    "Explainability",
    "Policy",
    "Data Profile",
    "Model Eval",
    "Architecture"
])


# ── TAB 1: Forecast Charts with Confidence Bands ──
with tab1:
    st.subheader("Forecast with Confidence Intervals")

    countries = sorted(features_df["COUNTRY_REGION"].unique())
    selected = st.selectbox("Select Country", countries, key="t1_country")

    # Historical data
    hist = features_df[features_df["COUNTRY_REGION"] == selected].copy()
    hist["DATE"] = pd.to_datetime(hist["DATE"])
    hist = hist.sort_values("DATE")

    # Forecast data
    fcast = forecast_df[
        (forecast_df["COUNTRY_REGION"] == selected) & (forecast_df["IS_FORECAST"] == True)
    ].copy()
    fcast["DS"] = pd.to_datetime(fcast["DS"])
    fcast = fcast.sort_values("DS")

    # Date range filter
    show_period = st.radio("Period", ["Last 6 months", "Last 1 year", "Full history"],
                           index=1, horizontal=True, key="t1_period")

    if show_period == "Last 6 months":
        cutoff = hist["DATE"].max() - pd.Timedelta(days=180)
    elif show_period == "Last 1 year":
        cutoff = hist["DATE"].max() - pd.Timedelta(days=365)
    else:
        cutoff = hist["DATE"].min()

    hist_display = hist[hist["DATE"] >= cutoff]

    # Build chart with confidence bands
    chart_data = hist_display[["DATE", "CASES_7D_AVG"]].rename(
        columns={"DATE": "date", "CASES_7D_AVG": "Actual (7d avg)"}
    )
    chart_data["Forecast"] = None
    chart_data["Upper Bound (95%)"] = None
    chart_data["Lower Bound (95%)"] = None

    if not fcast.empty and not hist.empty:
        # Bridge point
        last_date = hist["DATE"].max()
        last_val = hist[hist["DATE"] == last_date]["CASES_7D_AVG"].values[0]
        bridge = pd.DataFrame({
            "date": [last_date],
            "Actual (7d avg)": [last_val],
            "Forecast": [last_val],
            "Upper Bound (95%)": [last_val],
            "Lower Bound (95%)": [last_val]
        })

        fcast_chart = pd.DataFrame({
            "date": fcast["DS"],
            "Actual (7d avg)": None,
            "Forecast": fcast["FORECASTED_CASES"].values,
            "Upper Bound (95%)": fcast["FORECAST_UPPER"].values,
            "Lower Bound (95%)": fcast["FORECAST_LOWER"].values
        })

        chart_data = pd.concat([chart_data, bridge, fcast_chart], ignore_index=True)

    chart_data = chart_data.sort_values("date").set_index("date")
    st.line_chart(chart_data)

    # Country metrics
    if not fcast.empty:
        eval_row = forecast_eval[forecast_eval["COUNTRY_REGION"] == selected]
        c1, c2, c3, c4 = st.columns(4)
        if not hist.empty:
            c1.metric("Latest 7d Avg", f"{hist['CASES_7D_AVG'].iloc[-1]:,.0f}")
        c2.metric("Forecast Avg (30d)", f"{fcast['FORECASTED_CASES'].mean():,.0f}")
        if not fcast.empty and "FORECAST_UPPER" in fcast.columns:
            c3.metric("95% Upper Bound", f"{fcast['FORECAST_UPPER'].mean():,.0f}")
        if not eval_row.empty:
            c4.metric("sMAPE", f"{eval_row['SMAPE'].iloc[0]:.1f}%")

    if not fcast.empty:
        st.caption(
            f"Forecast: {fcast['DS'].min().strftime('%Y-%m-%d')} → "
            f"{fcast['DS'].max().strftime('%Y-%m-%d')} | "
            f"Shaded area = 95% prediction interval"
        )


# ── TAB 2: Risk Tiers ──
with tab2:
    st.subheader("Risk Tier Classification")
    st.markdown("**High** (>20% increase) | **Moderate** (5-20%) | **Low** (<5%)")

    tier_order = {"HIGH": 0, "MODERATE": 1, "LOW": 2}
    sorted_stats = country_stats.copy()
    sorted_stats["_order"] = sorted_stats["RISK_TIER"].map(tier_order)
    sorted_stats = sorted_stats.sort_values("_order")

    for _, row in sorted_stats.iterrows():
        tier = row["RISK_TIER"]
        color = {"HIGH": "red", "MODERATE": "orange", "LOW": "green"}.get(tier, "gray")
        col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
        col1.markdown(f"**{row['COUNTRY_REGION']}**")
        col2.markdown(f":{color}[{tier}]")
        col3.metric("30d Change", f"{row.get('PCT_CHANGE_30D', 'N/A')}%")
        col4.metric("Cases (30d)", f"{int(row.get('CASES_LAST_30D', 0)):,}")


# ── TAB 3: AI Outbreak Summaries ──
with tab3:
    st.subheader("Cortex AI Outbreak Narratives")
    st.markdown("Generated by Snowflake Cortex `COMPLETE()` for non-technical decision makers.")

    tier_filter = st.multiselect("Filter by Risk Tier",
        ["HIGH", "MODERATE", "LOW"], default=["HIGH", "MODERATE", "LOW"], key="t3_filter")
    filtered = narratives[narratives["RISK_TIER"].isin(tier_filter)]

    for _, row in filtered.iterrows():
        tier = row["RISK_TIER"]
        icon = {"HIGH": "🔴", "MODERATE": "🟡", "LOW": "🟢"}.get(tier, "⚪")
        with st.expander(f"{icon} {row['COUNTRY_REGION']} — {tier}", expanded=(tier == "HIGH")):
            st.markdown(row["NARRATIVE"])
            c1, c2, c3 = st.columns(3)
            c1.metric("Cases (30d)", f"{int(row.get('CASES_LAST_30D', 0)):,}")
            c2.metric("Forecast (30d)", f"{int(row.get('FORECAST_30D_CASES', 0)):,}")
            c3.metric("Trend", f"{row.get('PCT_CHANGE_30D', 0)}%")


# ── TAB 4: Country Comparison & Leaderboards ──
with tab4:
    st.subheader("Country Comparison & Leaderboards")

    if not country_stats.empty:
        cs = country_stats.copy()

        # Leaderboards
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("**🔻 Fastest Worsening (Rising Cases)**")
            worsening = cs.nlargest(5, "PCT_CHANGE_30D")[["COUNTRY_REGION", "PCT_CHANGE_30D", "CASES_LAST_30D", "RISK_TIER"]]
            for _, r in worsening.iterrows():
                color = {"HIGH": "red", "MODERATE": "orange", "LOW": "green"}.get(r["RISK_TIER"], "gray")
                st.markdown(f":{color}[{r['RISK_TIER']}] **{r['COUNTRY_REGION']}** — {r['PCT_CHANGE_30D']}% change, {int(r['CASES_LAST_30D']):,} cases")

        with col_right:
            st.markdown("**🔺 Fastest Improving (Declining Cases)**")
            improving = cs.nsmallest(5, "PCT_CHANGE_30D")[["COUNTRY_REGION", "PCT_CHANGE_30D", "CASES_LAST_30D", "RISK_TIER"]]
            for _, r in improving.iterrows():
                color = {"HIGH": "red", "MODERATE": "orange", "LOW": "green"}.get(r["RISK_TIER"], "gray")
                st.markdown(f":{color}[{r['RISK_TIER']}] **{r['COUNTRY_REGION']}** — {r['PCT_CHANGE_30D']}% change, {int(r['CASES_LAST_30D']):,} cases")

        st.markdown("---")

        # Multi-country comparison chart
        st.markdown("**Multi-Country Trend Comparison**")
        compare_countries = st.multiselect("Select countries to compare",
            sorted(features_df["COUNTRY_REGION"].unique()),
            default=["United States", "India", "Brazil", "Germany", "United Kingdom"],
            key="t4_compare"
        )

        if compare_countries:
            compare_df = features_df[features_df["COUNTRY_REGION"].isin(compare_countries)].copy()
            compare_df["DATE"] = pd.to_datetime(compare_df["DATE"])

            # Last 6 months
            compare_cutoff = compare_df["DATE"].max() - pd.Timedelta(days=180)
            compare_df = compare_df[compare_df["DATE"] >= compare_cutoff]

            # Pivot for chart
            pivot = compare_df.pivot_table(
                index="DATE", columns="COUNTRY_REGION", values="CASES_7D_AVG"
            )
            st.line_chart(pivot)

        st.markdown("---")

        # Situation matrix
        st.markdown("**Situation Matrix**")

        # High cases but declining
        high_declining = cs[(cs["CASES_LAST_30D"] > cs["CASES_LAST_30D"].median()) & (cs["PCT_CHANGE_30D"] < 0)]
        if not high_declining.empty:
            st.markdown("🟢 **High cases but declining** (positive trajectory):")
            for _, r in high_declining.iterrows():
                st.markdown(f"  - {r['COUNTRY_REGION']}: {int(r['CASES_LAST_30D']):,} cases, {r['PCT_CHANGE_30D']}% trend")

        # Low cases but rising
        low_rising = cs[(cs["CASES_LAST_30D"] <= cs["CASES_LAST_30D"].median()) & (cs["PCT_CHANGE_30D"] > 10)]
        if not low_rising.empty:
            st.markdown("🟡 **Low cases but rising fast** (watch closely):")
            for _, r in low_rising.iterrows():
                st.markdown(f"  - {r['COUNTRY_REGION']}: {int(r['CASES_LAST_30D']):,} cases, {r['PCT_CHANGE_30D']}% trend")

        # Stable
        stable = cs[(cs["PCT_CHANGE_30D"].abs() <= 5)]
        if not stable.empty:
            st.markdown("⚪ **Stable** (minimal change):")
            for _, r in stable.iterrows():
                st.markdown(f"  - {r['COUNTRY_REGION']}: {int(r['CASES_LAST_30D']):,} cases, {r['PCT_CHANGE_30D']}% trend")


# ── TAB 5: Explainability ──
with tab5:
    st.subheader("Explainable Forecast — Why is the model predicting this?")

    countries_exp = sorted(features_df["COUNTRY_REGION"].unique())
    sel_country = st.selectbox("Select Country", countries_exp, key="t5_country")

    # Feature importance chart
    if not feature_imp.empty:
        country_imp = feature_imp[feature_imp["SERIES"] == sel_country].copy()
        if not country_imp.empty:
            st.markdown("**Top Feature Contributions**")
            chart_imp = country_imp.head(7).set_index("FEATURE_NAME")[["SCORE"]]
            st.bar_chart(chart_imp)

            st.markdown("**Feature Ranking**")
            for _, r in country_imp.head(7).iterrows():
                score = r.get("SCORE", 0)
                name = r.get("FEATURE_NAME", "Unknown")
                rank = int(r.get("RANK", 0))
                st.markdown(f"{rank}. **{name}** — importance: {score:.4f}")
        else:
            st.warning(f"No feature importance data for {sel_country}")
    else:
        st.warning("Feature importance table not found. Run Phase E of Step 3.")

    # Cortex explainability narrative
    st.markdown("---")
    st.markdown("**AI-Generated Explanation**")
    if not explain_df.empty:
        country_explain = explain_df[explain_df["COUNTRY_REGION"] == sel_country]
        if not country_explain.empty:
            st.info(country_explain["EXPLANATION"].iloc[0])
        else:
            st.warning(f"No explainability narrative for {sel_country}")
    else:
        st.warning("Explainability narratives not generated yet. Run Step 4 section 4E.")


# ── TAB 6: Policy Recommendations ──
with tab6:
    st.subheader("AI-Generated Policy Recommendations")
    st.markdown("Actionable recommendations generated by Snowflake Cortex, tailored per country.")

    if not policy_recs.empty:
        pol_filter = st.radio("Filter", ["All", "HIGH", "MODERATE", "LOW"],
                              horizontal=True, key="t6_filter")
        if pol_filter != "All":
            pol_display = policy_recs[policy_recs["RISK_TIER"] == pol_filter]
        else:
            pol_display = policy_recs

        for _, row in pol_display.iterrows():
            tier = row["RISK_TIER"]
            icon = {"HIGH": "🔴", "MODERATE": "🟡", "LOW": "🟢"}.get(tier, "⚪")
            with st.expander(f"{icon} {row['COUNTRY_REGION']} — {tier} Risk", expanded=(tier == "HIGH")):
                st.markdown(row["POLICY_RECOMMENDATIONS"])
                c1, c2 = st.columns(2)
                c1.metric("Vaccination Coverage", f"{row.get('VACC_COVERAGE_PCT', 0):.1f}%")
                c2.metric("30d Trend", f"{row.get('PCT_CHANGE_30D', 0)}%")
    else:
        st.warning("Policy recommendations not generated yet. Run Step 4 section 4D.")


# ── TAB 7: Data Profile + Fairness Note ──
with tab7:
    st.subheader("Data Profile — 15 Countries")

    if not data_profile.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Countries", len(data_profile))
        col2.metric("Date Range", f"{data_profile['SERIES_START'].min()} → {data_profile['SERIES_END'].max()}")
        col3.metric("Days per Country", int(data_profile["TOTAL_DAYS"].mean()))

        display_cols = [
            "COUNTRY_REGION", "TOTAL_DAYS", "TOTAL_CONFIRMED_CASES", "TOTAL_DEATHS",
            "CASE_FATALITY_RATE_PCT", "MAX_FULLY_VACCINATED_PCT",
            "VACCINATION_START_DATE", "ZERO_CASE_PCT"
        ]
        available_cols = [c for c in display_cols if c in data_profile.columns]
        st.dataframe(data_profile[available_cols], use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("**Data Gaps & Geographic Bias (Fairness Note)**")
        st.warning("""
        - **Reporting inconsistency**: Zero-case days vary by country (see ZERO_CASE_PCT). Higher values may indicate reporting gaps, not actual zero cases.
        - **Vaccination data lag**: OWID vaccination data has intermittent gaps; forward-filled in pipeline.
        - **Recovery data discontinued**: JHU stopped tracking recoveries mid-2021 — excluded from ML model.
        - **Geographic bias**: Dataset skews toward countries with stronger reporting infrastructure. Low/middle-income country forecasts may be less reliable.
        - **End-of-pandemic surveillance decline**: Late 2022/2023 data reflects reduced testing, not necessarily fewer infections.
        - **Model limitation**: Forecast accuracy (sMAPE) varies significantly by country, reflecting data quality differences.
        """)


# ── TAB 8: Model Evaluation ──
with tab8:
    st.subheader("ML Forecast Model Evaluation")
    st.markdown("Snowflake ML Forecasting | 30-day holdout evaluation | Exogenous features: vaccination lags, death averages")

    if not forecast_eval.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg MAE", f"{forecast_eval['MAE'].mean():,.0f} cases/day")
        col2.metric("Avg sMAPE", f"{forecast_eval['SMAPE'].mean():.1f}%")
        col3.metric("Avg WMAPE", f"{forecast_eval['WMAPE'].mean():.1f}%")

        st.markdown("**Per-Country Metrics**")
        eval_display = forecast_eval[[
            "COUNTRY_REGION", "EVAL_DAYS", "MAE", "SMAPE", "WMAPE",
            "AVG_DAILY_ACTUAL", "LOW_COUNT_DAYS"
        ]].copy().sort_values("SMAPE")
        st.dataframe(eval_display, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.info("""
        **Why sMAPE over MAPE?** Traditional MAPE divides by actual values — when cases approach zero,
        small errors produce enormous MAPE (>1000%). Symmetric MAPE (sMAPE) uses the average of actual
        and forecast as denominator, giving a bounded 0-200% range. WMAPE weights errors by case volume.
        Countries with `LOW_COUNT_DAYS > 15` have predominantly near-zero case counts, making percentage
        metrics inherently noisy.
        """)


# ── TAB 9: Architecture ──
with tab9:
    st.subheader("System Architecture")
    st.markdown("""
    ### Three-Layer Data Architecture
    ```
    ┌─────────────────────────────────────────────────────────────────┐
    │  STG (Staging)          Raw Marketplace data as views          │
    │  ├── STG_JHU_COVID_19        Cases, deaths, recovered         │
    │  ├── STG_OWID_VACCINATIONS   Global vaccination data          │
    │  └── STG_ECDC_GLOBAL         Population, continent data       │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │
    ┌──────────────────────────────▼──────────────────────────────────┐
    │  INT (Intermediate)     Cleaned, joined, gap-filled            │
    │  ├── TARGET_COUNTRIES        15-country mapping view           │
    │  ├── DAILY_COUNTRY_SERIES    Pivoted daily time-series         │
    │  └── BASE_FILLED             Date-spine filled, forward-fill   │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │
    ┌──────────────────────────────▼──────────────────────────────────┐
    │  MARTS (Consumption)    ML-ready features & outputs            │
    │  ├── FEATURES_ENGINEERED     23 features, 15 countries         │
    │  ├── FORECAST_OUTPUT         Actuals + 30-day projections      │
    │  ├── FORECAST_EVAL           MAPE/sMAPE/WMAPE per country      │
    │  ├── FEATURE_IMPORTANCE      ML model feature rankings         │
    │  ├── OUTBREAK_NARRATIVES     Cortex AI per-country briefings   │
    │  ├── POLICY_RECOMMENDATIONS  Cortex AI policy suggestions      │
    │  ├── EXPLAINABILITY_NARRATIVES  Why the model predicts this    │
    │  ├── GLOBAL_SUMMARY          Executive situation overview      │
    │  └── DATA_PROFILE            Data quality & fairness notes     │
    └────────────────────────────────────────────────────────────────┘
    ```

    ### Pipeline Steps
    | Step | What | Tech |
    |------|------|------|
    | 1. Ingest & Profile | Load, clean, join COVID + vaccination data | SQL, Snowpark |
    | 2. Feature Engineering | Rolling averages, doubling time, vaccination lags | Snowpark DataFrame API |
    | 3. ML Forecasting | Train per-country models, evaluate, project 30 days | Snowflake ML Forecasting |
    | 4. AI Summarization | Outbreak narratives, policy recs, explainability | Snowflake Cortex COMPLETE() |
    | 5. Dashboard | Interactive visualization of all outputs | Streamlit in Snowflake |

    ### Key Features
    - **23 engineered features** including vaccination rate lags (14/21/28 day) and explainability metrics
    - **Exogenous ML forecasting** — vaccination and death data improve predictions
    - **3 types of Cortex AI outputs** — outbreak briefings, policy recommendations, and explainability narratives
    - **Fairness documentation** — data gaps, geographic bias, and model limitations noted
    """)


# ── Footer ──
st.markdown("---")
st.markdown("Built with Snowflake ML Forecasting + Cortex AI | TAMU CSEGSA x Snowflake Hackathon 2026")
