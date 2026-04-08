import streamlit as st
import pandas as pd
from utils import read_file
from etl import transform_data, aggregate_data, load_to_db
from db import create_tables, get_connection
from logger import log_etl

# PAGE CONFIG
st.set_page_config(page_title="Retail ETL", layout="wide")

# SIDEBAR
st.sidebar.title("⚙️ Tech Stack")
st.sidebar.markdown("""
- 🐍 Python (Pandas)
- 🗄️ SQLite Database
- 📊 Streamlit UI
- 🔄 ETL Pipeline
""")

# MAIN TITLE (CENTER + BLUE + LOGO)
st.markdown("""
<h1 style='text-align: center; color: #1f77b4;'>
🛒 Retail Sales ETL & Analytics Platform
</h1>
""", unsafe_allow_html=True)

create_tables()

uploaded_file = st.file_uploader("📂 Upload File", type=["csv", "json", "xlsx"])

if uploaded_file:

    file_name = uploaded_file.name
    df = read_file(uploaded_file)

    # RAW DATA
    st.markdown("## 🔵 Raw Data")
    st.dataframe(df)

    # CLEAN
    clean_df = transform_data(df)
    st.markdown("## 🔵 Cleaned Data")
    st.dataframe(clean_df)

    # AGG
    agg_df = aggregate_data(clean_df)
    st.markdown("## 🔵 Aggregated Data")
    st.dataframe(agg_df)

    if st.button("🚀 Load to Database"):

        load_to_db(df, clean_df, agg_df, file_name)
        log_etl(file_name, len(df), len(clean_df), len(agg_df))

        st.success("✅ Data Loaded Successfully!")

        conn = get_connection()

        # =========================
        # 📊 VISUALIZATION SECTION
        # =========================
        st.markdown("## 📊 Sales Analytics")

        # 1️⃣ SALES BY MONTH
        st.subheader("📅 Sales by Month")
        monthly = pd.read_sql("SELECT * FROM sales_summary_view", conn)
        st.line_chart(monthly.set_index("order_month"))

        # 2️⃣ SALES BY STORE
        st.subheader("🏬 Sales by Store")
        store_data = pd.read_sql("""
            SELECT store_id, SUM(total_amount) as total_sales
            FROM clean_sales
            GROUP BY store_id
        """, conn)
        st.bar_chart(store_data.set_index("store_id"))

        # 3️⃣ SALES BY CATEGORY
        st.subheader("🛍️ Sales by Category")
        category_data = pd.read_sql("""
            SELECT category, SUM(total_amount) as total_sales
            FROM clean_sales
            GROUP BY category
        """, conn)
        st.bar_chart(category_data.set_index("category"))

        # =========================
        # 🔍 FILTER SECTION
        # =========================
        st.markdown("## 🔍 Filter Data")

        col1, col2 = st.columns(2)

        with col1:
            selected_store = st.multiselect(
                "Select Store", clean_df["store_id"].unique()
            )

        with col2:
            selected_category = st.multiselect(
                "Select Category", clean_df["category"].unique()
            )

        filtered_df = clean_df.copy()

        if selected_store:
            filtered_df = filtered_df[filtered_df["store_id"].isin(selected_store)]

        if selected_category:
            filtered_df = filtered_df[filtered_df["category"].isin(selected_category)]

        st.dataframe(filtered_df)

        # =========================
        # 📜 LOGS
        # =========================
        st.markdown("## 📜 ETL Logs")

        logs = pd.read_sql("SELECT * FROM etl_logs", conn)
        st.dataframe(logs)

        conn.close()