import streamlit as st
import pandas as pd
from utils import read_file
from etl import transform_data, aggregate_data, load_to_db
from db import engine, create_tables
from logger import log_etl, create_log_table

# -----------------------------
# EXPECTED SCHEMA
# -----------------------------
EXPECTED_COLUMNS = {
    "order_id": "object",
    "order_date": "object",
    "store_id": "object",
    "product_id": "object",
    "category": "object",
    "quantity": "int64",
    "unit_price": "float64"
}

# -----------------------------
# VALIDATION FUNCTION
# -----------------------------
def validate_raw_data(df):
    errors = []

    # 1. Check missing columns
    missing_cols = set(EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing columns: {list(missing_cols)}")

    # 2. Check extra columns
    extra_cols = set(df.columns) - set(EXPECTED_COLUMNS.keys())
    if extra_cols:
        errors.append(f"Unexpected columns: {list(extra_cols)}")

    # 3. Check datatypes (basic validation)
    for col, expected_type in EXPECTED_COLUMNS.items():
        if col in df.columns:
            if col in ["quantity"]:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    errors.append(f"{col} should be numeric")
            if col in ["unit_price"]:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    errors.append(f"{col} should be float")

    return errors


# PAGE CONFIG
st.set_page_config(page_title="Retail ETL", layout="wide")

# SIDEBAR
st.sidebar.title("⚙️ Tech Stack")
st.sidebar.markdown("""
- 🐍 Python (Pandas)
- 🗄️ SQLite + SQLAlchemy ORM
- 📊 Streamlit UI
- 🔄 ETL Pipeline
""")

# TITLE
st.markdown("""
<h1 style='text-align: center; color: #1f77b4;'>
🛒 Retail Sales ETL & Analytics Platform
</h1>
""", unsafe_allow_html=True)

create_tables()
create_log_table()
uploaded_file = st.file_uploader("📂 Upload File", type=["csv", "json", "xlsx"])

if uploaded_file:

    file_name = uploaded_file.name

    try:
        df = read_file(uploaded_file)
    except Exception as e:
        st.error(f"❌ Error reading file: {e}")
        st.stop()

    # -----------------------------
    # VALIDATE RAW DATA
    # -----------------------------
    validation_errors = validate_raw_data(df)

    if validation_errors:
        st.error("❌ Data Validation Failed")
        for err in validation_errors:
            st.error(err)
        st.stop()

    st.success("✅ File validation successful!")

    # -----------------------------
    # RAW DATA
    # -----------------------------
    st.markdown("## 🔵 Raw Data")
    st.dataframe(df)

    # -----------------------------
    # TRANSFORM
    # -----------------------------
    clean_df, error_log = transform_data(df)

    st.markdown("## 🔵 Cleaned Data")
    st.dataframe(clean_df)

    # DATA QUALITY LOGS
    st.markdown("## ⚠️ Data Quality Logs")

    if error_log:
        for log in error_log:
            st.warning(log)
    else:
        st.success("No issues found ✅")

    # -----------------------------
    # AGGREGATION
    # -----------------------------
    agg_df = aggregate_data(clean_df)

    st.markdown("## 🔵 Aggregated Data")
    st.dataframe(agg_df)

    # -----------------------------
    # FILTERS (ALWAYS WORKING)
    # -----------------------------
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

    # -----------------------------
    # LOAD BUTTON
    # -----------------------------
    if st.button("🚀 Load to Database"):

        load_to_db(df, clean_df, agg_df, file_name)
        log_etl(file_name, len(df), len(clean_df), len(agg_df))

        st.success("✅ Data Loaded Successfully!")

        # =========================
        # 📊 VISUALIZATION
        # =========================
        st.markdown("## 📊 Sales Analytics")

        # MONTHLY
        st.subheader("📅 Sales by Month")
        monthly = pd.read_sql("SELECT * FROM sales_summary_view", engine)
        st.line_chart(monthly.set_index("order_month"))

        # STORE
        st.subheader("🏬 Sales by Store")
        store_data = pd.read_sql("""
            SELECT store_id, SUM(total_amount) as total_sales
            FROM clean_sales
            GROUP BY store_id
        """, engine)
        st.bar_chart(store_data.set_index("store_id"))

        # CATEGORY
        st.subheader("🛍️ Sales by Category")
        category_data = pd.read_sql("""
            SELECT category, SUM(total_amount) as total_sales
            FROM clean_sales
            GROUP BY category
        """, engine)
        st.bar_chart(category_data.set_index("category"))

        # -----------------------------
        # LOGS TABLE
        # -----------------------------
        st.markdown("## 📜 ETL Logs")
        logs = pd.read_sql("SELECT * FROM etl_logs", engine)
        st.dataframe(logs)