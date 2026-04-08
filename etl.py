import pandas as pd
from db import get_connection

def transform_data(df):

    # Remove negative quantity
    df = df[df["quantity"] > 0]

    # Handle missing price
    df["unit_price"] = df["unit_price"].fillna(df["unit_price"].mean())

    # Fix date format
    df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')

    df = df.dropna(subset=["order_date"])

    # Derived columns
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["order_month"] = df["order_date"].dt.to_period("M").astype(str)
    df["order_day"] = df["order_date"].dt.day_name()

    return df


def aggregate_data(df):
    agg = df.groupby(["store_id", "category", "order_month"])["total_amount"].sum().reset_index()
    agg.rename(columns={"total_amount": "total_sales"}, inplace=True)
    return agg


def load_to_db(raw_df, clean_df, agg_df, file_name):
    conn = get_connection()

    raw_df["file_name"] = file_name
    clean_df["file_name"] = file_name

    raw_df.to_sql("raw_sales", conn, if_exists="append", index=False)
    clean_df.to_sql("clean_sales", conn, if_exists="append", index=False)
    agg_df.to_sql("agg_sales", conn, if_exists="append", index=False)

    conn.close()