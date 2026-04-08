import pandas as pd
from datetime import datetime


def transform_data(df):

    error_log = []

    # -----------------------------
    # ORDER ID
    # -----------------------------
    before = len(df)
    df = df.dropna(subset=["order_id"])
    df = df.drop_duplicates(subset=["order_id"])
    after = len(df)
    error_log.append(f"Order ID duplicates/null removed: {before - after}")

    # -----------------------------
    # STORE ID
    # -----------------------------
    before = len(df)
    df = df.dropna(subset=["store_id"])
    after = len(df)
    error_log.append(f"Store ID null removed: {before - after}")

    # -----------------------------
    # PRODUCT ID
    # -----------------------------
    before = len(df)
    df = df.dropna(subset=["product_id"])
    after = len(df)
    error_log.append(f"Product ID null removed: {before - after}")

    # -----------------------------
    # CATEGORY
    # -----------------------------
    null_category = df["category"].isna().sum()
    df["category"] = df["category"].fillna("N/A")
    error_log.append(f"Category null replaced with N/A: {null_category}")

    # -----------------------------
    # QUANTITY
    # -----------------------------
    neg_qty = (df["quantity"] < 0).sum()
    df["quantity"] = df["quantity"].apply(lambda x: max(x, 0))
    error_log.append(f"Negative quantity converted to 0: {neg_qty}")

    # -----------------------------
    # UNIT PRICE
    # -----------------------------
    neg_price = (df["unit_price"] < 0).sum()
    df["unit_price"] = df["unit_price"].apply(lambda x: max(x, 0) if pd.notnull(x) else x)

    null_price = df["unit_price"].isna().sum()
    df["unit_price"] = df["unit_price"].fillna(df["unit_price"].mean())

    error_log.append(f"Negative unit_price set to 0: {neg_price}")
    error_log.append(f"Null unit_price filled with mean: {null_price}")

    # -----------------------------
    # ORDER DATE
    # -----------------------------
    df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')

    null_dates = df["order_date"].isna().sum()
    df["order_date"] = df["order_date"].fillna(pd.to_datetime("2024-01-01"))
    error_log.append(f"Null dates replaced with default: {null_dates}")

    today = pd.to_datetime(datetime.today())
    future_dates = (df["order_date"] > today).sum()

    df = df[df["order_date"] <= today]
    error_log.append(f"Future dates removed: {future_dates}")

    # -----------------------------
    # DERIVED COLUMNS
    # -----------------------------
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["order_month"] = df["order_date"].dt.to_period("M").astype(str)
    df["order_day"] = df["order_date"].dt.day_name()

    return df, error_log


def aggregate_data(df):
    agg = df.groupby(
        ["store_id", "category", "order_month"]
    )["total_amount"].sum().reset_index()

    agg.rename(columns={"total_amount": "total_sales"}, inplace=True)
    return agg


from db import SessionLocal, RawSales, CleanSales, AggSales

def load_to_db(raw_df, clean_df, agg_df, file_name):

    session = SessionLocal()

    # RAW DATA
    for _, row in raw_df.iterrows():
        record = RawSales(
            order_id=row["order_id"],
            order_date=str(row["order_date"]),
            store_id=row["store_id"],
            product_id=row["product_id"],
            category=row["category"],
            quantity=int(row["quantity"]),
            unit_price=float(row["unit_price"]),
            file_name=file_name
        )
        session.merge(record)  # prevents duplicates

    # CLEAN DATA
    for _, row in clean_df.iterrows():
        record = CleanSales(
            order_id=row["order_id"],
            order_date=row["order_date"],
            store_id=row["store_id"],
            product_id=row["product_id"],
            category=row["category"],
            quantity=int(row["quantity"]),
            unit_price=float(row["unit_price"]),
            total_amount=float(row["total_amount"]),
            order_month=row["order_month"],
            order_day=row["order_day"],
            file_name=file_name
        )
        session.merge(record)

    # AGG DATA
    for _, row in agg_df.iterrows():
        record = AggSales(
            store_id=row["store_id"],
            category=row["category"],
            order_month=row["order_month"],
            total_sales=float(row["total_sales"])
        )
        session.add(record)

    session.commit()
    session.close()