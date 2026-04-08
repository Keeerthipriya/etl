import sqlite3

DB_NAME = "retail.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # RAW TABLE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_sales (
        order_id TEXT,
        order_date TEXT,
        store_id TEXT,
        product_id TEXT,
        category TEXT,
        quantity INTEGER,
        unit_price REAL,
        file_name TEXT
    )
    """)

    # CLEAN TABLE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clean_sales (
        order_id TEXT,
        order_date DATE,
        store_id TEXT,
        product_id TEXT,
        category TEXT,
        quantity INTEGER,
        unit_price REAL,
        total_amount REAL,
        order_month TEXT,
        order_day TEXT,
        file_name TEXT
    )
    """)

    # AGG TABLE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS agg_sales (
        store_id TEXT,
        category TEXT,
        order_month TEXT,
        total_sales REAL
    )
    """)

    # VIEW
    cursor.execute("""
    CREATE VIEW IF NOT EXISTS sales_summary_view AS
    SELECT order_month, SUM(total_amount) as total_sales
    FROM clean_sales
    GROUP BY order_month
    """)

    conn.commit()
    conn.close()