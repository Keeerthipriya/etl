import sqlite3
from datetime import datetime

def log_etl(file_name, raw_count, clean_count, agg_count):
    conn = sqlite3.connect("retail.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl_logs (
        file_name TEXT,
        raw_count INTEGER,
        clean_count INTEGER,
        agg_count INTEGER,
        timestamp TEXT
    )
    """)

    cursor.execute("""
    INSERT INTO etl_logs VALUES (?, ?, ?, ?, ?)
    """, (file_name, raw_count, clean_count, agg_count, datetime.now()))

    conn.commit()
    conn.close()