from sqlalchemy import create_engine, Column, String, Integer, Float, Date, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# DATABASE
DATABASE_URL = "sqlite:///retail.db"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

# -----------------------------
# TABLES
# -----------------------------

class RawSales(Base):
    __tablename__ = "raw_sales"

    order_id = Column(String, primary_key=True)
    order_date = Column(String)
    store_id = Column(String)
    product_id = Column(String)
    category = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    file_name = Column(String)


class CleanSales(Base):
    __tablename__ = "clean_sales"

    order_id = Column(String, primary_key=True)
    order_date = Column(Date)
    store_id = Column(String)
    product_id = Column(String)
    category = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    total_amount = Column(Float)
    order_month = Column(String)
    order_day = Column(String)
    file_name = Column(String)


class AggSales(Base):
    __tablename__ = "agg_sales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String)
    category = Column(String)
    order_month = Column(String)
    total_sales = Column(Float)


# -----------------------------
# CREATE TABLES + VIEW
# -----------------------------
def create_tables():
    Base.metadata.create_all(bind=engine)

    # CREATE VIEW
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE VIEW IF NOT EXISTS sales_summary_view AS
        SELECT order_month, SUM(total_amount) as total_sales
        FROM clean_sales
        GROUP BY order_month
        """))