from sqlalchemy import text
import pandas as pd
from src.load import get_engine

engine = get_engine()

with engine.connect() as conn:
    # check total row count
    result = conn.execute(text("SELECT COUNT(*) FROM sales_transactions;"))
    print(f"Total rows in sales_transactions: {result.scalar()}")

    df_clean = pd.read_sql(
        "SELECT * FROM sales_transactions LIMIT 5;", conn
    )
    print(f"\nClean Dataset Sample Data:")
    print(df_clean)

    # checks for any rejected rows
    rejected_result = conn.execute(text("SELECT COUNT(*) FROM rejected_rows;"))
    rejected_count = rejected_result.scalar()
    print(f"\nTotal rejected rows: {rejected_count}")

    df_rejected = pd.read_sql(
        "SELECT * FROM rejected_rows LIMIT 5;", conn
    )
    print(f"\nRejected dataset sample data:")
    print(df_rejected)