import pandas as pd
import logging 
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os 

load_dotenv()
logger = logging.getLogger(__name__)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def get_engine() -> Engine: 
    url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(url)

def create_table_if_not_exists(engine: Engine) -> None:
    """
    Creates the target table if it doesn't already exist.
    
    Why here and not in a separate migrations file?
    For a project at this scale, keeping it here is acceptable.
    In production you'd use a migration tool like Alemic or Flyway 
    so that schema changes are versioned and auditable
    """
    ddl = """
        CREATE TABLE IF NOT EXISTS sales_transactions (
        transaction_id   VARCHAR(20)    PRIMARY KEY,
        customer_id      INTEGER        NOT NULL,
        product_name     VARCHAR(100)   NOT NULL,
        quantity         INTEGER        NOT NULL,
        unit_price       NUMERIC(10,2)  NOT NULL,
        transaction_date DATE           NOT NULL,
        region           VARCHAR(50),
        status           VARCHAR(20),
        total_sale       NUMERIC(10,2),
        loaded_at        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS rejected_rows (
        id               SERIAL         PRIMARY KEY,
        transaction_id   VARCHAR(20),
        raw_data         TEXT,
        rejection_reason TEXT           NOT NULL,
        rejected_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
        );
        """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
        logger.info("Tables verified/created successfully")

def load_clean(df: pd.DataFrame, engine: Engine) -> int:
    """Upserts clean rows into sales_transactions
    
    Return the number of rows processed
    
    Why upsert instead of append or replace?
    - Append creates duplicates if the pipeline reruns
    - Replace truncates everything and reloads - expensive and risky at scale
    - Upsert keeps the database in sync with the source without either problem
    """
    if df.empty:
        logger.warning("Clean DataFrame is empty - nothing to load")
        return 0
    
    # Build upsert statement
    # We construct it explicitly so we know exactly what SQL is executing
    # Never use df.to_sql() for production upserts - it only does append or replace
    upsert_sql = """
    INSERT INTO sales_transactions (
        transaction_id, customer_id, product_name,
        quantity, unit_price, transaction_date,
        region, status, total_sale
    ) VALUES (
        :transaction_id, :customer_id, :product_name,
        :quantity, :unit_price, :transaction_date,
        :region, :status, :total_sale
    )
    ON CONFLICT (transaction_id) DO UPDATE SET 
        customer_id      = EXCLUDED.customer_id,
        product_name     = EXCLUDED.product_name,
        quantity         = EXCLUDED.quantity,
        unit_price       = EXCLUDED.unit_price,
        transaction_date = EXCLUDED.transaction_date,
        region           = EXCLUDED.region,
        status           = EXCLUDED.status,
        total_sale       = EXCLUDED.total_sale;
    """
    rows = df.to_dict(orient="records")

    with engine.connect() as conn:
        conn.execute(text(upsert_sql), rows)
        conn.commit()

    logger.info(f"Upserted {len(rows)} rows into sales_transactions")
    return len(rows)

def load_rejected(df: pd.DataFrame, engine: Engine) -> int:
    """Inserts rejected rows into the rejected_rows table
    
    Why store rejected rows in the database rather than just logging them?
    Because logs are ephemeral and hard to query. A rejected_rows table means 
    a data engineer can run:  
        SELECT rejection_reason, COUNT(*) 
        FROM rejected_rows 
        GROUP BY rejection_reason
    and immediately understand what's failing and why.
    This is the difference between observable and unobservable pipelines.
    """
    if df.empty:
        logger.info("No rejected rows to load")
        return 0

    rejected_sql = """
    INSERT INTO rejected_rows (transaction_id, raw_data, rejection_reason) 
    VALUES (:transaction_id, :raw_data, :rejection_reason)
    """

    rows = [
        {
        "transaction_id" : str(row.get("transaction_id", "UNKNOWN")),
        "raw_data" : str(row.to_dict()),
        "rejection_reason" : row['rejection_reason']
        }
        for _, row in df.iterrows()
    ]

    with engine.connect() as conn:
        conn.execute(text(rejected_sql), rows)
        conn.commit()

    logger.info(f"Stored {len(rows)} rejected rows in rejected_table")
    return len(rows)