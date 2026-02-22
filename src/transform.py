import pandas as pd
import logging 
from datetime import datetime

logger = logging.getLogger(__name__)

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%b %d %Y"
]

def parse_date(date_str: str) -> datetime | None:
    if pd.isna(date_str):
        return None 
    
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    logger.warning(f"Unrecognized date format: {date_str} - setting to None")
    return None 

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, casts and enriches the raw DataFrame.
    Assumes structural validation has already passed
    Returns a single transformed DataFrame
    Row-level validation happens in validate.py after this step
    """
    logger.info(f"Starting transformation - input shape: {df.shape}")

    df = df.copy()

    # type casting
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")

    # date parsing
    df["transaction_date"] = df["transaction_date"].apply(parse_date)

    # string normalization
    df["region"] = df["region"].str.strip().str.lower()
    df["product_name"] = df["product_name"].str.strip()
    df["transaction_id"] = df["transaction_id"].str.strip().str.upper()

    # status normalization
    df["status"] = df["status"].str.strip().str.lower()
    df["status"] = df["status"].fillna("unknown")

    # derived column
    df["total_sale"] = (df["quantity"] * df["unit_price"]).round(2)

    logger.info(f"Transformation complete - shape: {df.shape}")
    return df