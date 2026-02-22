import pandas as pd
import logging 

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = {
    "transaction_id",
    "customer_id",
    "product_name",
    "quantity",
    "unit_price",
    "transaction_date",
    "region",
    "status"
}

VALID_STATUSES = {"completed", "pending", "cancelled", "unknown"}

def validate_structure(df: pd.DataFrame) -> None:
    """
    Validates the raw DataFrame before any transformation occurs.
    Checks shape and schema 

    Raises ValueError immediately on any structural violation.
    There is no partial failure here - if structure is wrong,
    the entire DataFrame is unusable and the pipeline must halt.
    """
    logger.info("Running structural validation")
    # column check
    actual_columns = set(df.columns.str.strip().str.lower())
    missing_columns = EXPECTED_COLUMNS - actual_columns

    if missing_columns:
        raise ValueError(f"Structural validation failed - missing columns; {missing_columns}")
    
    # row count check
    if len(df) == 0:
        raise ValueError(f"Structural validation failed - DataFrame has no rows")
    
    # Duplicate primary keys in source
    # If transaction_id is not unique in the source file itself,
    # that's a source system problem we should know about immediately
    duplicate_count = df["transaction_id"].duplicated().sum()
    if duplicate_count > 0:
        logger.warning(f"Source file contains {duplicate_count} duplicate transaction_ids")

def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates individual rows against business rules after transformation
    Separates clean rows from rejected once

    Returns (clean_df, rejected_df)
    Never drops rows silently - rejected rows are preserved with reasons
    """
    logger.info(f"Running business rule validation")

    clean_rows = []
    rejected_rows = []

    for _, row in df.iterrows():
        reasons = _check_row(row)
        if reasons:
            rejected_row = row.to_dict()
            # join all violations - post-transform we want ALL reasons,
            # not just the first, becuase the data engineer fixing the
            # source needs the full picture
            rejected_row["rejection_reason"] = " | ".join(reasons)
            rejected_rows.append(rejected_row)

        else:
            clean_rows.append(row.to_dict())

    clean_df = pd.DataFrame(clean_rows) if clean_rows else pd.DataFrame()
    rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else pd.DataFrame()

    logger.info(
        f"Row validation complete - "
        f"clean: {len(clean_df)}, rejected: {len(rejected_df)}"
    )

    rejected_rows
    if not rejected_df.empty:
        logger.warning(
            "Rejected rows:\n",rejected_df[['transaction_id', "rejection_reason"]]
        )

    return clean_df, rejected_df

def _check_row(row: pd.Series) -> list[str]:
    """
    Checks a single row against all business rules
    Returns a list of all violations found - empty list means valid
    
    Post-transformation validation collects ALL violations per row
    because t this stage we want the full picture for debuffing
    """
    reasons = []
    if pd.isna(row['transaction_id']) or str(row['transaction_id']).strip() == "":
            reasons.append("missing_transaction_id")

    if pd.isna(row["transaction_date"]):
        reasons.append("unparseable or missing transaction date")

    if pd.isna(row["unit_price"]) or row["unit_price"] <= 0:
        reasons.append("unit_price must be a positive number")

    if pd.isna(row["total_sale"]):
        reasons.append("total_sale could not be computed")

    if row["status"] not in VALID_STATUSES:
        reasons.append(f"invalid status value: ",row["status"])

    return reasons