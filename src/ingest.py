# Ingest module for reading CSV files

# import necessary libraries
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def ingest_csv(filepath: str) -> pd.DataFrame:
    """
    Reads a CSV from disk and returns a raw DataFrame.

    Responsible for: 
        - file existence, 
        - encoding detection, 
        - returning raw unmodified data

    Raises:
        FileNotFoundError: if the file does not exist at the given path
        RuntimeError: if the file cannot be read with any supported encoding
    """
    path = Path(filepath)

    # Fail loudly and early - never let a missing file
    # produce a confusing error three stages later
    if not path.exists():
        logger.error(f"File not found: {filepath}")
        raise FileNotFoundError(f'CSV file does not exist: {filepath}')
    
    logger.info(f"Reading file: {filepath}")

    # Try UTF-8 first, fallbaack to latin-1
    # Read data from disk in chunks of 10,000
    # Latin-1 covers virtually all single-byte encodings and never fails
    # THis is a common pattern for handling encoding issues in CSV files
    try:
        chunks = []
        for chunk in pd.read_csv(path, chunksize=10000, encoding="utf-8"):
            chunks.append(chunk)
        df = pd.concat(chunks)
        return df
        logger.info(f"File read with UTF-8 encoding")

    except UnicodeDecodeError: 
        logger.warning(f"UTF-8 failed, retrying with latin-1 encoding")

    try:
        chunks = []
        for chunk in pd.read_csv(path, chunksize=10000, encoding="latin-1"):
            chunks.append(chunk)
        df = pd.concat(chunks)
        return df 
    
    except Exception as e:
        logger.error(f"Failed to read file with any supported encoding: {e}")
        raise RuntimeError(f"Could not read file: {filepath}") from e

    
