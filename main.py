import logging
import sys
import uuid 
from datetime import datetime
from pathlib import Path 

from src.ingest import ingest_csv
from src.validate import validate_rows, validate_structure
from src.transform import transform
from src.load import get_engine, create_table_if_not_exists, load_clean, load_rejected


# LOGGING SETUP
# configure once at the top level - all modules inherit this configurtion
# because they use logging.getLogger(__name__) which connects to this root setup

def setup_logging(run_id: str) -> None:
    """
    Configures logging to write to both console and a log file simultaneously.
    Every log line includes the run_id so you can grep a specific pipeline run
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = f"%(asctime)s | run={run_id} | %(levelname)s | %(name)s | %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            #console hanlder - see logs in real time
            logging.StreamHandler(sys.stdout),
            #file handler - permanent record of every run
            logging.FileHandler(log_dir / f"pipeline_{run_id}.log")
        ]
    )

logger = logging.getLogger(__name__)

    # ==============PIPELINE===============
def run_pipeline(filepath: str) -> None:
    """
    Orchestrates the full CSV -> PostgreSQL pipeline

    Stage order:
        1. Ingest       — read file from disk
        2. Validate     — check structure before touching data
        3. Transform    — clean, cast, derive
        4. Validate     — check rows against business rules
        5. Load         — upsert clean rows, store rejected rows   

    Each stage is wrapped in explicit error handling so failures
    are logged with context before the pipeline exits     
    """
    # Generate a unique ID for this pipeline run
    # Every log line, every rejected row, every metric ties back to this ID
    # Without it, debugging a failure across multiple runs is guesswork
    run_id = str(uuid.uuid4())[:8]
    setup_logging(run_id)

    start_time = datetime.now()
    logger.info(f"Pipeline started - file: {filepath}")

    engine = get_engine()

    # =======STAGE 0: Schema setup============
    try:
        create_table_if_not_exists(engine=engine)
    
    except Exception as e:
        logger.error(f"Failed to verify/create tables: {e}")
        sys.exit(1)

    # ===========STAGE 1: INGESTION============
    try:
        raw_data = ingest_csv(filepath=filepath)
    
    except FileNotFoundError as e:
        # File missing - could be a delivery delay, worth retrying
        logger.error(f"Ingestion failed - file not found: {e}")
        logger.error(f"Check upstream file delivery - pipeline halted")
        sys.exit(1) 

    except RuntimeError as e:
        # File unreadable - encoding or corruption issue
        logger.error(f"Ingestion failed - file unreadable: {e}")
        sys.exit(1)

    # ===========STAGE 2: Structural validation==========
    try:
        validate_structure(raw_data)

    except ValueError as e:
        # structure is broken - transformation cannot proceed
        logger.error(f'Structural validation failed: {e}')
        logger.error(f"Pipeline halted - fix source data before rerunning")
        sys.exit(1)

    # ============STAGE 3: TRANSFORMATION==============
    try:
        transformed_data = transform(raw_data)

    except Exception as e:
        # unexpected transformation error - surface it fully
        logger.error(f"Transformation failed unexpectedly: {e}", exc_info=True)
        sys.exit(1)

    # =========STAGE 4: BUSINESS RULE VALIDATION=========
    clean_data, rejected_data = validate_rows(transformed_data)

    if clean_data.empty:
        logger.error(f"No clean rows after validation - pipeline halted")
        logger.error(f"All rows were rejected - investigte rejection reasons")
        sys.exit(1)

    # ======STAGE 5: LOAD========
    try:
        clean_count = load_clean(clean_data, engine)
        rejected_count = load_rejected(rejected_data, engine)

    except Exception as e:
        logger.error(f"Load stage failed: {e}", exc_info=True)
        sys.exit(1)

    # PIPELINE SUMMARY
    duration = (datetime.now() - start_time).total_seconds()

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Run ID        : {run_id}")
    logger.info(f"  Duration      : {duration:.2f}s")
    logger.info(f"  Input rows    : {len(raw_data)}")
    logger.info(f"  Clean loaded  : {clean_count}")
    logger.info(f"  Rejected      : {rejected_count}")
    logger.info(f"  Rejection rate: {rejected_count/len(raw_data)*100:.1f}%")
    logger.info("=" * 60)

if __name__ == "__main__":
    run_pipeline("data/raw/sales_data.csv")