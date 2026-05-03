# sales_etl_functions.py
# Each function = one step in the pipeline
# Written to be readable for beginners — every section is explained

import os
import json
import shutil
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Import our config and logger
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from etl_pipeline.src.utils.config import (
    CONNECTION_STRING, RAW_DATA_PATH,
    PROCESSED_DATA_PATH, INVALID_SALES_LOG
)
from etl_pipeline.src.utils.logger import get_logger

logger = get_logger("sales_etl")


# ─────────────────────────────────────────────
# HELPER: Get database connection
# ─────────────────────────────────────────────
def get_engine():
    """
    Creates a connection to SQL Server.
    Called at the start of each function that needs the DB.
    """
    try:
        engine = create_engine(CONNECTION_STRING)
        logger.info("Database connection established.")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


# ─────────────────────────────────────────────
# STEP 1: EXTRACT — Read JSON → raw.sales_raw
# ─────────────────────────────────────────────
def extract_to_raw():
    """
    Reads ALL JSON files from data/raw/ and inserts them into
    raw.sales_raw exactly as they are — no cleaning, no changes.
    Think of this as a landing zone / safety net.
    If something breaks later, the raw data is always here.
    """
    logger.info("=" * 60)
    logger.info("STEP 1: EXTRACT — Loading JSON files into raw layer")
    logger.info("=" * 60)

    engine = get_engine()

    # Find all JSON files in the raw folder
    json_files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".json")]

    if not json_files:
        logger.warning("No JSON files found in data/raw/ — nothing to process.")
        return 0

    total_inserted = 0

    for file_name in json_files:
        file_path = os.path.join(RAW_DATA_PATH, file_name)
        logger.info(f"Processing file: {file_name}")

        try:
            # Read the JSON file
            with open(file_path, "r") as f:
                records = json.load(f)

            if not records:
                logger.warning(f"{file_name} is empty — skipping.")
                continue

            # Convert to DataFrame
            df = pd.DataFrame(records)

            # Add ingestion timestamp — this is used for incremental loading
            df["insert_date"] = datetime.now()

            # Keep only columns that match raw.sales_raw table
            columns_to_keep = [
                "transaction_id", "customer_id", "product_name",
                "category", "price", "quantity", "discount",
                "region", "transaction_date", "insert_date"
            ]

            # Only keep columns that exist in both the data and our list
            df = df[[col for col in columns_to_keep if col in df.columns]]

            # Insert into raw.sales_raw
            # if_exists="append" means: add to existing rows, don't replace
            df.to_sql(
                name="sales_raw",
                schema="raw",
                con=engine,
                if_exists="append",
                index=False
            )

            total_inserted += len(df)
            logger.info(f"✅ Inserted {len(df)} records from {file_name}")

        except Exception as e:
            logger.error(f"❌ Failed to process {file_name}: {e}")
            continue

    logger.info(f"EXTRACT complete. Total records inserted: {total_inserted}")
    return total_inserted


# ─────────────────────────────────────────────
# STEP 2: GET WATERMARK — Incremental load check
# ─────────────────────────────────────────────
def get_watermark():
    """
    Reads the last successful run timestamp from silver.etl_metadata.
    Only records inserted AFTER this timestamp will be processed.

    First run: returns 1900-01-01 (loads everything)
    Every run after: returns the previous run's timestamp
    """
    logger.info("STEP 2: Reading watermark from silver.etl_metadata")

    engine = get_engine()

    query = """
        SELECT last_insert_date
        FROM silver.etl_metadata
        WHERE pipeline_name = 'sales_pipeline'
    """

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()

    watermark = result[0]
    logger.info(f"Watermark (last run): {watermark}")
    return watermark


# ─────────────────────────────────────────────
# STEP 3: VALIDATE — Catch all bad records
# ─────────────────────────────────────────────
def validate_records(df):
    """
    Checks every record against 5 data quality rules.
    Returns two DataFrames: valid_df and invalid_df.

    Rules:
    1. transaction_id must not be NULL
    2. customer_id must not be NULL
    3. price must not be NULL
    4. quantity must be > 0
    5. discount must be between 0 and 1 (e.g. 0.10 = 10%)
    """
    logger.info("STEP 3: Validating records — checking 5 DQ rules")

    # We'll store error messages for each bad record
    errors = []

    for idx, row in df.iterrows():
        row_errors = []

        # Rule 1: transaction_id cannot be null
        if pd.isnull(row.get("transaction_id")):
            row_errors.append("NULL transaction_id")

        # Rule 2: customer_id cannot be null
        if pd.isnull(row.get("customer_id")):
            row_errors.append("NULL customer_id")

        # Rule 3: price cannot be null
        if pd.isnull(row.get("price")):
            row_errors.append("NULL price")

        # Rule 4: quantity must be positive
        qty = row.get("quantity")
        if pd.isnull(qty) or int(qty) <= 0:
            row_errors.append(f"Invalid quantity: {qty}")

        # Rule 5: discount must be 0.0 to 1.0
        disc = row.get("discount")
        if not pd.isnull(disc):
            if float(disc) < 0 or float(disc) > 1:
                row_errors.append(f"Invalid discount: {disc}")

        errors.append("; ".join(row_errors))  # e.g. "NULL customer_id; Invalid quantity: -2"

    df["dq_errors"] = errors

    # Split into valid and invalid
    valid_df   = df[df["dq_errors"] == ""].copy()
    invalid_df = df[df["dq_errors"] != ""].copy()

    logger.info(f"✅ Valid records:   {len(valid_df)}")
    logger.info(f"❌ Invalid records: {len(invalid_df)}")

    return valid_df, invalid_df


# ─────────────────────────────────────────────
# STEP 4: TRANSFORM — Clean & calculate
# ─────────────────────────────────────────────
def transform(valid_df):
    """
    Cleans valid records and adds calculated fields.
    All transformations use vectorized Pandas (fast, no loops).

    Transformations:
    - Normalize customer_id to uppercase
    - Parse transaction_date to proper date type
    - Fill missing discounts with 0
    - Calculate total_value = price * quantity * (1 - discount)
    - Add update_date audit column
    """
    logger.info("STEP 4: Transforming valid records")

    df = valid_df.copy()

    # 1. Normalize customer_id — remove whitespace, uppercase
    df["customer_id"] = df["customer_id"].str.strip().str.upper()

    # 2. Normalize product_name — remove leading/trailing spaces
    df["product_name"] = df["product_name"].str.strip()

    # 3. Parse dates — handles multiple date formats automatically
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], infer_datetime_format=True
    ).dt.date

    # 4. Fill missing discounts with 0
    df["discount"] = df["discount"].fillna(0)

    # 5. Calculate total_value (the key business metric)
    df["total_value"] = (
        df["price"] * df["quantity"] * (1 - df["discount"])
    ).round(2)

    # 6. Add audit timestamp
    df["update_date"] = datetime.now()

    # Keep only silver layer columns
    silver_columns = [
        "transaction_id", "customer_id", "product_name", "category",
        "price", "quantity", "discount", "region",
        "transaction_date", "total_value", "update_date"
    ]
    df = df[silver_columns]

    logger.info(f"✅ Transform complete. {len(df)} records ready to load.")
    return df


# ─────────────────────────────────────────────
# STEP 5: LOAD — UPSERT into silver.sales
# ─────────────────────────────────────────────
def load_to_silver(df):
    """
    Loads clean records into silver.sales using UPSERT logic.

    UPSERT = UPDATE if exists, INSERT if new.
    This means:
    - Running the pipeline twice won't create duplicates
    - Re-processing fixed records will update them
    - Safe to re-run anytime (idempotent)
    """
    logger.info("STEP 5: Loading to silver.sales (UPSERT)")

    if df.empty:
        logger.warning("No valid records to load.")
        return 0

    engine = get_engine()

    # We use a staging table approach for UPSERT:
    # 1. Load new data into a temp staging table
    # 2. Run MERGE to upsert from staging → silver.sales
    # 3. Drop staging table

    with engine.begin() as conn:

        # Step A: Create a temporary staging table
        conn.execute(text("DROP TABLE IF EXISTS silver.sales_staging"))
        conn.execute(text("""
            CREATE TABLE silver.sales_staging (
                transaction_id   VARCHAR(100),
                customer_id      VARCHAR(100),
                product_name     VARCHAR(200),
                category         VARCHAR(100),
                price            DECIMAL(10,2),
                quantity         INT,
                discount         DECIMAL(5,4),
                region           VARCHAR(100),
                transaction_date DATE,
                total_value      DECIMAL(12,2),
                update_date      DATETIME
            )
        """))

    # Step B: Load the DataFrame into staging
    df.to_sql(
        name="sales_staging",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False
    )

    # Step C: MERGE staging → silver.sales
    merge_query = """
        MERGE silver.sales AS target
        USING silver.sales_staging AS source
        ON target.transaction_id = source.transaction_id

        -- Record exists → UPDATE it
        WHEN MATCHED THEN UPDATE SET
            target.customer_id      = source.customer_id,
            target.product_name     = source.product_name,
            target.category         = source.category,
            target.price            = source.price,
            target.quantity         = source.quantity,
            target.discount         = source.discount,
            target.region           = source.region,
            target.transaction_date = source.transaction_date,
            target.total_value      = source.total_value,
            target.update_date      = source.update_date

        -- Record is new → INSERT it
        WHEN NOT MATCHED THEN INSERT (
            transaction_id, customer_id, product_name, category,
            price, quantity, discount, region,
            transaction_date, total_value, update_date
        )
        VALUES (
            source.transaction_id, source.customer_id, source.product_name,
            source.category, source.price, source.quantity, source.discount,
            source.region, source.transaction_date, source.total_value,
            source.update_date
        );
    """

    with engine.begin() as conn:
        conn.execute(text(merge_query))
        # Clean up staging table
        conn.execute(text("DROP TABLE IF EXISTS silver.sales_staging"))

    logger.info(f"✅ UPSERT complete. {len(df)} records merged into silver.sales.")
    return len(df)


# ─────────────────────────────────────────────
# STEP 6: LOG INVALID RECORDS
# ─────────────────────────────────────────────
def log_invalid_records(invalid_df):
    """
    Saves all bad records to logs/invalid_sales.json
    Each record includes a dq_errors field explaining WHY it failed.
    This is your audit trail.
    """
    if invalid_df.empty:
        logger.info("No invalid records to log.")
        return

    os.makedirs("logs", exist_ok=True)

    # Convert to JSON-friendly format
    records = invalid_df.copy()
    records["transaction_date"] = records["transaction_date"].astype(str)

    records.to_json(
        INVALID_SALES_LOG,
        orient="records",
        indent=2
    )

    logger.info(f"❌ {len(invalid_df)} invalid records saved to {INVALID_SALES_LOG}")


# ─────────────────────────────────────────────
# STEP 7: UPDATE WATERMARK
# ─────────────────────────────────────────────
def update_watermark(new_timestamp):
    """
    Updates the watermark to RIGHT NOW.
    Next pipeline run will only process records newer than this timestamp.
    This is how incremental loading works.
    """
    logger.info(f"STEP 7: Updating watermark to {new_timestamp}")

    engine = get_engine()

    query = """
        UPDATE silver.etl_metadata
        SET last_insert_date = :new_ts
        WHERE pipeline_name = 'sales_pipeline'
    """

    with engine.begin() as conn:
        conn.execute(text(query), {"new_ts": new_timestamp})

    logger.info("✅ Watermark updated successfully.")


# ─────────────────────────────────────────────
# STEP 8: MOVE PROCESSED FILES
# ─────────────────────────────────────────────
def move_processed_files():
    """
    Moves successfully processed JSON files from
    data/raw/ → data/processed/
    So they won't be picked up again on the next run.
    """
    logger.info("STEP 8: Moving processed files to data/processed/")

    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

    json_files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".json")]

    for file_name in json_files:
        src  = os.path.join(RAW_DATA_PATH, file_name)
        dest = os.path.join(PROCESSED_DATA_PATH, file_name)
        shutil.move(src, dest)
        logger.info(f"✅ Moved: {file_name} → data/processed/")