# config.py — all settings live here, change once, works everywhere

# Your SQL Server connection details
DB_CONFIG = {
    "server":   r".\SQLEXPRESS",          # or .\SQLEXPRESS — use whatever SSMS connected with
    "database": "sales_analytics",
    "driver":   "ODBC Driver 17 for SQL Server"  # installed with SQL Server
}

# SQLAlchemy connection string for Python
CONNECTION_STRING = (
    f"mssql+pyodbc://{DB_CONFIG['server']}/{DB_CONFIG['database']}"
    f"?driver={DB_CONFIG['driver'].replace(' ', '+')}"
    f"&trusted_connection=yes"        # uses Windows login — no password needed!
)

# File paths
RAW_DATA_PATH       = "data/raw/"
PROCESSED_DATA_PATH = "data/processed/"
INVALID_SALES_LOG   = "logs/invalid_sales.json"
SALES_LOG_FILE      = "logs/sales_etl_logs.log"