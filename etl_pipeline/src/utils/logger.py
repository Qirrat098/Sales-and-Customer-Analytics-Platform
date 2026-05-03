# logger.py — writes logs to file AND prints to screen simultaneously
import logging
import os

os.makedirs("logs", exist_ok=True)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # prevent duplicate log lines
        # --- Write to file ---
        fh = logging.FileHandler("logs/sales_etl_logs.log")
        fh.setLevel(logging.INFO)

        # --- Print to VS Code terminal ---
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger