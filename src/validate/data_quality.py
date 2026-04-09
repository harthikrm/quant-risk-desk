import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "risk_db")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def check_data_quality():
    print("Running data quality checks...")
    errors = 0
    with engine.connect() as conn:
        # Check 1: No missing prices
        missing_prices = pd.read_sql("SELECT count(*) FROM raw.prices WHERE close IS NULL;", conn).iloc[0, 0]
        if missing_prices > 0:
            print(f"[ERROR] Found {missing_prices} rows with missing close prices!")
            errors += 1
        else:
            print("[OK] No missing close prices.")

        # Check 2: Dates are recent
        max_date = pd.read_sql("SELECT max(date) FROM raw.prices;", conn).iloc[0, 0]
        print(f"[OK] Latest price date in raw schema: {max_date}")

        # Check 3: Expected ticker count (14)
        tickers = pd.read_sql("SELECT count(DISTINCT ticker) FROM raw.prices;", conn).iloc[0, 0]
        if tickers != 14:
            print(f"[ERROR] Expected 14 tickers, found {tickers}!")
            errors += 1
        else:
            print("[OK] All 14 tickers present.")
            
        # Check 4: Expected macro series count (6)
        series_count = pd.read_sql("SELECT count(DISTINCT series_id) FROM raw.macro_indicators;", conn).iloc[0, 0]
        if series_count != 6:
            print(f"[ERROR] Expected 6 macro series, found {series_count}!")
            errors += 1
        else:
            print("[OK] All 6 macro indicators present.")

    if errors > 0:
        print(f"Data validation failed with {errors} errors.")
        raise ValueError("Data Validation Gates Failed!")
    else:
        print("All Data Quality Gates Passed!")

if __name__ == "__main__":
    check_data_quality()
