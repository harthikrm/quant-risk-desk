import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Build postgres connection URL
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")  # Dynamics override for Airflow in docker
DB_NAME = os.getenv("POSTGRES_DB", "risk_db")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_prices():
    prices_files = glob.glob("data/raw/prices/*.csv")
    if not prices_files:
        print("[WARNING] No price files found.")
        return

    print("Loading prices to Postgres...")
    dfs = []
    for f in prices_files:
        df = pd.read_csv(f)
        # some data formatting sanity checks
        if 'adj_close' not in df.columns:
            if 'adj_close' in df.columns:
                df.rename(columns={'adj close': 'adj_close'}, inplace=True)
        dfs.append(df)
    
    combined_prices = pd.concat(dfs, ignore_index=True)
    
    # Send to PostgreSQL
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
    
    combined_prices.to_sql("prices", schema="raw", con=engine, if_exists="replace", index=False)
    print(f"[OK] Loaded {len(combined_prices)} price rows into raw.prices.")

def load_macro():
    macro_files = glob.glob("data/raw/macro/*.csv")
    if not macro_files:
        print("[WARNING] No macro files found.")
        return

    print("Loading macro indicators to Postgres...")
    dfs = []
    for f in macro_files:
        dfs.append(pd.read_csv(f))
        
    combined_macro = pd.concat(dfs, ignore_index=True)
    
    combined_macro.to_sql("macro_indicators", schema="raw", con=engine, if_exists="replace", index=False)
    print(f"[OK] Loaded {len(combined_macro)} macro rows into raw.macro_indicators.")

if __name__ == "__main__":
    load_prices()
    load_macro()
    print("Database loading complete!")
