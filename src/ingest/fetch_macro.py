from fredapi import Fred
import pandas as pd
import os
from dotenv import load_dotenv

# Load .env to get FRED_API_KEY
load_dotenv()
API_KEY = os.getenv("FRED_API_KEY")

if not API_KEY or API_KEY == "your_api_key_here":
    raise ValueError("Missing FRED_API_KEY. Please ensure you update the .env file with your actual key.")

fred = Fred(api_key=API_KEY)

# 6 Macro Indicators mappings
MACRO_SERIES = {
    "DFF": "Federal Funds Rate",
    "CPIAUCSL": "CPI Inflation",
    "VIXCLS": "VIX Volatility Index",
    "T10Y2Y": "10Y-2Y Treasury Spread",
    "BAMLH0A0HYM2": "High Yield Credit Spread",
    "USREC": "NBER Recession Indicator"
}

START_DATE = "2020-01-01"
RAW_DIR = "data/raw/macro"

def fetch_and_save_macro():
    os.makedirs(RAW_DIR, exist_ok=True)
    print(f"Fetching {len(MACRO_SERIES)} macro indicators from FRED...")

    for series_id, name in MACRO_SERIES.items():
        print(f"  -> Downloading {name} ({series_id})...")
        try:
            # Fetch series
            data = fred.get_series(series_id, observation_start=START_DATE)
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=['value'])
            df.index.name = 'date'
            df.reset_index(inplace=True)
            
            # Add metadata columns
            df['series_id'] = series_id
            df['series_name'] = name
            
            # Handle standard missing values if any
            df.dropna(subset=['value'], inplace=True)
            
            output_file = os.path.join(RAW_DIR, f"{series_id}.csv")
            df.to_csv(output_file, index=False)
            print(f"     [OK] Saved {len(df)} rows to {output_file}")
            
        except Exception as e:
            print(f"     [ERROR] Failed to fetch {series_id}: {e}")

if __name__ == "__main__":
    fetch_and_save_macro()
    print("Macro indicator ingestion complete!")
