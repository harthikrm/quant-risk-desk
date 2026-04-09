import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# 11 S&P 500 Sector ETFs + Benchmarks
TICKERS = [
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
    "SPY", "IEF", "^IRX"
]

START_DATE = "2020-01-01"
END_DATE = "2025-12-31"
RAW_DIR = "data/raw/prices"

def fetch_and_save_prices():
    os.makedirs(RAW_DIR, exist_ok=True)
    print(f"Fetching daily data for {len(TICKERS)} tickers from {START_DATE} to {END_DATE}...")
    
    for ticker in TICKERS:
        print(f"  -> Downloading {ticker}...")
        try:
            # Download using yfinance
            data = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)
            
            if data.empty:
                print(f"     [WARNING] No data found for {ticker}")
                continue
                
            # Formatting standard columns
            data.reset_index(inplace=True)
            
            # In latest yfinance, columns might be multi-index. Flatten if necessary.
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [col[0].lower().replace(' ', '_') for col in data.columns]
            else:
                data.columns = [str(col).lower().replace(' ', '_') for col in data.columns]
            
            # The column date might be capitalized
            if 'date' not in data.columns:
                data.rename(columns={'index': 'date', 'Date': 'date'}, inplace=True)
            
            # Add ticker column
            data['ticker'] = ticker.replace('^', '')
            
            # Save to CSV
            output_file = os.path.join(RAW_DIR, f"{ticker.replace('^', '')}_daily.csv")
            data.to_csv(output_file, index=False)
            print(f"     [OK] Saved to {output_file}")
            
        except Exception as e:
            print(f"     [ERROR] Failed to download {ticker}: {e}")

if __name__ == "__main__":
    fetch_and_save_prices()
    print("Market price ingestion complete!")
