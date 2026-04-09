import pandas as pd
import numpy as np

def sanity_check():
    print("=== QUANT RISK DESK: PHASE 5 SANITY CHECK ===")
    
    # 1. Row Counts & File Existence
    files = {
        "Overlay": "data/processed/mart_macro_overlay.csv",
        "VaR": "data/processed/mart_var_cvar.csv",
        "Risk Ratios": "data/processed/mart_risk_ratios.csv",
        "Drawdown": "data/processed/mart_drawdown.csv",
        "Correlation": "data/processed/mart_correlation.csv",
        "Sector Summary": "data/processed/mart_sector_summary.csv"
    }
    
    for name, path in files.items():
        try:
            df = pd.read_csv(path)
            print(f"[OK] {name} CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            print(f"[ERROR] Could not load {name}: {str(e)}")
            return
            
    print("\n--- Deep Metrics Validation ---")
    
    # 2. Date Ranges & Regimes
    macro = pd.read_csv(files["Overlay"])
    macro['date'] = pd.to_datetime(macro['date'])
    print(f"Date Range: {macro['date'].min().date()} to {macro['date'].max().date()}")
    regimes = macro['regime_label'].value_counts().to_dict()
    print(f"Macro Regimes Identified: {regimes}")
    
    # 3. VaR Validation
    var_df = pd.read_csv(files["VaR"])
    positive_var = len(var_df[var_df['var_95'] > 0])
    print(f"VaR Sanity Check: Found {positive_var} rows with mathematically impossible positive VaR > 0.")
    
    # 4. Crisis Correlation Validation (COVID-19)
    corr = pd.read_csv(files["Correlation"])
    corr['date'] = pd.to_datetime(corr['date'])
    # COVID Peak: March 2020
    mar_2020 = corr[(corr['date'] >= '2020-03-01') & (corr['date'] <= '2020-04-30')]
    mar_mean = mar_2020['correlation_63d'].mean()
    # Normal Period: Mid 2021
    mid_2021 = corr[(corr['date'] >= '2021-06-01') & (corr['date'] <= '2021-07-31')]
    mid_mean = mid_2021['correlation_63d'].mean()
    print(f"COVID-19 Correlation Spike Check: Avg correlation in Mar 2020: {mar_mean:.2f} (Expected high)")
    print(f"Normal Period Correlation Check: Avg correlation in Mid 2021: {mid_mean:.2f} (Expected lower)")
    
    # 5. Risk Ratios (Beta)
    ratios = pd.read_csv(files["Risk Ratios"])
    spy_beta = ratios[ratios['ticker'] == 'SPY']['beta_1yr'].mean()
    # Note: Beta of SPY vs SPY should be exactly 1.0!
    print(f"Beta Baseline Check (SPY vs SPY): {spy_beta:.2f} (Expected: 1.00)")
    
    print("\n=== SANITY CHECK COMPLETE ===")

if __name__ == "__main__":
    sanity_check()
