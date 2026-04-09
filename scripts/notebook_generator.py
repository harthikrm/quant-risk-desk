import nbformat as nbf
import os

def create_notebook(filename, cells):
    nb = nbf.v4.new_notebook()
    nb['cells'] = []
    
    for cell_type, content in cells:
        if cell_type == 'markdown':
            nb['cells'].append(nbf.v4.new_markdown_cell(content))
        elif cell_type == 'code':
            nb['cells'].append(nbf.v4.new_code_cell(content))
            
    with open(f"notebooks/{filename}", 'w') as f:
        nbf.write(nb, f)
    print(f"Created {filename}")

def main():
    os.makedirs('notebooks', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Notebook 00: Data Exploration
    nb00 = [
        ('markdown', '# 00 Data Exploration & Base Statistics\nInitial profile of the raw daily return series to verify data hygiene.'),
        ('code', 'import pandas as pd\nimport numpy as np\nimport matplotlib.pyplot as plt\nimport seaborn as sns\nfrom sqlalchemy import create_engine\nimport os\nfrom dotenv import load_dotenv\n\nplt.style.use("seaborn-v0_8-darkgrid")\nload_dotenv("../.env")\n\nDB_USER = os.getenv("POSTGRES_USER", "postgres")\nDB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")\nDB_HOST = os.getenv("POSTGRES_HOST", "localhost")\nDB_PORT = "5433"\nDB_NAME = os.getenv("POSTGRES_DB", "risk_db")\nengine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")'),
        ('code', 'returns_df = pd.read_sql("SELECT * FROM int_daily_returns", engine)\nreturns_df["date"] = pd.to_datetime(returns_df["date"])\nreturns_df.head()'),
        ('code', '# Plot cumulative returns for SPY and XLK\nplt.figure(figsize=(12,6))\nfor ticker in ["SPY", "XLK"]:\n    ticker_data = returns_df[returns_df["ticker"] == ticker].sort_values("date")\n    cumulative = np.exp(ticker_data["log_return"].cumsum()) - 1\n    plt.plot(ticker_data["date"], cumulative, label=ticker)\nplt.title("Cumulative Returns: SPY vs Tech (XLK)")\nplt.legend()\nplt.show()')
    ]
    create_notebook("00_data_exploration.ipynb", nb00)

    # Notebook 01: Risk Metrics
    nb01 = [
        ('markdown', '# 01 Risk Metrics Validation\nValidating the Mathematical accuracy of our 95% computing tail risks (VaR).'),
        ('code', 'import pandas as pd\nimport matplotlib.pyplot as plt\nimport seaborn as sns\nfrom sqlalchemy import create_engine\nimport os\nfrom dotenv import load_dotenv\n\nload_dotenv("../.env")\nengine = create_engine(f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5433/risk_db")'),
        ('code', 'var_df = pd.read_sql("SELECT * FROM mart_var_cvar WHERE ticker = \'SPY\'", engine)\nvar_df["date"] = pd.to_datetime(var_df["date"])\n\nplt.figure(figsize=(12,5))\nplt.plot(var_df["date"], var_df["var_95"], color="red", label="95% VaR (Rolling 252d)")\nplt.plot(var_df["date"], var_df["var_99"], color="darkred", label="99% VaR")\nplt.title("Rolling Value at Risk Profile: SPY")\nplt.legend()\nplt.show()')
    ]
    create_notebook("01_risk_metrics_validation.ipynb", nb01)

    # Notebook 02: Macro
    nb02 = [
        ('markdown', '# 02 Macro Regime Analysis\nHow do different sectors perform across specific macro shifts (Rate hikes vs cuts, High vs low inflation).'),
        ('code', 'import pandas as pd\nimport matplotlib.pyplot as plt\nimport seaborn as sns\nfrom sqlalchemy import create_engine\nimport os; from dotenv import load_dotenv\n\nload_dotenv("../.env")\nengine = create_engine(f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5433/risk_db")'),
        ('code', 'macro_df = pd.read_sql("SELECT * FROM mart_macro_overlay", engine)\n\n# Filter to Tech vs Energy\nplot_df = macro_df[macro_df["ticker"].isin(["XLK", "XLE"])]\n\nplt.figure(figsize=(10,6))\nsns.barplot(data=plot_df, x="regime_label", y="sharpe_1yr", hue="ticker")\nplt.title("Sharpe Ratio by Economic Regime (Tech vs Energy)")\nplt.axhline(0, color="black", linestyle="--")\nplt.show()')
    ]
    create_notebook("02_macro_regime_analysis.ipynb", nb02)

    # Notebook 03: Correlation
    nb03 = [
        ('markdown', '# 03 Correlation Dynamics\nInvestigating the evaporation of portfolio diversification during Black Swan events (e.g. COVID 2020).'),
        ('code', 'import pandas as pd\nimport matplotlib.pyplot as plt\nimport seaborn as sns\nfrom sqlalchemy import create_engine\nimport os; from dotenv import load_dotenv\n\nload_dotenv("../.env")\nengine = create_engine(f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5433/risk_db")'),
        ('code', 'corr_df = pd.read_sql("SELECT * FROM mart_correlation WHERE etf_1 = \'XLK\' AND etf_2 = \'XLF\'", engine)\ncorr_df["date"] = pd.to_datetime(corr_df["date"])\n\nplt.figure(figsize=(12,5))\nplt.plot(corr_df["date"], corr_df["correlation_63d"], color="purple")\nplt.axvspan(pd.to_datetime("2020-02-20"), pd.to_datetime("2020-04-01"), color="red", alpha=0.3, label="COVID Crash")\nplt.title("Tech & Financials Rolling Correlation (Spikes to 1.0 in Crash)")\nplt.legend()\nplt.show()')
    ]
    create_notebook("03_correlation_dynamics.ipynb", nb03)

    # Notebook 04: Dashboard
    nb04 = [
        ('markdown', '# 04 Tableau Data Finalization\nExporting all mathematically confirmed tables into static CSVs for ingestion into Tableau Public.'),
        ('code', 'import pandas as pd\nfrom sqlalchemy import create_engine\nimport os; from dotenv import load_dotenv\n\nload_dotenv("../.env")\nengine = create_engine(f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5433/risk_db")\n\ntables = ["mart_var_cvar", "mart_risk_ratios", "mart_drawdown", "mart_correlation", "mart_macro_overlay", "mart_sector_summary"]\n\nfor tbl in tables:\n    df = pd.read_sql(f"SELECT * FROM {tbl}", engine)\n    df.to_csv(f"../data/processed/{tbl}.csv", index=False)\n    print(f"Exported {tbl}.csv ({len(df)} rows)")\n\nprint("Tableau data pipeline complete!")')
    ]
    create_notebook("04_dashboard_prep.ipynb", nb04)

if __name__ == '__main__':
    main()
