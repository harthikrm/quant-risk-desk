import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def export_to_csv():
    # Load env vars for local development
    load_dotenv()
    
    # Connection details - priorities Environment Variables (set by Airflow/Docker)
    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", "5433") # Default to 5433 for local host access
    DB_NAME = os.getenv("POSTGRES_DB", "risk_db")
    
    # Construct connection string
    # If running inside Docker, DB_HOST will be 'postgres' and DB_PORT '5432'
    conn_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_url)
    
    # List of marts to export
    tables = [
        "mart_var_cvar", 
        "mart_risk_ratios", 
        "mart_drawdown", 
        "mart_correlation", 
        "mart_macro_overlay", 
        "mart_sector_summary"
    ]
    
    # Export directory
    # Inside Airflow, the project root is /opt/airflow/project/
    base_path = os.getenv("PROJECT_ROOT", ".")
    output_dir = os.path.join(base_path, "data/processed")
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Starting data export to {output_dir}...")
    
    for tbl in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {tbl}", engine)
            output_file = os.path.join(output_dir, f"{tbl}.csv")
            df.to_csv(output_file, index=False)
            print(f"Successfully exported {tbl}.csv ({len(df)} rows)")
        except Exception as e:
            print(f"Error exporting {tbl}: {str(e)}")

if __name__ == "__main__":
    export_to_csv()
