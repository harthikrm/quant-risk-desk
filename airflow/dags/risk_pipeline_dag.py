from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'quant-risk',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'financial_risk_pipeline',
    default_args=default_args,
    description='End-to-end quant risk desk ETL and dbt pipeline',
    schedule_interval='0 6 * * 1-5', # 6:00 AM on Weekdays
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'risk'],
) as dag:

    # ---------------------------------------------------------
    # PHASE 2: Data Ingestion & Quality Gates
    # ---------------------------------------------------------
    
    # We use BashOperator to trigger our isolated python scripts 
    # instead of PythonOperator to prevent dependency clashes internally.
    fetch_prices = BashOperator(
        task_id='fetch_prices',
        bash_command='python /opt/airflow/project/src/ingest/fetch_prices.py',
    )

    fetch_macro = BashOperator(
        task_id='fetch_macro',
        bash_command='python /opt/airflow/project/src/ingest/fetch_macro.py',
    )

    load_postgres = BashOperator(
        task_id='load_postgres',
        bash_command='python /opt/airflow/project/src/ingest/load_postgres.py',
    )

    validate_data = BashOperator(
        task_id='validate_data',
        bash_command='python /opt/airflow/project/src/validate/data_quality.py',
    )
    
    # ---------------------------------------------------------
    # PHASE 3: Analytics Engineering (dbt)
    # ---------------------------------------------------------

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/project/dbt_project && dbt run --models staging',
    )
    
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /opt/airflow/project/dbt_project && dbt test --models staging',
    )
    
    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /opt/airflow/project/dbt_project && dbt run --models intermediate',
    )
    
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/project/dbt_project && dbt run --models marts',
    )
    
    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command='cd /opt/airflow/project/dbt_project && dbt test --models marts',
    )

    export_data = BashOperator(
        task_id='export_data',
        bash_command='python /opt/airflow/project/src/export/export_data.py',
        env={'PROJECT_ROOT': '/opt/airflow/project'}
    )

    # ---------------------------------------------------------
    # Execution DAG Routing
    # ---------------------------------------------------------
    
    [fetch_prices, fetch_macro] >> load_postgres >> validate_data >> \
    dbt_run_staging >> dbt_test_staging >> \
    dbt_run_intermediate >> dbt_run_marts >> dbt_test_marts >> export_data
