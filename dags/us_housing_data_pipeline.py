from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from urllib import request
import os
from datetime import datetime, timedelta

path_local_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 25),
    "depends_on_past": False,
    "retries": 1
}

#DAG definition:
with DAG(
    dag_id="us_housing_data_pipeline",
    schedule="59 23 * * 3", # Housing dataset is updated every wednesday EOD
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['us-housing']
) as dag:

    fetching_data_job = BashOperator(
        task_id="download_dataset_from_source_server_job",
        bash_command=f"python -u {path_local_home}/src/jobs/fetching_data.py" # Run the py script in unbuffered mode to see the logs in real time.
    )

    gcs_to_bq_job = BashOperator(
        task_id="Writing_data_from_gcs_bq_job",
        bash_command=f"python -u {path_local_home}/src/jobs/gcs_to_bq.py" # Run the py script in unbuffered mode to see the logs in real time.
    )

    dbt_model_job = BashOperator(
        task_id="Running_dbt_model_job",
        bash_command="dbt deps && dbt build",
        cwd= f"{path_local_home}/housing_market_data" # Current working directory for dbt commands  
    )   

#Job dependencies
fetching_data_job >> gcs_to_bq_job >> dbt_model_job