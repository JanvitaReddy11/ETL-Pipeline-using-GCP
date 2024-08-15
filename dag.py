from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 12),
    'depends_on_past': False,
    'email': ['reddyjanu11@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('employee_data',
          default_args=default_args,
          description='Runs an external Python script and triggers a Data Fusion pipeline',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py'
    )

    start_pipeline = CloudDataFusionStartPipelineOperator(
        task_id='start_datafusion_pipeline',
        location="us-west1",
        pipeline_name="etl-pipeline",
        instance_name="datafusion-dev",
        
    )

    run_script_task >> start_pipeline
