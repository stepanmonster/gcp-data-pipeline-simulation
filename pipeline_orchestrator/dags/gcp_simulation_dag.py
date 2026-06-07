from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'student_intern',
    'start_date': datetime(2026, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='gcp_internship_pipeline',
    default_args = default_args,
    schedule_interval = None, # Manually triggered via the UI
    catchup = False
) as dag:
    
    # Task 1: Command Airflow to use venv Python tool to run the transformation script
    run_beam_pardo = BashOperator(
        task_id='execute_apache_beam_pardo',
        bash_command='C:\\Users\\HP\\Documents\\GitHub\\gcp-data-pipeline-simulation\\transformation_layer\\venv\\Scripts\\python.exe C:\\Users\\HP\\Documents\\GitHub\\gcp-data-pipeline-simulation\\transformation_layer\\clean_logs.py'
    )

    # Task 2: Emulate automated triage check by displaying the generated dataset in the logs
    triage_check = BashOperator(
        task_id = 'triage_and_verify_outputs',
        bash_command = 'type C:\\Users\\HP\\Documents\\gcp-data-pipeline-simulation\\transformation_layer\\cleaned_incidents.csv'
    )

    run_beam_pardo >> triage_check