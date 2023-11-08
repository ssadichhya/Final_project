from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable



default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

dag = DAG( 
    'Final_Project',
    default_args=default_args,
    start_date= datetime(2023, 11, 1),
    catchup= False,
)

# using bashoperator to activate env
activate_venv_task = BashOperator(
    task_id='activate_virtualenv',
    bash_command=f"source {Variable.get('location')}/Final_project/venv/bin/activate", 
    dag=dag,
)


# using file sensor to check for raw csv in raw_data 
file_sensor = FileSensor(
    task_id = 'check_for_raw_file',
    poke_interval = 30,
    timeout = 3600,
    filepath = f"{Variable.get('location')}/Final_project/Raw_Data/job_descriptions.csv",
    mode = 'poke',
    dag=dag,
)

extract_data = BashOperator(
    task_id = 'extract_data',
    bash_command = f"python3 {Variable.get('location')}/Final_project/ETL/extract.py",
    dag=dag,
)

validate_data = BashOperator(
    task_id = 'validate_data',
    bash_command = f"pytest {Variable.get('location')}/Final_project/ETL/validation.py",
    dag=dag,
)

clean_data = BashOperator(
    task_id = 'clean_data',
    bash_command = f"python3 {Variable.get('location')}/Final_project/ETL/clean.py",
    dag=dag,
)

transform_data = BashOperator(
    task_id = 'transformations',
    bash_command = f"python3 {Variable.get('location')}/Final_project/ETL/transform.py",
    dag=dag,
)

load_data = BashOperator(
    task_id = 'loading',
    bash_command = f"python3 {Variable.get('location')}/Final_project/ETL/load.py",
    dag=dag,
)

activate_venv_task >> file_sensor >> extract_data >> validate_data >> clean_data >> transform_data >> load_data