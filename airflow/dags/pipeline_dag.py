from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = '/opt/project' #Путь внутри контейнера

with DAG(
    dag_id='cbr_pipeline', #название дага
    start_date=datetime(2026, 3, 1),
    schedule=None, #Теория: запускаем вручнуюб
    catchup=False, #Чтобы не догонял все даты до
    default_args={
        'retries': 2
    },
    tags=['cbr', 'portfolio']
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command=f'python3 {PROJECT_DIR}/src/extract.py'
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f'python3 {PROJECT_DIR}/src/transform.py'
    )

    analytics = BashOperator(
        task_id='analytics',
        bash_command=f'python3 {PROJECT_DIR}/src/analytics.py'
    )

    extract >> transform >> analytics