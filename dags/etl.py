from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
from app.src.extract.extract import save_to_staging_area
from app.src.transform.transform import transform
from app.src.load.load import load



default_args = { 
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


dag = DAG(
    "IMDB_testing_DAG",
    start_date=datetime(2024,11,15),
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily'
)

extract_task = PythonOperator(
    task_id = "extract",
    python_callable= save_to_staging_area,
    dag = dag
)

transform_task = PythonOperator(
    task_id = "transform",
    python_callable = transform,
    dag = dag
)

load_task = PythonOperator(
    task_id = "load",
    python_callable = load,
    dag = dag 
)

extract_task >> transform_task >> load_task
