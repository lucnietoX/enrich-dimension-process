from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'Luciano Nieto',
    'start_date': datetime(2023, 10, 14),
    'retries': 1,
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    description='First DAG..',
    schedule_interval=None,
)

def first_dag():
    print("Hello, World! - My First DAG..")

hello_world = PythonOperator(
    task_id='first_dag',
    python_callable=first_dag,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
