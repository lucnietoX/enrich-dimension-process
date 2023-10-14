from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'Luciano Nieto',
    'start_date': datetime(2023, 10, 14),
    'retries': 1,
}

dag = DAG(
    'dim_product_process_dag',
    default_args=default_args,
    description='DAG for the Dim Product process validation',
    schedule_interval=None,
)

sqlquery = "select * from dim_product where product_cluster is null"

postgres_task = PostgresOperator(
    task_id='verify_dim_product',
    sql=sqlquery,
    postgres_conn_id='postdw',
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()

