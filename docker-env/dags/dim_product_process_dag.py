from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# Defina o DAG
@dag(schedule_interval=None, start_date=datetime(2023, 10, 15), catchup=False)
def process_product_workflow():
    @task()
    def exec_select():
        conn_id = 'postdw'
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = "SELECT * FROM dim_product where product_cluster is null"
        df = pd.read_sql(sql, con=hook.get_sqlalchemy_engine())
        return df

    @task()
    def processar_dataframe(df):
        print(df.head())

    #resultado_query = exec_select()
    

    send_email = EmailOperator(
        task_id='send_email',
        to='luciano.nieto@gmail.com',
        subject="teste",
        html_content="teste.."
    )

    #processar_dataframe(resultado_query) >> 
    send_email

dag = process_product_workflow()