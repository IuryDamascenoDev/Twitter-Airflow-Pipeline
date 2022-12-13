from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from tweepy_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['tweepy@etlproject.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "tweepy_dag",
    default_args=default_args,
    description='ETL with Twitter data'
)

run_etl = PythonOperator(
    task_id="twitter_data_etl",
    python_callable=run_twitter_etl,
    dag=dag
)
