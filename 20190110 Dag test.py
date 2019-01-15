from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('20190110 Dag test', default_args=default_args, schedule_interval=timedelta(days=1))

t1 = PostgresOperator(
    task_id='read_postgres',
    sql='SELECT * FROM test;',
    postgres_conn_id='postgres_local',
    dag=dag
)

t2 = PostgresOperator(
    task_id='write_postgres',
    sql='INSERT INTO test VALUES',
    postgres_conn_id='postgres_local',
    dag=dag
)

t1 >> t2