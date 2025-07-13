from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from airflow.utils import timezone

import pandas as pd
from datetime import timedelta

from pipeline.extract import fetch_and_load
from pipeline.transform import transformation
from pipeline.load import load

default_args = {
    'owner': 'airflow',
    'start_date':timezone.utcnow() - timedelta(days=1)}

with DAG(
    dag_id = 'WWAI_Pipeline',
    default_args=default_args,
    schedule = '@daily',
    catchup=False) as dag:

    t1 = PythonOperator(
        task_id = 'extract',
        python_callable = fetch_and_load,
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transformation,
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t1 >> t2 >> t3