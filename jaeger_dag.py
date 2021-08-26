from airflow import DAG

import sys
import logging

from airflow.operators.http_operator import SimpleHttpOperator

log = logging.getLogger(__name__)

sys.path.append("../")


default_arg = {'owner': 'airflow', 'start_date': '2020-02-28'}

dag = DAG(
    'jaeger_dag',
    default_args=default_arg,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

jeager = SimpleHttpOperator(
    task_id='jeager',
    method='GET',
    endpoint='api/traces?service=orders&lookback=20m&prettyPrint=true&limit=1',
    headers={"Content-Type": "application/json"},
    http_conn_id='test',
    xcom_push=True,
    dag=dag,
)

jeager
