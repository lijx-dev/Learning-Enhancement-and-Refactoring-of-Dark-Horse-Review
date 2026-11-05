"""Manually parameterized daily/backfill DAG; local bridge has no Docker socket."""
from datetime import timedelta
from pathlib import Path
import sys

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bridge import submit, wait_result


def run_daily(**context):
    run = context['dag_run']
    config = dict(run.conf or {})
    key = submit(config, run.run_id, int(context['ti'].try_number))
    print('Waiting for host worker attempt=' + key, flush=True)
    result = wait_result(key, config)
    print('Published immutable version=' + result['version'], flush=True)
    return {k: result[k] for k in ('request_id', 'version', 'manifest_sha256')}


with DAG(dag_id='hmdp_offline_daily', description='冻结批次的按日数仓运行与补数',
         start_date=pendulum.datetime(2026, 10, 1, tz='Asia/Shanghai'), schedule=None,
         catchup=False, max_active_runs=1, tags=['hmdp', 'offline', 'local-demo']) as dag:
    PythonOperator(task_id='build_verify_publish', python_callable=run_daily,
        retries=1, retry_delay=timedelta(seconds=10),
        execution_timeout=timedelta(minutes=18), do_xcom_push=True)
