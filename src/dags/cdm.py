import logging
from lib.pg_connect import ConnectionBuilder
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime

log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm_load_dag',
    start_date=datetime(2022, 10, 1),
    schedule_interval="30 2 10 * *",
    catchup=True,
) as dag:
    dm_courier_ledger_load = PostgresOperator(
        task_id='dm_courier_ledger',
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql="mart/00.cdm.dm_courier_legder.sql")

    dm_courier_ledger_load

