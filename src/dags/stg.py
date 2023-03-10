import logging

import pendulum
from airflow.decorators import dag, task
from lib.api_reader import ApiReader
from lib import ConnectionBuilder
from airflow.models.variable import Variable
from lib.etl_wrapper import ETL
from lib.pg_api_loader import PgApiLoader


log = logging.getLogger(__name__)


def get_api_reader(endpoint: str):
    return ApiReader(
        url=Variable.get('API_URL_BASE') + '/' + endpoint,
        login=Variable.get('API_LOGIN'),
        cohort=Variable.get('API_COHORT'),
        key=Variable.get('API_KEY')
    )

@dag(
    schedule_interval='0/1 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True
)
def stg_load_dag():
    pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="courier_load")
    def courier_load():
        reader = get_api_reader(endpoint='couriers')
        loader = PgApiLoader(table='stg.couriers')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='stg.srv_wf_settings',
            settings_key='couriers_offset',
            log=log,
            object_key='_id'
        )
        etl.read_and_load()

    couriers = courier_load()

    @task(task_id="delivery_load")
    def delivery_load():
        reader = get_api_reader(endpoint='deliveries')
        loader = PgApiLoader(table='stg.deliveries')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='stg.srv_wf_settings',
            settings_key='deliveries_offset',
            log=log,
            object_key='order_id'
        )
        etl.read_and_load()

    deliveries = delivery_load()

    couriers >> deliveries


stg_load = stg_load_dag()
