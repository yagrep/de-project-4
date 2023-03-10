import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib.etl_wrapper import ETL, pg_load_function
from lib.dds_addresses import AddressLoader, AddressReader
from lib.dds_couriers import CourierLoader, CourierReader
from lib.dds_orders import OrderLoader, OrderReader
from lib.dds_deliveries import DeliveryLoader, DeliveryReader
from lib.dds_fct_deliveries import FctDeliveryLoader, FctDeliveryReader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True
)
def dds_load_dag():
    pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="address_load")
    def address_load():
        reader = AddressReader(pg)
        loader = AddressLoader(table='dds.addresses')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='dds.srv_wf_settings',
            settings_key='addresses_offset',
            log=log,
            object_key='id',
            load_function=pg_load_function
        )
        etl.read_and_load()

    addresses = address_load()

    @task(task_id="courier_load")
    def courier_load():
        reader = CourierReader(pg)
        loader = CourierLoader(table='dds.couriers')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='dds.srv_wf_settings',
            settings_key='couriers_offset',
            log=log,
            object_key='id',
            load_function=pg_load_function
        )
        etl.read_and_load()

    couriers = courier_load()

    @task(task_id="order_load")
    def order_load():
        reader = OrderReader(pg)
        loader = OrderLoader(table='dds.orders')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='dds.srv_wf_settings',
            settings_key='orders_offset',
            log=log,
            object_key='id',
            load_function=pg_load_function
        )
        etl.read_and_load()

    orders = order_load()

    @task(task_id="delivery_load")
    def delivery_load():
        reader = DeliveryReader(pg)
        loader = DeliveryLoader(table='dds.deliveries')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='dds.srv_wf_settings',
            settings_key='deliveries_offset',
            log=log,
            object_key='id',
            load_function=pg_load_function
        )
        etl.read_and_load()

    deliveries = delivery_load()


    @task(task_id="fct_delivery_load")
    def fct_delivery_load():
        reader = FctDeliveryReader(pg)
        loader = FctDeliveryLoader(table='dds.fct_deliveries')
        etl = ETL(
            reader=reader,
            pg=pg,
            loader=loader,
            settings_repositary='dds.srv_wf_settings',
            settings_key='fct_deliveries_offset',
            log=log,
            object_key='id',
            load_function=pg_load_function
        )
        etl.read_and_load()

    fct_deliveries = fct_delivery_load()


    addresses >> couriers >> orders >> deliveries >> fct_deliveries


dds_load = dds_load_dag()

