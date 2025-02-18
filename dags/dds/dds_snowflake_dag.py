import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder

from dds.dds_settings_repository import DdsEtlSettingsRepository
from dds.fct_products_loader import FctProductsLoader
from dds.order_loader import OrderLoader
from dds.products_loader import ProductLoader
from dds.restaurant_loader import RestaurantLoader
from dds.timestamp_loader import TimestampLoader
from dds.user_loader import UserLoader
from dds.dm_couriers_prj5 import CourierLoader
from dds.dm_deliveries_prj5 import DeliveriesLoader

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_case_dds_snowflake',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    settings_repository = DdsEtlSettingsRepository(dwh_pg_connect)

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()

    @task(task_id="dm_products_load")
    def load_dm_products(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()

    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()

    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()

    @task(task_id="fct_order_products_load")
    def load_fct_order_products(ds=None, **kwargs):
        fct_loader = FctProductsLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_product_facts()

    @task(task_id="dm_courier_load")
    def load_dm_courier(ds=None, **kwargs):
        rest_loader = CourierLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_courier()

    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries(ds=None, **kwargs):
        rest_loader = DeliveriesLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_deliveries()
        
        
    dm_restaurants = load_dm_restaurants()
    dm_products = load_dm_products()
    dm_timestamps = load_dm_timestamps()
    dm_users = load_dm_users()
    dm_orders = load_dm_orders()
    fct_order_products = load_fct_order_products()
    dm_courier = load_dm_courier()
    dm_deliveries = load_dm_deliveries()
    
    dm_restaurants >> dm_products  # type: ignore
    dm_restaurants >> dm_orders  # type: ignore
    dm_timestamps >> dm_orders  # type: ignore
    dm_users >> dm_orders  # type: ignore
    dm_products >> fct_order_products  # type: ignore
    dm_orders >> fct_order_products  # type: ignore
    dm_courier 
    dm_deliveries
