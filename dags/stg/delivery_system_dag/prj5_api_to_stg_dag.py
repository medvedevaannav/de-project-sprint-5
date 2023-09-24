import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.delivery_system_dag.pg_saver import PgSaver
from stg.delivery_system_dag.courier_loader import CourierLoader, CourierReader
from stg.delivery_system_dag.delivery_loader import DeliveryLoader, DeliveryReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def prj5_api_to_stg_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    base_url ="https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"

    headers  = {
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": "AnnaM",
        "X-Cohort": "15"
    }

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CourierReader(base_url, headers)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = DeliveryReader(base_url, headers, log)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    courier_loader = load_couriers()
    delivery_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    courier_loader >> delivery_loader # type: ignore


delivery_stg_dag = prj5_api_to_stg_dag()  # noqa
