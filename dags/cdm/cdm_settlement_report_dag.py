import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder

from cdm.settlement_report import SettlementReportLoader
from cdm.dm_courier_ledger_group import CourierLedgerReportLoader
import logging
from dds.dds_settings_repository import DdsEtlSettingsRepository

log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm_reports',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    settings_repository = DdsEtlSettingsRepository(dwh_pg_connect)


    @task(task_id="cdm_settlement_report")
    def settlement_daily_report_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report_by_days()


    @task(task_id="dm_courier_ledger_group")
    def courier_ledger_group_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        rest_loader = CourierLedgerReportLoader(dwh_pg_connect)
        rest_loader.load_CourierLedgerRepor()
        
    cdm_settlement_report=settlement_daily_report_load()  # type: ignore
    dm_courier_ledger_group=courier_ledger_group_load()  # type: ignore

    cdm_settlement_report 
    dm_courier_ledger_group