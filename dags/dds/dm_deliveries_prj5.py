import json
from datetime import datetime
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel
from repositories.pg_connect import PgConnect

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class DeliveriesJsonObj(BaseModel):
    id: int
    message: str

class DeliveriesDdsObj(BaseModel):
    id: int
    delivery_id: str
    courier_id: str
    order_id: str
    order_ts: datetime
    address: str
    delivery_ts: datetime
    rate: float
    sum: float 
    tip_sum: float

class DeliveriesRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_deliveries(self, last_loaded_record_id: int) -> List[DeliveriesJsonObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveriesJsonObj)) as cur:
            cur.execute(
                """
                    select id,
                           message
                      from stg.orderdelivery_deliveries 
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_deliveries(self, deliveries: DeliveriesDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.dm_deliveries (delivery_id, courier_id, order_id, order_ts, address, delivery_ts, rate, sum, tip_sum,load_dt)
                        VALUES (%(delivery_id)s, 
                                %(courier_id)s,
                                %(order_id)s, 
                                %(order_ts)s, 
                                %(address)s, 
                                %(delivery_ts)s, 
                                %(rate)s, 
                                %(sum)s, 
                                %(tip_sum)s,                                
                                now()) 
                        on conflict (delivery_id,courier_id,order_id)
                        do update  set order_ts=EXCLUDED.order_ts::timestamp, 
		                               address=EXCLUDED.address, 
		                               delivery_ts=EXCLUDED.delivery_ts, 
		                               rate=EXCLUDED.rate, 
		                               sum=EXCLUDED.sum, 
		                               tip_sum =EXCLUDED.tip_sum; 

                    """,
                    {
                        "delivery_id": deliveries.delivery_id,
                        "courier_id": deliveries.courier_id,
                        "order_id": deliveries.order_id,
                        "order_ts": deliveries.order_ts,
                        "address": deliveries.address,
                        "delivery_ts": deliveries.delivery_ts,
                        "rate": deliveries.rate,
                        "sum": deliveries.sum,
                        "tip_sum": deliveries.tip_sum                  
                        
                    },
                )
                conn.commit()

class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = DeliveriesRawRepository(pg)
        self.dds = DeliveriesDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_deliveries(self, raws: List[DeliveriesJsonObj]) -> List[DeliveriesDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.message)
            t = DeliveriesDdsObj(id=r.id,
                                 delivery_id=rest_json['delivery_id'],
                                 courier_id=rest_json['courier_id'],
                                 order_id=rest_json['order_id'],
                                 order_ts=rest_json['order_ts'],
                                 address=rest_json['address'],
                                 delivery_ts=rest_json['delivery_ts'],
                                 rate=rest_json['rate'],
                                 sum=rest_json['sum'],
                                 tip_sum=rest_json['tip_sum']
                                 )

            res.append(t)
        return res

    def load_deliveries(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_deliveries(last_loaded_id)
        deliveries_to_load = self.parse_deliveries(load_queue)
        for r in deliveries_to_load:
            self.dds.insert_deliveries(r)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                r.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
