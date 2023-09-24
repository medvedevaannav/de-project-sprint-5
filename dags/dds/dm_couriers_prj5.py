import json
from datetime import datetime
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel
from repositories.pg_connect import PgConnect

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class CourierJsonObj(BaseModel):
    id: int
    message: str

class CourierDdsObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CourierRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_courier(self, last_loaded_record_id: int) -> List[CourierJsonObj]:
        with self._db.client().cursor(row_factory=class_row(CourierJsonObj)) as cur:
            cur.execute(
                """
                    select id,
                           message
                      from stg.orderdelivery_couriers 
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class CourierDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_courier(self, couriers: CourierDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_couriers (courier_id, courier_name, load_dt)
                        VALUES (%(courier_id)s, %(courier_name)s,now()) 
                        on conflict  (courier_id)
                        DO UPDATE set courier_name=EXCLUDED.courier_name;
                    """,
                    {
                        "courier_id": couriers.courier_id,
                        "courier_name": couriers.courier_name
                    },
                )
                conn.commit()

class CourierLoader:
    WF_KEY = "Courier_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = CourierRawRepository(pg)
        self.dds = CourierDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_courier(self, raws: List[CourierJsonObj]) -> List[CourierDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.message)
            t = CourierDdsObj(id=r.id,
                                 courier_id=rest_json['_id'],
                                 courier_name=rest_json['name']
                                 )

            res.append(t)
        return res

    def load_courier(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_courier(last_loaded_id)
        courier_to_load = self.parse_courier(load_queue)
        for r in courier_to_load:
            self.dds.insert_courier(r)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                r.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
