from datetime import datetime
from logging import Logger

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from stg.delivery_system_dag.pg_saver import PgSaver
from lib import PgConnect
from lib.dict_util import json2str

from typing import Dict, List
import requests
import json


class DeliveryReader:
    def __init__(self, base_url: str, headers: Dict, log) -> None:
        self.base_url = base_url
        self.headers = headers
        self.tbl_name = '/deliveries'
        self.log = log

    def get_deliveries(self, load_threshold: datetime) -> List[Dict]:

        params = {
            'from': load_threshold.strftime("%Y-%m-%d %H:%M:%S"),
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.base_url + self.tbl_name, headers=self.headers, params=params)
        while r.text != '[]':
            self.log.info(str(r.content))
            self.log.info(str(r.url))
            response_list = json.loads(r.content)

            for delivery_dict in response_list:
                result_list.append({'object_id': delivery_dict['delivery_id'], 
                                    'object_value': delivery_dict,
                                    'update_ts': datetime.fromisoformat(delivery_dict['delivery_ts'])})
                
            params['offset'] += len(response_list)

            r = requests.get(self.base_url + self.tbl_name, headers=self.headers, params=params)

        return result_list


class DeliveryLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "prj5_api_to_stg_dag_Delivery"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository(pg_dest)
        self.log = logger

    def run_copy(self) -> int:

        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    self.WF_KEY,
                    {
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2023, 9, 24).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            self.log.info(f"starting to load")

            load_queue = self.collection_loader.get_deliveries(last_loaded_ts)
            self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_delivery(conn, str(d["object_id"]), d["update_ts"], d['object_value'])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)           
            self.settings_repository.save_setting_my(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
