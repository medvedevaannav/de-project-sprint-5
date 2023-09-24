from logging import Logger
from stg.delivery_system_dag.pg_saver import PgSaver
from lib import PgConnect
from lib.dict_util import json2str
from datetime import datetime
from typing import Dict, List
import requests
import json
from typing import Any
from psycopg import Connection


class CourierReader:
    def __init__(self, base_url: str, headers: Dict) -> None:
        self.base_url = base_url
        self.headers = headers
        self.tbl_name = '/couriers'

    def get_couriers(self) -> List[Dict]:

        params = {
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.base_url + self.tbl_name, headers=self.headers, params=params)
        while r.text != '[]':
            response_list = json.loads(r.content)

            for courier_dict in response_list:
                result_list.append({'object_value': courier_dict,
                                    'update_ts': datetime.now()})
                
                params['offset'] += 1

            r = requests.get(self.base_url + self.tbl_name, headers=self.headers, params=params)

        return result_list


class CourierLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            self.log.info(f"starting to load")

            load_queue = self.collection_loader.get_couriers()
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_courier(conn, d["update_ts"], d['object_value'])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing couriers.")

            self.log.info(f"Finishing work.")

            return len(load_queue)
