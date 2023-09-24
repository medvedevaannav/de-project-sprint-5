from logging import Logger

from stg.delivery_system_dag.pg_saver import PgSaver
from stg.delivery_system_dag.delivery_reader import DeliveryReader
from lib import PgConnect
from lib.dict_util import json2str


class DeliveryLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
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

            load_queue = self.collection_loader.get_deliveries()
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_delivery(conn, str(d["object_id"]), d["update_ts"], d['object_value'])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing couriers.")

            self.log.info(f"Finishing work.")

            return len(load_queue)
