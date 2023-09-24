from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_courier(self, conn: Connection, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.orderdelivery_couriers (message, update_ts)
                    VALUES (%(val)s, %(update_ts)s);
                """,
                {
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

    def save_delivery(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.orderdelivery_deliveries(message, update_ts)
                    VALUES ( %(val)s, %(update_ts)s);
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

