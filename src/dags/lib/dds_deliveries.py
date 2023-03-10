from pydantic import BaseModel
from pg_connect import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from datetime import datetime


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    delivery_ts: datetime
    rate: int
    tip_sum: float


class DeliveryReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get(self, offset: str, limit: str = '1000') -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id "id", 
                        object_value::json->'delivery_id' "delivery_id",
                        object_value::json->'delivery_ts' "delivery_ts",
                        object_value::json->'rate' "rate",
                        object_value::json->'tip_sum' "tip_sum"
                    FROM stg.deliveries
                    WHERE id > %(offset)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "offset": offset,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryLoader:
    def __init__(self, table: str):
        self.table = table

    def load(self, conn: Connection, object: DeliveryObj, key: str = None):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(id, delivery_id, delivery_ts, rate, tip_sum)
                    VALUES (%(id)s, %(delivery_id)s, %(delivery_ts)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO NOTHING
                    RETURNING ID
                """.format(self.table),
                {
                    "id": object.id,
                    "delivery_id": object.delivery_id,
                    "delivery_ts": object.delivery_ts,
                    "rate": object.rate,
                    "tip_sum": object.tip_sum
                },
            )

            value = cur.fetchone()
            if value is None:
                return None
            return value[0]
