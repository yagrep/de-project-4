from pydantic import BaseModel
from pg_connect import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from datetime import datetime


class OrderObj(BaseModel):
    id: int
    order_id: str
    order_ts: datetime
    sum: float


class OrderReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get(self, offset: str, limit: str = '1000') -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id "id", 
                        object_value::json->'order_id' "order_id",
                        object_value::json->'order_ts' "order_ts",
                        object_value::json->'sum' "sum"
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


class OrderLoader:
    def __init__(self, table: str):
        self.table = table

    def load(self, conn: Connection, object: OrderObj, key: str = None):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(id, order_id, order_ts, sum)
                    VALUES (%(id)s, %(order_id)s, %(order_ts)s, %(sum)s)
                    ON CONFLICT (order_id) DO NOTHING
                    RETURNING ID
                """.format(self.table),
                {
                    "id": object.id,
                    "order_id": object.order_id,
                    "order_ts": object.order_ts,
                    "sum": object.sum
                },
            )

            value = cur.fetchone()
            if value is None:
                return None
            return value[0]
