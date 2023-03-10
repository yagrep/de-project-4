from pydantic import BaseModel
from pg_connect import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection


class CourierObj(BaseModel):
    id: int
    courier_id: str
    name: str


class CourierReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get(self, offset: str, limit: str = '1000') -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id "id", 
                        object_value::json->'_id' "courier_id",
                        object_value::json->'name' "name"                       
                    FROM stg.couriers
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


class CourierLoader:
    def __init__(self, table: str):
        self.table = table

    def load(self, conn: Connection, object: CourierObj, key: str = None):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(id, courier_id, name)
                    VALUES (%(id)s, %(courier_id)s, %(name)s)
                    ON CONFLICT (courier_id) DO NOTHING
                    RETURNING ID
                """.format(self.table),
                {
                    "id": object.id,
                    "courier_id": object.courier_id,
                    "name": object.name
                },
            )

            value = cur.fetchone()
            if value is None:
                return None
            return value[0]
