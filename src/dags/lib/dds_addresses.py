from pydantic import BaseModel
from pg_connect import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection


class AddressObj(BaseModel):
    id: int
    address: str


class AddressReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get(self, offset: str, limit: str = '1000') -> List[AddressObj]:
        with self._db.client().cursor(row_factory=class_row(AddressObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id, 
                        object_value::json->'address' "address"
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


class AddressLoader:
    def __init__(self, table: str):
        self.table = table

    def load(self, conn: Connection, object: AddressObj, key: str = None):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(id, address)
                    VALUES (%(id)s, %(address)s)
                    ON CONFLICT (address) DO NOTHING
                    RETURNING ID
                """.format(self.table),
                {
                    "id": object.id,
                    "address": object.address
                },
            )

            value = cur.fetchone()
            if value is None:
                return None
            return value[0]
