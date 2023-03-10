from psycopg import Connection
from dict_util import json2str

class PgApiLoader:
    def __init__(self, table: str):
        self.table = table

    def load(self, conn: Connection, object, key: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(object_key, object_value)
                    VALUES (%(object_key)s, %(object_value)s)
                    ON CONFLICT (object_key) DO NOTHING
                    RETURNING ID
                """.format(self.table),
                {
                    "object_key": object[key],
                    "object_value": json2str(object)
                },
            )
            return cur.rowcount
