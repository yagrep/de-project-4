from pydantic import BaseModel
from pg_connect import PgConnect
from typing import List
from psycopg.rows import class_row
from psycopg import Connection
from datetime import datetime
from pydantic.error_wrappers import ValidationError


class FctDeliveryObj(BaseModel):
    id: int
    order_id: int
    courier_id: int
    address_id: int
    order_ts: datetime
    delivery_ts: datetime
    sum: float
    rate: int
    tip_sum: float
    nodata: int


class FctDeliveryReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get(self, offset: str, limit: str = '1000') -> List[FctDeliveryObj]:
        try:
            with self._db.client().cursor(row_factory=class_row(FctDeliveryObj)) as cur:
                cur.execute(
                    """
                        with tmp as (
                            select 
                                d.id,
                                o.id "order_id",
                                c.id "courier_id",
                                a.id "address_id",
                                o.order_ts,
                                d.delivery_ts,
                                o.sum,
                                d.rate,
                                d.tip_sum 
                            from 
                                stg.deliveries sd
                            left join
                                dds.addresses a 
                                on a.address = sd.object_value::json->>'address'
                            left join	
                                dds.deliveries d  
                                on d.delivery_id  = sd.object_value::json->>'delivery_id'
                            left join	
                                dds.orders o
                                on o.order_id  = sd.object_value::json->>'order_id'
                            left join	
                                dds.couriers c
                                on c.courier_id  = sd.object_value::json->>'courier_id'
                            WHERE d.id > %(offset)s 
                            ORDER BY d.id ASC 
                            LIMIT %(limit)s 
                        ) 
                        select 
                            tmp.*,
                            case 
                                when order_id is null or courier_id is null or address_id is null then 1
                                else 0
                            end "nodata"
                        from 
                            tmp
                        order by tmp.id
                    """, {
                        "offset": offset,
                        "limit": limit
                    }
                )
                objs = cur.fetchall()
        except ValidationError as e:
            return []
        return objs


class FctDeliveryLoader:
    def __init__(self, table: str):
        self.table = table

    def load(self, conn: Connection, object: FctDeliveryObj, key: str = None):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(id, order_id, courier_id, address_id, order_ts, delivery_ts, sum, rate, tip_sum)
                    VALUES (%(id)s, %(order_id)s, %(courier_id)s, %(address_id)s, %(order_ts)s, %(delivery_ts)s, %(sum)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING ID
                """.format(self.table),
                {
                    "id": object.id,
                    "order_id": object.order_id,
                    "courier_id": object.courier_id,
                    "address_id": object.address_id,
                    "order_ts": object.order_ts,
                    "delivery_ts": object.delivery_ts,
                    "sum": object.sum,
                    "rate": object.rate,
                    "tip_sum": object.tip_sum
                },
            )
            value = cur.fetchone()
            if value is None:
                return None
            return value[0]
