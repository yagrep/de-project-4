from api_reader import ApiReader
from pg_connect import PgConnect
from logging import Logger
from dict_util import json2str

from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from pg_api_loader import PgApiLoader



class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class EtlSettingsRepository:

    def __init__(self, repositary: str, key: str):
        self.repositary = repositary
        self.key = key

    def get_setting(self, conn: Connection) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {0}
                    WHERE workflow_key = %(etl_key)s;
                """.format(self.repositary),
                {"etl_key": self.key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """.format(self.repositary),
                {
                    "etl_key": self.key,
                    "etl_setting": workflow_settings
                },
            )


def json_load_function(load_queue, conn, loader, key, last_loaded: str = '0') -> int:
    updated = 0
    for x in load_queue:
        updated += loader.load(conn, x, key=key)
    return str(int(last_loaded) + updated)


def pg_load_function(load_queue, conn, loader, key, last_loaded: str = '0') -> int:
    offsets = [int(last_loaded)]
    for x in load_queue:
        offset = loader.load(conn, x, key=key)
        if offset is not None:
            offsets.append(int(offset))
    return str(max(offsets))


class ETL:

    def __init__(self,
                 reader: ApiReader,
                 pg: PgConnect,
                 loader: PgApiLoader,
                 settings_repositary: str,
                 settings_key: str,
                 log: Logger,
                 object_key: str = '_id',
                 load_function=json_load_function) -> None:
        self.object_key = object_key
        self.reader = reader
        self.loader = loader
        self.pg = pg
        self.settings_repositary = settings_repositary
        self.settings_key = settings_key
        self.settings = EtlSettingsRepository(settings_repositary, settings_key)
        self.log = log
        self.load_function = load_function

    def read_and_load(self):
        with self.pg.connection() as conn:
            wf_setting = self.settings.get_setting(conn)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.settings_key, workflow_settings={self.settings_key: 0})

            last_loaded = wf_setting.workflow_settings[self.settings_key]
            load_queue = self.reader.get(offset=last_loaded)
            self.log.info(f"Found {len(load_queue)} objects to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            offset = self.load_function(load_queue, conn, self.loader, self.object_key, last_loaded)

            wf_setting.workflow_settings[self.settings_key] = offset
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings.save_setting(conn,  wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.settings_key]}")
