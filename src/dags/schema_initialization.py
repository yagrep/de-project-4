import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.pg_connect import ConnectionBuilder, PgConnect
from logging import Logger
from pathlib import Path
import os


log = logging.getLogger(__name__)

class SchemaDdl:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log

    def init_schema(self, path_to_scripts: str) -> None:

        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} files to apply changes.")

        i = 1
        for fp in file_paths:
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            script = fp.read_text()

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

            self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")
            i += 1


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True
)
def schema_initialization_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="schema_initialization")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(Variable.get("DDL_PATH"))

    init_schema = schema_init()

    init_schema


schema_initialization = schema_initialization_dag()
