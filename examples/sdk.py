"""
This example shows an example sdk built on top of the dbapi implementation.

Used for local testing, but could be the base for a project.
"""

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import confluent_sql


logger = logging.getLogger("confluent_sql")


@dataclass
class Table:
    name: str


@dataclass
class Database:
    name: str
    cluster_id: str


@dataclass
class Catalog:
    name: str
    env_id: str


class ConfluentCloud:
    def __init__(self):
        self._catalogs = None
        self._databases = None
        self._tables = None
        self._current_db = None
        self._init_connection()

    def _init_connection(self, dbname: Optional[str] = None):
        flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
        flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
        environment = os.getenv("CONFLUENT_ENV_ID")
        organization_id = os.getenv("CONFLUENT_ORG_ID")
        compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
        cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER")
        cloud_region = os.getenv("CONFLUENT_CLOUD_REGION")

        if not all(
            [
                flink_api_key,
                flink_api_secret,
                environment,
                organization_id,
                compute_pool_id,
                cloud_region,
                cloud_provider,
            ]
        ):
            raise ValueError("Missing keys")
        self.connection = confluent_sql.connect(
            flink_api_key=flink_api_key,
            flink_api_secret=flink_api_secret,
            environment=environment,
            organization_id=organization_id,
            compute_pool_id=compute_pool_id,
            cloud_region=cloud_region,
            cloud_provider=cloud_provider,
            dbname=dbname,
        )
        self._current_db = dbname

    def use_database(self, dbname: str):
        logger.warning("Closing existing connection and all the cursors to create a new one")
        self.connection.close()
        self._init_connection(dbname)

    @contextmanager
    def cursor(self, with_schema=False):
        cursor = self.connection.cursor(with_schema)
        try:
            yield cursor
        finally:
            cursor.close()

    def run(self, statement, **execute_kwargs) -> list:
        with self.cursor() as cur:
            cur.execute(statement, **execute_kwargs)
            results = cur.fetchall()
        return results

    @property
    def tables(self) -> list[Table]:
        if not self._current_db:
            logger.warning("No database set, call `ConfluentCloud.use_database` first")
            return []
        if not self._tables:
            self._fetch_tables()
        return self._tables

    @property
    def databases(self) -> list[Database]:
        if not self._databases:
            self._fetch_databases()
        return self._databases

    @property
    def catalogs(self) -> list[Catalog]:
        if not self._catalogs:
            self._fetch_catalogs()
        return self._catalogs

    def _fetch_tables(self):
        results = self.run("SHOW TABLES")
        self._tables = [Table(*res["row"]) for res in results]

    def _fetch_databases(self):
        results = self.run("SHOW DATABASES")
        self._databases = [Database(*res["row"]) for res in results]

    def _fetch_catalogs(self):
        results = self.run("SHOW CATALOGS")
        self._catalogs = [Catalog(*res["row"]) for res in results]


if __name__ == "__main__":
    # Setup logging so we show info level messages from our library only
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger().handlers[0].addFilter(logging.Filter("confluent_sql"))

    # Do stuff
    print("==> Initialize the SDK with 'cc = ConfluentCloud()'")
    cc = ConfluentCloud()

    print("==> Get the first 3 catalogs with 'cc.catalogs[:3]'")
    print(cc.catalogs[:3])

    print("==> Get available databases with 'cc.databases'")
    print(cc.databases)

    print("==> Trying to get tables without setting dbname gives a warning: 'cc.tables'")
    print(cc.tables)

    print("==> Set a dbname with 'cc.use_database(db_name)'")
    db_name = cc.databases[0].name
    cc.use_database(db_name)
    print(f"==> Get tables in database '{db_name}' with 'cc.tables'")
    print(cc.tables)

    with cc.cursor(with_schema=True) as cur:
        print("==> Creating table 'test_table' on 'dbt_adapter'")
        cur.execute("CREATE TABLE IF NOT EXISTS test_table (`id` BIGINT, `name` STRING)")
        print("==> Inserting values into the table")
        cur.execute("INSERT INTO test_table VALUES (1, 'test_name'), (2, 'test_name_2')")
        print("Retrieving values from table")
        print(cc.run("SELECT * FROM test_table"))
        print("==> Deleting table")
        cur.execute("DROP TABLE IF EXISTS test_table")
