"""
This example shows an example sdk built on top of the dbapi implementation.

Used for local testing, but could be the base for a project.
"""

import os
from contextlib import contextmanager
from dataclasses import dataclass

import confluent_sql

from utils import setup_logging


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
        flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
        flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
        environment = os.getenv("CONFLUENT_ENV_ID")
        organization_id = os.getenv("CONFLUENT_ORG_ID")
        compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
        cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER", "aws")
        cloud_region = os.getenv("CONFLUENT_CLOUD_REGION", "us-east-2")

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
        )
        self._catalogs = None
        self._databases = None
        self._tables = None

    @contextmanager
    def cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def run(self, statement, keyed = False) -> list:
        with self.cursor() as cur:
            cur.execute(statement)
            res = cur.fetchall()
            if keyed:
                res = [map_tuple_to_schema(cur._statement.schema, row) for row in res]
        return res

    @property
    def tables(self) -> list[Table]:
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
        res = self.run("SHOW TABLES")
        self._tables = [Table(*row) for row in res]

    def _fetch_databases(self):
        res = self.run("SHOW DATABASES")
        self._databases = [Database(*row) for row in res]

    def _fetch_catalogs(self):
        res = self.run("SHOW CATALOGS")
        self._catalogs = [Catalog(*row) for row in res]


def map_tuple_to_schema(schema, values):
    """
    Recursively transforms a tuple or list of data values into a 
    dictionary based on the provided schema.
    """
    result_dict = {}
    
    for field_schema, value in zip(schema, values):
        field_name = field_schema['name']
        type_info = field_schema.get('type')
        if type_info is None:
            type_info = field_schema.get('field_type')
        if isinstance(type_info, dict) and type_info.get('type') == 'ROW':
            sub_schema_fields = type_info['fields']
            result_dict[field_name] = map_tuple_to_schema(sub_schema_fields, value)
        else:
            result_dict[field_name] = value
    return result_dict

if __name__ == '__main__':
    # Setup logging so we show info level messages from our library only
    setup_logging()

    # Do stuff
    print("==> Initializing the SDK...")
    cc = ConfluentCloud()

    print("==> First 3 Catalogs:")
    print("==>", cc.catalogs[:3])

    print("==> Databases:")
    print("==>", cc.databases)

    cc.connection.use_database("dbt_adapter")
    print("==> Tables:")
    print("==>", cc.tables)

    with cc.cursor() as cur:
        print("==> Creating table 'test_table'")
        cur.execute("CREATE TABLE IF NOT EXISTS test_table (`id` BIGINT, `name` STRING)")
        print("==> Inserting values into the table")
        cur.execute("INSERT INTO test_table VALUES (1, 'test_name'), (2, 'test_name_2')")
        print("==> Retrieving values from table")
        print("==>", cc.run("SELECT * FROM test_table", keyed=True))
        print("==> Deleting table")
        cur.execute("DROP TABLE IF EXISTS test_table")
