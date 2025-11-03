from __future__ import annotations

from clickhouse_connect.driver.client import Client

from feature_store_ml.infrastructure.config import ClickHouseConfig


from clickhouse_connect import get_client

class ClickHouseFactory:
    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config

    def create(self):
        return get_client(
            host=self._config.host,
            port=self._config.port,
            username=self._config.username,
            password=self._config.password,
            database=self._config.database,
        )

    @property
    def database(self):
        return self._config.database
