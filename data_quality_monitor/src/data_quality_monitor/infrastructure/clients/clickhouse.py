from __future__ import annotations

from clickhouse_connect.driver.client import Client

from data_quality_monitor.infrastructure.config import ClickHouseConfig


class ClickHouseFactory:

    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config

    def create(self) -> Client:
        return Client(
            host=self._config.host,
            port=self._config.port,
            username=self._config.user,
            password=self._config.password,
            database=self._config.database,
        )
