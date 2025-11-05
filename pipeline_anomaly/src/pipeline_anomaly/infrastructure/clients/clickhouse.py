from __future__ import annotations

from contextlib import contextmanager

from clickhouse_connect import get_client
from clickhouse_connect.driver import Client


class ClickHouseFactory:
    def __init__(
        self, host: str, port: int, username: str, password: str, database: str
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database

    @contextmanager
    def connect(self) -> Client:
        client = get_client(
            host=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            database=self._database,
        )
        try:
            yield client
        finally:
            client.close()
