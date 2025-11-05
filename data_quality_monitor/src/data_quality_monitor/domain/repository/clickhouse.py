from loguru import logger


class ClickHouseRepositoryDomain:
    def __init__(self, client, database: str) -> None:
        self.client = client
        self.database = database

    def drop_table(self, table_name: str) -> None:
        full_table_name = (
            table_name if "." in table_name else f"{self.database}.{table_name}"
        )
        try:
            self.client.command(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info("✓ Dropped table {}", full_table_name)
        except Exception as e:
            logger.error("❌ Failed to drop table {}: {}", full_table_name, e)
            raise
