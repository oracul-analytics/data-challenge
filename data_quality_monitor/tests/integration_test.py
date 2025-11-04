"""
Интеграционный тест для проверки rules.yaml через Kafka
Заполняет таблицу dq.events данными и проверяет их через Redpanda
"""
import pandas as pd
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.repositories.redpanda_consumer import RedpandaConsumer
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from data_quality_monitor.infrastructure.clients.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from loguru import logger
import sys


logger.remove()
logger.add(sys.stderr, level="INFO")

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "rules.yaml"


def create_topic(bootstrap_servers: str, topic: str):
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    
    try:
        new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic_name, f in fs.items():
            try:
                f.result()
                logger.info("Topic created: {}", topic_name)
            except Exception as e:
                logger.warning("Failed to create topic {}: {}", topic_name, e)
    except Exception as e:
        logger.warning("Create topic error: {}", e)


def delete_topic(bootstrap_servers: str, topic: str):
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    
    try:
        fs = admin_client.delete_topics([topic], operation_timeout=30)
        for topic_name, f in fs.items():
            try:
                f.result()
                logger.info("Topic deleted: {}", topic_name)
            except Exception as e:
                logger.warning("Failed to delete topic {}: {}", topic_name, e)
    except Exception as e:
        logger.warning("Delete topic error: {}", e)


def test_rules_yaml_via_kafka():
    bootstrap_servers = "localhost:39092"
    topic_name = f"events_{uuid.uuid4().hex}"
    group_id = f"dq_monitor_{uuid.uuid4().hex}"
    
    create_topic(bootstrap_servers, topic_name)
    time.sleep(0.5)
    
    config = RuleConfig.load(CONFIG_PATH)
    logger.info("✓ Loaded config from {}", CONFIG_PATH)
    logger.info("  Database: {}", config.clickhouse.database)
    logger.info("  Rules: {}", len(config.rules))
    
    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory)
    repo.ensure_schema()
    
    try:
        repo.client.command("TRUNCATE TABLE dq.events")
        repo.client.command("TRUNCATE TABLE dq.reports")
        logger.info("✓ Cleared events and reports tables")
    except Exception as e:
        logger.warning("Could not truncate tables: {}", e)
    
    num_rows = 1000
    start_time = datetime(2025, 11, 4, 0, 0, 0)
    
    events_data = pd.DataFrame({
        "event_id": range(1, num_rows + 1),  # Все уникальные (100% > 98%)
        "value": [float(i % 1000) for i in range(num_rows)],  # Все в [0, 999] ⊆ [0, 1000], все заполнены (100% > 99%)
        "ts": [start_time + timedelta(seconds=i) for i in range(num_rows)]
    })
    
    repo.insert_events(events_data)
    logger.info("✓ Inserted {} events into dq.events", num_rows)
    
    producer = RedpandaProducer(bootstrap_servers=bootstrap_servers, topic=topic_name)
    
    total_expectations = 0
    for rule in config.rules:
        frame = repo.fetch_table(rule.table)
        report = engine.evaluate(rule, frame)
        
        logger.info("Report for {}: {} expectations", rule.table, len(report.results))
        for result in report.results:
            total_expectations += 1
            status = "✓ PASSED" if result.passed else "✗ FAILED"
            logger.info("  {} - {}: {}", status, result.rule, result.details)
        

        producer.send_report(report)
    
    logger.info("✓ Sent {} reports to Kafka (total {} expectations)", len(config.rules), total_expectations)
    
    time.sleep(1.5)
    
    consumer = RedpandaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic_name,
        group_id=group_id
    )
    
    try:
        logger.info("Starting to consume {} messages...", total_expectations)
        consumer.consume(callback=repo.save_from_message, max_messages=total_expectations)
        
        time.sleep(1.0)
        
        reports = repo.list_reports()
        
        logger.info("Reports found: {}", len(reports))
        
        assert len(reports) == total_expectations, f"Expected {total_expectations} reports, got {len(reports)}"
        
        for idx, row in reports.iterrows():
            logger.info("Report {}: table={}, rule={}, passed={}", 
                       idx, row["table_name"], row["rule"], row["passed"])
            assert row["passed"] == 1, f"Rule {row['rule']} failed: {row['details']}"
        
        logger.info("✓ All {} assertions passed!", total_expectations)
        logger.info("✓ All rules from rules.yaml passed successfully")
        
        logger.info("\n=== Test Summary ===")
        logger.info("Events in dq.events: {}", len(events_data))
        logger.info("Reports in dq.reports: {}", len(reports))
        logger.info("All checks passed: {}", all(reports["passed"] == 1))
        
        unique_rules = reports["rule"].unique()
        for rule_name in unique_rules:
            logger.info("  - {}: PASSED", rule_name)
        
    finally:
        consumer.close()
        time.sleep(0.5)
        delete_topic(bootstrap_servers, topic_name)
        logger.info("✓ Test cleanup completed")


if __name__ == "__main__":
    test_rules_yaml_via_kafka()
