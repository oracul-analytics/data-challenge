import pandas as pd
import time
import uuid
from confluent_kafka.admin import AdminClient, NewTopic
from data_quality_monitor.domain.models.rule import Expectation, TableRule
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.repositories.redpanda_consumer import RedpandaConsumer
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from loguru import logger


def create_topic(bootstrap_servers: str, topic: str):
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    
    try:
        new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic_name, f in fs.items():
            try:
                f.result()
                logger.info("Topic {} created", topic_name)
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
                logger.info("Topic {} deleted", topic_name)
            except Exception as e:
                logger.warning("Failed to delete topic {}: {}", topic_name, e)
    except Exception as e:
        logger.warning("Delete topic error: {}", e)


def test_completeness_passes():
    bootstrap_servers = "localhost:39092"
    topic_name = f"dq_reports_{uuid.uuid4().hex}"
    group_id = f"dq_monitor_{uuid.uuid4().hex}"
    
    logger.info("Using topic: {}", topic_name)
    
    create_topic(bootstrap_servers, topic_name)
    
    frame = pd.DataFrame({"value": [1, 2, 3]})
    rule = TableRule(
        table="t", 
        expectations=(Expectation("completeness", {"column": "value", "threshold": 1.0}),)
    )
    report = engine.evaluate(rule, frame)
    
    assert report.results[0].passed

    producer = RedpandaProducer(bootstrap_servers=bootstrap_servers, topic=topic_name)
    producer.send_report(report)
    
    time.sleep(0.5)
    
    repo = ClickHouseRepository()
    repo.ensure_schema()
    
    consumer = RedpandaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic_name,
        group_id=group_id
    )
    
    try:
        consumer.consume(callback=repo.save_from_message, max_messages=1)
        
        reports = repo.list_reports(limit=1)
        assert len(reports) > 0
        assert reports.iloc[0]["table_name"] == "t"
    finally:
        consumer.close()
        time.sleep(0.5)
        delete_topic(bootstrap_servers, topic_name)
        logger.info("Test completed and topic cleaned up")
