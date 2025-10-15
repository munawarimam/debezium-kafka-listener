import os

from utils.helpers import ClickHouseConnector, ClickHouseMessageProcessor, ClickHouseLogger
from utils.kafka import KafkaConsumerHelper

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_SCHEMA_REGISTRY = os.getenv("KAFKA_SCHEMA_REGISTRY")
KAFKA_TOPIC_PATTERN = os.getenv("KAFKA_TOPIC_PATTERN")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS")
CLICKHOUSE_DB_AUDIT = os.getenv("CLICKHOUSE_DB_AUDIT")
CLICKHOUSE_DB_STG = os.getenv("CLICKHOUSE_DB_STG")

if __name__ == "__main__":
    log_connector = ClickHouseConnector(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB_AUDIT
    )

    staging_connector = ClickHouseConnector(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB_STG
    )

    logger = ClickHouseLogger.init_logger(log_connector, "debezium_kafka_logs")
    processor = ClickHouseMessageProcessor(staging_connector)

    consumer_helper = KafkaConsumerHelper(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id=KAFKA_GROUP_ID,
        schema_registry_url=KAFKA_SCHEMA_REGISTRY,
        topic_pattern=KAFKA_TOPIC_PATTERN,
        logger=logger,
        processor=processor
    )

    consumer_helper.start()
