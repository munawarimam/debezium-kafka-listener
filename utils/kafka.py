import re
import time
from confluent_kafka import DeserializingConsumer, KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class KafkaConsumerHelper:
    def __init__(self, 
                 bootstrap_servers: str,
                 group_id: str,
                 schema_registry_url: str,
                 topic_pattern: str,
                 logger,
                 processor):
        """
        Kafka Consumer Helper Class

        Args:
            bootstrap_servers: Kafka broker(s)
            group_id: Consumer group ID
            schema_registry_url: Schema Registry endpoint
            topic_pattern: Regex pattern for topic subscription
            logger: Logger instance
            processor: Processor instance (for message handling)
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.schema_registry_url = schema_registry_url
        self.topic_pattern = topic_pattern
        self.logger = logger
        self.processor = processor
        self.consumer = None

    def _create_consumer(self):
        """Create and return a Kafka DeserializingConsumer"""
        self.logger.info("Creating Kafka consumer...")

        schema_registry_conf = {'url': self.schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        avro_deserializer = AvroDeserializer(schema_registry_client)

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "key.deserializer": avro_deserializer,
            "value.deserializer": avro_deserializer,
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        }

        consumer = DeserializingConsumer(conf)
        metadata = consumer.list_topics(timeout=10)
        all_topics = list(metadata.topics.keys())
        matched_topics = [topic for topic in all_topics if re.match(self.topic_pattern, topic)]

        consumer.subscribe(matched_topics)
        return consumer

    def _handle_message(self, msg):
        """Handle individual Kafka message"""
        value = msg.value()
        if not value:
            return

        try:
            self.processor.process_message(value)
            self.logger.info(
                f"Processed message from topic {msg.topic()} at offset {msg.offset()}",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
        except Exception as e:
            self.logger.error(f"Failed to process message: {e}",
                              extra={
                                  "topic": msg.topic(),
                                  "partition": msg.partition(),
                                  "offset": msg.offset(),
                              })
            raise

    def start(self):
        """Start consuming messages from Kafka"""
        while True:
            try:
                if self.consumer is None:
                    self.consumer = self._create_consumer()
                    self.logger.info("Connected to Kafka broker")

                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                self._handle_message(msg)
                self.consumer.commit(msg, asynchronous=False)

            except KeyboardInterrupt:
                self.logger.warning("Stopping consumer manually...")
                if self.consumer:
                    self.consumer.close()
                break

            except Exception as e:
                self.logger.error(f"Error occurred: {e}")
                if self.consumer:
                    try:
                        self.consumer.close()
                    except Exception:
                        pass
                self.consumer = None
                self.logger.warning("Disconnected from Kafka. Waiting 1 minutes before reconnect...")
                time.sleep(60)