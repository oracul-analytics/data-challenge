from dataclasses import dataclass
import uuid


@dataclass
class RedpandaProducerConfig:
    bootstrap_servers: str
    topic: str
    linger_ms: int
    batch_size: int
    compression_type: str
    acks: int
    max_in_flight: int
    queue_buffering_max_messages: int
    queue_buffering_max_kbytes: int


@dataclass
class RedpandaConsumerConfig:
    bootstrap_servers: str
    group_id: str
    topic: str
    timeout_seconds: float
    auto_offset_reset: str
    enable_auto_commit: bool


@dataclass
class KafkaRuntimeConfig:
    topic_name: str
    group_id: str

    @classmethod
    def random(cls) -> "KafkaRuntimeConfig":
        uid = uuid.uuid4().hex
        return cls(f"events_{uid}", f"dq_monitor_{uid}")
