"""
Central configuration for the Telecom Network Analytics platform.

Every setting is read from environment variables (see .env.example for the
full list and safe sample values). No component should read os.environ
directly outside this module - import `settings` instead so there is a
single source of truth for configuration.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class PostgresSettings(BaseModel):
    """Warehouse Postgres: raw / staging / curated schemas."""

    user: str = os.getenv("POSTGRES_USER", "telecom_app")
    password: str = os.getenv("POSTGRES_PASSWORD", "")
    db: str = os.getenv("POSTGRES_DB", "telecom_dwh")
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class SourcePostgresSettings(BaseModel):
    """The OLTP-style source database the generator writes into.

    Debezium (Kafka Connect) reads this database's WAL via logical replication
    and streams inserts onto the Kafka topic - nothing else touches it.
    """

    user: str = os.getenv("SOURCE_POSTGRES_USER", "source_app")
    password: str = os.getenv("SOURCE_POSTGRES_PASSWORD", "")
    db: str = os.getenv("SOURCE_POSTGRES_DB", "telecom_source")
    host: str = os.getenv("SOURCE_POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("SOURCE_POSTGRES_PORT", "5433"))

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class KafkaSettings(BaseModel):
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_network_events: str = os.getenv("KAFKA_TOPIC_NETWORK_EVENTS", "telecom.source.network_events")


class KafkaConnectSettings(BaseModel):
    url: str = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")


class SparkSettings(BaseModel):
    master_url: str = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")


class RustFSSettings(BaseModel):
    endpoint_url: str = os.getenv("RUSTFS_ENDPOINT_URL", "http://localhost:9000")
    access_key: str = os.getenv("RUSTFS_ACCESS_KEY", "")
    secret_key: str = os.getenv("RUSTFS_SECRET_KEY", "")
    raw_bucket: str = os.getenv("RUSTFS_RAW_BUCKET", "raw-network-events")


class GeneratorSettings(BaseModel):
    events_per_second: float = float(os.getenv("GENERATOR_EVENTS_PER_SECOND", "5"))
    dirty_record_rate: float = float(os.getenv("GENERATOR_DIRTY_RECORD_RATE", "0.05"))
    schema_drift_rate: float = float(os.getenv("GENERATOR_SCHEMA_DRIFT_RATE", "0.02"))
    subscriber_pool_size: int = int(os.getenv("GENERATOR_SUBSCRIBER_POOL_SIZE", "2000"))
    cell_count: int = int(os.getenv("GENERATOR_CELL_COUNT", "60"))
    seed: int = int(os.getenv("GENERATOR_SEED", "42"))


class Settings(BaseModel):
    postgres: PostgresSettings = PostgresSettings()
    source_postgres: SourcePostgresSettings = SourcePostgresSettings()
    kafka: KafkaSettings = KafkaSettings()
    kafka_connect: KafkaConnectSettings = KafkaConnectSettings()
    spark: SparkSettings = SparkSettings()
    rustfs: RustFSSettings = RustFSSettings()
    generator: GeneratorSettings = GeneratorSettings()


settings = Settings()
