CREATE DATABASE IF NOT EXISTS telecom_db;
USE telecom_db;

CREATE TABLE IF NOT EXISTS cdr_logs (
    event_id UUID,
    timestamp DateTime,
    msisdn String,
    imsi String,
    event_type Enum8('VOICE' = 1, 'SMS' = 2, 'DATA' = 3),
    duration_sec UInt32,
    bytes_transferred UInt64,
    cell_id String,
    ip_address String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_type, timestamp, msisdn);

DROP TABLE IF EXISTS cdr_logs_mv;
DROP TABLE IF EXISTS cdr_logs_kafka;

CREATE TABLE cdr_logs_kafka (
    event_id String,
    timestamp String,
    msisdn String,
    imsi String,
    event_type String,
    duration_sec UInt32,
    bytes_transferred UInt64,
    cell_id String,
    ip_address String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'telecom.cdr.events',
         kafka_group_name = 'clickhouse_cdr_consumer_final',
         kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW cdr_logs_mv TO cdr_logs AS
SELECT 
    toUUIDOrNull(event_id) AS event_id,
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    msisdn,
    imsi,
    CAST(event_type AS Enum8('VOICE' = 1, 'SMS' = 2, 'DATA' = 3)) AS event_type,
    duration_sec,
    bytes_transferred,
    cell_id,
    ip_address
FROM cdr_logs_kafka;
