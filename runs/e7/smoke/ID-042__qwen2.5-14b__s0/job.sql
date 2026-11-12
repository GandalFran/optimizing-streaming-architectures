CREATE TABLE trades (
    instrument_id STRING,
    trade_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'trades',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'trades-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE trades_enriched (
    instrument_id STRING,
    enriched_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'trades-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.KeyPartitioner',
    'sink.partitioner.key-extractor' = 'org.apache.flink.connector.kafka.sink.KeySelector'
);

INSERT INTO trades_enriched (`instrument_id`, `enriched_data`, `__sent_ms`, `__seq`)
SELECT `instrument_id`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM trades;
