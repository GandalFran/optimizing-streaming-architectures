CREATE TABLE clicks (
    session_id STRING,
    event_data STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'clicks-consumer-group'
);

CREATE TABLE clicks_enriched (
    session_id STRING,
    event_data STRING,
    enriched_field STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.KeyPartitioner',
    'sink.partitioner-keys' = 'session_id',
    'sink.parallelism' = '25'
);

INSERT INTO clicks_enriched (`session_id`, `event_data`, `enriched_field`, `event_time`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `event_data`,
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM clicks;
