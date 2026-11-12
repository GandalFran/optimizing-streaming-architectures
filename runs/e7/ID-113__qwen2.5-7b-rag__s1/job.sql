CREATE TABLE clicks (
    session_id STRING,
    event_time TIMESTAMP(3),
    event TEXT,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'sink.partitioner' = 'fixed',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.checkpoint.min-pause' = '0',
    'sink.checkpoint.max-pause' = '0',
    'sink.checkpoint.interval' = '5000',
    'scan.startup.mode' = 'earliest-offset',
    'scan.startup.max-offsets-per-trigger' = '100000',
    'connector.table_partition_discovery.interval' = '300000'
);

CREATE TABLE clicks_enriched (
    session_id STRING,
    event_time TIMESTAMP(3),
    event TEXT,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'sink.partitioner' = 'fixed',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.checkpoint.min-pause' = '0',
    'sink.checkpoint.max-pause' = '0',
    'sink.checkpoint.interval' = '5000',
    'sink.partitioner' = 'fixed',
    'sink.parallelism' = '8'
);

INSERT INTO clicks_enriched (`session_id`, `event_time`, `event`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `event_time`,
       `event`,
       `__sent_ms`,
       `__seq`
FROM clicks;
