CREATE TABLE session_events (
    user_id STRING,
    event_json STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'sink.partitioner' = 'round-robin'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_json STRING,
    event_time TIMESTAMP(3),
    derived_field1 INT,
    derived_field2 STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'sink.partitioner' = 'round-robin',
    'sink.checkpoint.min-records-per-batch' = '1000'
);

INSERT INTO session_events_enriched (`user_id`, `event_json`, `event_time`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_json`,
       `event_time`,
       CAST(NULL AS INT),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM session_events;
