CREATE TABLE session_events (
    user_id STRING,
    event_json STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap-server' = 'kafka:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'value.format' = 'json',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'json'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_json STRING,
    enriched_field1 STRING,
    enriched_field2 STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap-server' = 'kafka:9092',
    'format' = 'json',
    'key.format' = 'json',
    'sink.checkpointing-mode' = 'METADATA',
    'sink.parallelism' = '3',
    'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO session_events_enriched (`user_id`, `event_json`, `enriched_field1`, `enriched_field2`, `event_time`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_json`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM session_events;
