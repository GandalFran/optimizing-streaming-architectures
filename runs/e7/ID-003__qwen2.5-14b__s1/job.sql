CREATE TABLE session_events (
    user_id STRING,
    event_time TIMESTAMP(3),
    session_id STRING,
    event STRING,
    event_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'session_events_consumer'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_time TIMESTAMP(3),
    session_id STRING,
    event STRING,
    derived_field1 STRING,
    derived_field2 INT,
    derived_field3 TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json'
);

INSERT INTO session_events_enriched (`user_id`, `event_time`, `session_id`, `event`, `derived_field1`, `derived_field2`, `derived_field3`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_time`,
       `session_id`,
       `event`,
       CAST(NULL AS STRING),
       CAST(NULL AS INT),
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM session_events;
