CREATE TABLE session_events (
    user_id STRING,
    event_data STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_data STRING,
    enriched_field STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);

INSERT INTO session_events_enriched (`user_id`, `event_data`, `enriched_field`, `event_time`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_data`,
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM session_events;
