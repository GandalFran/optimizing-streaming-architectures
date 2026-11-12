CREATE TABLE session_events (
    user_id STRING,
    event_data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'session-events-source',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_data JSON,
    derived_field1 INT,
    derived_field2 STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'session-events-enriched-sink'
);

INSERT INTO session_events_enriched (`user_id`, `event_data`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_data`,
       CAST(NULL AS INT),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM session_events;
