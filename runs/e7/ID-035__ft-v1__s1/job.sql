CREATE TABLE session_events (
    user_id STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'connector.partition-subset-strategy' = 'hash'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event JSON,
    derived_field1 INT,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json'
);

INSERT INTO session_events_enriched (`user_id`, `event`, `derived_field1`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event`,
       CAST(NULL AS INT),
       `__sent_ms`,
       `__seq`
FROM session_events;
