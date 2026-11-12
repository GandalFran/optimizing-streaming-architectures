CREATE TABLE session_events (
    user_id STRING,
    event JSON,
    event_time TIMESTAMP(3) GENERATED ALWAYS AS (event['time'] AS TIMESTAMP(3)),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'sink.topic.name' = 'session-events-enriched'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event JSON,
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

INSERT INTO session_events_enriched (`user_id`, `event`, `event_time`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event`,
       CAST(`event_time` AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM session_events;
