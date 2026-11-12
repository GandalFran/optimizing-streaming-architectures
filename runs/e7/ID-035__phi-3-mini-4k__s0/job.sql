CREATE TABLE session_events (
    user_id STRING,
    event_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'session-events',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'properties.group.id' = 'flink-session-group',
  'format' = 'json'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_data STRING,
    enriched_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'session-events-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'properties.group.id' = 'flink-session-group',
  'format' = 'json'
);

INSERT INTO session_events_enriched (`user_id`, `event_data`, `enriched_data`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_data`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM session_events;
