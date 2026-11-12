CREATE TABLE session_events_source (
    user_id STRING,
    event_json STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'session-events-source',
  'format' = 'json',
  'topic' = 'session-events',
  'key' = 'user_id',
  'value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_json STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'session-events-enriched',
  'topic' = 'session-events-enriched',
  'key' = 'user_id',
  'value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer'
);

INSERT INTO session_events_enriched (`user_id`, `event_json`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_json`,
       `__sent_ms`,
       `__seq`
FROM session_events_source;
