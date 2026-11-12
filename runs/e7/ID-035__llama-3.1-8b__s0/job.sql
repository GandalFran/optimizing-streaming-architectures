CREATE TABLE session_events (
    `user_id` STRING,
    `event` STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'session-events',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'session-events-group',
  'scan.startup.mode' = 'latest-offset',
  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer'
);

CREATE TABLE session_events_enriched (
    `user_id` STRING,
    `event` STRING,
    `derived_field1` STRING,
    `derived_field2` STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'session-events-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'properties.producer.acks' = 'all',
  'properties.retries' = '3',
  'properties.retry.backoff.ms' = '100'
);

INSERT INTO session_events_enriched (`user_id`, `event`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM session_events;
