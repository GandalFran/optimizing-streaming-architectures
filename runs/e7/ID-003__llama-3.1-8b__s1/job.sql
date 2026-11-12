CREATE TABLE session_events (
    user_id STRING,
    event_data VARCHAR,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'session-events',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'session_events_group',
  'scan.startup.mode' = 'latest-offsets',
  'value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_data VARCHAR,
    field1 VARCHAR,
    field2 VARCHAR,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'session-events-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.producer.acks' = 'all',
  'properties.retries' = '3',
  'properties.retry.backoff.ms' = '100',
  'format' = 'json',
  'json.timestamp-format' = 'millis',
  'json.fail-on-missing-field' = 'true'
);

INSERT INTO session_events_enriched (`user_id`, `event_data`, `field1`, `field2`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_data`,
       CAST(NULL AS VARCHAR),
       CAST(NULL AS VARCHAR),
       `__sent_ms`,
       `__seq`
FROM session_events;
