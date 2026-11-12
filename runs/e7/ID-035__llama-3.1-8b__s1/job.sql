CREATE TABLE session_events (
    `user_id` VARCHAR,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'session-events',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'flink-group',
  'scan.startup.mode' = 'latest-offset',
  'value.format' = 'json'
);

CREATE TABLE session_events_enriched (
    `user_id` VARCHAR,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'session-events-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
  'properties.value.converter' = 'org.apache.kafka.connect.storage.StringConverter',
  'sink.partition.roterator' = 'round-robin'
);

INSERT INTO session_events_enriched (`user_id`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `__sent_ms`,
       `__seq`
FROM session_events;
