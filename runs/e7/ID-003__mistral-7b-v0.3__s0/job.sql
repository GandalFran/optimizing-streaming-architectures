CREATE TABLE session_events (
    user_id STRING,
    event_data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'properties.group.id' = 'session-events-source',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_data JSON,
    derived_field1 INT,
    derived_field2 STRING
) WITH (
    'connector' = 'print'
);

CREATE TABLE session_events_enriched_sink (
    user_id STRING,
    event_data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'properties.key.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
    'properties.value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer'
);

INSERT INTO session_events_enriched_sink (`user_id`, `event_data`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_data`,
       `__sent_ms`,
       `__seq`
FROM session_events;
