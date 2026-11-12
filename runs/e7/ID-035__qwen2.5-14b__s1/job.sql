CREATE TABLE session_events (
    user_id STRING,
    event_data STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'properties.group.id' = 'session-event-consumer',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_data STRING,
    derived_field_1 STRING,
    derived_field_2 STRING,
    event_time TIMESTAMP(3),
    event_timestamp AS PROCTIME(),
    ROWTIME() AS event_time,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.DefaultPartitioner',
    'sink.max-concurrent-requests' = '128'
);

INSERT INTO session_events_enriched (`user_id`, `event_data`, `derived_field_1`, `derived_field_2`, `event_time`, `ROWTIME`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_data`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `event_time`,
       CAST(NULL AS () AS event_time),
       `__sent_ms`,
       `__seq`
FROM session_events;
