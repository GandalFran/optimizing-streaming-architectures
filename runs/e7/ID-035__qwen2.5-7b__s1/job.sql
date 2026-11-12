CREATE TABLE session_events (
    user_id STRING,
    event_time TIMESTAMP(3),
    event_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'scan.max.wait.time' = '10000',
    'sink.partitioner.class' = 'org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_time TIMESTAMP(3),
    event_data STRING,
    derived_field STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'sink.partitioner.class' = 'org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner'
);

INSERT INTO session_events_enriched (`user_id`, `event_time`, `event_data`, `derived_field`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_time`,
       `event_data`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM session_events;
