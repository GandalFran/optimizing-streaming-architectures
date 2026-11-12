CREATE TABLE session_events (
    user_id STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
)
WITH (
    'connector' = 'kafka',
    'topic' = 'session-events',
    'properties.bootstrap-server' = 'kafka:9092',
    'format' = 'json',
    'scan.start-offset' = 'latest',
    'value.format.option' = 'skipMalformedRecords=true'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
)
WITH (
    'connector' = 'kafka',
    'topic' = 'session-events-enriched',
    'properties.bootstrap-server' = 'kafka:9092',
    'format' = 'json',
    'sink.partitioner' = 'fixed',
    'sink.checkpointing-mode' = 'exactly-once',
    'sink.checkpointing-interval' = '5000',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.max-bytes' = '5242880', 
    'sink.buffer-flush.interval' = '2000',
    'sink.parallelism' = '3',
    'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO session_events_enriched (`user_id`, `event_time`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM session_events;
