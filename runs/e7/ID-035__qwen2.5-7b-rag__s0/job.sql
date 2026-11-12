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
  'bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'value.format' = 'json',
  'key.format' = 'json',
  'update-mode' = 'upsert',
  'scan.startup.mode' = 'earliest-offset',
  'sink.partitioner' = 'round-robin'
);

CREATE TABLE session_events_enriched (
    user_id STRING,
    event_time TIMESTAMP(3),
    event_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'session-events-enriched',
  'bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'sink.partitioner' = 'round-robin',
  'sink.delivery.guarantee' = 'at-least-once'
);

INSERT INTO session_events_enriched (`user_id`, `event_time`, `event_data`, `__sent_ms`, `__seq`)
SELECT `user_id`,
       `event_time`,
       `event_data`,
       `__sent_ms`,
       `__seq`
FROM session_events;
