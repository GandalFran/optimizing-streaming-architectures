CREATE TABLE clicks (
    session_id STRING,
    event STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'scan.startup.offsets.file.path' = '/tmp/flink_state/flink-state/checkpoints/startup-offsets.json',
  'connector.incremental-poll.enabled' = 'true',
  'connector.incremental-poll.interval' = '1000',
  'connector.incremental-poll.max-offset-difference' = '200000',
  'connector.incremental-poll.max-offset-difference-per-partition' = '100000',
  'sink.type' = 'kafka',
  'sink.topic' = 'clicks-enriched',
  'sink.bootstrap.servers' = 'kafka:9092',
  'sink.partition-grouper' = 'round-robin',
  'sink.partition-assigner' = 'round-robin',
  'sink.partitioner' = 'round-robin',
  'sink.max-rate' = '5000'
);

CREATE TABLE clicks_enriched (
    session_id STRING,
    event STRING,
    derived_field STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'scan.startup.offsets.file.path' = '/tmp/flink_state/flink-state/checkpoints/startup-offsets.json',
  'connector.incremental-poll.enabled' = 'true',
  'connector.incremental-poll.interval' = '1000',
  'connector.incremental-poll.max-offset-difference' = '200000',
  'connector.incremental-poll.max-offset-difference-per-partition' = '100000',
  'sink.type' = 'kafka',
  'sink.topic' = 'clicks-enriched',
  'sink.bootstrap.servers' = 'kafka:9092',
  'sink.partition-grouper' = 'round-robin',
  'sink.partition-assigner' = 'round-robin',
  'sink.partitioner' = 'round-robin',
  'sink.max-rate' = '5000'
);

INSERT INTO clicks_enriched (`session_id`, `event`, `derived_field`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `event`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM clicks;
