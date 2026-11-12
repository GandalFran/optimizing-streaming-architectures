CREATE TABLE app_logs (
    service STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'app-logs',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'sink.partitioner' = 'round-robin',
  'sink.delivery-guarantee' = 'exactly-once',
  'execution.checkpointing.interval' = '5s',
  'execution.checkpointing.mode' = 'EXACTLY_ONCE'
);

CREATE TABLE app_logs_parsed (
    service STRING,
    event JSON,
    derived_field1 STRING,
    derived_field2 STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'app-logs-parsed',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'sink.partitioner' = 'round-robin',
  'sink.delivery-guarantee' = 'exactly-once',
  'execution.checkpointing.interval' = '5s',
  'execution.checkpointing.mode' = 'EXACTLY_ONCE'
);

INSERT INTO app_logs_parsed (`service`, `event`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `service`,
       `event`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM app_logs;
