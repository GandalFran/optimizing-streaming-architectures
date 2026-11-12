CREATE TABLE app_logs (
    service STRING,
    event_json STRING,
    error_code STRING,
    timestamp TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'app-logs',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'flink-app-logs-consumer',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json'
);

CREATE TABLE app_logs_parsed (
    service STRING,
    event_json STRING,
    error_code STRING,
    timestamp TIMESTAMP(3),
    parsed_field STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'app-logs-parsed',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'flink-app-logs-parsed-consumer',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'value.format' = 'json'
);

INSERT INTO app_logs_parsed (`service`, `event_json`, `error_code`, `timestamp`, `parsed_field`, `__sent_ms`, `__seq`)
SELECT `service`,
       `event_json`,
       `error_code`,
       `timestamp`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM app_logs;
