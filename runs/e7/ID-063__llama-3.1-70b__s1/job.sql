CREATE TABLE sensor_readings (
    `sensor_id` STRING,
    `reading` STRING,
    `event_time` TIMESTAMP(3),
    `PROCTIME` AS PROCTIME(),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'key.format' = 'json',
  'value.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'value.json.ignore-parse-errors' = 'false',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE sensor_readings_clean (
    `sensor_id` STRING,
    `reading` STRING,
    `event_time` TIMESTAMP(3),
    `derived_field1` STRING,
    `derived_field2` STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'key.format' = 'json',
  'value.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'value.json.ignore-parse-errors' = 'false',
  'json.timestamp-format.standard' = 'ISO-8601',
  'upsert-kafka-partitioner' = 'fixed'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `reading`, `event_time`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `reading`,
       `event_time`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
