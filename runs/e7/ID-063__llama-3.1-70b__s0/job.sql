CREATE TABLE sensor_readings (
    `sensor_id` STRING,
    `reading` STRING,
    `event_time` TIMESTAMP(3),
    `watermark` WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'sensor-group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE sensor_readings_clean (
    `sensor_id` STRING,
    `reading` STRING,
    `event_time` TIMESTAMP(3),
    `derived_field` STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'sensor-group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true',
  'sink.partitioner' = 'round-robin'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `reading`, `event_time`, `derived_field`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `reading`,
       `event_time`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
