CREATE TABLE sensor_readings (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    event_value DOUBLE,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json',
  'update-mode' = 'append'
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    event_value DOUBLE,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json',
  'update-mode' = 'append'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `event_time`, `event_value`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `event_time`,
       `event_value`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
