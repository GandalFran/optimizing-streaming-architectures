CREATE TABLE sensor_readings (
    sensor_id STRING,
    data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'format.derived-columns' = 'clean_data = data->>\'clean_data\''
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'sink.partition/table' = 'sensor_readings'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `data`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `data`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
