CREATE TABLE sensor_readings (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'value.format' = 'json',
  'key.format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'sink.checkpointing-mode' = 'EXACTLY_ONCE',
  'sink.checkpointing-interval' = '5000',
  'sink.parallelism' = '3'
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'format' = 'json',
  'key.format' = 'json',
  'value.format' = 'json',
  'sink.checkpointing-mode' = 'EXACTLY_ONCE',
  'sink.checkpointing-interval' = '5000',
  'sink.parallelism' = '3'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `event_time`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
