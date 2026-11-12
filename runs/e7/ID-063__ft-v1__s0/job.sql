CREATE TABLE sensor_readings (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    temperature DOUBLE,
    humidity DOUBLE,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'sink.partition-commit.policy.type' = 'all-to-same'
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    temperature DOUBLE,
    humidity DOUBLE,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'format' = 'json',
  'sink.partitioner.type' = 'round-robin',
  'sink.partition-commit.policy.type' = 'all-to-same'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `event_time`, `temperature`, `humidity`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `event_time`,
       `temperature`,
       `humidity`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
