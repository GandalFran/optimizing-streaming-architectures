CREATE TABLE sensor_readings (
    sensor_id INT,
    timestamp BIGINT,
    data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id, timestamp)
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'flink-sensor-source',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE sensor_readings_clean_sink (
    sensor_id INT,
    timestamp BIGINT,
    data JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id, timestamp)
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.key.serializer' = 'org.apache.kafka.common.serialization.LongSerializer',
  'properties.value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer'
);

INSERT INTO sensor_readings_clean_sink (`sensor_id`, `timestamp`, `data`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `timestamp`,
       `data`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
