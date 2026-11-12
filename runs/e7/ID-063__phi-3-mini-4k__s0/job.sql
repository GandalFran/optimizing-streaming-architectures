CREATE TABLE sensor_readings (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'sensor-reading-group',
  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'format' = 'json'
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'sensor-reading-group',
  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'format' = 'json'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `event_time`, `data`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `event_time`,
       `data`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
