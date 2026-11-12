CREATE TABLE sensor_readings (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    derived_field1 INT,
    derived_field2 STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'value.format' = 'json',
  'key.format' = 'json',
  'value deserialization-schema' = 'org.apache.flink.formats.json.JsonDeserializationSchema',
  'key deserialization-schema' = 'org.apache.flink.formats.json.JsonDeserializationSchema',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'flink-consumer-group',
  'update-mode' = 'upsert'
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    event_time TIMESTAMP(3),
    derived_field1 INT,
    derived_field2 STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'value.format' = 'json',
  'key.format' = 'json',
  'value deserialization-schema' = 'org.apache.flink.formats.json.JsonDeserializationSchema',
  'key deserialization-schema' = 'org.apache.flink.formats.json.JsonDeserializationSchema',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'flink-consumer-group',
  'update-mode' = 'upsert'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `event_time`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `event_time`,
       `derived_field1`,
       `derived_field2`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
