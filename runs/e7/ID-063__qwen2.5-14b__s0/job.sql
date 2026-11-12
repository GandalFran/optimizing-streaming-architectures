CREATE TABLE sensor_readings (
    sensor_id STRING,
    reading_data STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'sensor-readings-group',
    'scan.subtasks' = '5'
);

CREATE TABLE sensor_readings_clean (
    sensor_id STRING,
    reading_data STRING,
    derived_field1 STRING,
    derived_field2 STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings-clean',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin',
    'sink.parallelism' = '5'
);

INSERT INTO sensor_readings_clean (`sensor_id`, `reading_data`, `derived_field1`, `derived_field2`, `event_time`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `reading_data`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings;
