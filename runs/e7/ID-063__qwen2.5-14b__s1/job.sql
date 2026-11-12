CREATE TABLE sensor_readings_source (
    sensor_id STRING,
    reading_data STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'sensor-readings-group'
);

CREATE TABLE sensor_readings_sink (
    sensor_id STRING,
    reading_data STRING,
    enriched_data STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-readings-clean',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.GreedyPartitioner',
    'sink.parallelism' = '10'
);

INSERT INTO sensor_readings_sink (`sensor_id`, `reading_data`, `enriched_data`, `event_time`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `reading_data`,
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM sensor_readings_source;
