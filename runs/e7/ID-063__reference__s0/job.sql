CREATE TABLE src (
    `sensor_id` STRING,
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'sensor-readings-clean-job',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE snk (
    `sensor_id` STRING,
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `enriched_at` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor-readings-clean',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'sensor-readings-clean-tx',
  'properties.transaction.timeout.ms' = '600000'
);

INSERT INTO snk (`sensor_id`, `payload`, `event_time`, `enriched_at`, `__sent_ms`, `__seq`)
SELECT `sensor_id`,
       `payload`,
       `event_time`,
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM src;
