CREATE TABLE src (
    `card_id` STRING,
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'card-transactions',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'card-transactions-enriched-job',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE snk (
    `card_id` STRING,
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `enriched_at` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'card-transactions-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'card-transactions-enriched-tx',
  'properties.transaction.timeout.ms' = '600000'
);

INSERT INTO snk (`card_id`, `payload`, `event_time`, `enriched_at`, `__sent_ms`, `__seq`)
SELECT `card_id`,
       `payload`,
       `event_time`,
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM src;
