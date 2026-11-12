CREATE TABLE src (
    `campaign_id` STRING,
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ad-impressions',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'ad-impressions-enriched-job',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE snk (
    `campaign_id` STRING,
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `enriched_at` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ad-impressions-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO snk (`campaign_id`, `payload`, `event_time`, `enriched_at`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       `payload`,
       `event_time`,
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM src;
