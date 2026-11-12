CREATE TABLE ad_impressions (
    campaign_id STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ad-impressions',
  'properties.bootstrap-server' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'update-mode' = 'incrementing',
  'key.fields' = 'campaign_id',
  'value.format.derive-missing-values' = 'true'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ad-impressions-enriched',
  'properties.bootstrap-server' = 'kafka:9092',
  'format' = 'json',
  'sink.partitioner' = 'round-robin',
  'sink.flush.max-rows' = '10000',
  'sink.flush.interval' = '1000'
);

INSERT INTO ad_impressions_enriched (`campaign_id`, `event_time`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM ad_impressions;
