CREATE TABLE ad_impressions (
    campaign_id STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR campaign_id AS PROCTIME(),
    PRIMARY KEY (campaign_id) NOT ENFORCED
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'ad-impressions',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'properties.group.id' = 'ad-impressions-group',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR campaign_id AS PROCTIME(),
    PRIMARY KEY (campaign_id) NOT ENFORCED
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'ad-impressions-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'properties.transactional.id' = 'ad-impressions-enriched-group'
);

INSERT INTO ad_impressions_enriched (`campaign_id`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       `__sent_ms`,
       `__seq`
FROM ad_impressions;
