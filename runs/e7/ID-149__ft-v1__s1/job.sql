CREATE TABLE ad_impressions (
    campaign_id STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin',
    'sink.partitioner.num-partitions' = '5',
    'sink.checkpointing' = 'true'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id STRING,
    event JSON
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json'
);

INSERT INTO ad_impressions_enriched (`campaign_id`, `event`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       `event`,
       `__sent_ms`,
       `__seq`
FROM ad_impressions;
