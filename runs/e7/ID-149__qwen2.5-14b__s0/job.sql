CREATE TABLE ad_impressions (
    campaign_id STRING,
    impression_data STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'ad-impressions-group',
    'scan.subtasks' = '5'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id STRING,
    impression_data STRING,
    enriched_data STRING,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);

INSERT INTO ad_impressions_enriched (`campaign_id`, `impression_data`, `enriched_data`, `event_time`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       `impression_data`,
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM ad_impressions;
