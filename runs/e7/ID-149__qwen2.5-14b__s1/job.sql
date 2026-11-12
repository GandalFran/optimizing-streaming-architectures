CREATE TABLE ad_impressions (
    campaign_id STRING,
    impression_time TIMESTAMP(3),
    device STRING,
    location STRING,
    ad_size STRING,
    user_id BIGINT,
    event_time AS PROCTIME(),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '500' MS
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'ad-impressions-group'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id STRING,
    impression_time TIMESTAMP(3),
    device STRING,
    location STRING,
    ad_size STRING,
    user_id BIGINT,
    enriched_field STRING,
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

INSERT INTO ad_impressions_enriched (`campaign_id`, `impression_time`, `device`, `location`, `ad_size`, `user_id`, `enriched_field`, `event_time`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       `impression_time`,
       `device`,
       `location`,
       `ad_size`,
       `user_id`,
       CAST(NULL AS STRING),
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM ad_impressions;
