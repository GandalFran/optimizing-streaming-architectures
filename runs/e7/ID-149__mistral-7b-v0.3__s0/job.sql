CREATE TABLE ad_impressions (
    campaign_id BIGINT,
    event_data STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'properties.group.id' = 'flink-ad-impressions-source',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ad_impressions_enriched (
    campaign_id BIGINT,
    impression_id STRING,
    timestamp BIGINT,
    ad_id STRING,
    user_id STRING,
    ip_address STRING,
    device_type STRING,
    location STRING,
    impression_type STRING,
    ad_creative STRING,
    impression_url STRING,
    impression_category STRING,
    impression_duration BIGINT,
    click_url STRING,
    click_timestamp BIGINT,
    click_id STRING,
    click_type STRING,
    conversion_url STRING,
    conversion_timestamp BIGINT,
    conversion_id STRING,
    conversion_type STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-impressions-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json'
);

INSERT INTO ad_impressions_enriched (`campaign_id`, `impression_id`, `timestamp`, `ad_id`, `user_id`, `ip_address`, `device_type`, `location`, `impression_type`, `ad_creative`, `impression_url`, `impression_category`, `impression_duration`, `click_url`, `click_timestamp`, `click_id`, `click_type`, `conversion_url`, `conversion_timestamp`, `conversion_id`, `conversion_type`, `__sent_ms`, `__seq`)
SELECT `campaign_id`,
       CAST(NULL AS STRING),
       CAST(NULL AS BIGINT),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS BIGINT),
       CAST(NULL AS STRING),
       CAST(NULL AS BIGINT),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS BIGINT),
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM ad_impressions;
