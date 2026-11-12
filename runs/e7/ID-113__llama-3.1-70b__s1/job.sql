CREATE TABLE clicks (
    `session_id` STRING,
    `event` STRING,
    `event_time` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'clicks-group',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false'
);

CREATE TABLE clicks_enriched (
    `session_id` STRING,
    `event` STRING,
    `event_time` TIMESTAMP(3),
    `derived_field_1` STRING,
    `derived_field_2` STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (`session_id`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.transactional.id.prefix' = 'clicks-enriched-',
    'upsert-kafka' = 'true',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false'
);

INSERT INTO clicks_enriched (`session_id`, `event`, `event_time`, `derived_field_1`, `derived_field_2`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `event`,
       `event_time`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM clicks;
