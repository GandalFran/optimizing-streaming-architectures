CREATE TABLE clicks (
    session_id BIGINT,
    click_event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR processing_time AS processing_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE clicks_enriched (
    session_id BIGINT,
    click_event JSON,
    derived_field1 STRING,
    derived_field2 INT,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);

INSERT INTO clicks_enriched (`session_id`, `click_event`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `click_event`,
       CAST(NULL AS STRING),
       CAST(NULL AS INT),
       `__sent_ms`,
       `__seq`
FROM clicks;
