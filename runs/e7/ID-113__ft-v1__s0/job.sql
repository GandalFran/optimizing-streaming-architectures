CREATE TABLE clicks (
    session_id STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE clicks_enriched (
    session_id STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json'
);

INSERT INTO clicks_enriched (`session_id`, `event`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `event`,
       `__sent_ms`,
       `__seq`
FROM clicks;
