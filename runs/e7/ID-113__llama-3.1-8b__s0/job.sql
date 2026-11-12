CREATE TABLE clicks (
    session_id STRING,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'kafka.bootstrap.servers' = 'kafka:9092',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'scan.startup.mode' = 'latest-offsets',
  'value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer'
);

CREATE TABLE clicks_enriched (
    session_id STRING,
    event JSON,
    derived_field1 STRING,
    derived_field2 STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'kafka.bootstrap.servers' = 'kafka:9092',
  'topic' = 'clicks-enriched',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer'
);

CREATE TABLE filtered_and_enriched (
    session_id STRING,
    event JSON,
    derived_field1 STRING,
    derived_field2 STRING
) WITH (
  'connector.type' = 'sql',
  'connector.version' = 'universal'
);

INSERT INTO clicks_enriched (`session_id`, `event`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `session_id`,
       `event`,
       CAST(NULL AS STRING),
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM clicks;
