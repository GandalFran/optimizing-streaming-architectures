CREATE TABLE app_logs (
    service VARCHAR,
    event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'app-logs-source',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE app_logs_parsed (
    service VARCHAR,
    enriched_event JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs-parsed',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.key.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
    'properties.value.serializer' = 'org.apache.kafka.common.serialization.JsonSerializer',
    'format' = 'json'
);

INSERT INTO app_logs_parsed (`service`, `enriched_event`, `__sent_ms`, `__seq`)
SELECT `service`,
       CAST(NULL AS JSON),
       `__sent_ms`,
       `__seq`
FROM app_logs;
