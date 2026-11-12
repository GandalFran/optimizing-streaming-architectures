CREATE TABLE app_logs (
    service VARCHAR,
    TIMESTAMP_TYPE AS PROCTIME(),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR TIMESTAMP_TYPE AS PROCTIME() - INTERVAL '10 seconds'
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'app-logs-consumer-group',
    'format' = 'json'
);

CREATE TABLE app_logs_parsed (
    service VARCHAR,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs-parsed',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.key.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
    'properties.value.serializer' = 'org.apache.kafka.common.serialization.KafkaJsonSerializer'
);

INSERT INTO app_logs_parsed (`service`, `event_time`, `__sent_ms`, `__seq`)
SELECT `service`,
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM app_logs;
