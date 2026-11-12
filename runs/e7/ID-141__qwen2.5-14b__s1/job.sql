CREATE TABLE app_logs (
    log_event JSONB,
    service STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR processing_time AS processing_time - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset',
    'properties.group.id' = 'app_logs_consumer_group'
);

CREATE TABLE app_logs_parsed (
    log_event JSONB,
    service STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs-parsed',
    'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);

INSERT INTO app_logs_parsed (`log_event`, `service`, `__sent_ms`, `__seq`)
SELECT `log_event`,
       `service`,
       `__sent_ms`,
       `__seq`
FROM app_logs;
