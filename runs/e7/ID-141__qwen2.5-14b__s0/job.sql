CREATE TABLE app_logs (
    service STRING,
    log_event JSONB,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'properties.group.id' = 'app-logs-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE app_logs_parsed (
    service STRING,
    log_event JSONB,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs-parsed',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);

INSERT INTO app_logs_parsed (`service`, `log_event`, `event_time`, `__sent_ms`, `__seq`)
SELECT `service`,
       `log_event`,
       `event_time`,
       `__sent_ms`,
       `__seq`
FROM app_logs;
