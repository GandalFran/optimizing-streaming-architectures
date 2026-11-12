CREATE TABLE app_logs (
    service STRING,
    log STRUCT<timestamp: TIMESTAMP,
    message: STRING>,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE app_logs_parsed (
    service STRING,
    log STRUCT<timestamp: TIMESTAMP,
    message: STRING,
    parsed_message: STRUCT<user: STRING,
    action: STRING,
    status: INT>>,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'app-logs-parsed',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partition-commit.trigger' = 'retained-after',
    'sink.partition-commit.retained-after' = '1d',
    'sink.partitioner' = 'round-robin',
    'sink.parallelism' = '1'
);

INSERT INTO app_logs_parsed (`service`, `log`, `message:`, `parsed_message:`, `action:`, `status:`, `__sent_ms`, `__seq`)
SELECT `service`,
       `log`,
       CAST(`message:` AS STRING),
       CAST(NULL AS STRUCT<user: STRING),
       CAST(NULL AS STRING),
       CAST(NULL AS INT>>),
       `__sent_ms`,
       `__seq`
FROM app_logs;
