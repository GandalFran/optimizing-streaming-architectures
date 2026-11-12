CREATE TABLE banking_transactions (
    account_id BIGINT,
    transaction_data ROW<...>,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json'
);

CREATE TABLE banking_transactions_validated (
    account_id BIGINT,
    transaction_data ROW<...>,
    event_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions-validated',
    'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
    'format' = 'json',
    'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.KeyPartitioner',
    'key.format' = 'raw'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction_data`, `event_time`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction_data`,
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
