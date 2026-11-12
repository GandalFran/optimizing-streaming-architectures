CREATE TABLE banking_transactions (
    `account_id` STRING,
    `transaction_id` STRING,
    `amount` DECIMAL(10, 2),
    `timestamp` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR `timestamp` AS `timestamp`
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'banking-transactions-group',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE banking_transactions_validated (
    `account_id` STRING,
    `transaction_id` STRING,
    `amount` DECIMAL(10, 2),
    `timestamp` TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions-validated',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'banking-transactions-validated-group',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'sink.partitioner' = 'org.apache.flink.streaming.connectors.kafka.FlinkKafkaPartitioner',
    'sink.semantic' = 'exactly-once'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction_id`, `amount`, `timestamp`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction_id`,
       `amount`,
       `timestamp`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
