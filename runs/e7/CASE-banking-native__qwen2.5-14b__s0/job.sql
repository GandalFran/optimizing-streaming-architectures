CREATE TABLE banking_transactions (
    account_id BIGINT,
    transaction_id BIGINT,
    amount DECIMAL(19,4),
    transaction_time TIMESTAMP(3),
    transaction_type STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'banking-transactions-group',
    'scan.subtasks' = '25'
);

CREATE TABLE banking_transactions_validated (
    account_id BIGINT,
    transaction_id BIGINT,
    amount DECIMAL(19,4),
    transaction_time TIMESTAMP(3),
    transaction_type STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions-validated',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin',
    'sink.parallelism' = '25'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction_id`, `amount`, `transaction_time`, `transaction_type`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction_id`,
       `amount`,
       `transaction_time`,
       `transaction_type`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
