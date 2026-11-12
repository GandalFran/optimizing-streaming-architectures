CREATE TABLE banking_transactions (
    account_id STRING,
    transaction_id STRING,
    amount DECIMAL(10,2),
    timestamp TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'banking-transactions',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'update-mode' = 'append',
  'sink.partitioner' = 'round-robin',
  'sink.partition-commit.policy.type' = 'all-complete'
);

CREATE TABLE banking_transactions_validated (
    account_id STRING,
    transaction_id STRING,
    amount DECIMAL(10,2),
    timestamp TIMESTAMP(3),
    validated BOOLEAN,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'banking-transactions-validated',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'format' = 'json',
  'sink.partitioner' = 'round-robin',
  'sink.partition-commit.policy.type' = 'all-complete'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction_id`, `amount`, `timestamp`, `validated`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction_id`,
       `amount`,
       `timestamp`,
       CAST(NULL AS BOOLEAN),
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
