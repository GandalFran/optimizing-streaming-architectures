CREATE TABLE banking_transactions (
    account_id STRING,
    transaction JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'banking-transactions',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json'
);

CREATE TABLE banking_transactions_validated (
    account_id STRING,
    transaction JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'banking-transactions-validated',
  'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
