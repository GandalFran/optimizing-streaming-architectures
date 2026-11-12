CREATE TABLE banking_transactions (
    account_id BIGINT,
    transaction_id BIGINT,
    amount DECIMAL(10, 2),
    timestamp TIMESTAMP(3),
    event_time AS PROCTIME(),
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'banking-transactions',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'banking-transactions-group',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE banking_transactions_validated (
    account_id BIGINT,
    transaction_id BIGINT,
    amount DECIMAL(10, 2),
    timestamp TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'banking-transactions-validated',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'format' = 'json',
  'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.KeyPartitioner',
  'sink.parallelism' = '25'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction_id`, `amount`, `timestamp`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction_id`,
       `amount`,
       `timestamp`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
