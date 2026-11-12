CREATE TABLE banking_transactions (
    account_id VARCHAR,
    event_timestamp TIMESTAMP,
    amount DECIMAL(10, 2),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'banking-transactions',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'banking-transactions-group',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json',
  'json.timestamp-format.standard' = 'ISO 8601',
  'json.fail-on-missing-field' = 'true',
  'json.timestamp-format' = 'ISO 8601'
);

CREATE TABLE banking_transactions_validated (
    account_id VARCHAR,
    event_timestamp TIMESTAMP,
    amount DECIMAL(10, 2),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'banking-transactions-validated',
  'properties.bootstrap.servers' = '127.0.0.1:13000,127.0.0.1:13001,127.0.0.1:13002',
  'properties.group.id' = 'banking-transactions-validated-group',
  'producer.default-acks' = 'all',
  'producer.acks' = 'all',
  'producer.retries' = 3,
  'producer.max.in.flight.requests.per.connection' = 1,
  'producer.linger.ms' = 100,
  'producercompression.type' = 'gzip'
);

INSERT INTO banking_transactions_validated (`account_id`, `event_timestamp`, `amount`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `event_timestamp`,
       `amount`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
