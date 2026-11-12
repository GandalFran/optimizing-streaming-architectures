CREATE TABLE banking_transactions (
    account_id STRING,
    transaction_id STRING,
    amount DECIMAL(10, 2),
    timestamp TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'banking-transactions',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'properties.group.id' = 'banking-transactions-group',
  'scan.startup.mode' = 'latest-offset',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.security.protocol.map' = 'PLAIN:SSL',
  'properties.ssl.truststore.location' = '/path/to/truststore.jks',
  'properties.ssl.truststore.password' = 'truststore-password',
  'properties.ssl.keystore.location' = '/path/to/keystore.jks',
  'properties.ssl.keystore.password' = 'keystore-password',
  'properties.ssl.key.password' = 'key-password'
);

CREATE TABLE banking_transactions_validated (
    account_id STRING,
    transaction_id STRING,
    amount DECIMAL(10, 2),
    timestamp TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'topic' = 'banking-transactions-validated',
  'properties.bootstrap.servers' = '127.0.0.1:12500,127.0.0.1:12501,127.0.0.1:12502',
  'properties.group.id' = 'banking-transactions-group',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.security.protocol.map' = 'PLAIN:SSL',
  'properties.ssl.truststore.location' = '/path/to/truststore.jks',
  'properties.ssl.truststore.password' = 'truststore-password',
  'properties.ssl.keystore.location' = '/path/to/keystore.jks',
  'properties.ssl.keystore.password' = 'keystore-password',
  'properties.ssl.key.password' = 'key-password'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction_id`, `amount`, `timestamp`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction_id`,
       `amount`,
       `timestamp`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
