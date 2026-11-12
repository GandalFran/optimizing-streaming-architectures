CREATE TABLE banking_transactions (
    account_id BIGINT,
    transaction JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'banking-transactions-source',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE banking_transactions_validated (
    account_id BIGINT,
    transaction JSON,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'banking-transactions-validated',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.key.serializer' = 'org.apache.kafka.common.serialization.LongSerializer',
    'properties.value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer'
);

INSERT INTO banking_transactions_validated (`account_id`, `transaction`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `transaction`,
       `__sent_ms`,
       `__seq`
FROM banking_transactions;
