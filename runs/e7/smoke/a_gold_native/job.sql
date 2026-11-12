CREATE TABLE src (
    account_id STRING,
    amount DOUBLE,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'banking-transactions', 'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'properties.group.id' = 'validator', 'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest', 'format' = 'json');

CREATE TABLE snk (
    account_id STRING,
    amount DOUBLE,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'banking-transactions-validated', 'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
  'format' = 'json', 'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'bank-val',
  'properties.transaction.timeout.ms' = '600000');

INSERT INTO snk (`account_id`, `amount`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `amount`,
       `__sent_ms`,
       `__seq`
FROM src;
