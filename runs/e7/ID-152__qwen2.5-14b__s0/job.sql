CREATE TABLE payments (
    account_id BIGINT,
    event_time TIMESTAMP(3),
    payload STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'payment-validation-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE payments_validated (
    account_id BIGINT,
    event_time TIMESTAMP(3),
    payload STRING,
    derived_field STRING,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments-validated',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json'
);

INSERT INTO payments_validated (`account_id`, `event_time`, `payload`, `derived_field`, `__sent_ms`, `__seq`)
SELECT `account_id`,
       `event_time`,
       `payload`,
       CAST(NULL AS STRING),
       `__sent_ms`,
       `__seq`
FROM payments;
