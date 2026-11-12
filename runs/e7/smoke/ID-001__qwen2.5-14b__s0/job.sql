CREATE TABLE orders (
    order_id STRING,
    order_details STRING,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'properties.group.id' = 'orders-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE orders_validated (
    order_id STRING,
    order_details STRING,
    validation_time TIMESTAMP(3),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders-validated',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);

INSERT INTO orders_validated (`order_id`, `order_details`, `validation_time`, `__sent_ms`, `__seq`)
SELECT `order_id`,
       `order_details`,
       CAST(NULL AS TIMESTAMP(3)),
       `__sent_ms`,
       `__seq`
FROM orders;
