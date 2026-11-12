CREATE TABLE card_transactions (
    card_id STRING,
    transaction_time TIMESTAMP(3),
    amount DECIMAL(10, 2),
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'card-transactions',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'properties.group.id' = 'card-transactions-group'
);

CREATE TABLE card_transactions_enriched (
    card_id STRING,
    transaction_time TIMESTAMP(3),
    amount DECIMAL(10, 2),
    derived_field1 STRING,
    derived_field2 INT,
    `__sent_ms` BIGINT,
    `__seq` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'card-transactions-enriched',
    'properties.bootstrap.servers' = '127.0.0.1:12000,127.0.0.1:12001,127.0.0.1:12002',
    'format' = 'json',
    'sink.partitioner' = 'org.apache.flink.connector.kafka.sink.KeyPartitioner',
    'sink.partitioner.key-extractor' = 'org.apache.flink.connector.kafka.sink.KeySelector',
    'sink.parallelism' = '20',
    'sink.max-parallelism' = '20'
);

INSERT INTO card_transactions_enriched (`card_id`, `transaction_time`, `amount`, `derived_field1`, `derived_field2`, `__sent_ms`, `__seq`)
SELECT `card_id`,
       `transaction_time`,
       `amount`,
       CAST(NULL AS STRING),
       CAST(NULL AS INT),
       `__sent_ms`,
       `__seq`
FROM card_transactions;
