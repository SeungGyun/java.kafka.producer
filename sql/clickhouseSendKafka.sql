CREATE TABLE kafka_raw
(
    sender String,
    message String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'local.kafka:9092',
    kafka_topic_list = 'clickHouse-topic',
    kafka_group_name = 'clickhouse-group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_commit_every_batch = 1;

CREATE TABLE default.clickhouse_table (
  `id` UUID DEFAULT generateUUIDv4(),
  `sender` String,
  `message` String,
  `created_at` DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY
  created_at SETTINGS index_granularity = 8192



CREATE MATERIALIZED VIEW kafka_to_clickhouse_table
TO clickhouse_table
AS
SELECT generateUUIDv4() AS id, sender, message, now() AS created_at
FROM kafka_raw;