package com.art.flink.examples.cdc;


/**
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-connector-kafka_2.11</artifactId>
 *   <version>1.13.6</version>
 * </dependency>
 *
 *
 * CREATE TABLE KafkaTable (
 *   `user_id` BIGINT,
 *   `item_id` BIGINT,
 *   `behavior` STRING,
 *   `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
 * ) WITH (
 *   'connector' = 'kafka',
 *   'topic' = 'user_behavior',
 *   'properties.bootstrap.servers' = 'localhost:9092',
 *   'properties.group.id' = 'testGroup',
 *   'scan.startup.mode' = 'earliest-offset',
 *   'format' = 'csv'  -- json格式需要添加依赖 flink-json
 * )
 *
 * 'connector' = 'upsert-kafka'
 */
public class SaveToKafka {
}
