package com.art.flink.cdc;


/**
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-connector-hbase-1.4_2.11</artifactId>
 *   <version>1.13.6</version>
 * </dependency>
 *
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-connector-hbase-2.2_2.11</artifactId>
 *   <version>1.13.6</version>
 * </dependency>
 *
 * -- register the HBase table 'mytable' in Flink SQL
 * CREATE TABLE hTable (
 *  rowkey INT,
 *  family1 ROW<q1 INT>,
 *  family2 ROW<q2 STRING, q3 BIGINT>,
 *  family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,
 *  PRIMARY KEY (rowkey) NOT ENFORCED
 * ) WITH (
 *  'connector' = 'hbase-1.4',
 *  'table-name' = 'mytable',
 *  'zookeeper.quorum' = 'localhost:2181'
 * );
 *
 * -- use ROW(...) construction function construct column families and write data into the HBase table.
 * -- assuming the schema of "T" is [rowkey, f1q1, f2q2, f2q3, f3q4, f3q5, f3q6]
 * INSERT INTO hTable
 * SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3), ROW(f3q4, f3q5, f3q6) FROM T;
 *
 * -- scan data from the HBase table
 * SELECT rowkey, family1, family3.q4, family3.q6 FROM hTable;
 *
 * -- temporal join the HBase table as a dimension table
 * SELECT * FROM myTopic
 * LEFT JOIN hTable FOR SYSTEM_TIME AS OF myTopic.proctime
 * ON myTopic.key = hTable.rowkey;
 */
public class SaveToHBase {


}
