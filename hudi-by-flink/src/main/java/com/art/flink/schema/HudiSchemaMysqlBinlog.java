package com.art.flink.schema;

public class HudiSchemaMysqlBinlog {

    private final static String BasePath = "/user/work/tmp/tables/";

    public final static String hudi_mysql_binlog_local =
            "create table if not exists hudi_mysql_binlog_local (\n" +
            "    id string,\n" +
            "    server_id string,\n" +
            "    file string,\n" +
            "    pos bigint,\n" +
            "    before string,\n" +
            "    after string,\n" +
            "    op string,\n" +
            "    ts_ms bigint,\n" +
            // "    transaction string,\n" +
            "    primary_key_data string,\n" +
            "    db string,\n" +
            "    tb string\n" +
            ") \n" +
            "partitioned by (db, tb) \n" +
            "with (\n" +
            "    'connector' = 'hudi',\n" +
            "    'path' = 'hdfs://" + BasePath + "hudi_mysql_binlog_local',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
            "    'hoodie.precombine.field' = 'ts_ms',\n" +
            "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
            "    'read.streaming.enabled' = 'true',\n" +
            "    'read.streaming.check-interval' = '1',\n" +
            "    'write.operation' = 'upsert',\n" +
            "    'write.task' = '1'\n" +
            ")";

    public final static String hudi_mysql_binlog_hive_metastore =
            "create table if not exists hudi_mysql_binlog_hive_metastore (\n" +
            "    id string,\n" +
            "    server_id string,\n" +
            "    file string,\n" +
            "    pos bigint,\n" +
            "    before string,\n" +
            "    after string,\n" +
            "    op string,\n" +
            "    ts_ms bigint,\n" +
            // "    transaction string,\n" +
            "    primary_key_data string,\n" +
            "    db string,\n" +
            "    tb string\n" +
            ") \n" +
            "partitioned by (db, tb) \n" +
            "with (\n" +
            "    'connector' = 'hudi',\n" +
            "    'path' = 'hdfs://" + BasePath + "hudi_mysql_binlog_hive_metastore',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
            "    'hoodie.precombine.field' = 'ts_ms',\n" +
            "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
            "    'read.streaming.enabled' = 'true',\n" +
            "    'read.streaming.check-interval' = '1',\n" +
            "    'write.operation' = 'upsert',\n" +
            "    'write.task' = '1'\n" +
            ")";

}
