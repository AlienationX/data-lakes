package com.art.flink.schema;

public class HudiSchemaDW {

    private final static String BasePath = "/user/work/tmp/tables/";

    final static String dw_test4 =
            "create table if not exists dw_test4 (\n" +
            "    id int,\n" +
            "    data string,\n" +
            "    data2 string,\n" +
            "    data2 string,\n" +
            "    cnt double\n" +
            ") \n" +
            "with (\n" +
            "    'connector' = 'hudi',\n" +
            "    'path' = 'hdfs://" + BasePath + "dw_test4',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
            "    'hoodie.precombine.field' = 'id',\n" +
            // "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
            "    'read.streaming.enabled' = 'true',\n" +
            "    'read.streaming.check-interval' = '1',\n" +
            "    'write.operation' = 'upsert',\n" +
            "    'write.task' = '1'\n" +
            ")";

}
