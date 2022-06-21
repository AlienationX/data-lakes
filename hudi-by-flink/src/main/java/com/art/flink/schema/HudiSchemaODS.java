package com.art.flink.schema;

import java.util.HashMap;
import java.util.Map;

/**
 * java8 没有文本块（Text Blocks）的新功能，换行拼接字符串太难受了，不方便复制粘贴和维护
 * 使用sql文件代替？ 一个表一个文件方便读取？
 */
public class HudiSchemaODS {

    private final static String BasePath = "/user/work/tmp/tables/";

    private final static String ods_test_test4 =
            "create table if not exists ods_test_test4 (\n" +
            "    id int,\n" +
            "    data string,\n" +
            "    data2 string,\n" +
            "    data2 string,\n" +
            "    cnt double,\n" +
            "    primary key(id) not enforced" +
            ") \n" +
            "with (\n" +
            "    'connector' = 'hudi',\n" +
            "    'path' = 'hdfs://" + BasePath + "ods_test_test4',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
            "    'hoodie.precombine.field' = 'id',\n" +
            // "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
            "    'read.streaming.enabled' = 'true',\n" +
            "    'read.streaming.check-interval' = '1',\n" +
            "    'write.operation' = 'upsert',\n" +
            "    'write.task' = '1'\n" +
            ")";

    private final static String ods_test_student =
            "create table if not exists ods_test_student (\n" +
            "    id int,\n" +
            "    name string,\n" +
            "    sex string,\n" +
            "    birthday string,\n" +
            "    create_time timestamp(3),\n" +
            "    primary key(id) not enforced" +
            ") \n" +
            "with (\n" +
            "    'connector' = 'hudi',\n" +
            "    'path' = 'hdfs://" + BasePath + "ods_test_student',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
            "    'hoodie.precombine.field' = 'create_time',\n" +
            // "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
            "    'read.streaming.enabled' = 'true',\n" +
            "    'read.streaming.check-interval' = '1',\n" +
            "    'write.operation' = 'upsert',\n" +
            "    'write.task' = '1'\n" +
            ")";

    public static final Map<String, String> TABLES = new HashMap<>();
    static {
        TABLES.put("ods_test_test4", ods_test_test4);
        TABLES.put("ods_test_student", ods_test_student);
    }

}
