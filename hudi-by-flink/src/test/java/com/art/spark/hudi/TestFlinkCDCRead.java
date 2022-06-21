package com.art.spark.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestFlinkCDCRead {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String BasePath = "/user/work/tmp/tables/";
        String tableName = "hudi_mysql_binlog_hive_metastore";

        String createSql = "create table if not exists ods_student (\n" +
                           "    id int,\n" +
                           "    name string,\n" +
                           "    sex string,\n" +
                           "    birthday string,\n" +
                           "    create_time timestamp(3),\n" +
                           "    primary key(id) not enforced" +
                           ") \n" +
                           "with (\n" +
                           "    'connector' = 'hudi',\n" +
                           "    'path' = 'hdfs://" + BasePath + "ods_student',\n" +
                           "    'table.type' = 'MERGE_ON_READ',\n" +
                           "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
                           "    'hoodie.precombine.field' = 'create_time',\n" +
                           // "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
                           "    'read.streaming.enabled' = 'true',\n" +
                           "    'read.streaming.check-interval' = '1',\n" +
                           "    'write.operation' = 'upsert',\n" +
                           "    'write.task' = '1'\n" +
                           ")";
        tableEnv.executeSql(createSql);
        tableEnv.executeSql("select * from ods_student").print();

        env.execute("test read hudi data");
    }
}
