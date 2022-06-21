package com.art.spark.hudi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TestFlinkCDC {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()  // 设置批模式
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String basePath = "/user/work/tmp/tables/";
        String tableName = "hudi_mysql_binlog_hive_metastore";

        String createSql = "create table if not exists " + tableName + " (\n"
                           + "    id bigint,\n"
                           + "    before string,\n"
                           + "    after string,\n"
                           + "    source string,\n"
                           + "    op string,\n"
                           + "    ts_ms bigint,\n"
                           + "    transaction string,\n"
                           + "    db string,\n"
                           + "    tb string\n"
                           + ") \n"
                           + "partitioned by (db, tb) \n"
                           + "with (\n"
                           + "    'connector' = 'hudi',\n"
                           + "    'path' = 'hdfs://" + basePath + tableName + "',\n"  //   -- /tmp/external/  /user/work/tmp/tables/
                           + "    'table.type' = 'MERGE_ON_READ',\n"
                           + "    'hoodie.datasource.write.recordkey.field' = 'id',\n"
                           + "    'hoodie.precombine.field' = 'ts_ms',\n"
                           + "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n"
                           + "    'write.operation' = 'upsert',\n"
                           + "    'write.task' = '1'\n"
                           + ")";
        tableEnv.executeSql(createSql);

        String selectSql = "select 1650511650849 as id, 'null' as before, '{\"id\":1,\"name\":\"小明\",\"sex\":\"男\",\"birthday\":\"1996-05-06\",\"create_time\":\"2021-05-13T14:30:45Z\"}' as after, '{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":0,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"student\",\"server_id\":0,\"gtid\":null,\"file\":\"\",\"pos\":0,\"row\":0,\"thread\":null,\"query\":null}' as source, 'r' as op, 1650511650849 as ts_ms, 'null' as transaction, 'test' as db, 'test4' as tb";
        tableEnv.executeSql("insert into " + tableName + " " + selectSql);

        tableEnv.executeSql("show tables").print();
        tableEnv.executeSql("select * from " + tableName).print();
    }
}
