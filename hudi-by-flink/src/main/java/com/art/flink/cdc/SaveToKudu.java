package com.art.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SaveToKudu {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String sourceDDL = "create table mysql_cdc_test4(\n"
                           + "    id int PRIMARY KEY NOT ENFORCED,\n"
                           + "    data varchar(255),\n"
                           + "    data2 varchar(255),\n"
                           + "    data3 varchar(255),\n"
                           + "    cnt double\n"
                           + ") with (\n"
                           + "    'connector' = 'mysql-cdc',\n"
                           + "    'hostname' = 'localhost',\n"
                           + "    'port' = '3306',\n"
                           + "    'username' = 'root',\n"
                           + "    'password' = 'root',\n"
                           + "    'server-time-zone' = 'Asia/Shanghai',\n"
                           // + "    'debezium.snapshot.mode' = 'initial',\n"  // initial, latest-offset, never, schema_only 无效
                           + "    'database-name' = 'test',\n"
                           + "    'table-name' = 'test4'\n"
                           + ")";

        /**
         * use test_kudu;
         * CREATE TABLE if not exists kudu_test4
         * (
         *   id BIGINT COMMENT 'id',
         *   `data` string,
         *   data2 string,
         *   data3 string,
         *   cnt double,
         *   ts timestamp,
         *   dt string,
         *   PRIMARY KEY(id)
         * )
         * PARTITION BY HASH PARTITIONS 16
         * STORED AS KUDU;
         */


        // 实时同步到kudu
        // Error 不能通过jdbc方式，需要使用第三方jar包 github.com/apache/bahir-flink
        String sinkDDL = "create table kudu_test4 (\n"
                         + "    id int PRIMARY KEY NOT ENFORCED,\n"
                         + "    data string,\n"
                         + "    data2 string,\n"
                         + "    data3 string,\n"
                         + "    cnt double,\n"
                         + "    ts timestamp,\n"
                         + "    dt string\n"
                         + ") with (\n"
                         + "    'connector' = 'jdbc',\n"
                         + "    'driver' = 'org.apache.hive.jdbc.HiveDriver',\n"
                         + "    'url' = 'jdbc:hive2://hadoop-prod03:21050/',\n"
                         + "    'username' = 'work',\n"
                         + "    'password' = 'TwkdFNAdS1nIikzk',\n"
                         + "    'database-name' = 'test',\n"
                         + "    'table-name' = 'kudu_test4'\n"
                         + ")";

        String transformSQL = "insert into kudu_test4 "
                              + "select t.id, t.data, t.data2, t.data3, t.cnt, now() as ts, substr(cast(now() as string), 1, 10) as dt from mysql_cdc_test4 t";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL);

        tableEnv.executeSql("select * from kudu_test4").print();

        env.execute("mysql-to-kudu");
    }
}
