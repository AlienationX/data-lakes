package com.art.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SaveToMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

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

        // 实时同步到关系数据库
        // String sinkDDL = "create table hudi_test4 (\n"
        //                  + "    id int PRIMARY KEY NOT ENFORCED,\n"
        //                  + "    data varchar(255),\n"
        //                  + "    data2 varchar(255),\n"
        //                  + "    data3 varchar(255),\n"
        //                  + "    ts timestamp(3),\n"
        //                  + "    dt varchar(20)\n"
        //                  + ") with (\n"
        //                  + "   'connector' = 'jdbc',\n"
        //                  + "   'driver' = 'com.mysql.cj.jdbc.Driver',\n"  // com.mysql.cj.jdbc.Driver
        //                  + "   'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n"  // &serverTimezone=UTC, mysql8.x的jdbc升级了，增加了时区（serverTimezone）属性，并且不允许为空
        //                  + "   'table-name' = 'test4_copy',\n"
        //                  + "   'username' = 'root',\n"
        //                  + "   'password' = 'root'\n"
        //                  + ")";

        // 实时计算并存储到mysql
        String sinkDDL = "create table mysql_test4_metric (\n"
                         + "    ts timestamp(3) PRIMARY KEY NOT ENFORCED,\n"
                         + "    dimension varchar(60),\n"
                         + "    metric double\n"
                         + ") with (\n"
                         + "    'connector' = 'jdbc',\n"
                         + "    'driver' = 'com.mysql.cj.jdbc.Driver',\n"  // com.mysql.cj.jdbc.Driver
                         + "    'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n"  // &serverTimezone=UTC, mysql8.x的jdbc升级了，增加了时区（serverTimezone）属性，并且不允许为空
                         + "    'username' = 'root',\n"
                         + "    'password' = 'root',\n"
                         + "    'table-name' = 'test4_metric'\n"
                         + ")";

        // 实时将结果写入，是否结果太多，定时清空两天前的数据，或者发送kafka只保留两天，或者redis设置过期时间？
        // 必须要设置主键，并且插入数据 java.lang.IllegalStateException: please declare primary key for sink table when query contains update/delete record.
        // mysql设置自增id也没用，insert语句必须指定主键，主键相同会覆盖。flink sql 的 insert 相当于 upsert
        String transformSQL = "insert into mysql_test4_metric "
                              + "select now() as ts, 'total_amount' as dimension, sum(t.cnt) as metric "
                              + "from mysql_cdc_test4 t "
                              + "where substr(t.data2, 1, 10)=current_date ";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL);

        // mysql_test4_metric 表不能实时打印 java.lang.IllegalStateException: No operators defined in streaming topology. Cannot execute.
        // tableEnv.executeSql("select * from mysql_test4_metric").print();

        tableEnv.executeSql("select * from mysql_cdc_test4").print();

        env.execute("mysql-to-hudi");
    }

}
