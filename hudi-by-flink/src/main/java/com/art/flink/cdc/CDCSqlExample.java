package com.art.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CDCSqlExample {

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

        // 实时同步到hudi/hive
        String sinkDDL = "create table hudi_test4(\n"
                         + "    id int primary key NOT ENFORCED,\n"
                         + "    data varchar(255),\n"
                         + "    data2 varchar(255),\n"
                         + "    data3 varchar(255),\n"
                         + "    ts bigint,\n"
                         + "    dt varchar(20)\n"
                         + ") \n"
                         + "partitioned by (dt) \n"
                         + "with (\n"
                         + "    'connector' = 'hudi',\n"
                         + "    'path' = '/user/work/tmp/tables/hudi_test4',\n"  //   -- /tmp/external/  /user/work/tmp/tables/
                         + "    'table.type' = 'MERGE_ON_READ',\n"
                         // + "    'read.streaming.enabled' = 'true',\n"
                         // + "    'read.streaming.check-interval' = '1',\n"
                         + "    'hoodie.datasource.write.recordkey.field' = 'id',\n"
                         + "    'hoodie.precombine.field' = 'ts',\n"
                         + "    'write.operation' = 'upsert',\n"
                         + "    'write.task' = '1'\n"
                         + ")";

        String transformSQL = "insert into hudi_test4 "
                              + "select t.id, t.data, t.data2, t.data3, 1 as ts, substr(cast(now() as string), 1, 10) as dt from mysql_cdc_test4 t";

        tableEnv.executeSql(sourceDDL);
        // 实时计算，将结果存储到？
        // tableEnv.executeSql("select count(*) from mysql_cdc_test4").print();
        // tableEnv.executeSql("select sum(id) from mysql_cdc_test4").print();
        // tableEnv.executeSql("select sum(t.cnt) from mysql_cdc_test4 t where substr(t.data2, 1, 10)=current_date").print();  // 计算当天指标

        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL);

        tableEnv.executeSql("select * from hudi_test4").print();

        env.execute("mysql-to-hudi");
    }

}
