package com.art.flink.cdc;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CDCSqlExample {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 生成checkpoint的时间，以及默认为CheckpointingMode.EXACTLY_ONCE，一般采用默认就可以。如果任务有超低延时需求，可以使用至少一次 CheckpointingMode.AT_LEAST_ONCE
        // 影响时效性，一般1s即可
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        // 不需要设置checkpoints的存放路径？
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:///user/work/flink-checkpoints");
        // 设置两次checkpoint之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在1分钟内完成，或者被丢弃（checkpoint的超时时间，建议结合资源和占用情况，可以适当加大。时间短可能存在无法成功的情况）
        env.getCheckpointConfig().setCheckpointTimeout(1000L * 60);
        // 设置并发checkpoint的数目
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // // 设置savepoint的存储位置（savepoint 和 checkpoint 的区别？）
        // // flink sql client 通过设置SET 'execution.savepoint.path' = '该应用的checkpoint路径'
        // // 命令行恢复使用 ./bin/flink run -s 该应用的checkpoint路径 ...
        // env.setDefaultSavepointDirectory("hdfs:///user/work/flink-savepoints");
        //
        // // 目前代码不能设置保留的checkpoint个数 默认值时保留一个 假如要保留3个
        // // 可以在flink-conf.yaml中配置 state.checkpoints.num-retained: 3
        // // 设置外部检查点。可以将检查点的元数据信息定期写入外部系统，这样当job失败时，检查点不会被清除。这样如果job失败，可以从检查点恢复job

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // tableEnv.getConfig().getConfiguration().setString("dfs.client.use.datanode.hostname", "true");

        String sourceDDL = "create table mysql_cdc_test4(\n"
                           + "    id int PRIMARY KEY,\n"
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

        // 实时同步到hudi/hive，路径会自动创建
        String sinkDDL = "create table hudi_test4(\n"
                         + "    id int primary key,\n"
                         + "    data string,\n"
                         + "    data2 string,\n"
                         + "    data3 string,\n"
                         + "    ts timestamp(3),\n"
                         + "    dt string\n"
                         + ") \n"
                         + "partitioned by (dt) \n"
                         + "with (\n"
                         + "    'connector' = 'hudi',\n"
                         + "    'path' = '/user/work/tmp/tables/hudi_test4',\n"  //   -- /tmp/external/  /user/work/tmp/tables/
                         + "    'table.type' = 'MERGE_ON_READ',\n"
                         + "    'hoodie.datasource.write.recordkey.field' = 'id',\n"
                         + "    'hoodie.precombine.field' = 'ts',\n"
                         + "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n"
                         + "    'write.operation' = 'upsert',\n"
                         + "    'write.task' = '1'\n"
                         + ")";

        String transformSQL = "insert into hudi_test4 "
                              + "select t.id, t.data, t.data2, t.data3, now() as ts, substr(cast(now() as string), 1, 10) as dt from mysql_cdc_test4 t";

        tableEnv.executeSql(sourceDDL);
        // 实时计算，将结果存储到？
        // tableEnv.executeSql("select count(*) from mysql_cdc_test4").print();
        // tableEnv.executeSql("select sum(id) from mysql_cdc_test4").print();
        // tableEnv.executeSql("select sum(t.cnt) from mysql_cdc_test4 t where substr(t.data2, 1, 10)=current_date").print();  // 计算当天指标

        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL);

        String readDDL = "create table read_hudi_test4(\n"
                         + "    `_hoodie_commit_time` string,\n"
                         + "    `_hoodie_commit_seqno` string,\n"
                         + "    `_hoodie_record_key` string,\n"
                         + "    `_hoodie_partition_path` string,\n"
                         + "    `_hoodie_file_name` string,\n"
                         + "    id int,\n"
                         + "    data string,\n"
                         + "    data2 string,\n"
                         + "    data3 string,\n"
                         + "    ts timestamp(3),\n"
                         + "    dt string\n"
                         + ")\n"
                         + "partitioned by (dt)\n"
                         + "with (\n"
                         + "    'connector' = 'hudi',\n"
                         + "    'path' = '/user/work/tmp/tables/hudi_test4',\n"  //   -- /tmp/external/  /user/work/tmp/tables/
                         + "    'table.type' = 'MERGE_ON_READ',\n"
                         + "    'hoodie.datasource.write.recordkey.field' = 'id',\n"
                         + "    'hoodie.precombine.field' = 'ts',\n"
                         + "    'read.streaming.enabled' = 'true',\n"
                         + "    'read.streaming.check-interval' = '1',\n"
                         // + "    'read.start-commit' = '20210316134557',\n"
                         + "    'read.task' = '1'\n"
                         + ")";
        tableEnv.executeSql(readDDL);
        tableEnv.executeSql("select * from read_hudi_test4").print();
        // tableEnv.executeSql("select * from mysql_cdc_test4").print();

        env.execute("mysql-to-hudi");
    }

}
