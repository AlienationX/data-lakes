package com.art.flink.examples.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 1.多少个表就会产生多少个会话，可以通过 show processlist 查看，表太多会影响服务器性能
 * 2.任务意外停止如何检查点恢复？
 * 3.中途增加表如何解决？
 * 4.元数据可以保存在hive中，但是通过 show create table 可以查看到password，授权限制？
 */
public class SaveToMysqlWithJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String sourceDDL1 = "create table mysql_cdc_test4(\n"
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

        String sourceDDL2 = "create table mysql_cdc_student(\n"
                           + "    id int PRIMARY KEY NOT ENFORCED,\n"
                           + "    name varchar(255),\n"
                           + "    sex varchar(255),\n"
                           + "    birthday varchar(100)\n"
                           + ") with (\n"
                           + "    'connector' = 'mysql-cdc',\n"
                           + "    'hostname' = 'localhost',\n"
                           + "    'port' = '3306',\n"
                           + "    'username' = 'root',\n"
                           + "    'password' = 'root',\n"
                           + "    'server-time-zone' = 'Asia/Shanghai',\n"
                           // + "    'debezium.snapshot.mode' = 'initial',\n"  // initial, latest-offset, never, schema_only 无效
                           + "    'database-name' = 'test',\n"
                           + "    'table-name' = 'student'\n"
                           + ")";

        // 实时计算并存储到mysql
        String sinkDDL = "create table mysql_test4_result (\n"
                         + "    id int PRIMARY KEY NOT ENFORCED,\n"
                         + "    data varchar(255),\n"
                         + "    data2 varchar(255),\n"
                         + "    data3 varchar(255),\n"
                         + "    name varchar(255),\n"
                         + "    sex varchar(255),\n"
                         + "    birthday varchar(255)\n"
                         + ") with (\n"
                         + "    'connector' = 'jdbc',\n"
                         + "    'driver' = 'com.mysql.cj.jdbc.Driver',\n"  // com.mysql.cj.jdbc.Driver
                         + "    'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n"  // &serverTimezone=UTC, mysql8.x的jdbc升级了，增加了时区（serverTimezone）属性，并且不允许为空
                         + "    'username' = 'root',\n"
                         + "    'password' = 'root',\n"
                         + "    'table-name' = 'test4_result'\n"
                         + ")";

        // 实时将结果写入，是否结果太多，定时清空两天前的数据，或者发送kafka只保留两天，或者redis设置过期时间？
        // 必须要设置主键，并且插入数据 java.lang.IllegalStateException: please declare primary key for sink table when query contains update/delete record.
        // mysql设置自增id也没用，insert语句必须指定主键，主键相同会覆盖。flink sql 的 insert 相当于 upsert
        String transformSQL = "insert into mysql_test4_result "
                              + "select t.id, t.data, t.data2, t.data3, s.name, s.sex, s.birthday "
                              + "from mysql_cdc_test4 t "
                              + "left join mysql_cdc_student s on t.data3=cast(s.id as string)";

        // TODO 维表后插入数据 left join 主表也能关联出来，也太太太太太太太太太强了吧！！！！！
        tableEnv.executeSql(sourceDDL1);
        tableEnv.executeSql(sourceDDL2);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL);

        // mysql_test4_metric 表不能实时打印 java.lang.IllegalStateException: No operators defined in streaming topology. Cannot execute.
        // tableEnv.executeSql("select * from mysql_test4_metric").print();

        tableEnv.executeSql("select * from mysql_cdc_test4").print();

        env.execute("mysql-to-hudi");

        /**
         * CREATE TABLE `student`  (
         *   `id` int(11) PRIMARY KEY,
         *   `name` varchar(100),
         *   `sex` varchar(100),
         *   `birthday` varchar(100),
         *   `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)
         * ) ENGINE = InnoDB;
         *
         * INSERT INTO `student` VALUES (-1, 'unknown', 'unknown', NULL, NULL);
         * INSERT INTO `student` VALUES (1, '小明', '男', '1996-05-06', '2021-05-13 14:30:45');
         * INSERT INTO `student` VALUES (2, '小花', '女', '1995-12-23', '2021-05-13 14:30:45');
         * INSERT INTO `student` VALUES (3, '张三', '男', '1995-04-05', '2021-05-13 14:30:45');
         * INSERT INTO `student` VALUES (4, '李四', '男', '1996-01-01', '2021-05-13 14:30:45');
         *
         */
    }

}
