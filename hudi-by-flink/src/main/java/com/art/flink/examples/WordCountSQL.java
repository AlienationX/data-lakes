package com.art.flink.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Arrays;

public class WordCountSQL {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()  // 设置批模式
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // access flink configuration
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // set low-level key-value options
        // configuration.setString("table.exec.mini-batch.enabled", "true");
        // configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        // configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");

        /**
        SET 'sql-client.execution.result-mode' = 'tableau';
        SET 'execution.runtime-mode' = 'batch';
        SELECT
                name,
                COUNT(*) AS cnt
        FROM
                (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name)
         GROUP BY name;
        */
        tableEnv.executeSql("SELECT word, SUM(frequency) AS `count`\n" +
                "FROM (VALUES ('Hello', 1), ('Ciao', 1), ('Hello', 2)) AS WordTable(word, frequency)\n" +
                "GROUP BY word"
        ).print();

        // 需要flink-connector-jdbc依赖
        tableEnv.executeSql("CREATE TABLE flink_test_student (\n"
                + "  id int,\n"
                + "  name string,\n"
                // + "  sex string,\n"  // 可以省略字段
                // + "  full_name as concat(name, sex),\n"  // 不支持省略的字段，as的名称在前面，注意是反写
                + "  full_name as concat(name, '_sex'),\n"  // price * quanitity,  // evaluate expression and supply the result to queries
                + "  birthday string,\n"
                + "  create_time timestamp,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"  // 必须增加 NOT ENFORCED
                + ") WITH (\n"
                + "   'connector' = 'jdbc',\n"
                + "   'driver' = 'com.mysql.cj.jdbc.Driver',\n"  // com.mysql.cj.jdbc.Driver
                + "   'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n"  // &serverTimezone=UTC, mysql8.x的jdbc升级了，增加了时区（serverTimezone）属性，并且不允许为空
                + "   'table-name' = 'student',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root'\n"
                + ")");

        tableEnv.executeSql("select * from flink_test_student").print();  // 表名区分大小写

        TableResult tr = tableEnv.executeSql("show databases");
        tr.print();
        tableEnv.executeSql("use default_database");
        tableEnv.executeSql("show tables").print();

        // read hive
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        String catalogName     = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "\\Codes\\Java\\datalakes\\hudi-by-flink\\src\\main\\resources";  // 集群路径/etc/hive/conf，如何使用resources目录下的配置文件?
        String hiveVersion     = "1.1.0";

        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        // 注册 catalog
        tableEnv.registerCatalog("myhive", hive);
        // 选择一个 catalog, set the HiveCatalog as the current catalog of the session
        System.out.println("catalog: " + Arrays.toString(tableEnv.listCatalogs()));
        tableEnv.useCatalog("myhive");  // 会覆盖掉默认的 default_database

        tableEnv.executeSql("show databases").print();  // 不会再显示 default_database
        tableEnv.executeSql("select * from tmp.dim_date t limit 10").print();

        tableEnv.executeSql("drop table if exists tmp.dim_date_flink");
        tableEnv.executeSql("create table tmp.dim_date_flink like tmp.dim_date");

        tableEnv.executeSql("drop table if exists tmp.dim_date_flink1");
        tableEnv.executeSql("create table tmp.dim_date_flink1 (etl_time timestamp(9)) like tmp.dim_date");  // 必须是timestamp(9)
        tableEnv.executeSql("insert overwrite tmp.dim_date_flink1 select t.*, now() as etl_time from tmp.dim_date t");
        System.out.println("==> stage 1 done.");

        tableEnv.executeSql("drop table if exists tmp.dim_date_flink2");
        // tableEnv.executeSql("create table tmp.dim_date_flink2 as select t.*, now() as etl_time from tmp.dim_date t");  // stored as parquet会报错，和hql还是有些区别，且不支持create table as？


        // Flink目前支持两种SQL方言(SQL dialects),分别为：default和hive。默认的SQL方言是default，如果要使用Hive的语法，需要将SQL方言切换到hive。
        // default方言扩展性很强，hive支持完整的hql
        // Hive dialect只能用于操作Hive表，不能用于普通表。Hive方言应与HiveCatalog一起使用。
        // 使用hive dialect
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // tableEnv.executeSql("set table.sql-dialect=hive");  // 不推荐会报错，推荐使用 setSqlDialect 方式

        // pom必须增加 flink-sql-connector-hive-1.2.2_2.11，maven上很难找到，有点无语。否则会报 Exception in thread "main" java.lang.NullPointerException: org.apache.flink.table.catalog.hive.client.HiveShimV100.registerTemporaryFunction
        // tableEnv.loadModule(catalogName, new HiveModule(hiveVersion));  // 注册函数？不是必须的
        // 选择 database
        tableEnv.useDatabase("default");
        tableEnv.executeSql("create table tmp.dim_date_flink2 stored as parquet as select t.*, 'current_timestamp()' as etl_time from tmp.dim_date t");

        // tableEnv.executeSql("create table tmp.dim_date_flink2 stored as parquet as select t.*, current_timestamp() as etl_time from tmp.dim_date t");  // 低版本hive不能使用内置函数，会报错
        // 值得注意的是，对于不同的Hive版本，可能在功能方面有所差异，这些差异取决于你使用的Hive版本，而不取决于Flink，一些版本的功能差异如下：
        //
        // Hive 内置函数在使用 Hive-1.2.0 及更高版本时支持。  // TODO 所以低版本hive不支持内置函数，巨坑
        // 列约束，也就是 PRIMARY KEY 和 NOT NULL，在使用 Hive-3.1.0 及更高版本时支持。
        // 更改表的统计信息，在使用 Hive-1.2.0 及更高版本时支持。
        // DATE列统计信息，在使用 Hive-1.2.0 及更高版时支持。
        // 使用 Hive-2.0.x 版本时不支持写入 ORC 表。

    }
}

