package com.art.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Arrays;

/**
 * 在 flink-sql 中创建的表在hive中就可以查看，但是hive不能查询数据，只能当作元数据存储
 * hive中的表在flink中可以查询，推荐统一使用 flink-sql 的默认方言
 */

public class SaveSchemaToHiveMetastore {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()  // 设置批模式
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setString("mysql_password", "root");

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
        // 使用hive dialect
        // tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // 需要flink-connector-jdbc依赖
        tableEnv.executeSql("create table if not exists tmp.flink_test_student (\n"
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

        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("use tmp");
        tableEnv.executeSql("show tables").print();
        tableEnv.executeSql("show create table tmp.flink_test_student").print();  // 明文密码的问题如何解决？

    }
}
