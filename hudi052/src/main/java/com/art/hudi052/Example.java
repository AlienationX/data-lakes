package com.art.hudi052;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Example {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("hudi example")
                .config("spark.some.config.option", "some-value")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 使用hudi必须设置
                .config("spark.sql.hive.convertMetastoreParquet", "false") // Uses Hive SerDe, this is mandatory for MoR tables
                // .enableHiveSupport()  // 不能使用 spark-sql 创建 hive 的外部表 hudi 表，会报权限不足
                .getOrCreate();

        spark.sql("show databases").show();

        String basePath = "/user/work/tmp/tables/";
        String tableName = "h0";

        // 插入, double类型目前会报错，还未解决
        Dataset<Row> data = spark.sql("" +
                "select 1 as id, 'aaa' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 2 as id, 'bbb' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 3 as id, 'ccc' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date"
        );
        data.show();

        data.write()
                .format("org.apache.hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "update_date")  // 设置???
                .option(HoodieWriteConfig.TABLE_NAME, tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Overwrite)
                .save(basePath + tableName);

        // 更新
        Dataset<Row> data1 = spark.sql("" +
                "select 2 as id, 'bbb' as name, 99  as price, 'U' as flag, '2020-01-01' as update_date union all " +
                "select 4 as id, 'ddd' as name, 100 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 9 as id, 'zzz' as name, -99 as price, 'D' as flag, '2020-01-01' as update_date union all " +
                "select 0 as id, 'zzz' as name, 0   as price, 'D' as flag, '2020-01-01' as update_date"
        );
        data1.write()
                .format("org.apache.hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "update_date")  // 设置合并字段，如果主键重复保留该字段值最大的记录
                .option(HoodieWriteConfig.TABLE_NAME, tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Append)
                .save(basePath + tableName);

        Dataset<Row> h0 = spark.read().format("org.apache.hudi").load(basePath + tableName + "/*");
        h0.show();
        h0.createOrReplaceTempView("h0");
        spark.sql("select t.*, current_timestamp() as dt from h0 t").show();

        // 删除
        Dataset<Row> data2 = spark.sql("select 9 as id union all select 0 as id");
        data2.write()
                .format("org.apache.hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL())  // 设置delete操作
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "id")  // 设置主键
                // .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "update_date")  // delete不需要设置
                .option(HoodieWriteConfig.TABLE_NAME, tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Append)
                .save(basePath + tableName);

        spark.read().format("org.apache.hudi")
                .load(basePath + tableName + "/*")
                .createOrReplaceTempView("h0");

        spark.sql("show tables").show();
        spark.sql("desc h0").show();
        spark.sql("select * from h0").printSchema();
        // spark.sql("insert into h0 select 9 as id, 'unknown' as name, -1 as price, '2099-12-31' as update_date");  // 无法执行因为字段个数不一致，hudi默认的5个字段也不推荐手工写入
        spark.sql("select * from h0").show();

        // 增量查询，没搞明白和 sql where 过滤 _hoodie_commit_time 的区别
        spark.read().format("org.apache.hudi")
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), "20220317153118")  // 20220317153110  // as.of.instant
                // .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(), "")
                .load(basePath + tableName + "/*")
                .createOrReplaceTempView("h0_incremental_query");
        spark.sql("select * from h0_incremental_query").show();

        /**
         * create table if not exists hudi_table_p0 (
         * id bigint,
         * name string,
         * dt string，
         * hh string
         * ) using hudi
         * location '/tmp/hudi/hudi_table_p0'
         * options (
         *   type = 'cow',
         *   primaryKey = 'id',
         *   preCombineField = 'ts'
         *  )
         * partitioned by (dt, hh);
         */

        // sql1 建表语句通过，但是创建外部表需要权限 MetaException(message:User work does not have privileges for CREATETABLE);
        String sql1 = "-- create a managed cow table\n"
                + "create table if not exists h1 (\n"
                + "  id int, \n"
                + "  name string, \n"
                + "  price double\n"
                + ") using hudi\n"
                // + "location '/user/work/tmp/tables/h1'\n"
                + "location '/tmp/external/h1'\n"
                + "options (\n"
                + "  type = 'cow',\n"
                + "  primaryKey = 'id'\n"
                + ")";
        // org.apache.hudi.exception.HoodieException: 'hoodie.table.name', 'path' must be set.
        String sql2 = "create table h2 using hudi\n" + "options (type = 'cow', primaryKey = 'id')\n" + "partitioned by (dt)\n" + "as\n" + "select 1 as id, 'a1' as name, 10 as price, 1000 as dt";
        String sql3 = "create table h3 using hudi location '/user/work/tmp/tables/h3/' options (type = 'cow', primaryKey = 'id') as select 1 as id, 'a1' as name, 10 as price";
        String sql4 = "create table h4 using hudi as select 1 as id, 'a1' as name, 10 as price";

        try { System.out.println(sql1); spark.sql(sql1); System.out.println("++ done.");} catch (Exception e) { e.printStackTrace(); }
        Thread.sleep(1000);
        try { System.out.println(sql2); spark.sql(sql2); } catch (Exception e) { e.printStackTrace(); }
        Thread.sleep(1000);
        try { System.out.println(sql3); spark.sql(sql3); } catch (Exception e) { e.printStackTrace(); }
        Thread.sleep(1000);
        try { System.out.println(sql4); spark.sql(sql4); } catch (Exception e) { e.printStackTrace(); }

        spark.sql("show tables").show();

        spark.stop();
    }
}
