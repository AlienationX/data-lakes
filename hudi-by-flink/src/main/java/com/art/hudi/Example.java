package com.art.hudi;

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
                // .enableHiveSupport()
                .getOrCreate();

        spark.sql("show databases").show();

        // 插入, double类型目前会报错，还未解决
        Dataset<Row> data = spark.sql("" +
                "select 1 as id, 'aaa' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 2 as id, 'bbb' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 3 as id, 'ccc' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date"
        );
        data.show();
        String basePath = "/user/work/tmp/tables/";
        String tableName = "h1";
        // java.lang.NoSuchFieldError: NULL_VALUE 版本问题，高版本需要spark2.4.3+
        data.write()
                .format("hudi")
                .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")  // 设置???
                .option(HoodieWriteConfig.TBL_NAME.key(), tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Overwrite)
                .save(basePath + tableName);

        // 更新
        Dataset<Row> data1 = spark.sql("" +
                "select 2 as id, 'bbb' as name, 99  as price, 'U' as flag, '2020-01-01' as update_date union all " +
                "select 4 as id, 'ddd' as name, 100 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 9 as id, 'zzz' as name, -99 as price, 'D' as flag, '2020-01-01' as update_date"
        );
        data1.write()
                .format("hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")  // 设置???
                .option(HoodieWriteConfig.TBL_NAME.key(), tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Append)
                .save(basePath + tableName);

        // 删除

        Dataset<Row> h1 = spark.read().format("org.apache.hudi").load(basePath + tableName + "/*");
        h1.show();
        h1.createOrReplaceTempView("h1");
        spark.sql("select t.*, current_timestamp() as dt from h1 t").show();

        spark.sql("show tables").show();
        spark.sql("desc h1").show();
        spark.sql("select * from h1").printSchema();
        // spark.sql("insert into h1 select 9 as id, 'unknown' as name, -1 as price, '2099-12-31' as update_date");
        spark.sql("select * from h1").show();

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

        spark.sql(sql4);
        spark.sql("show tables").show();

        spark.stop();
    }
}