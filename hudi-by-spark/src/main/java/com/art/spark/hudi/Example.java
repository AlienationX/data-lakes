package com.art.spark.hudi;

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
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                // .config("spark.sql.hive.convertMetastoreParquet", "false")  // Uses Hive SerDe, this is mandatory for MoR tables
                // .enableHiveSupport()
                .getOrCreate();

        spark.sql("show databases").show();
        spark.sql("show tables").show();

        // 插入, double类型目前会报错，还未解决
        Dataset<Row> data1 = spark.sql("" +
                "select 1 as id, 'aaa' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 2 as id, 'bbb' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 3 as id, 'ccc' as name, 55 as price, 'I' as flag, '2020-01-01' as update_date"
        );
        data1.show();
        String basePath = "/user/work/tmp/tables/";
        String tableName = "h1";
        // java.lang.NoSuchFieldError: NULL_VALUE 版本问题，高版本需要spark2.4.3+
        data1.write()
                .format("hudi")
                .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")  //
                .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true")  // 使用hive分区的样式，类似 dt=2019-12-31
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .option(HoodieWriteConfig.TBL_NAME.key(), tableName)  // 设置表名
                .mode(SaveMode.Overwrite)
                .save(basePath + tableName);

        // 更新
        Dataset<Row> data2 = spark.sql("" +
                "select 2 as id, 'bbb' as name, 99  as price, 'U' as flag, '2020-01-01' as update_date union all " +
                "select 4 as id, 'ddd' as name, 100 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                "select 9 as id, 'zzz' as name, -99 as price, 'D' as flag, '2020-01-01' as update_date"
        );
        data2.write()
                .format("hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")  // 设置???
                .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true")
                .option(HoodieWriteConfig.TBL_NAME.key(), tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Append)
                .save(basePath + tableName);

        // 删除 只需要 primary key 的数据即可
        Dataset<Row> data3 = spark.sql("" +
                "select 9 as id union all " +
                "select 10 as id"
        );
        data3.write()
                .format("hudi")  // 高版本使用hudi也可
                .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL())  // 设置写入方式
                .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())   // 设置表类型
                .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")  // 设置主键
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")
                .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true")
                .option(HoodieWriteConfig.TBL_NAME.key(), tableName)  // 设置表名
                .option("hoodie.upsert.shuffle.parallelism", "2")  // 设置并行数
                .mode(SaveMode.Append)
                .save(basePath + tableName);

        Dataset<Row> h1 = spark.read().format("hudi").load(basePath + tableName);
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

        // spark必须2.4.3以上（2.4.0会报错）
        // 创建已存在的表不需要指定字段和option等schema信息，会自动读取指定路径数据和metadata数据。（如果指定了和读取的metadata不一致还会报错）
        // 执行sql1相当于把h1的元数据的表名修改为h1_bak，造成路径表名和元数据表名不一致，会报错。HoodieException: Config conflict(key	current value	existing value): hoodie.table.name:	h1	h1_bak
        String sql1 = "-- create a managed cow table\n" +
                      "create table if not exists h1 \n" +
                      "using hudi\n" +
                      "location '/user/work/tmp/tables/h1'\n";
        // location不指定的默认使用hive配置的warehouse路径/user/hive/warehouse，(应该也是外部表)。
        // java.lang.AssertionError: assertion failed: Path 'hdfs://hadoop-prod01:8020/user/hive/warehouse/h2' should be empty for CTAS
        String sql2 = "create table if not exists h2\n" +
                      "using hudi\n" +
                      "tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts')\n" +
                      "partitioned by (dt)\n" +
                      "as\n" +
                      "select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-12-01' as dt";
        // 默认值如果不存在的话必须指定，否则报错
        // default type='cow'
        // default primaryKey='uuid'
        // default preCombineField='ts'
        // default partition='partitionpath'
        String sql3 = "create table if not exists h3 using hudi\n" +
                      "tblproperties (\n" +
                      "  primaryKey = 'id'\n" +  // 必须指定主键
                      ")\n" +
                      "as\n" +
                      "select 1 as id, 'a1' as name, 10 as price, 1000 as dt";

        String sql4 = "create table if not exists hudi_cow_pt_tbl (\n" +
                      "  id bigint,\n" +
                      "  name string,\n" +
                      "  ts bigint,\n" +
                      "  dt string,\n" +
                      "  hh string\n" +
                      ") using hudi\n" +
                      "tblproperties (\n" +
                      "  type = 'cow',\n" +
                      "  primaryKey = 'id',\n" +
                      "  preCombineField = 'ts'\n" +
                      " )\n" +
                      "partitioned by (dt, hh)";
        String deleteSql40 = "truncate table hudi_cow_pt_tbl";  // 主键重复upsert，所以不需要truncate
        String insertSql41 = "-- insert dynamic partition\n" +
                      "insert into hudi_cow_pt_tbl partition (dt, hh)\n" +
                      "select 1 as id, 'a1' as name, 1000 as ts, '2021-12-09' as dt, '10' as hh";
        String insertSql42 = "-- insert static partition\n" +
                             "insert into hudi_cow_pt_tbl partition(dt = '2021-12-09', hh='11') " +
                             "select 2 as id, 'a2' as name, 1000 as ts union all select 3 as id, 'a3' as name, 1000 as ts";
        String updateSql43 = "update hudi_cow_pt_tbl set ts=1999 where id=2";
        String deleteSql44 = "delete from hudi_cow_pt_tbl where id=3";

        spark.sql(sql1);
        // spark.sql(sql2);
        // spark.sql(sql3);
        spark.sql(sql4);
        spark.sql(insertSql41);
        spark.sql(insertSql42);
        spark.sql(updateSql43);
        spark.sql(deleteSql44);
        spark.sql("select * from hudi_cow_pt_tbl").show();

        spark.sql("show tables").show();

        // Time Travel Query 时间线历史版本查询
        Dataset<Row> df = spark.sql("select _hoodie_commit_time from h1 group by _hoodie_commit_time order by _hoodie_commit_time desc");
        df.show();
        // 删除数据其实是有隐藏的commit time，因为没有数据所以无法查询出来
        String updateDT = df.takeAsList(2).get(0).getString(0);  // 降序，所以第一行的数据为最大时间
        String insertDT = df.takeAsList(2).get(1).getString(0);
        System.out.println("insertDT: " + insertDT + ", updateDT: " + updateDT);

        System.out.println("插入后的数据: ");
        spark.read()
                .format("hudi")
                .option("as.of.instant", insertDT)  // 闭区间（大于等于），包括当前时间戳，注意id为2和4的数据
                .load(basePath + tableName)
                .show();

        System.out.println("更新后的数据: ");
        spark.read()
                .format("hudi")
                .option("as.of.instant", updateDT)  // DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key()
                .load(basePath + tableName)
                .show();

        System.out.println("删除后的数据，也就是最新数据: ");
        spark.read()
                .format("hudi")
                .load(basePath + tableName)
                .show();

        // Incremental query
        spark.sql("select * from h1 where _hoodie_commit_time>='" + updateDT + "'").show();  // 当前镜像数据过滤
        spark.read()
                .format("hudi")
                .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())  // 不加该参数指示时间过滤查询，增加该参数变成 时间线 查询，同 as.of.instant
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), "000")  // Represents all commits > this time.
                .option(DataSourceReadOptions.END_INSTANTTIME().key(), updateDT)  // beginTime 推荐 000，设置endTime commit时间即可
                .load(basePath + tableName)
                .show();

        // test flink cdc insert data
        // spark.read()
        //         .format("hudi")
        //         .load("/user/work/tmp/tables/hudi_test4")
        //         .show();
        //
        spark.read()
                .format("hudi")
                .load("/user/work/tmp/tables/hudi_mysql_binlog_hive_metastore")
                .show(100,false);

        spark.stop();
    }
}
