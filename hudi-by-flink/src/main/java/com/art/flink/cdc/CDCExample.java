package com.art.flink.cdc;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CDCExample {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test")  // set captured database
                .tableList("test.student, test.test4")  // 表名必须加上库名，且必须存在主键。库下所有表可以使用 test.*
                .username("root")
                .password("root")
                // .deserializer(new JsonDebeziumDeserializationSchema())  // converts SourceRecord to JSON String
                // .deserializer(new JsonDebeziumDeserializationSchema(true))  // 包括字段类型及备注，信息太多
                .deserializer(new StringDebeziumDeserializationSchema())
                // 启动参数 提供了如下几个静态方法
                // StartupOptions.initial() 第一次启动的时候，会把历史数据读过来（全量）做快照，后续读取binlog加载新的数据，如果不做 chackpoint 会存在重启又全量一遍。
                // StartupOptions.earliest() 只从binlog开始的位置读（源头），这里注意，如果binlog开启的时间比你建库时间晚，可能会读不到建库语句会报错，earliest要求能读到建表语句
                // StartupOptions.latest() 只从binlog最新的位置开始读
                // StartupOptions.specificOffset("mysql-bin.000008", 156431) 自指定从binlog的什么位置开始读
                // StartupOptions.timestamp(1639831160000L) 自指定binlog的开始时间戳
                // .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print().setParallelism(1);  // use parallelism 1 for sink to keep message ordering;

        env.execute("Print MySQL Snapshot + Binlog");
    }

    /**
     {
         "before": null,
         "after": {
             "id": 4,
             "data": "i am dt testing",
             "data2": "2021-05-13 11:36:52"
         },
         "source": {
             "version": "1.5.4.Final",
             "connector": "mysql",
             "name": "mysql_binlog_source",
             "ts_ms": 1646806242214,
             "snapshot": "false",
             "db": "test",
             "sequence": null,
             "table": "test4",
             "server_id": 0,
             "gtid": null,
             "file": "",
             "pos": 0,
             "row": 0,
             "thread": null,
             "query": null
         },
         "op": "r",  // 初始化读取为r，创建为c，更新为u，删除为d
         "ts_ms": 1646806242214,
         "transaction": null
     }
     TODO: 1. 测试ddl语句无法识别，可能低版本问题
           2. 无法确定 primary 字段和 combine 字段, 也就无法使用 df.write 方式
           3. 可以使用 spark sql 的 merge 语句实现增删改，但是也需要 primary key 进行 on 关联
           4. 使用 flink sql ?
    */
    public static class HudiSink extends RichSinkFunction<String> {

        SparkSession spark;
        String basePath = "/tmp/external/";  // /user/work/tmp/tables/

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            spark = SparkSession.builder()
                    .master("local")
                    .appName("hudi example")
                    .config("spark.some.config.option", "some-value")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 使用hudi必须设置
                    .config("spark.sql.hive.convertMetastoreParquet", "false") // Uses Hive SerDe, this is mandatory for MoR tables
                    .enableHiveSupport()
                    .getOrCreate();
        }

        // 每条记录插入时调用一次
        @Override
        public void invoke(String value, Context context) throws Exception {
            //{"database":"test","data":{"name":"jacky","description":"fffff","id":8},"type":"insert","table":"test_cdc"}
            // Gson t = new Gson();
            // HashMap<String,Object> hs = t.fromJson(value,HashMap.class);
            // String database = (String)hs.get("database");
            // String table = (String)hs.get("table");
            // String type = (String)hs.get("type");
            //
            // if("test".equals(database) && "test_cdc".equals(table)){
            //     if("insert".equals(type)){
            //         System.out.println("insert => "+value);
            //         LinkedTreeMap<String,Object> data = (LinkedTreeMap<String,Object>)hs.get("data");
            //         String name = (String)data.get("name");
            //         String description = (String)data.get("description");
            //         Double id = (Double)data.get("id");
            //         // 未前面的占位符赋值
            //         pstmt.setInt(1, id.intValue());
            //         pstmt.setString(2, name);
            //         pstmt.setString(3, description);
            //
            //         pstmt.executeUpdate();
            //     }
            // }

            Dataset<Row> df = spark.sql("" +
                    "select 2 as id, 'bbb' as name, 99  as price, 'U' as flag, '2020-01-01' as update_date union all " +
                    "select 4 as id, 'ddd' as name, 100 as price, 'I' as flag, '2020-01-01' as update_date union all " +
                    "select 9 as id, 'zzz' as name, -99 as price, 'D' as flag, '2020-01-01' as update_date union all " +
                    "select 0 as id, 'zzz' as name, 0   as price, 'D' as flag, '2020-01-01' as update_date"
            );
            String tableName = "";
            String opType = "";
            if (opType.equals("r")) {
                df.write()
                        .format("hudi")  // 高版本使用hudi也可
                        .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())  // 设置写入方式insert
                        .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型为mor
                        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
                        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")
                        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
                        .option("hoodie.upsert.shuffle.parallelism", "4")
                        .mode(SaveMode.Overwrite)  // 初始化覆盖
                        .save(basePath + tableName);
            } else if (opType.equals("d")) {
                df.write()
                        .format("hudi")  // 高版本使用hudi也可
                        .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL())  // 设置写入方式delete
                        .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型为mor
                        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
                        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")
                        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
                        .option("hoodie.upsert.shuffle.parallelism", "1")  // 设置并行数
                        .mode(SaveMode.Append)
                        .save(basePath + tableName);
            } else {
                df.write()
                        .format("hudi")  // 高版本使用hudi也可
                        .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())  // 设置写入方式upsert
                        .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())   // 设置表类型为mor
                        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id")
                        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "update_date")
                        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
                        .option("hoodie.upsert.shuffle.parallelism", "1")
                        .mode(SaveMode.Append)
                        .save(basePath + tableName);
            }

        }

        @Override
        public void close() throws Exception {
            super.close();

            if(spark != null) {
                spark.stop();
            }
        }
    }

}
