package com.art.flink.examples.cdc;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;


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
                .startupOptions(StartupOptions.initial())
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
}
