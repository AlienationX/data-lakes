package com.art.flink.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.util.Collector;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTableSink;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * flink-sql cdc 的缺点，后续需要解决：
 * 1.每个表就开启1个session，影响mysql服务会造成一定压力
 * 2.如何新增表?
 * 3.建表语句及表结构变化的问题?
 * 4.断点续传问题?
 */

public class MysqlBinlogSyncToHudiWithSinkError {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "work");
        System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test")  // set captured database
                .tableList("test.student, test.test4")  // 表名必须加上库名，且必须存在主键。支持正则。不写该属性代表监控库下所有表
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema())  // converts SourceRecord to JSON String
                // .deserializer(new StringDebeziumDeserializationSchema())
                // .deserializer(new CustomJsonDebeziumDeserializationSchema())
                // 启动参数 提供了如下几个静态方法
                // StartupOptions.initial() 第一次启动的时候，会把历史数据读过来（全量）做快照，后续读取binlog加载新的数据，如果不做 chackpoint 会存在重启又全量一遍。
                // StartupOptions.earliest() 只从binlog开始的位置读（源头），这里注意，如果binlog开启的时间比你建库时间晚，可能会读不到建库语句会报错，earliest要求能读到建表语句
                // StartupOptions.latest() 只从binlog最新的位置开始读
                // StartupOptions.specificOffset("mysql-bin.000008", 156431) 自指定从binlog的什么位置开始读
                // StartupOptions.timestamp(1639831160000L) 自指定binlog的开始时间戳
                .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // enable checkpoint
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        // // 需要设置checkpoints的存放路径，默认使用的是MemoryStateBackend
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:///user/work/flink-checkpoints/sync_hudi");
        // // env.setStateBackend(new FsStateBackend("hdfs:///user/work/flink-checkpoints/sync"));  // deprecated
        // // 设置两次checkpoint之间的最小时间间隔
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // // 检查点必须在1分钟内完成，或者被丢弃（checkpoint的超时时间，建议结合资源和占用情况，可以适当加大。时间短可能存在无法成功的情况）
        // env.getCheckpointConfig().setCheckpointTimeout(1000L * 30);
        // // 设置并发checkpoint的数目
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        // //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        // //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        // env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                // .setParallelism(1)
                .addSink(new HudiFlinkSqlSink());  // 写入未成功，未解决
                // .addSink(new HoodieTableSink())
                // .print().setParallelism(1);  // use parallelism 1 for sink to keep message ordering;


        env.execute("Print MySQL Snapshot + Binlog");
    }

    public static class CustomJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

        private Map<String, Object> structConvertToMap(Struct struct) {
            Map<String, Object> beforeMap = new HashMap<>();
            if (struct != null) {
                //获取列信息
                Schema schema = struct.schema();
                List<Field> fieldList = schema.fields();
                for (Field field:fieldList) {
                    beforeMap.put(field.name(), struct.get(field));
                }
            }
            return beforeMap;
        }

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            /**
             * StringDebeziumDeserializationSchema 的内容，不确定ConnectRecord的内容就等于SourceRecord？
             * SourceRecord{
             *     sourcePartition={server=mysql_binlog_source},
             *     sourceOffset={transaction_id=null, ts_sec=1650782056, file=, pos=0}
             *     }
             * ConnectRecord{
             *     topic='mysql_binlog_source.test.student',
             *     kafkaPartition=null,
             *     key=Struct{id=3},
             *     keySchema=Schema{mysql_binlog_source.test.student.Key:STRUCT},
             *     value=Struct{
             *         after=Struct{id=3,name=张三,sex=男,birthday=1995-04-05,create_time=2021-05-13T14:30:45Z},
             *         source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,db=test,table=student,server_id=0,file=,pos=0,row=0},
             *         op=r,
             *         ts_ms=1650782056551
             *         },
             *     valueSchema=Schema{mysql_binlog_source.test.student.Envelope:STRUCT},
             *     timestamp=null,
             *     headers=ConnectHeaders(headers=)
             *     }
             */
            Map<String, Object>  result = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();

            String topic = sourceRecord.topic();
            String[] fields = topic.split("\\.");
            result.put("db", fields[1]) ;
            result.put("tableName", fields[2]);
            // 获取before 数据. 以下的class都是引入的kafka里面的类，import org.apache.kafka.connect.data.Struct;
            Struct value = (Struct) sourceRecord.value();
            Struct before = value.getStruct("before");
            Map<String, Object> beforeMap = structConvertToMap(before);
            result.put("before", mapper.writeValueAsString(beforeMap));
            // 获取after 数据
            Struct after = value.getStruct("after");
            Map<String, Object> afterMap = structConvertToMap(after);
            result.put("after", mapper.writeValueAsString(afterMap));

            // 获取操作类型
            // Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            // result.put("op_class", operation.toString());  // READ(r), CREATE(r), UPDATE(u), DELETE(d), TRUNCATE(t)

            // custom
            result.put("id", UUID.randomUUID().toString().replace("-", ""));
            Struct key = (Struct) sourceRecord.key();
            Map<String, Object> keyMap = structConvertToMap(key);
            result.put("primary_key_data", mapper.writeValueAsString(keyMap));

            Struct source = value.getStruct("source");
            result.put("server_id", source.get("server_id"));
            result.put("file", source.get("file"));
            result.put("pos", source.get("pos"));
            result.put("op", value.get("op"));
            result.put("ts_ms", value.get("ts_ms"));

            //输出数据
            collector.collect(mapper.writeValueAsString(result));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return  BasicTypeInfo.STRING_TYPE_INFO;
        }
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
             "file": "",  // 读取快照该字段为空，读取binglog为 "mysql-bin.000190"
             "pos": 0,    // 读取快照该字段为0，读取binglog为 358
             "row": 0,
             "thread": null,
             "query": null
         },
         "op": "r",  // CRUD，初始化读取为r，创建为c，更新为u，删除为d
         "ts_ms": 1646806242214,
         "transaction": null
     }

     create table if not exists hudi_mysql_binlog_hive_metastore (
        id bigint,
        before string,
        after string,
        source string,
        op string,
        ts_ms bigint,
        transaction string,
        db string,
        tb string
     ) using hudi
     tblproperties  (
        type = 'mor',
        primaryKey = 'id',
        -- preCombineField = 'ts_ms',
        hoodie.sql.bulk.insert.enable = 'true',
        hoodie.sql.insert.mode = 'bulk_insert'
     )
     partitioned by (db, tb)
     location '/user/work/tmp/tables/hudi_mysql_binlog_hive_metastore'
    */

    public static String parseJson(String valueJson) throws IOException {
        //{"database":"test","data":{"name":"jacky","description":"fffff","id":8},"type":"insert","table":"test_cdc"}

        // gson会将int、long和double都识别为number类型，没有细分。这里会将int转换成double
        // 解决方法，使用javaBean，或者使用Jackson
        // Gson g = new Gson();
        // Map map = g.fromJson(value, HashMap.class);
        // System.out.println(map);

        // Double id = (Double) map.get("ts_ms");  // id.intValue() 可以再将double转换成int
        // String before = g.toJsonTree(map.get("before")).getAsJsonObject().toString());
        // String after = g.toJsonTree(map.get("after")).getAsJsonObject().toString();
        // String source = g.toJsonTree(map.get("source")).getAsJsonObject().toString();
        // String op = (String) map.get("op");
        // Double ts_ms = (Double)  map.get("ts_ms");
        // String transaction = (String) map.get("transaction");
        // String db = (String) map.get("db");
        // String tb = (String) map.get("table");

        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(valueJson, Map.class);
        // System.out.println(map);

        Long id = (Long) map.get("ts_ms");
        String before = mapper.writeValueAsString(map.get("before"));
        String after = mapper.writeValueAsString(map.get("after"));
        String source = mapper.writeValueAsString(map.get("source"));
        String op = (String) map.get("op");
        Long ts_ms = (Long)  map.get("ts_ms");
        String transaction = (String) map.get("transaction");
        String db = (String)((LinkedHashMap)map.get("source")).get("db");
        String tb = (String)((LinkedHashMap)map.get("source")).get("table");

        // null值暂时当作字符串处理
        String selectSql = String.format("select %s as id, '%s' as before, '%s' as after, '%s' as source, '%s' as op, %s as ts_ms, '%s' as transaction, '%s' as db, '%s' as tb", id, before, after, source, op, ts_ms, transaction, db, tb);
        // String valuesStr = String.format("(%s, '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')", id, before, after, source, op, ts_ms, transaction, db, tb);
        String valuesStr = "(1, 'a', 'b', 'c', 'd', 2, 'e', 'f', 'g')";
        System.out.println(selectSql);
        return valuesStr;
    }

    public static class HudiFlinkSqlSink extends RichSinkFunction<String> {

        EnvironmentSettings settings = null;
        TableEnvironment tableEnv = null;
        String basePath = "/user/work/tmp/tables/";
        String tableName = "hudi_mysql_binlog_hive_metastore";
        String parallelism = "4";  // 设置并行度，初始化 op=r

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            settings = EnvironmentSettings
                    .newInstance()
                    // .inBatchMode()  // 设置批模式
                    .build();
            tableEnv = TableEnvironment.create(settings);

            // Configuration execConf = tableEnv.getConfig().getConfiguration();
            // execConf.setString("execution.checkpointing.interval", "2s");
            // // configure not to retry after failure
            // execConf.setString("restart-strategy", "fixed-delay");
            // execConf.setString("restart-strategy.fixed-delay.attempts", "0");

            String createSql = "create table if not exists " + tableName + " (\n"
                               + "    id bigint,\n"
                               + "    before string,\n"
                               + "    after string,\n"
                               + "    source string,\n"
                               + "    op string,\n"
                               + "    ts_ms bigint,\n"
                               + "    transaction string,\n"
                               + "    db string,\n"
                               + "    tb string\n"
                               + ") \n"
                               + "partitioned by (db, tb) \n"
                               + "with (\n"
                               + "    'connector' = 'hudi',\n"
                               + "    'path' = 'hdfs://" + basePath + tableName + "',\n"
                               // + "    'path' = 'file:///E:/flink_hudi_table',\n"
                               + "    'table.type' = 'MERGE_ON_READ',\n"
                               + "    'hoodie.datasource.write.recordkey.field' = 'id',\n"
                               + "    'hoodie.precombine.field' = 'ts_ms',\n"
                               + "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n"
                               + "    'write.operation' = 'upsert',\n"
                               // + "    'hoodie.embed.timeline.server' = 'false',\n"
                               + "    'write.task' = '1'\n"
                               + ")";
            tableEnv.executeSql(createSql);
        }

        // 每条记录插入时调用一次
        @Override
        public void invoke(String value, Context context) throws Exception {
            String selectSql = parseJson(value);
            String insertSql = "insert into " + tableName + " values " + selectSql;
            System.out.println(insertSql);
            tableEnv.executeSql(insertSql);
            // TableResult tableResult = tableEnv.executeSql(insertSql);
            // // wait to finish
            // tableResult.getJobClient().get().getJobExecutionResult().get();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}

