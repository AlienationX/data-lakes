package com.art.flink.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

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

public class MysqlBinlogSyncToHudiExample {
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
                // .deserializer(new JsonDebeziumDeserializationSchema())  // converts SourceRecord to JSON String
                // .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new CustomJsonDebeziumDeserializationSchema())
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

        // SingleOutputStreamOperator<String> dataSourceRD = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        //         .map(x -> x);

        // 将 DataStream 转换成 Table
        SingleOutputStreamOperator<Record> dataSourceRD = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map(line -> {
                    ObjectMapper mapper = new ObjectMapper();
                    Map result = mapper.readValue(line, Map.class);
                    // System.out.println(result);

                    // GenericRowData row = new GenericRowData(9);
                    // row.setField(0, result.get("id"));
                    // row.setField(1, result.get("before"));
                    // row.setField(2, result.get("after"));
                    // row.setField(3, result.get("source"));
                    // row.setField(4, result.get("op"));
                    // row.setField(5, result.get("tm_ms"));
                    // row.setField(6, result.get("transaction"));
                    // row.setField(7, result.get("db"));
                    // row.setField(8, result.get("tb"));
                    // return row;

                    return new Record(
                            result.get("id").toString(),
                            result.get("server_id").toString(),
                            result.get("file").toString(),
                            Long.valueOf(result.get("pos").toString()),
                            result.get("before").toString(),
                            result.get("after").toString(),
                            result.get("op").toString(),
                            Long.valueOf(result.get("ts_ms").toString()),
                            // result.get("transaction").toString(),
                            result.get("primary_key_data").toString(),
                            result.get("db").toString(),
                            result.get("tb").toString()
                    );
                });

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String basePath = "/user/work/tmp/tables/";
        String tableName = "hudi_mysql_binlog_hive_metastore";
        String createSql = "create table if not exists " + tableName + " (\n" +
                           "    id string,\n" +
                           "    server_id string,\n" +
                           "    file string,\n" +
                           "    pos bigint,\n" +
                           "    before string,\n" +
                           "    after string,\n" +
                           "    op string,\n" +
                           "    ts_ms bigint,\n" +
                           // "    transaction string,\n" +
                           "    primary_key_data string,\n" +
                           "    db string,\n" +
                           "    tb string\n" +
                           ") \n" +
                           "partitioned by (db, tb) \n" +
                           "with (\n" +
                           "    'connector' = 'hudi',\n" +
                           "    'path' = 'hdfs://" + basePath + tableName + "',\n" +
                           "    'table.type' = 'MERGE_ON_READ',\n" +
                           "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
                           "    'hoodie.precombine.field' = 'ts_ms',\n" +
                           "    'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
                           "    'read.streaming.enabled' = 'true',\n" +
                           "    'read.streaming.check-interval' = '1',\n" +
                           "    'write.operation' = 'upsert',\n" +
                           "    'write.task' = '1'\n" +
                           ")";
        tableEnv.executeSql(createSql);

        Table dataSource = tableEnv.fromDataStream(dataSourceRD);
        tableEnv.executeSql("insert into " + tableName + " select * from " + dataSource);
        tableEnv.executeSql("select * from hudi_mysql_binlog_hive_metastore").print();

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
            result.put("tb", fields[2]);
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

    /** Simple POJO. */
    public static class Record {
        public String id;
        public String server_id;
        public String file;
        public Long pos;
        public String before;
        public String after;
        public String op;
        public Long ts_ms;
        // public String transaction;
        public String primary_key_data;
        public String db;
        public String tb;

        // for POJO detection in DataStream API
        public Record() {}

        // for structured type detection in Table API
        public Record(String id, String server_id, String file, Long pos, String before, String after, String op, Long ts_ms, String primary_key_data, String db, String tb) {
            this.id = id;
            this.server_id = server_id;
            this.file = file;
            this.pos = pos;
            this.before = before;
            this.after = after;
            this.op = op;
            this.ts_ms = ts_ms;
            this.primary_key_data = primary_key_data;
            this.db = db;
            this.tb = tb;
        }

        @Override
        public String toString() {
            return "Order {" +
                   "id=" + id +
                   "server_id=" + server_id +
                   "file=" + file +
                   "pos=" + pos +
                   "before=" + before +
                   "after=" + after +
                   "op=" + op +
                   "primary_key_data=" + primary_key_data +
                   "db=" + db +
                   "tb=" + tb +
                   "}";
        }
    }
}