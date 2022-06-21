package com.art.flink.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.art.flink.schema.HudiSchemaMysqlBinlog;
import com.art.flink.schema.HudiSchemaODS;
import com.art.flink.utils.FlinkTool;

public class ParseMysqlBinlogMergeTable {

    public static void main(String[] args) throws Exception {

        FlinkTool.setEnvironment();
        StreamExecutionEnvironment env = FlinkTool.createFlinkEnv();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String databaseName = parameter.getRequired("d");  // 单横杠和双横杠都支持
        String tableName = parameter.getRequired("t");
        System.out.println("dbName: " + databaseName);
        System.out.println("tableName: " + tableName);
        String hostName = parameter.get("host.name", "-");
        System.out.println("hostName: " + hostName);

        tableEnv.executeSql(HudiSchemaMysqlBinlog.hudi_mysql_binlog_local);
        String odsTableName = "ods_" + databaseName + "_" + tableName;
        String createSql = HudiSchemaODS.TABLES.get(odsTableName);
        System.out.println(createSql);
        tableEnv.executeSql(createSql);
        Table table = tableEnv.sqlQuery("select * from " + odsTableName + " limit 0");
        List<Column> columns = table.getResolvedSchema().getColumns();
        List<String> columnsName = new ArrayList<>();
        List<String> columnsType = new ArrayList<>();
        for (Column column : columns) {
            String columnName = column.getName();
            String columnType = column.getDataType().toString();
            columnsName.add(columnName);
            columnsType.add(columnType);
        }

        Table binlogTable = tableEnv.sqlQuery("select * from hudi_mysql_binlog_local where db = '" + databaseName + "' and tb='" + tableName + "'");
        tableEnv.toDataStream(binlogTable).print("log");
        tableEnv.toDataStream(binlogTable).map(new MyRichMapper(odsTableName, columnsName, columnsType)).print("map");
        // tableEnv.toDataStream(binlogTable).map(new MyRichMapper(odsTableName, columnsName, columnsType)).print("map");


        // CloseableIterator 关闭流不推荐？
        // CloseableIterator<Row> iterator = binlogTable.select($("before"), $("after"), $("op"), $("primary_key_data")).execute().collect();
        // iterator.forEachRemaining(row -> {
        //     System.out.println(row);
        //     String op = Objects.requireNonNull(row.getField("op")).toString();
        //     String beforeJson = Objects.requireNonNull(row.getField("before")).toString();
        //     String afterJson = Objects.requireNonNull(row.getField("after")).toString();
        //     String primaryKeyDataJson = Objects.requireNonNull(row.getField("primary_key_data")).toString();
        //     String sql = null;
        //     try {
        //         // crud
        //         if ("c".equals(op) || "r".equals(op)) {
        //             sql = parseInsertMode(afterJson, tableName, columns);
        //         } else if ("u".equals(op)) {
        //             sql = parseUpdateMode(afterJson, primaryKeyDataJson, tableName, columns);
        //         } else if ("d".equals(op)) {
        //             sql = parseDeleteMode(beforeJson, tableName, columns);
        //         } else {
        //             System.out.println("ERROR: op(" + op + ") not catch ! " + row);
        //         }
        //         System.out.println(sql);
        //         // tableEnv.executeSql(sql);
        //     } catch (Exception e) {
        //         e.printStackTrace();
        //     }
        // });

        // tableEnv.executeSql("select * from hudi_mysql_binlog_local where tb='" + srcTableName + "'").print();
        env.execute("Merge " + tableName + " Data To Hudi");
    }

    public static class MyRichMapper extends RichMapFunction<Row, String> {
        private String tableName;
        private List<String> columnsName;
        private List<String> columnsType;

        public MyRichMapper(String tableName, List<String> columnsName, List<String> columnsType) {
            this.tableName = tableName;
            this.columnsName = columnsName;
            this.columnsType = columnsType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open 生命周期被调用：" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public String map(Row row) throws Exception {
            System.out.println("row: " + row);
            String op = Objects.requireNonNull(row.getField("op")).toString();
            // String tableName = Objects.requireNonNull(row.getField("tb")).toString();
            String beforeJson = Objects.requireNonNull(row.getField("before")).toString();
            String afterJson = Objects.requireNonNull(row.getField("after")).toString();
            String primaryKeyDataJson = Objects.requireNonNull(row.getField("primary_key_data")).toString();
            String sql = null;

            // crud
            if ("c".equals(op) || "r".equals(op)) {
                sql = parseInsertModeWithList(afterJson, tableName, columnsName, columnsType);
            // } else if ("u".equals(op)) {
            //     sql = parseUpdateMode(afterJson, primaryKeyDataJson, tableName, columns);
            // } else if ("d".equals(op)) {
            //     sql = parseDeleteMode(beforeJson, tableName, columns);
            } else {
                System.out.println("ERROR: op(" + op + ") not catch ! " + row);
            }
            // System.out.println("sql: " + sql);
            // tableEnv.executeSql(sql);
            return sql;
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close 生命周期被调用：" + getRuntimeContext().getIndexOfThisSubtask() + "号任务关闭");
        }
    }

    public static String parseInsertModeWithNull(String dataJson, String tableName, List<Column> columns) throws Exception {
        // {"birthday":null,"create_time":"2022-03-07T16:47:13Z","sex":null,"name":"aaa","id":-1}
        ObjectMapper mapper = new ObjectMapper();
        Map data = mapper.readValue(dataJson, Map.class);
        // System.out.println(dataJson);

        StringBuilder insertSql = new StringBuilder().append("insert into ").append(tableName).append(" values (");
        int length = columns.size();
        for (int i = 0; i < length; i++) {
            String columnName = columns.get(i).getName();
            String columnType = columns.get(i).getDataType().toString();
            Object value = data.get(columnName);
            if (value == null) {
                insertSql.append("null");
            } else {
                String valueStr = convertValue(columnType, value);
                insertSql.append(valueStr);
            }
            if (i != length - 1) {
                insertSql.append(", ");
            }
        }
        insertSql.append(")");
        return insertSql.toString();
    }

    public static String parseInsertModeWithList(String dataJson, String tableName, List<String> columnsName, List<String> columnsType) throws Exception {
        // {"birthday":null,"create_time":"2022-03-07T16:47:13Z","sex":null,"name":"aaa","id":-1}
        // flink sql 不支持单条数据的 insert 不能包含null值？？？
        ObjectMapper mapper = new ObjectMapper();
        Map data = mapper.readValue(dataJson, Map.class);
        // System.out.println(dataJson);

        StringBuilder insertSql = new StringBuilder();
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();
        int length = columnsName.size();
        for (int i = 0; i < length; i++) {
            String columnName = columnsName.get(i);
            String columnType = columnsType.get(i);
            Object value = data.get(columnName);
            if (value == null) {
                continue;
            }
            String valueStr = convertValue(columnType, value);
            keys.append(columnName);
            values.append(valueStr);

            if (i != length - 1) {
                keys.append(", ");
                values.append(", ");
            }
        }
        insertSql.append("insert into ").append(tableName).append(" (").append(keys).append(") values (").append(values).append(")");
        return insertSql.toString();
    }

    public static String parseInsertMode(String dataJson, String tableName, List<Column> columns) throws Exception {
        // {"birthday":null,"create_time":"2022-03-07T16:47:13Z","sex":null,"name":"aaa","id":-1}
        // flink sql 不支持单条数据的 insert 不能包含null值？？？
        ObjectMapper mapper = new ObjectMapper();
        Map data = mapper.readValue(dataJson, Map.class);
        // System.out.println(dataJson);

        StringBuilder insertSql = new StringBuilder();
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();
        int length = columns.size();
        for (int i = 0; i < length; i++) {
            String columnName = columns.get(i).getName();
            String columnType = columns.get(i).getDataType().toString();
            Object value = data.get(columnName);
            if (value == null) {
                continue;
            }
            String valueStr = convertValue(columnType, value);
            keys.append(columnName);
            values.append(valueStr);

            if (i != length - 1) {
                keys.append(", ");
                values.append(", ");
            }
        }
        insertSql.append("insert into ").append(tableName).append(" (").append(keys).append(") values (").append(values).append(")");
        return insertSql.toString();
    }

    public static String parseUpdateMode(String dataJson, String primaryKeyDataJson, String tableName, List<Column> columns) throws Exception {
        // {"birthday":null,"create_time":"2022-03-07T16:47:13Z","sex":null,"name":"aaa","id":-1}
        // flink sql 不支持单条数据的 insert 不能包含null值？？？
        ObjectMapper mapper = new ObjectMapper();
        Map data = mapper.readValue(dataJson, Map.class);
        Map primaryKeyData = mapper.readValue(primaryKeyDataJson, Map.class);
        // System.out.println(dataJson);

        StringBuilder insertSql = new StringBuilder();
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();
        int length = columns.size();
        for (int i = 0; i < length; i++) {
            String columnName = columns.get(i).getName();
            String columnType = columns.get(i).getDataType().toString();
            Object value = data.get(columnName);
            if (value == null) {
                continue;
            }
            String valueStr = convertValue(columnType, value);
            if (primaryKeyData.containsKey(columnName)) {
                keys.append(columnName).append("=").append(columnName);
            } else {
                values.append(columnName).append("=").append(valueStr);
            }

            if (i != length - 1) {
                keys.append(" and ");
                values.append(", ");
            }
        }
        insertSql.append("update from ").append(tableName).append("set ").append(values).append(" where ").append(keys);
        return insertSql.toString();
    }

    public static String parseDeleteMode(String primaryKeyDataJson, String tableName, List<Column> columns) throws Exception {
        // {"id":1,"name":"abc"}
        // flink sql 不支持单条数据的 insert 不能包含null值？？？
        ObjectMapper mapper = new ObjectMapper();
        Map data = mapper.readValue(primaryKeyDataJson, Map.class);
        // System.out.println(dataJson);

        StringBuilder deleteSql = new StringBuilder();
        StringBuilder whereSql = new StringBuilder();
        int length = columns.size();
        for (int i = 0; i < length; i++) {
            String columnName = columns.get(i).getName();
            if (!data.containsKey(columnName)) {
                continue;
            }

            // 理论上主键不可能为null，暂不做null值判断
            Object value = data.get(columnName);
            // if (value == null) {
            //     continue;
            // }

            String columnType = columns.get(i).getDataType().toString();
            String valueStr = convertValue(columnType, value);
            whereSql.append(columnName).append("=").append(valueStr);
            if (i != length - 1) {
                whereSql.append(" and ");
            }
        }
        deleteSql.append("delete from ").append(tableName).append(" where ").append(whereSql);
        return deleteSql.toString();
    }

    public static String convertValue(String columnType, Object value) {
        String convertValue;
        if (columnType.contains("INT") || columnType.contains("FLOAT") || columnType.contains("DOUBLE") || columnType.contains("DECIMAL")) {
            convertValue = value.toString();
        } else if (columnType.contains("TIMESTAMP")) {
            convertValue = "to_timestamp('" + value + "')";
        } else {
            // TODO: boolean, date 的特殊处理
            // 单引号转义
            if (value.toString().contains("'")) {
                value = value.toString().replaceAll("'", "''");
            }
            convertValue = "'" + value + "'";
        }
        return convertValue;
    }

}
