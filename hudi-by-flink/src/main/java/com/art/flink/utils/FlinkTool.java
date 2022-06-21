package com.art.flink.utils;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class FlinkTool {

    public static boolean isDevelopment() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String ip = InetAddress.getLocalHost().getHostAddress();
            // System.out.println(hostname);
            // System.out.println(ip);
            return !hostname.contains("hadoop");
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void setEnvironment() {
        if (isDevelopment()) {
            System.setProperty("HADOOP_USER_NAME", "work");
            // 不添加yarn-site.xml等文件，使用本地模式运行时需要设置hadoop.home.dir路径
            // Could not locate executablenull\bin\winutils.exe in the Hadoop binaries。Windows下的特殊配置
            // System.setProperty("hadoop.home.dir", "E:\\Application\\hadoop-2.6.0");
            System.setProperty("hadoop.home.dir", "E:\\Appilaction\\hadoop-common-2.6.0-bin");
        }
    }

    public static StreamExecutionEnvironment createFlinkEnv() {
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
        // //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        // env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;
    }
}
