package com.art.flink.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void example(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> text = null;
        // if (params.has("input")) {
        //     UnionOperator unionOperator;
        //     for (String input : params.getMultiParameterRequired("input")) {
        //         DataSource dataSource;
        //         if (text == null) {
        //             dataSource = env.readTextFile(input);
        //             continue;
        //         }
        //         unionOperator = dataSource.union((DataSet)env.readTextFile(input));
        //     }
        //     Preconditions.checkNotNull(unionOperator, "Input DataSet should not be null.");
        // } else {
        //     System.out.println("Executing WordCount example with default input data set.");
        //     System.out.println("Use --input to specify file input.");
        //     DataSet<String> wordCountData = (DataSet<String>)env.fromElements((Object[])WORDS);
        //     text = wordCountData.getDefaultTextLineDataSet(env);
        // }
        // String[] words = new String[] {
        //         "To be, or not to be,--that is the question:--", "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune", "Or to take arms against a sea of troubles,", "And by opposing end them?--To die,--to sleep,--", "No more; and by a sleep to say we end", "The heartache, and the thousand natural shocks", "That flesh is heir to,--'tis a consummation", "Devoutly to be wish'd. To die,--to sleep;--", "To sleep! perchance to dream:--ay, there's the rub;",
        //         "For in that sleep of death what dreams may come,", "When we have shuffled off this mortal coil,", "Must give us pause: there's the respect", "That makes calamity of so long life;", "For who would bear the whips and scorns of time,", "The oppressor's wrong, the proud man's contumely,", "The pangs of despis'd love, the law's delay,", "The insolence of office, and the spurns", "That patient merit of the unworthy takes,", "When he himself might his quietus make",
        //         "With a bare bodkin? who would these fardels bear,", "To grunt and sweat under a weary life,", "But that the dread of something after death,--", "The undiscover'd country, from whose bourn", "No traveller returns,--puzzles the will,", "And makes us rather bear those ills we have", "Than fly to others that we know not of?", "Thus conscience does make cowards of us all;", "And thus the native hue of resolution", "Is sicklied o'er with the pale cast of thought;",
        //         "And enterprises of great pith and moment,", "With this regard, their currents turn awry,", "And lose the name of action.--Soft you now!", "The fair Ophelia!--Nymph, in thy orisons", "Be all my sins remember'd." };
        String[] words = new String[] {
                "python python java",
                "shell java javascript",
                "C C++ C# SQL",
                "SQL java scala java python html"
        };
        text = env.fromElements(words);

        AggregateOperator aggregateOperator = text.flatMap(new Tokenizer())
                .groupBy(new int[] { 0 })
                .sum(1);
        // if (params.has("output")) {
        //     aggregateOperator.writeAsCsv(params.get("output"), "\n", " ");
        //     env.execute("WordCount Example");
        // } else {
        //     System.out.println("Printing result to stdout. Use --output to specify output path.");
        //     aggregateOperator.print();
        // }
        aggregateOperator.print();

        // 设置成1个分区并排序打印
        System.out.println("----");
        aggregateOperator.setParallelism(1).sortPartition(1, Order.DESCENDING).print();
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0)
                    out.collect(new Tuple2(token, Integer.valueOf(1)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 并行度设置为1，方便全局排序打印出结果

        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                // .flatMap(new LineSplitter())
                .flatMap(new LineSplitter())
                .groupBy(0)  // 按照字段位置0 groupby
                .sum(1)  // 字段1使用sum
                // .setParallelism(1)  // 并行度设置为1，方便全局排序打印出结果
                .sortPartition(1, Order.DESCENDING);  // 按照字段1降序排列，注意仅仅是分区排序

        wordCounts.print();

        example(args);
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
