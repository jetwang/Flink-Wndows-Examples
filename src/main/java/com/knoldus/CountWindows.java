package com.knoldus;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Logger;

public class CountWindows {

    private static final Logger LOGGER = Logger.getLogger(SlidingWindows.class);

    public static void main(String[] args) {
        LOGGER.info("Count window word count example.");

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = executionEnvironment
                .socketTextStream("localhost", 9000, '\n', 6);

        final DataStream<WordWithCount> reduce =

                text.flatMap((FlatMapFunction<String, WordWithCount>)
                        (textStream, wordCountKeyPair) -> {
                            for (String word : textStream.split("\\W")) {
                                wordCountKeyPair.collect(new WordWithCount(word, 1L));
                            }
                        }, TypeInformation.of(WordWithCount.class))
                        .keyBy((KeySelector<WordWithCount, String>) wordWithCount -> wordWithCount.word,
                                TypeInformation.of(String.class))
                        .countWindow(4)
                        .reduce((ReduceFunction<WordWithCount>)
                                (a, b) -> new WordWithCount(a.word, a.count + b.count));

        // print the results with a single thread, rather than in parallel
        reduce.print();

        executionEnvironment.execute("Socket Window WordCount");
    }
}
