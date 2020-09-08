package com.knoldus;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import scala.Function1;
import scala.collection.Seq;

import java.util.Arrays;

public class TumblingWindows {

    private static final Logger LOGGER = Logger.getLogger(SlidingWindows.class);

    public static void main(String[] args) {
        LOGGER.info("Tumbling window word count example.");

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = executionEnvironment
                .socketTextStream("localhost", 9000, '\n',6);

        final DataStream<WordWithCount> reduce = text
                .flatMap((FlatMapFunction<String, WordWithCount>) (textStream, wordCountKeyPair) -> {
                    for (String word : textStream.split("\\W")) {
                        wordCountKeyPair.collect(new WordWithCount(word, 1L));
                    }
                }, TypeInformation.of(WordWithCount.class))
                .keyBy((KeySelector<WordWithCount, String>) wordWithCount -> wordWithCount.word,
                        TypeInformation.of(String.class))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<WordWithCount>)
                        (a, b) -> new WordWithCount(a.word, a.count + b.count));

        // print the results with a single thread, rather than in parallel
        reduce.print();

        executionEnvironment.execute("Socket Window WordCount");
    }


}
