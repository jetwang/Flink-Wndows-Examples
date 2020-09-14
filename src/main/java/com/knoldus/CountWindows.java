package com.knoldus;

import model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * CountWindows Class contains a method CountWindows that contains
 * implementation of word count problem using Count window.
 */
public final class CountWindows {


    public final void countWindow() {

        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<String> text = executionEnvironment
                .socketTextStream("localhost", 9000, '\n', 6);

        final DataStream<WordWithCount> countWindowWordCount =

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
        countWindowWordCount.print();

        executionEnvironment.execute("Flink Count window Example");
    }
}
