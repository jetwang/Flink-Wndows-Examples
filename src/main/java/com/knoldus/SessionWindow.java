package com.knoldus;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Logger;

public class SessionWindow {

    private static final Logger LOGGER = Logger.getLogger(SlidingWindows.class);

    public static void main(String[] args) throws NoSuchFieldException {
        LOGGER.info("Session window example.");

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = executionEnvironment
                .socketTextStream("localhost", 9000, '\n', 6);

        DataStream<Tuple3<String,String, Double>> userClickStream = text.map(row -> {
            String[] fields = row.split(",");
            if (fields.length == 3) {
                return new Tuple3<>(
                        fields[0],
                        fields[1],
                        Double.parseDouble(fields[2])
                );
            }
            throw new Exception("Not valid arg passed");
        }, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
        }));

        DataStream<Tuple3<String, String, Double>> maxPageVisitTime =
                userClickStream.keyBy(((KeySelector<Tuple3<String, String, Double>,
                        Tuple2<String, String>>) stringStringDoubleTuple3 ->
                new Tuple2<>(stringStringDoubleTuple3.f0, stringStringDoubleTuple3.f1)),
                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }))
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2);

        maxPageVisitTime.print();

        executionEnvironment.execute("Session window example.");
    }
}
