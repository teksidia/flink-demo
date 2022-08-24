package com.pluralsight.flink.module2;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *  Solution to the task from video 15.
 *  Improves partitioning scheme and displays number of votes for each movie.
 */
public class UpdatedTop10Movies {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, Double, Long>> sorted = env.readCsvFile("ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long,Double>, Tuple3<Long, Double, Long>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple3<Long, Double, Long>> out) throws Exception {
                        Long movieId = null;
                        double total = 0;
                        long count = 0;
                        for (Tuple2<Long, Double> value : values) {
                            movieId = value.f0;
                            total += value.f1;
                            count++;
                        }

                        // Ignore movies that don't have enough ratings
                        if (count > 50) {
                            // Preserving number of votes
                            out.collect(new Tuple3<>(movieId, total / count, count));
                        }

                    }
                })
                .partitionCustom(new Partitioner<Double>() {
                    @Override
                    public int partition(Double key, int numPartitions) {
                        if (key.intValue() == 4 || key.intValue() == 5) {
                            return 3;
                        }
                        return key.intValue() % numPartitions - 1;
                    }
                }, 1)
                .setParallelism(4)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Long, Double, Long>> values, Collector<Tuple3<Long, Double, Long>> out) throws Exception {
                        Iterator<Tuple3<Long, Double, Long>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            out.collect(iter.next());
                        }
                    }
                })
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .mapPartition(new MapPartitionFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Long, Double, Long>> values, Collector<Tuple3<Long, Double, Long>> out) throws Exception {
                        Iterator<Tuple3<Long, Double, Long>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            out.collect(iter.next());
                        }
                    }
                });

        DataSet<Tuple2<Long, String>> movies = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .includeFields(true, true, false)
                .types(Long.class, String.class);

        DataSet<Tuple3<Long, String, Double>> result = movies.join(sorted)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long,String>, Tuple3<Long, Double, Long>, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> join(Tuple2<Long, String> first, Tuple3<Long, Double, Long> second) throws Exception {
                        // Output number of votes instead of movie ids
                        return new Tuple3<>(second.f2, first.f1, second.f1);
                    }
                });

        result.print();
    }
}
