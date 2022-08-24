package com.pluralsight.flink.module2;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

/**
 *  Solution to the task from video 10.
 *  Finds how many movies of genres there is in the movies dataset.
 */
public class GenresDistribution {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, String, String>> movies = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        movies
                // Extract genre of each movie from the movies dataset
                .map(new MapFunction<Tuple3<Long, String, String>, Tuple1<StringValue>>() {

                    StringValue movieGenre = new StringValue();

                    Tuple1<StringValue> result = new Tuple1<>(movieGenre);

                    @Override
                    public Tuple1<StringValue> map(Tuple3<Long, String, String> movie) throws Exception {
                        String genreValue = movie.f2.split("\\|")[0];

                        movieGenre.setValue(genreValue);
                        return result;
                    }
                }) // DataSet<Tuple1<StringValue>>
                // Count how many movies of each genre do we have
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple1<StringValue>, Tuple2<StringValue, LongValue>>() {

                    StringValue genre = new StringValue();
                    LongValue count = new LongValue();

                    Tuple2<StringValue, LongValue> result = new Tuple2<>(genre, count);

                    @Override
                    public void reduce(Iterable<Tuple1<StringValue>> values, Collector<Tuple2<StringValue, LongValue>> out) throws Exception {
                        long countValue = 0;
                        String genreValue = null;
                        for (Tuple1<StringValue> value : values) {
                            countValue++;
                            genreValue = value.f0.getValue();
                        }

                        genre.setValue(genreValue);
                        count.setValue(countValue);
                        out.collect(result);
                    }
                }) // DataSet<Tuple2<StringValue, LongValue>>
                .print();

    }
}
