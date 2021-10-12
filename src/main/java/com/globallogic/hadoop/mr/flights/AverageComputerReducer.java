package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.OptionalDouble;
import java.util.stream.StreamSupport;

public class AverageComputerReducer extends Reducer<
        Text,           // AIRLINE
        IntWritable,    // DEPARTURE_DELAY
        Text,           // AIRLINE
        DoubleWritable  // average departure delay
        > {

    @Override
    protected void reduce(Text airline, Iterable<IntWritable> delays, Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        OptionalDouble average = StreamSupport.stream(delays.spliterator(), false)
                .map(IntWritable::get)
                .mapToDouble(v -> v)
                .average();
        if (average.isPresent()) {
            context.write(airline, new DoubleWritable(average.getAsDouble()));
        }
    }
}
