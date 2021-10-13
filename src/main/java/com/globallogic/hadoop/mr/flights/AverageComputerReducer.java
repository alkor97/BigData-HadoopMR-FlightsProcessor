package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AverageComputerReducer extends Reducer<
        Text,            // IATA_CODE
        VariantWritable, // DEPARTURE_DELAY or AIRLINE
        Text,            // IATA_CODE
        DoubleWritable   // average departure delay
        > {

    private final Map<String, String> airlineNames = new HashMap<>();
    private final Map<String, Double> averageDelays = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<VariantWritable> data, Reducer<Text, VariantWritable, Text, DoubleWritable>.Context context) {
        final String iataCode = key.toString();

        double sum = 0.0;
        long count = 0;
        for (VariantWritable variant : data) {
            Writable value = variant.get();
            if (value instanceof IntWritable) {
                sum += variant.getInt().get();
                ++count;
            } else if (value instanceof Text) {
                airlineNames.put(key.toString(), variant.getText().toString());
            }
        }

        if (count > 0) {
            averageDelays.put(key.toString(), sum / count);
        }
    }

    @Override
    protected void cleanup(Reducer<Text, VariantWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        try {
            averageDelays.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(5)
                    .forEach(entry -> report(context, entry));
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof InterruptedException) {
                throw (InterruptedException) e.getCause();
            }
        }
    }

    private void report(Reducer<Text, VariantWritable, Text, DoubleWritable>.Context context, Map.Entry<String, Double> entry) {
        try {
            String iataCode = entry.getKey();
            String airlineName = airlineNames.getOrDefault(iataCode, null);
            String text = airlineName != null ? (airlineName + " (" + iataCode + ")") : (iataCode);
            context.write(
                    new Text(text),
                    new DoubleWritable(entry.getValue()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
