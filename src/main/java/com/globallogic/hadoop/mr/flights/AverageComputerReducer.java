package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class AverageComputerReducer extends Reducer<
        Text,            // IATA_CODE
        Payload,         // name + sum of delays + count
        Text,            // name + IATA code
        DoubleWritable   // average departure delay
        > {

    private final Map<String, Payload> data = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Payload> values, Reducer<Text, Payload, Text, DoubleWritable>.Context context) {
        final String iataCode = key.toString();
        Payload stored = data.getOrDefault(iataCode, new Payload());
        stored.mergeInto(values);
        data.put(iataCode, stored);
    }

    @Override
    protected void cleanup(Reducer<Text, Payload, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        try {
            data.entrySet().stream()
                    .filter(entry -> entry.getValue().getCount() > 0)
                    .map(this::map)
                    .sorted(Comparator.comparing(Payload::getDelay).reversed())
                    .limit(5)
                    .forEach(payload -> report(payload, context));
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof InterruptedException) {
                throw (InterruptedException) e.getCause();
            }
        }
    }

    private Payload map(Map.Entry<String, Payload> entry) {
        String iataCode = entry.getKey();
        Payload payload = entry.getValue();

        String airlineName = payload.getAirline();
        String text = !airlineName.isEmpty() ? (airlineName + " (" + iataCode + ")") : (iataCode);

        double average = payload.getDelay() / payload.getCount();

        return new Payload().setAirline(text).setDelay(average);
    }

    private void report(Payload payload, Reducer<Text, Payload, Text, DoubleWritable>.Context context) {
        try {
            context.write(new Text(payload.getAirline()), new DoubleWritable(payload.getDelay()));
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
