package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Extracts AIRLINE and DEPARTURE_DELAY from CSV file.
 */
public class DataExtractorMapper extends Mapper<
        LongWritable,   // ?
        Text,           // single line of text
        Text,           // AIRLINE
        IntWritable     // DEPARTURE_DELAY
        > {

    // AIRLINE column index in flights.csv
    public static final int AIRLINE_INDEX = 4;

    // DEPARTURE_DELAY column index in flights.csv
    public static final int DEPARTURE_DELAY_INDEX = 11;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens.length > AIRLINE_INDEX) {
            String airline = tokens[AIRLINE_INDEX].trim();
            if (!airline.isEmpty() && tokens.length > DEPARTURE_DELAY_INDEX) {
                try {
                    int departureDelay = Integer.parseInt(tokens[DEPARTURE_DELAY_INDEX].trim());
                    context.write(new Text(airline), new IntWritable(departureDelay));
                } catch (NumberFormatException e) {
                    // skip silently
                }
            }
        }
    }
}
