package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlinesExtractorMapper extends Mapper<
        LongWritable,       // ?
        Text,               // single line of text
        Text,               // IATA_CODE
        Payload             // AIRLINE
        > {

    // IATA_CODE column index in airlines.csv
    public static final int IATA_CODE_INDEX = 0;

    // AIRLINE column index in airlines.csv
    public static final int AIRLINE_INDEX = 1;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Payload>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        if (tokens.length > IATA_CODE_INDEX) {
            String iataCode = tokens[IATA_CODE_INDEX].trim();
            if (!iataCode.isEmpty() && tokens.length > AIRLINE_INDEX) {
                String airline = tokens[AIRLINE_INDEX].trim();
                if (!airline.isEmpty()) {
                    context.write(new Text(iataCode), new Payload().setAirline(airline));
                }
            }
        }
    }
}
