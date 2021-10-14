package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ProcessorTest {

    private MapReduceDriver<LongWritable, Text, Text, Payload, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setup() {
        DelayExtractorMapper delaysMapper = new DelayExtractorMapper();
        PayloadCombiner combiner = new PayloadCombiner();
        AverageComputerReducer reducer = new AverageComputerReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(delaysMapper, reducer, combiner);
    }

    @Test
    public void testMapReduce() throws IOException {
        readAllFlights().forEach(line -> mapReduceDriver.withInput(new LongWritable(), new Text(line)));
        mapReduceDriver
                .withOutput(new Text("NK"), new DoubleWritable(9.5))
                .withOutput(new Text("US"), new DoubleWritable(6.0))
                .withOutput(new Text("DL"), new DoubleWritable(-4.25))
                .withOutput(new Text("AS"), new DoubleWritable(-5.0))
                .withOutput(new Text("UA"), new DoubleWritable(-6.0))
                .runTest();
    }

    private List<String> readAllFlights() throws IOException {
        return Files.readAllLines(Paths.get("src", "test", "resources", "flights.csv").toAbsolutePath());
    }
}
