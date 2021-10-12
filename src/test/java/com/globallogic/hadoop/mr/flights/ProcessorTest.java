package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProcessorTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;

    @Before
    public void setup() {
        mapDriver = MapDriver.newMapDriver(new DataExtractorMapper());
        reduceDriver = ReduceDriver.newReduceDriver(new AverageComputerReducer());
    }

    @Test
    public void testNoOutputForHeader() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(readLine(0)))
                .runTest();
    }

    @Test
    public void testAirlineDelayPairForRegularLine() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(readLine(1)))
                .withOutput(new Text("AS"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void testComputeAverateDepartureDelay() {
        reduceDriver.withInput(new Text("AB"), intWritables(-5, -3))
                .withInput(new Text("CD"), intWritables(-4, -8))
                .withOutput(new Text("AB"), new DoubleWritable(-4.0))
                .withOutput(new Text("CD"), new DoubleWritable(-6.0));
    }

    private List<IntWritable> intWritables(int... values) {
        return IntStream.of(values).mapToObj(IntWritable::new).collect(Collectors.toList());
    }

    private String readLine(int line) throws IOException {
        try (InputStream inputStream = ProcessorTest.class.getResourceAsStream("/flights.csv")) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String text;
                int counter = 0;
                while (null != (text = reader.readLine())) {
                    if (counter == line) {
                        return text;
                    }
                    ++counter;
                }
                throw new IllegalArgumentException("Could not find line " + line);
            }
        }
    }
}
