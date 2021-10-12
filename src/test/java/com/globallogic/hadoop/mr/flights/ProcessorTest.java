package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProcessorTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setup() {
        DataExtractorMapper mapper = new DataExtractorMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        AverageComputerReducer reducer = new AverageComputerReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
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
    public void testComputeAverageDepartureDelay() {
        reduceDriver.withInput(new Text("AB"), intWritables(-5, -3))
                .withInput(new Text("CD"), intWritables(-4, -8))
                .withOutput(new Text("AB"), new DoubleWritable(-4.0))
                .withOutput(new Text("CD"), new DoubleWritable(-6.0));
    }

    @Test
    public void testMapReduce() throws IOException {
        readAllLines().forEach(line -> mapReduceDriver.withInput(new LongWritable(), new Text(line)));
        mapReduceDriver.withOutput(new Text("AA"), new DoubleWritable(-8.0))
                .withOutput(new Text("AS"), new DoubleWritable(-5.0))
                .withOutput(new Text("DL"), new DoubleWritable(-4.25))
                .withOutput(new Text("NK"), new DoubleWritable(9.5))
                .withOutput(new Text("UA"), new DoubleWritable(-6.0))
                .withOutput(new Text("US"), new DoubleWritable(6.0))
                .runTest();
    }

    private List<IntWritable> intWritables(int... values) {
        return IntStream.of(values).mapToObj(IntWritable::new).collect(Collectors.toList());
    }

    private String readLine(int selectedLine) throws IOException {
        return readAllLines().get(selectedLine);
    }

    private List<String> readAllLines() throws IOException {
        return Files.readAllLines(Paths.get("src", "test", "resources", "flights.csv").toAbsolutePath());
    }
}
