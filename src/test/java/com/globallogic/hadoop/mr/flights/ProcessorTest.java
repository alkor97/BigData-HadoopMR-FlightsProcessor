package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
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
import java.util.Arrays;
import java.util.List;

public class ProcessorTest {

    private MapDriver<LongWritable, Text, Text, VariantWritable> delaysMapDriver;
    private MapDriver<LongWritable, Text, Text, VariantWritable> airlinesMapDriver;
    private ReduceDriver<Text, VariantWritable, Text, DoubleWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, VariantWritable, Text, DoubleWritable> delaysMarReduceDriver;

    @Before
    public void setup() {
        DelayExtractorMapper delaysMapper = new DelayExtractorMapper();
        delaysMapDriver = MapDriver.newMapDriver(delaysMapper);

        AirlinesExtractorMapper airlinesMapper = new AirlinesExtractorMapper();
        airlinesMapDriver = MapDriver.newMapDriver(airlinesMapper);

        AverageComputerReducer reducer = new AverageComputerReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        delaysMarReduceDriver = MapReduceDriver.newMapReduceDriver(delaysMapper, reducer);
    }

    @Test
    public void testNoOutputForDelaysHeader() throws IOException {
        delaysMapDriver.withInput(new LongWritable(), new Text(readGivenFlightLine(0)))
                .runTest();
    }

    @Test
    public void testAirlineDelayPairForRegularLine() throws IOException {
        delaysMapDriver.withInput(new LongWritable(), new Text(readGivenFlightLine(1)))
                .withOutput(new Text("AS"), new VariantWritable(-11))
                .runTest();
    }

    @Test
    public void testAirlineNamePairForRegularLine() throws IOException {
        airlinesMapDriver.withInput(new LongWritable(), new Text(readGivenAirlineLine(1)))
                .withOutput(new Text("UA"), new VariantWritable("United Air Lines Inc."))
                .runTest();
    }

    @Test
    public void testComputeAverageDepartureDelay() throws IOException {
        reduceDriver.withInput(new Text("AB"), Arrays.asList(
                        new VariantWritable(-5),
                        new VariantWritable(-3),
                        new VariantWritable("Aaa Bbb")
                ))
                .withInput(new Text("CD"), Arrays.asList(
                        new VariantWritable(-4),
                        new VariantWritable(-8)
                ))
                .withOutput(new Text("Aaa Bbb (AB)"), new DoubleWritable(-4.0))
                .withOutput(new Text("CD"), new DoubleWritable(-6.0))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        readAllFlights().forEach(line -> delaysMarReduceDriver.withInput(new LongWritable(), new Text(line)));
        delaysMarReduceDriver
                .withOutput(new Text("NK"), new DoubleWritable(9.5))
                .withOutput(new Text("US"), new DoubleWritable(6.0))
                .withOutput(new Text("DL"), new DoubleWritable(-4.25))
                .withOutput(new Text("AS"), new DoubleWritable(-5.0))
                .withOutput(new Text("UA"), new DoubleWritable(-6.0))
                .runTest();
    }

    private String readGivenFlightLine(int selectedLine) throws IOException {
        return readAllFlights().get(selectedLine);
    }

    private List<String> readAllFlights() throws IOException {
        return Files.readAllLines(Paths.get("src", "test", "resources", "flights.csv").toAbsolutePath());
    }

    private String readGivenAirlineLine(int selectedLine) throws IOException {
        return readAllAirlines().get(selectedLine);
    }

    private List<String> readAllAirlines() throws IOException {
        return Files.readAllLines(Paths.get("src", "test", "resources", "airlines.csv").toAbsolutePath());
    }
}
