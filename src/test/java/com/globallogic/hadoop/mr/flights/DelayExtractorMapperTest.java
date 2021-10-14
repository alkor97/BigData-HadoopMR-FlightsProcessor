package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class DelayExtractorMapperTest {

    private MapDriver<LongWritable, Text, Text, Payload> delaysMapDriver;

    @Before
    public void setup() {
        DelayExtractorMapper delaysMapper = new DelayExtractorMapper();
        delaysMapDriver = MapDriver.newMapDriver(delaysMapper);
    }

    @Test
    public void testNoOutputForDelaysHeader() throws IOException {
        delaysMapDriver.withInput(new LongWritable(), new Text(readGivenFlightLine(0)))
                .runTest();
    }

    @Test
    public void testAirlineDelayPairForRegularLine() throws IOException {
        delaysMapDriver.withInput(new LongWritable(), new Text(readGivenFlightLine(1)))
                .withOutput(new Text("AS"), new Payload().setDelay(-11).setCount(1))
                .runTest();
    }

    private String readGivenFlightLine(int selectedLine) throws IOException {
        return readAllFlights().get(selectedLine);
    }

    private List<String> readAllFlights() throws IOException {
        return Files.readAllLines(Paths.get("src", "test", "resources", "flights.csv").toAbsolutePath());
    }
}
