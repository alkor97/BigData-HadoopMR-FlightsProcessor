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

public class AirlinesExtractorMapperTest {

    private MapDriver<LongWritable, Text, Text, Payload> airlinesMapDriver;

    @Before
    public void setup() {
        AirlinesExtractorMapper airlinesMapper = new AirlinesExtractorMapper();
        airlinesMapDriver = MapDriver.newMapDriver(airlinesMapper);
    }

    @Test
    public void testAirlineNamePairForRegularLine() throws IOException {
        airlinesMapDriver.withInput(new LongWritable(), new Text(readGivenAirlineLine(1)))
                .withOutput(new Text("UA"), new Payload().setAirline("United Air Lines Inc."))
                .runTest();
    }

    private String readGivenAirlineLine(int selectedLine) throws IOException {
        return readAllAirlines().get(selectedLine);
    }

    private List<String> readAllAirlines() throws IOException {
        return Files.readAllLines(Paths.get("src", "test", "resources", "airlines.csv").toAbsolutePath());
    }
}
