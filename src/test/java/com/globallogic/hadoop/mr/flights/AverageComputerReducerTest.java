package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class AverageComputerReducerTest {

    private ReduceDriver<Text, Payload, Text, DoubleWritable> reduceDriver;


    @Before
    public void setup() {
        AverageComputerReducer reducer = new AverageComputerReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testComputeAverageDepartureDelay() throws IOException {
        reduceDriver.withInput(new Text("AB"), Arrays.asList(
                        new Payload().setDelay(-5).setCount(1),
                        new Payload().setDelay(-3).setCount(1),
                        new Payload().setAirline("Aaa Bbb")
                ))
                .withInput(new Text("CD"), Arrays.asList(
                        new Payload().setDelay(-4).setCount(1),
                        new Payload().setDelay(-8).setCount(1)
                ))
                .withOutput(new Text("Aaa Bbb (AB)"), new DoubleWritable(-4.0))
                .withOutput(new Text("CD"), new DoubleWritable(-6.0))
                .runTest();
    }
}
