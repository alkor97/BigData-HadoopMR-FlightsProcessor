package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class PayloadCombinerTest {

    private ReduceDriver<Text, Payload, Text, Payload> combineDriver;

    @Before
    public void setup() {
        PayloadCombiner combiner = new PayloadCombiner();
        combineDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombiner() throws IOException {
        combineDriver.withInput(new Text("AB"), Arrays.asList(
                        new Payload().setDelay(2.0).setCount(2),
                        new Payload().setDelay(4.0).setCount(1)
                ))
                .withOutput(new Text("AB"), new Payload().setDelay(6.0).setCount(3))
                .runTest();
    }
}
