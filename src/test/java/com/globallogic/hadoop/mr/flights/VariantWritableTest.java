package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VariantWritableTest {

    @Test
    public void testEquals() {
        VariantWritable v1 = new VariantWritable(new IntWritable(-11));
        VariantWritable v2 = new VariantWritable(new IntWritable(-11));
        assertEquals(v1, v2);
    }
}
