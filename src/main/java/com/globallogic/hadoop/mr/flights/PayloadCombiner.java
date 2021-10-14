package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PayloadCombiner extends Reducer<Text, Payload, Text, Payload> {

    @Override
    protected void reduce(Text key, Iterable<Payload> values, Reducer<Text, Payload, Text, Payload>.Context context) throws IOException, InterruptedException {
        context.write(key, new Payload().mergeInto(values));
    }
}
