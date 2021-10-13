package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Processor {

    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: flights-processor <flights-input-file> <airlines-input-file> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "flights-processor");
        job.setJarByClass(Processor.class);
        job.setReducerClass(AverageComputerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VariantWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(remainingArgs[0]), TextInputFormat.class, DelayExtractorMapper.class);
        MultipleInputs.addInputPath(job, new Path(remainingArgs[1]), TextInputFormat.class, AirlinesExtractorMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
