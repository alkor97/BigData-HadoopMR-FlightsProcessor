package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Optional;

public class VariantWritable extends GenericWritable {

    public VariantWritable() {}

    public VariantWritable(IntWritable value) {
        super.set(value);
    }

    public VariantWritable(Text value) {
        super.set(value);
    }

    public VariantWritable(int value) {
        this(new IntWritable(value));
    }

    public VariantWritable(String text) {
        this(new Text(text));
    }

    public IntWritable getInt() {
        return (IntWritable) get();
    }

    public Text getText() {
        return (Text) get();
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return new Class[] {IntWritable.class, Text.class};
    }

    @Override
    public int hashCode() {
        return Optional.ofNullable(get())
                .map(Object::hashCode)
                .orElse(0);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VariantWritable) {
            Optional<Writable> me = Optional.ofNullable(get());
            Optional<Writable> other = Optional.ofNullable(((VariantWritable) obj).get());
            return me.equals(other);
        }
        return false;
    }
}
