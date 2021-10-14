package com.globallogic.hadoop.mr.flights;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Payload implements Writable {

    private String airline = "";
    private double delay = 0.0;
    private long count = 0;

    public Payload() {}

    public Payload setAirline(String value) {
        this.airline = value;
        return this;
    }

    public String getAirline() {
        return airline;
    }

    public Payload setDelay(double value) {
        this.delay = value;
        return this;
    }

    public double getDelay() {
        return delay;
    }

    public Payload setCount(long value) {
        this.count = value;
        return this;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(airline);
        dataOutput.writeDouble(delay);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airline = dataInput.readUTF();
        delay = dataInput.readDouble();
        count = dataInput.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payload payload = (Payload) o;
        return Double.compare(payload.delay, delay) == 0
                && count == payload.count
                && airline.equals(payload.airline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(airline, delay, count);
    }

    @Override
    public String toString() {
        return "Payload{" +
                "airline='" + airline + '\'' +
                ", delay=" + delay +
                ", count=" + count +
                '}';
    }

    public Payload mergeInto(Iterable<Payload> payloads) {
        for (Payload payload : payloads) {
            if (!payload.getAirline().isEmpty()) {
                setAirline(payload.getAirline());
            }
            setDelay(getDelay() + payload.getDelay());
            setCount(getCount() + payload.getCount());
        }
        return this;
    }
}
