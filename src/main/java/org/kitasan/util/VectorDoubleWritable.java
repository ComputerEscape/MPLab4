package org.kitasan.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

public class VectorDoubleWritable extends Vector<DoubleWritable> implements Writable, Comparable<VectorDoubleWritable> {

    public VectorDoubleWritable() {
        super();
    }

    public VectorDoubleWritable(String str) {
        super();
        String[] data = str.split(",");
        for (String d : data) {
            add(new DoubleWritable(Double.parseDouble(d)));
        }
    }

    public VectorDoubleWritable(Text text) {
        this(text.toString());
    }

    public void add(VectorDoubleWritable vec) throws RuntimeException {
        int vecSize = vec.size();
        if (vecSize != size()) throw new RuntimeException("Vector dimensions are not equal");
        for (int i = 0; i < vecSize; ++i) {
            DoubleWritable d = get(i);
            d.set(d.get()+vec.get(i).get());
        }
    }

    public void multiply(int k) {
        for (DoubleWritable d : this) {
            d.set(d.get()*k);
        }
    }

    public void divide(int k) {
        for (DoubleWritable d : this) {
            d.set(d.get()/k);
        }
    }

    public static double getDistance(VectorDoubleWritable a, VectorDoubleWritable b) throws RuntimeException {
        int aSize = a.size();
        int bSize = b.size();
        if (aSize != bSize) throw new RuntimeException("Vector dimensions are not equal");
        double res = 0;
        for (int i = 0; i < aSize; ++i) {
            double x = a.get(i).get();
            double y = b.get(i).get();
            res += (x-y)*(x-y);
        }
        return Math.sqrt(res);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        IntWritable sz = new IntWritable(size());
        sz.write(dataOutput);
        for (DoubleWritable d : this) {
            d.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        IntWritable sz = new IntWritable();
        sz.readFields(dataInput);
        setSize(sz.get());
        for (int i = 0; i < sz.get(); ++i) {
            DoubleWritable d = new DoubleWritable();
            d.readFields(dataInput);
            set(i, d);
        }
    }

    @Override
    public int compareTo(VectorDoubleWritable o) throws RuntimeException {
        int oSize = o.size();
        if (oSize != size()) throw new RuntimeException("Vector dimensions are not equal");
        for (int i = 0; i < oSize; ++i) {
            double x = get(i).get();
            double y = o.get(i).get();
            if (y-x > 1e-6) return -1;
            if (x-y > 1e-6) return 1;
        }
        return 0;
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); ++i) {
            if (i > 0) sb.append(',');
            sb.append(String.format("%.5f", get(i).get()));
        }
        return sb.toString();
    }

}
