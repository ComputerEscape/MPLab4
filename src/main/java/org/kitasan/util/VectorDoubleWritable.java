package org.kitasan.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

public class VectorDoubleWritable extends Vector<DoubleWritable> implements Writable {

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

}
