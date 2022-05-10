package org.kitasan.util;

import org.apache.hadoop.io.IntWritable;

public class PairVectorDoubleInt extends PairWritable<VectorDoubleWritable, IntWritable> {

    public PairVectorDoubleInt() {
        super(new VectorDoubleWritable(), new IntWritable());
    }

    public PairVectorDoubleInt(VectorDoubleWritable key, IntWritable value) {
        super(key, value);
        if (key == null) key = new VectorDoubleWritable();
        if (value == null) value = new IntWritable();
    }

}
