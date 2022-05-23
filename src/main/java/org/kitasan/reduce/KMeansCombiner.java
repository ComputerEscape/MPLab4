package org.kitasan.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.kitasan.util.PairVectorDoubleInt;
import org.kitasan.util.VectorDoubleWritable;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, PairVectorDoubleInt, IntWritable, PairVectorDoubleInt> {

    @Override
    protected void reduce(IntWritable key, Iterable<PairVectorDoubleInt> values, Reducer<IntWritable, PairVectorDoubleInt, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        VectorDoubleWritable pm = new VectorDoubleWritable();
        boolean flag = false;
        int n = 0;
        for (PairVectorDoubleInt i: values) {
            if (flag) {
                i.getKey().multiply(i.getValue().get());
                pm.add(i.getKey());
            }else {
                int vecSize = i.getKey().size();
                i.getKey().multiply(i.getValue().get());
                for (int k = 0; k < vecSize; ++k) {
                    DoubleWritable d = new DoubleWritable(i.getKey().get(k).get());
                    pm.add(d);
                }
                flag = true;
            }
            n += i.getValue().get();
        }
        pm.divide(n);
        context.write(key, new PairVectorDoubleInt(pm, new IntWritable(n)));
    }
}
