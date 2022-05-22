package org.kitasan.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.kitasan.util.PairVectorDoubleInt;
import org.kitasan.util.VectorDoubleWritable;

import java.io.IOException;

public class ClusterReducer extends Reducer<IntWritable, PairVectorDoubleInt, IntWritable, VectorDoubleWritable> {
    private MultipleOutputs<IntWritable, VectorDoubleWritable> mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    public void cleanup(Context _context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<PairVectorDoubleInt> values, Reducer<IntWritable, PairVectorDoubleInt, IntWritable, VectorDoubleWritable>.Context context) throws IOException, InterruptedException {
        final Logger logger = Logger.getLogger(this.getClass());
        final String[] filenames = context.getConfiguration().getStrings("filenames");
        if (filenames != null && key.get() >= 0 && filenames.length > key.get()) {
            final String filename = filenames[key.get()];
            for (PairVectorDoubleInt i : values) {
                final int pid = i.getValue().get();
                final VectorDoubleWritable p = i.getKey();
                mos.write(filename, pid, p);
            }
        } else {
            logger.error(String.format("No file for cluster %d", key.get()));
        }
    }
}
