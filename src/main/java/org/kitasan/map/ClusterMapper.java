package org.kitasan.map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.kitasan.util.CenterFileOperation;
import org.kitasan.util.PairVectorDoubleInt;
import org.kitasan.util.VectorDoubleWritable;

import java.io.IOException;
import java.util.ArrayList;

public class ClusterMapper extends Mapper<Object, Text, IntWritable, PairVectorDoubleInt> {

    private final ArrayList<VectorDoubleWritable> centers = new ArrayList<>();

    @Override
    protected void setup(Mapper<Object, Text, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            CenterFileOperation.getCenterFromDir(context.getConfiguration(), new Path(context.getCacheFiles()[0]), centers);
        }
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        final Logger logger = Logger.getLogger(this.getClass());
        int index = -1;
        String[] point = value.toString().split(": ");
        int pid;
        try {
            pid = Integer.parseInt(point[0]);
        } catch (NumberFormatException e) {
            pid = -1;
            logger.error(String.format("Not a valid point ID: %s", point[0]));
        }
        point[1] = point[1].replace(" ", ",");
        double minDis = Double.MAX_VALUE;
        VectorDoubleWritable p = new VectorDoubleWritable(point[1]);
        for (int i = 0; i < centers.size(); ++i) {
            double dis = VectorDoubleWritable.getDistance(centers.get(i), p);
            if (Double.compare(dis, minDis) < 0) {
                minDis = dis;
                index = i;
            }
        }
        context.write(new IntWritable(index), new PairVectorDoubleInt(p, new IntWritable(pid)));
    }
}
