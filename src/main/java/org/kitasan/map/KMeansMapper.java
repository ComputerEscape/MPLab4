package org.kitasan.map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitasan.util.CenterFileOperation;
import org.kitasan.util.PairVectorDoubleInt;
import org.kitasan.util.VectorDoubleWritable;

import java.io.IOException;
import java.util.ArrayList;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, PairVectorDoubleInt> {

    private ArrayList<VectorDoubleWritable> centerList = new ArrayList<>();

    @Override
    protected void setup(Mapper<Object, Text, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0){
            CenterFileOperation.getCenterFromFile(context.getConfiguration(), new Path(context.getCacheFiles()[0]), centerList, false);
        }
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        int index = -1;
        String[] point = value.toString().split(": ");
        point[1] = point[1].replace(" ", ",");
        double minDis = Double.MAX_VALUE;
        VectorDoubleWritable p = new VectorDoubleWritable(point[1]);
        for (int i = 0; i < centerList.size(); ++i) {
            double dis = VectorDoubleWritable.getDistance(centerList.get(i), p);
            if (Double.compare(dis, minDis) < 0) {
                minDis = dis;
                index = i;
            }
        }
        context.write(new IntWritable(index), new PairVectorDoubleInt(p, new IntWritable(1)));
    }
}
