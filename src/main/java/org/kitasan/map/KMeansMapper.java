package org.kitasan.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitasan.util.PairVectorDoubleInt;
import org.kitasan.util.VectorDoubleWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, PairVectorDoubleInt> {

    private final ArrayList<String> Centers = new ArrayList<>();

    @Override
    protected void setup(Mapper<Object, Text, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0){
            String path = context.getCacheFiles()[0].getPath();
            Configuration cfg = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(cfg);
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null){
                Centers.add(line);
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, PairVectorDoubleInt>.Context context) throws IOException, InterruptedException {
        int index = -1;
        String[] point = value.toString().split(": ");
        point[1] = point[1].replace(" ", ",");
        double minDis = Double.MAX_VALUE;
        VectorDoubleWritable p = new VectorDoubleWritable(point[1]);
        for (String s: Centers) {
            String[] fields = s.split("\t");
            double dis = VectorDoubleWritable.getDistance(new VectorDoubleWritable(fields[1]), p);
            if (Double.compare(dis, minDis) < 0) {
                minDis = dis;
                index = Integer.parseInt(fields[0]);
            }
        }
        context.write(new IntWritable(index), new PairVectorDoubleInt(p, new IntWritable(1)));
    }
}
