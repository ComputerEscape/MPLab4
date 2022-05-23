package org.kitasan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.kitasan.map.ClusterMapper;
import org.kitasan.map.KMeansMapper;
import org.kitasan.reduce.ClusterReducer;
import org.kitasan.reduce.KMeansCombiner;
import org.kitasan.reduce.KMeansReducer;
import org.kitasan.util.CenterFileOperation;
import org.kitasan.util.PairVectorDoubleInt;
import org.kitasan.util.VectorDoubleWritable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

    private static Job getKMeansJob(Configuration conf, Path centerIn, Path pointIn, Path out) throws IOException, URISyntaxException {
        Job job = new Job(conf, "K-Means");
        job.setJarByClass(KMeans.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairVectorDoubleInt.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorDoubleWritable.class);
        job.setReducerClass(KMeansReducer.class);
        FileInputFormat.addInputPath(job, pointIn);
        FileOutputFormat.setOutputPath(job, out);
        job.addCacheFile(new URI(centerIn.toString()));
        return job;
    }

    private static Job getClusterJob(Configuration conf, Path centerIn, Path pointIn, Path out) throws IOException, URISyntaxException {
        Job job = new Job(conf, "Cluster");
        job.setJarByClass(KMeans.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairVectorDoubleInt.class);
        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(ClusterReducer.class);
        FileInputFormat.addInputPath(job, pointIn);
        FileOutputFormat.setOutputPath(job, out);
        List<VectorDoubleWritable> centerList = new ArrayList<>();
        CenterFileOperation.getCenterFromFile(job.getConfiguration(), centerIn, centerList, false);
        String[] names = new String[centerList.size()];
        for (int i=0; i<centerList.size(); ++i) {
            String filename = "cluster" + i;
            names[i] = filename;
            MultipleOutputs.addNamedOutput(job, filename, TextOutputFormat.class, IntWritable.class, VectorDoubleWritable.class);
        }
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ":");
        job.getConfiguration().setStrings("filenames", names);
        job.addCacheFile(new URI(centerIn.toString()));
        return job;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 4) {
                System.err.println("Usage: <center-in> <point-in> <k-means-out> <cluster-out>");
                System.exit(1);
            }
            Path centerIn = new Path(otherArgs[0]);
            Path pointIn = new Path(otherArgs[1]);
            Path kMeansOut = new Path(otherArgs[2]);
            Path clusterOut = new Path(otherArgs[3]);
            //File
            Path tempKMeansOut = new Path(kMeansOut + "/temp");
            //Directory
            Path finalKMeansOut = new Path(kMeansOut + "/final");
            initKMeansOut(conf, centerIn, kMeansOut, tempKMeansOut);
            do {
                clearKMeansOut(conf, finalKMeansOut);
                Job kMeansJob = getKMeansJob(conf, tempKMeansOut, pointIn, finalKMeansOut);
                boolean flag = kMeansJob.waitForCompletion(true);
                if (!flag) System.exit(1);
            } while(!check(conf, tempKMeansOut, finalKMeansOut));
            Job clusterJob = getClusterJob(conf, tempKMeansOut, pointIn, clusterOut);
            System.exit(clusterJob.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void initKMeansOut(Configuration conf, Path centerIn, Path kMeansOut, Path tempKMeansOut) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(kMeansOut)) {
            fs.mkdirs(kMeansOut);
        }
        List<VectorDoubleWritable> centerList = new ArrayList<>();
        CenterFileOperation.getCenterFromFile(conf, centerIn, centerList, true);
        CenterFileOperation.saveCenterToFile(conf, tempKMeansOut, centerList);
    }

    private static void clearKMeansOut(Configuration conf, Path kMeansOut) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(kMeansOut)) {
            fs.delete(kMeansOut, true);
        }
    }

    /**
     * Check if centers are converged and move them to centerIn.
     * @return True if converged.
     */
    private static boolean check(Configuration conf, Path centerIn, Path kMeansOut) throws IOException {
        List<VectorDoubleWritable> inCenter = new ArrayList<>();
        List<VectorDoubleWritable> outCenter = new ArrayList<>();
        CenterFileOperation.getCenterFromFile(conf, centerIn, inCenter, false);
        CenterFileOperation.getCenterFromDir(conf, kMeansOut, outCenter);
        boolean flag = true;
        for (int i = 0; i < inCenter.size(); ++i) {
            if (inCenter.get(i).compareTo(outCenter.get(i)) != 0) {
                flag = false;
                break;
            }
        }
        CenterFileOperation.saveCenterToFile(conf, centerIn, outCenter);
        return flag;
    }

}
