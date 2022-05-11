package org.kitasan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.kitasan.util.CenterFileOperation;
import org.kitasan.util.VectorDoubleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

    private static Job getKMeansJob(Configuration conf, Path centerIn, Path pointIn, Path out) throws IOException {
        Job job = new Job(conf, "K-Means");
        //TODO
        return job;
    }

    private static Job getClusterJob(Configuration conf, Path centerIn, Path pointIn, Path out) throws IOException {
        Job job = new Job(conf, "Cluster");
        //TODO
        return job;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 4) {
                System.err.println("Usage: <center-in> <point-in> <k-means-out> <cluster-out>");
            }
            Path centerIn = new Path(otherArgs[0]);
            Path pointIn = new Path(otherArgs[1]);
            Path kMeansOut = new Path(otherArgs[2]);
            Path clusterOut = new Path(otherArgs[3]);
            //File
            Path tempKMeansOut = new Path(kMeansOut.getName()+"/temp");
            //Directory
            Path finalKMeansOut = new Path(kMeansOut.getName()+"/final");
            initKMeansOut(conf, centerIn, kMeansOut, tempKMeansOut);
            do {
                clearKMeansOut(conf, finalKMeansOut);
                Job kMeansJob = getKMeansJob(conf, tempKMeansOut, pointIn, finalKMeansOut);
                boolean flag = kMeansJob.waitForCompletion(true);
                if (flag) System.exit(1);
            } while(!check(conf, tempKMeansOut, finalKMeansOut));
            Job clusterJob = getClusterJob(conf, finalKMeansOut, pointIn, clusterOut);
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
        CenterFileOperation.getCenterFromDir(conf, centerIn, inCenter);
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
