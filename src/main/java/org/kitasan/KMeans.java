package org.kitasan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class KMeans {

    private static Job getKMeansJob(Configuration conf, Path in, Path out) throws IOException {
        Job job = new Job(conf, "K-Means");
        //TODO
        return job;
    }

    /**
     * @return True if converged.
     */
    private static boolean check(Path kMeansOut) {
        //TODO-hmh
        return true;
    }

    private static Job getClusterJob(Configuration conf, Path in, Path out) throws IOException {
        Job job = new Job(conf, "Cluster");
        //TODO
        return job;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: <in> <k-means-out> <>");
            }
            Path in = new Path(otherArgs[0]);
            Path kMeansOut = new Path(otherArgs[1]);
            Path clusterOut = new Path(otherArgs[2]);
            do {
                Job kMeansJob = getKMeansJob(conf, in, kMeansOut);
                boolean flag = kMeansJob.waitForCompletion(true);
                if (flag) System.exit(1);
            } while(!check(kMeansOut));
            Job clusterJob = getClusterJob(conf, kMeansOut, clusterOut);
            System.exit(clusterJob.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
