package org.kitasan.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.List;

public class CenterFileOperation {

    public static void saveCenterToFile(Configuration conf, Path file, List<VectorDoubleWritable> center) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream output = fs.create(file);
        for (int i = 0; i < center.size(); ++i) {
            output.write((i+"\t"+center.get(i).toString()+"\n").getBytes());
        }
        output.close();
    }

    public static void getCenterFromFile(Configuration conf, Path file, List<VectorDoubleWritable> center, boolean isInit) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream input = fs.open(file);
        LineReader reader = new LineReader(input, conf);
        Text line = new Text();
        while (reader.readLine(line) > 0) {
            if (isInit) {
                String[] data = line.toString().split(" ");
                int index = Integer.parseInt(data[0].split(":")[0]);
                ensureSize(center, index);
                center.set(index, new VectorDoubleWritable(new Text(data[1])));
            } else {
                String[] data = line.toString().split("\t");
                int index = Integer.parseInt(data[0]);
                ensureSize(center, index);
                center.set(index, new VectorDoubleWritable(new Text(data[1])));
            }
        }
        reader.close();
    }

    public static void getCenterFromDir(Configuration conf, Path dir, List<VectorDoubleWritable> center) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(dir, false);
        while (listFiles.hasNext()) {
            LocatedFileStatus f = listFiles.next();
            if (f.isFile()) {
                getCenterFromFile(conf, f.getPath(), center, false);
            }
        }
    }

    private static void ensureSize(List<VectorDoubleWritable> list, int index) {
        while (index >= list.size()) {
            list.add(null);
        }
    }

}
