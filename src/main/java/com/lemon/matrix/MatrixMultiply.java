package com.lemon.matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by lizm on 16-12-12.
 */

public class MatrixMultiply {

    private static final int columnN = 3;
    private static final int rowM = 5;
    private static final int columnM = 6;

    public static class MatrixMap extends Mapper<Object, Text, Text, Text > {
        private Text map_key = new Text();
        private Text map_value = new Text();

        private static int mmi = 0;
        private static int ni = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            int mj = 0;
            if (fileName.contains("M")) {
                mmi++;
                String[] tuple = value.toString().split(",");
                for (String s : tuple) {
                    mj++;
                    for (int k = 1; k < columnN + 1; k++) {
                        map_key.set(mmi + "," + k);
                        map_value.set("M" + "," + mj + "," + s);
                        context.write(map_key, map_value);
                    }
                }
            } else if (fileName.contains("N")) {
                ni++;
                int nj = 0;
                String[] tuple = value.toString().split(",");
                for (String s : tuple) {
                    nj++;
                    for (int i = 1; i < rowM + 1; i++) {
                        Text str = new Text();
                        map_key.set(i + "," + nj);
                        map_value.set("N" + "," + ni + "," + s);
                        context.write(map_key, map_value);
                    }
                }
            }
        }
    }

    public static class MatrixReduce extends Reducer<Text, Text, Text, Text> {

        private int sum = 0;
        private int M[] = new int[columnM + 1];
        private int N[] = new int[columnM + 1];

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text val : values){
                String [] tuple = val.toString().split(",");
                if(tuple[0].equals("M")){
                    M[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
                }else{
                    N[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
                }
            }
            for(int i = 1; i < columnM + 1; i ++){
                sum += M[i] * N[i];
            }
            context.write(key, new Text(Integer.toString(sum)));
            sum = 0;
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapred.jar", "./out/artifacts/MatrixMultiply/MatrixMultiply.jar");
        conf.set("mapreduce.framework.name", "yarn");

        Job job = new Job(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMultiply.MatrixMap.class);
        job.setReducerClass(MatrixMultiply.MatrixReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
