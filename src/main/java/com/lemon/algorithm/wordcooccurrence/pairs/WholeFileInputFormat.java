package com.lemon.algorithm.wordcooccurrence.pairs;

/**
 * Created by lizm on 16-12-18.
 */
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileInputFormat extends  FileInputFormat<Text,BytesWritable>
{
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,
                                                                TaskAttemptContext context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
//		return null;
        return new SingleFileNameReader((FileSplit)split,context.getConfiguration());
    }
}

