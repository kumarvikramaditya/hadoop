package com.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by vikram on 10/6/15.
 */
public class DocInput extends FileInputFormat<IntWritable,Text> {

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<IntWritable, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new DocRecordReader();
    }
}
