package com.invertedindex;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class DocRecordReader extends RecordReader<IntWritable,Text> {
    private IntWritable key = new IntWritable();
    private Text value = new Text();
    int lineNum=0;
    private FSDataInputStream fileIn;
    private int maxFileLength;
    public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
    private long pos;
    private long start;
    private long end;
    private LineReader in;


    public DocRecordReader(){
        //Logic for the constructor here
       // lineRecordReader = new LineRecordReader(jobConf,fileSplit);
    }
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();
        this.maxFileLength = job.getInt(MAX_LINE_LENGTH,Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        // Open the file and seek the start of the file
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        fileIn.seek(start);
        in = new LineReader(fileIn,job);
        if(start !=0){
            // Logic to ignore the first record here
            start = in.readLine(new Text(),0,(int) Math.min(Integer.MAX_VALUE,end-start));
        }
        this.pos = start;
        //lineNum++;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(key == null){
            new IntWritable();
        }
        key.set(++lineNum);
        int newSize = 0;
        while(pos<end){
        // Read the first line and store the content to value
        newSize = in.readLine(value, maxFileLength,Math.max((int) Math.min(Integer.MAX_VALUE,end-start), maxFileLength));
        if(newSize == 0){
            break;
        }
        //Line is read. Setting the new position.
            pos += newSize;
        if(newSize<end){
            break;
        }
        if(newSize>end){
            System.out.println("Line too LONG");
        }


        }
        if(newSize==0){
            key =null;
            value=null;
            return false;
        }
        else{
            return true;
        }
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start==end){
            return 0.0f;
        }
        else{
            return Math.min(1.0f,(pos-start)/(float)(end-start));
        }
    }

    @Override
    public void close() throws IOException {

    }
}

