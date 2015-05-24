package com.hadooppractise;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.eclipse.jdt.core.JavaCore;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
//import org.apache.hadoop.mapreduce.mapred.*;

/**
 * Created by vikram on 15/5/15.
 */
public class WordCount {
   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
       public static final IntWritable one = new IntWritable(1);
       private Text word = new Text();
       public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException{
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);
           while(tokenizer.hasMoreTokens()){
               word.set(tokenizer.nextToken());
              // System.out.println(tokenizer.nextToken());
               output.collect(word,one);
           }

       }

    }
    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException{
            int sum =0;
            while(values.hasNext()){
                sum += values.next().get();
                //System.out.println(sum);
            }
            output.collect(key,new IntWritable(sum));
        }
    }
//    public static void main(String[] args) throws IOException {
//        JobConf conf = new JobConf(JobConf.class);
//        conf.setJobName("WordCount");
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
//
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//
//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//
//        JobClient.runJob(conf);
//
//    }

}