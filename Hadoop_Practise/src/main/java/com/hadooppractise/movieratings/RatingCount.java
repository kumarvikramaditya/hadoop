package com.hadooppractise.movieratings;

import com.hadooppractise.WordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by vikram on 23/5/15.
 */
public class RatingCount{
    public static class RatingMap extends MapReduceBase implements Mapper<LongWritable, Text,IntWritable,UserRatings>{
        @Override
        public void map(LongWritable inputKey, Text inputValue, OutputCollector<IntWritable, UserRatings> output, Reporter reporter) throws IOException {
            System.out.println("Map Function Begins");
            String inputString = inputValue.toString();
            //  IntWritable ratingCount = new IntWritable(1);
            UserRatings userRatings;
            String[] inputStringArray = inputString.split("\t");
            for(int i =0;i<inputStringArray.length;i+=4){
                Integer userId = Integer.parseInt(inputStringArray[i]);
                IntWritable userIdAsInputKey = new IntWritable(userId);
                Integer movieId = Integer.parseInt(inputStringArray[i+1]);
                Integer ratings = Integer.parseInt(inputStringArray[i+2]);
                Integer time = Integer.parseInt(inputStringArray[i+3]);
                userRatings = new UserRatings(movieId,ratings,time);
              //  System.out.println("Emitting Key->"+userIdAsInputKey.toString()+"Emitting Value"+userRatings.toString());
                output.collect(userIdAsInputKey,userRatings);
            }

        }
    }

    public static class RatingReduce extends MapReduceBase implements Reducer<IntWritable, UserRatings, IntWritable, IntWritable >{
        @Override
        public void reduce(IntWritable mapOutput,Iterator<UserRatings> mapOutputValue,OutputCollector<IntWritable,IntWritable> output, Reporter reporter) throws IOException{
            System.out.println("Reduce Method Begins");
            UserRatings ur = new UserRatings();
            System.out.println("Object Initialized, Initializing List");
            List<UserRatings> userRatingsList= new ArrayList<UserRatings>();
            System.out.println("List Initialized. Initializing while loop");
            while (mapOutputValue.hasNext()){
                ur = mapOutputValue.next();
                userRatingsList.add(ur);
//                System.out.println("Entered while loop");
//                //System.out.println(mapOutputValue.next().getMovieId().toString());
//                ur = mapOutputValue.next();
//                userRatingsList.add(ur);
//                System.out.println("Movie Id is----------------------------------->"+userRatingsList.get(0).getMovieId());
            }
            Iterator<UserRatings> iterator = userRatingsList.iterator();
            int sum=0;
            while(iterator.hasNext()){
               // sum += iterator.next().getMovieId();
                if(iterator.next().getMovieId() != null){
                    sum += 1;
                }
            }
            output.collect(mapOutput,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Main Function Begins");
        JobConf conf = new JobConf(JobConf.class);
        conf.setJobName("Rating Count");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(UserRatings.class);

        // Set the Mapper, Combiner and REducer Classes
        conf.setMapperClass(RatingMap.class);
        conf.setReducerClass(RatingReduce.class);

        //Set input and OutputFormat Classes
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //Declare Input and Output File Paths
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);


    }
}