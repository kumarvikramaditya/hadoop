package com.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;



public class InvertedIndex {
    public InvertedIndex(){}
    public static class InvertedMap extends Mapper<IntWritable,Text,Text,DocAttributes> {
        Integer docNum;
        String word;
        Integer docFrequency;
        Integer termFrequency;
        private Text key = new Text();
        @Override
        public void map(IntWritable mapIn,Text mapValIn,Context context) throws IOException,InterruptedException{
            try{
                String line = mapValIn.toString();
                String[] words = line.trim().split("\\s");
                List<String> wordList = new ArrayList<String>(Arrays.asList(words));
                for(int i=0; i<wordList.size();i++){
                   String k = wordList.get(i);
                    //Text key = new Text(k);
                  //  DocAttributes da = new DocAttributes();
                    key.set(k);
                    int sum=1;
                    for(int j=i;j<wordList.size();j++){

                        if(wordList.get(i).matches(wordList.get(j)) && j>i){
                            sum++;
                            docNum = mapIn.get();
                            docFrequency = sum;
                            word = wordList.get(i);
                            termFrequency = sum;
                            wordList.remove(j);
                        }


                        else{
                            docNum = mapIn.get();
                            docFrequency = sum;
                            word = wordList.get(i);
                            termFrequency = sum;
                        }

                    }
                    if(i == words.length-1){
                        docNum = mapIn.get();
                        docFrequency = sum;
                        word = wordList.get(i);
                        termFrequency = sum;
                    }
                    context.write(key, new DocAttributes(docNum,word,docFrequency,termFrequency));
                }
            }
            catch(NullPointerException ne){
                ne.printStackTrace();
            }
        }

    }
    public static class InvertedReduce extends Reducer<Text,DocAttributes,LongWritable,Text>{
       @Override
        public void reduce(Text key, Iterable<DocAttributes> value,Context context) throws IOException,InterruptedException{
            Iterator<DocAttributes> iterator = value.iterator();
            DocAttributes doc = new DocAttributes();
            List<DocAttributes> list = new ArrayList<DocAttributes>();
            while(iterator.hasNext()){
                list.add(new DocAttributes(iterator.next()));
            }
            Integer docFrequency = 0;
            Map<Integer,Integer> positions = new HashMap();
            String word=null;
            for(DocAttributes d : list){
                word = d.getWord();
                docFrequency += d.getDocFrequency();
                positions.put(d.getDocNum(),d.getDocFrequency());
              }

           context.write(new LongWritable(), new Text(word+docFrequency.toString()+positions.toString()));

       }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts","2048");
        conf.set("io.sort.mb","500");
//        conf.set("mapred.compress.map.output","true");
        Job job = Job.getInstance(conf);

        job.setMapperClass(InvertedMap.class);
        // job.setCombinerClass(InvertedCombine.class);
        job.setReducerClass(InvertedReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DocAttributes.class);

        job.setJarByClass(InvertedIndex.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setInputFormatClass(DocInput.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.submit();
        job.waitForCompletion(true);


    }

}
