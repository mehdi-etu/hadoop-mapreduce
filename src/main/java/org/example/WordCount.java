package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


import static com.fasterxml.jackson.databind.type.LogicalType.Map;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(","); // Split by whitespace

                word.set(tokens[2]);
                context.write(word, one);

        }
    }

    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);  // Do not modify the key in the combiner
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private HashMap<Text , IntWritable> CountMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
           /* result.set(sum);*/

            CountMap.put(new Text(key), new IntWritable(sum));

       /*     Text customKey = new Text("movie with id " + key.toString()+ " hase been whatched : ");*/
           /* context.write(customKey, result);*/
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            List<Map.Entry<Text, IntWritable>> list = new ArrayList<>(CountMap.entrySet());

            list.sort(new Comparator<Map.Entry<Text, IntWritable>>() {
                public int compare(Map.Entry<Text, IntWritable> o1 , Map.Entry<Text, IntWritable> o2){
                    return  o1.getValue().compareTo(o2.getValue());
                }
            });

            for(Map.Entry<Text, IntWritable>  entry : list) {
                context.write(new Text(entry.getKey()), entry.getValue());
            }

        }
    }

    public static void RunJob(String localFilePath , String outputPathStr ,String hdfsFilePathStr)  throws Exception {

        /*String inputPath = args[0];*/
        Path outputPath = new Path(outputPathStr);
        Path hdfsFilePath = new Path(hdfsFilePathStr);
        Path localPath = new Path(localFilePath);



        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        // Check if the HDFS path already exists
        if (fs.exists(hdfsFilePath)) {
            fs.delete(hdfsFilePath, true);
        }

        // Copy the file from local file system to HDFS
        fs.copyFromLocalFile(localPath, hdfsFilePath);
        System.out.println("File uploaded to HDFS: " + hdfsFilePath);

        if (fs.exists(outputPath)) {
            if (fs.delete(outputPath, true)) {
                System.out.println("File deleted successfully!");
            } else {
                System.out.println("File deletion failed. File may not exist.");
                System.exit(-1);
            }
        }
        FileInputFormat.addInputPath(job, hdfsFilePath);
        FileOutputFormat.setOutputPath(job, outputPath);


        boolean jobCompleted = job.waitForCompletion(true);
        if (!jobCompleted) {
            System.exit(1);
        }

        // Read and print the output
        System.out.println("MapReduce Job Completed. Reading Output...");
        Path outputFile = new Path(outputPathStr + "/part-r-00000"); // Default output file

        // Check if the file exists
        if (!fs.exists(outputFile)) {
            System.err.println("Output file not found!");
            System.exit(-1);
        }

        // Read the output file content
        FSDataInputStream inputStream = fs.open(outputFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        reader.close();
        inputStream.close();
    }
}
