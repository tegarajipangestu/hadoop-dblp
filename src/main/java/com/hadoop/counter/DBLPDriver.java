/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hadoop.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author tegarnization
 */
public class DBLPDriver {

    public static final String JOB_NAME = "dblp publication count";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // invalid arguments
        if (otherArgs.length != 2) {
            System.err.println("Missing parameter");
            System.exit(1);
        }
        String firstParam = args[0];
        String secParam = args[1];

        Job job = new Job(conf);
        job.setJarByClass(DBLPCounter.class);
        job.setMapperClass(DBLPCounter.PublicationMapper.class);
        job.setReducerClass(DBLPCounter.PublicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setNumReduceTasks(0);
        //job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(firstParam));
        Path outPath = new Path(secParam);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
        
        

//        Job job = Job.getInstance(conf, JOB_NAME);
//        job.setJar("publicationcount.jar");
//        job.setJarByClass(DBLPCounter.class);
//
//        job.setMapperClass(DBLPCounter.PublicationMapper.class);
////        job.setCombinerClass(DBLPCounter.PublicationReducer.class);
//        job.setReducerClass(DBLPCounter.PublicationReducer.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
