/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hadoop.counter;

/**
 *
 * @author tegarnization
 */
 
import java.io.IOException; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
 
 
public class DBLPCounter {

    /**
     * CLASS DEFINITION
     * ----------------
     * Emits (publication_type, 1) for each end tag found.
     */
    public static class PublicationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text publicationTag = new Text();
        private final IntWritable ONE = new IntWritable(1);
        public static final String[] TAGS = new String[]{"<article>", "<inproceedings>", "<phdthesis>", "<masterthesis>"};
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            for (String tag : TAGS) {
                if (line.contains(tag)) {
                    publicationTag.set(tag.substring(1, tag.length() - 1));
                    context.write(publicationTag, ONE);
                }
            }
        }
    }

    /**
     * CLASS DEFINITION
     * ----------------
     * Count the occurrence of a key.
     */
    public static class PublicationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            context.write(key, result);
        }
    }
}