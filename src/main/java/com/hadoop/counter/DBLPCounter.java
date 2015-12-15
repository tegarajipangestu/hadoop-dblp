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

        private final Text publicationType = new Text();
        private final IntWritable publicationCount = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            for (String tag : PUBLICATION_END_TAGS) {
                if (line.contains(tag)) {
                    publicationType.set(tag.substring(2, tag.length() - 1));
                    context.write(publicationType, publicationCount);
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            context.write(key, result);
        }
    }

    public static final String JOB_DESCRIPTION = "dblp publication count";
    public static final String USAGE = "Usage: publicationcount <in> <out>";
    public static final String[] PUBLICATION_END_TAGS = new String[]{
            "</article>", "</inproceedings>", "</phdthesis>", "</masterthesis>"};
}